#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager for handling ZooKeeper TLS configuration."""

import logging
import os
import re
import subprocess
from typing import Optional, Set

from charms.kafka.v0.kafka_snap import SNAP_CONFIG_PATH
from charms.tls_certificates_interface.v1.tls_certificates import (
    CertificateAvailableEvent,
    TLSCertificatesRequiresV1,
    generate_csr,
    generate_private_key
)
from ops.charm import CharmBase, RelationJoinedEvent
from ops.framework import EventBase, Object
from ops.model import MaintenanceStatus, Relation, Unit

from literals import CERTS_REL_NAME, KEY_PASSWORD, PEER
from utils import safe_write_to_file

logger = logging.getLogger(__name__)


class KeyNotFoundError(Exception):
    """Server private key not found in relation data."""

class CertNotFoundError(Exception):
    """Certificate not found not found in relation data."""


class ZooKeeperTLS(Object):
    def __init__(self, charm: CharmBase):
        super().__init__(charm, "tls")
        self.charm = charm
        self.cn = os.uname()[1]
        self.certificates = TLSCertificatesRequiresV1(self.charm, "certificates")

        self.framework.observe(
            getattr(self.certificates.on, "certificate_available"), self._on_certificate_available
        )
        self.framework.observe(
            getattr(self.charm.on, "certificates_relation_joined"), self._on_certificates_joined
        )

    @property
    def cluster(self) -> Relation:
        """Relation property to be used by both the instance and charm.

        Returns:
            The peer relation instance
        """
        return self.charm.model.get_relation(PEER)

    @property
    def relation(self) -> Relation:
        return self.charm.model.get_relation(CERTS_REL_NAME)

    @property
    def private_key(self) -> Optional[str]:
        return self.cluster.data[self.charm.unit].get("private-key", None)

    @property
    def csr(self) -> Optional[str]:
        return self.cluster.data[self.charm.unit].get("csr", None)

    @property
    def certificate(self) -> Optional[str]:
        return self.cluster.data[self.charm.unit].get("certificate", None)

    @property
    def alias(self) -> str:
        return self._get_unit_alias(self.charm.unit)

    @property
    def all_certs_generated(self) -> bool:
        for unit in getattr(self.charm, "cluster").peer_units:
            if not self.cluster.data[unit].get("certificate", None):
                return False

        return True

    @property
    def unit_aliases(self) -> Set[str]:
        return {
            self._get_unit_alias(unit)
            for unit in set([self.charm.unit] + list(self.cluster.units))
        }

    @property
    def server_aliases(self) -> Set[str]:
        try:
            result = subprocess.check_output(
                f"keytool -v -list -keystore truststore.jks -storepass {KEY_PASSWORD}",
                stderr=subprocess.PIPE,
                shell=True,
                universal_newlines=True,
                cwd=SNAP_CONFIG_PATH,
            )

            aliases = set()
            for line in result.splitlines():
                matched = re.search(pattern=r"Alias name\:\ (.+)", string=line)
                if matched:
                    aliases.add(matched[1])

            return aliases

        except subprocess.CalledProcessError as e:
            logger.info(str(e.output))
            logger.exception(e)
            return set()

    def _on_certificates_joined(self, event: RelationJoinedEvent) -> None:
        if not self.cluster:
            event.defer()
            return
        
        if not self.private_key:
            private_key = generate_private_key()
            self.cluster.data[self.charm.unit].update({"private-key": private_key.decode()})
            self.set_server_key()

        try:
            self._request_certificate()
        except KeyNotFoundError as e:
            logger.exception(e)
            event.defer()
            return

    def _on_certificate_available(self, event: CertificateAvailableEvent) -> None:
        if not self.cluster:
            event.defer()
            return

        if event.certificate_signing_request != self.cluster.data[self.charm.unit].get(
            "csr", None
        ):
            logger.info(f"NOT MY CERT")
            return

        logger.info(f"SETTING MY CERT")
        self.cluster.data[self.charm.unit].update({"certificate": event.certificate})

        self.set_truststore()
        self.set_p12_keystore()

    def _request_certificate(self) -> None:
        if not self.private_key:
            raise KeyNotFoundError(f"Private key missing for {self.charm.unit.name}")

        csr = generate_csr(
            private_key=self.private_key.encode("utf-8"),
            subject=self.cn,
        )

        logger.info("REQUESTING CSR - NEW")
        self.cluster.data[self.charm.unit].update({"csr": csr.decode("utf-8").strip()})
        self.certificates.request_certificate_creation(certificate_signing_request=csr)


    def set_server_key(self) -> None:
        if not self.private_key:
            raise KeyNotFoundError(f"Private key missing for {self.charm.unit.name}")

        safe_write_to_file(content=self.private_key, path=f"{SNAP_CONFIG_PATH}/server.key")

    def set_truststore(self) -> None:
        for unit in set([self.charm.unit] + list(self.cluster.units)):
            alias = self._get_unit_alias(unit)

            cert = self.cluster.data[unit].get("certificate", None)
            if not cert:
                logger.debug("Certificate not found for {unit.name}")
                continue

            safe_write_to_file(content=cert, path=f"{SNAP_CONFIG_PATH}/{alias}.pem")

            try:
                subprocess.check_output(
                    f"keytool -import -v -alias {alias} -file {alias}.pem -keystore truststore.jks -storepass {KEY_PASSWORD} -noprompt",
                    stderr=subprocess.PIPE,
                    shell=True,
                    universal_newlines=True,
                    cwd=SNAP_CONFIG_PATH,
                )
            except subprocess.CalledProcessError as e:
                logger.debug(e.output)
                if "already exists" in e.output:
                    continue
                else:
                    raise e

    def set_p12_keystore(self) -> None:
        try:
            subprocess.check_output(
                f"openssl pkcs12 -export -in {self.alias}.pem -inkey server.key -passin pass:{KEY_PASSWORD} -certfile {self.alias}.pem -out keystore.p12 -password pass:{KEY_PASSWORD}",
                stderr=subprocess.PIPE,
                shell=True,
                universal_newlines=True,
                cwd=SNAP_CONFIG_PATH,
            )
        except subprocess.CalledProcessError as e:
            logger.info(e.output)
            raise e

    def _get_unit_alias(self, unit: Unit) -> str:
        return f"{unit.name.replace('/','-')}"
