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
    generate_private_key,
)
from ops.charm import RelationJoinedEvent
from ops.framework import Object
from ops.model import Relation, Unit

from literals import CERTS_REL_NAME, KEY_PASSWORD, PEER
from utils import safe_write_to_file

logger = logging.getLogger(__name__)


class KeyNotFoundError(Exception):
    """Server private key not found in relation data."""


class CertNotFoundError(Exception):
    """Certificate not found not found in relation data."""


class NotUnitCertTurnError(Exception):
    """Unit cannot request certificate yet."""


class ZooKeeperTLS(Object):
    def __init__(self, charm):
        super().__init__(charm, "tls")
        self.charm = charm
        self.cn = os.uname()[1]
        self.certificates = TLSCertificatesRequiresV1(self.charm, "certificates")

        self.framework.observe(
            getattr(self.charm.on, "certificates_relation_created"), self._on_certificates_created
        )
        self.framework.observe(
            getattr(self.charm.on, "certificates_relation_joined"), self._on_certificates_joined
        )
        self.framework.observe(
            getattr(self.certificates.on, "certificate_available"), self._on_certificate_available
        )
        self.framework.observe(
            getattr(self.charm.on, "certificates_relation_broken"), self._on_certificates_broken
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
    def enabled(self) -> bool:
        return self.cluster.data[self.charm.app].get("tls", None)

    @property
    def upgrading(self) -> bool:
        return bool(self.cluster.data[self.charm.app].get("upgrading", None))

    @property
    def alias(self) -> str:
        return self._get_unit_alias(self.charm.unit)

    @property
    def all_units_unified(self) -> bool:
        for unit in getattr(self.charm, "cluster").started_units:
            if not self.cluster.data[unit].get("unified", None):
                return False

        return True

    @property
    def all_certs_generated(self) -> bool:
        for unit in getattr(self.charm, "cluster").peer_units:
            if not self.cluster.data[unit].get("certificate", None):
                return False

        return True

    def _on_certificates_created(self, _) -> None:
        if not self.charm.unit.is_leader():
            return

        self.cluster.data[self.charm.app].update({"tls": "enabled", "upgrading": "started"})

    def _on_certificates_joined(self, event: RelationJoinedEvent) -> None:
        if not self.cluster:
            logger.info(f"DEFERRING CERTIFICATE JOINED - NO CLUSTER")
            event.defer()
            return

        logger.info(f"GENERATING PRIVATE KEYS")
        private_key = generate_private_key()
        self.cluster.data[self.charm.unit].update({"private-key": private_key.decode().strip()})
        self.set_server_key()

        logger.info(f"CREATING CSR")
        csr = generate_csr(
            private_key=private_key,
            subject=self.cn,
        )
        self.cluster.data[self.charm.unit].update({"csr": csr.decode("utf-8").strip()})
        self.certificates.request_certificate_creation(certificate_signing_request=csr)

    def _on_certificate_available(self, event: CertificateAvailableEvent) -> None:
        if not self.cluster:
            logger.info(f"DEFERRING AVAILABLE CERT - NO CLUSTER")
            event.defer()
            return

        logger.info(f"SETTING MY CERT")
        self.cluster.data[self.charm.unit].update({"certificate": event.certificate})

        logger.info(f"SETTING STORES")
        self.set_truststore()
        self.set_p12_keystore()

    def _on_certificates_broken(self, _) -> None:
        self.cluster.data[self.charm.unit].update({"csr": "", "certificate": ""})
        self.remove_stores()

        if not self.charm.unit.is_leader():
            return

        self.cluster.data[self.charm.app].update({"tls": "", "upgrading": "started"})

    def set_server_key(self) -> None:
        if not self.private_key:
            raise KeyNotFoundError(f"Private key missing for {self.charm.unit.name}")

        logger.info("SETTING SERVER KEY")
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
                logger.info(f"SET {alias} TO TRUSTSTORE")
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
            logger.info("SET KEYSTORE")
        except subprocess.CalledProcessError as e:
            logger.info(e.output)
            raise e

    def remove_stores(self) -> None:
        try:
            subprocess.check_output(
                f"rm -r *.pem *.key *.p12 *.jks",
                stderr=subprocess.PIPE,
                shell=True,
                universal_newlines=True,
                cwd=SNAP_CONFIG_PATH,
            )
            logger.info("REMOVE STORES AND KEYS")
        except subprocess.CalledProcessError as e:
            logger.info(e.output)
            raise e

    def _get_unit_alias(self, unit: Unit) -> str:
        return f"{unit.name.replace('/','-')}"

    @property
    def unit_aliases(self) -> Set[str]:
        if not self.enabled:
            return set()

        aliases = set()
        for unit in getattr(self.charm, "cluster").peer_units:
            try:
                cert = self.relation.data[unit].get("certificate", None)
            except KeyError:
                continue

            if cert:
                aliases.add(self._get_unit_alias(unit))

        return aliases

    @property
    def server_aliases(self) -> Set[str]:
        if not self.enabled:
            return set()

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
