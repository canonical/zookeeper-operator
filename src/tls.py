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


class ZooKeeperTLS(Object):
    """Handler for managing the client and unit TLS keys/certs."""

    def __init__(self, charm):
        super().__init__(charm, "tls")
        self.charm = charm
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
        """Relation property to be used by both the instance and charm.

        Returns:
            The certificates relation instance
        """
        return self.charm.model.get_relation(CERTS_REL_NAME)

    @property
    def private_key(self) -> Optional[str]:
        """The unit private-key set during `certificates_joined`.

        Returns:
            String of key contents
            None if key not yet generated
        """
        return self.cluster.data[self.charm.unit].get("private-key", None)

    @property
    def csr(self) -> Optional[str]:
        """The unit cert signing requests.

        Returns:
            String of csr contents
            None if csr not yet generated
        """
        return self.cluster.data[self.charm.unit].get("csr", None)

    @property
    def certificate(self) -> Optional[str]:
        """The signed unit certificate from the provider relation.

        Returns:
            String of cert contents in PEM format
            None if cert not yet generated/signed
        """
        return self.cluster.data[self.charm.unit].get("certificate", None)

    @property
    def enabled(self) -> bool:
        """Flag to check the cluster should run with SSL quorum encryption.

        Returns:
            True if SSL quorum encryption should be active. Otherwise False
        """
        return self.cluster.data[self.charm.app].get("tls", None) == "enabled"

    @property
    def upgrading(self) -> bool:
        """Flag to check the cluster is switching between SSL <> non-SSL quorum encryption.

        Returns:
            True if the cluster is switching. Otherwise False
        """
        return bool(self.cluster.data[self.charm.app].get("upgrading", None)) == "started"

    @property
    def alias(self) -> str:
        """Grabs the unique unit alias from name, to be used in Java key/trust stores.

        Returns:
            String of current unit alias
        """
        return self._get_unit_alias(self.charm.unit)

    @property
    def all_units_unified(self) -> bool:
        """Flag to check whether all started units are currently running with `portUnification`.

        Returns:
            True if all units are running with `portUnification`. Otherwise False
        """
        for unit in getattr(self.charm, "cluster").started_units:
            if not self.cluster.data[unit].get("unified", None):
                return False

        return True

    @property
    def all_certs_generated(self) -> bool:
        """Flag to check whether all peer units have a valid signed cert.

        Returns:
            True if all units have a valid signed cert. Otherwise False
        """
        for unit in getattr(self.charm, "cluster").peer_units:
            if not self.cluster.data[unit].get("certificate", None):
                return False

        return True

    def _on_certificates_created(self, _) -> None:
        """Handler for `certificates_relation_created` event."""
        if not self.charm.unit.is_leader():
            return

        # if this event fired, we don't know whether the cluster was fully running or not
        # assume it's already running, and trigger `upgrade` from non-ssl -> ssl
        # ideally trigger this before any other `certificates_*` step
        self.cluster.data[self.charm.app].update({"tls": "enabled", "upgrading": "started"})

    def _on_certificates_joined(self, event: RelationJoinedEvent) -> None:
        """Handler for `certificates_relation_joined` event."""
        if not self.cluster:
            logger.debug("DEFERRING CERTIFICATE JOINED - NO CLUSTER")
            event.defer()
            return

        logger.debug("GENERATING PRIVATE KEYS")
        private_key = generate_private_key()

        # strip() necessary here due to tls-certificates lib also stripping, so that they match
        self.cluster.data[self.charm.unit].update({"private-key": private_key.decode().strip()})
        self.set_server_key()

        logger.debug("CREATING CSR")
        csr = generate_csr(private_key=private_key, subject=os.uname()[1])
        self.cluster.data[self.charm.unit].update({"csr": csr.decode("utf-8").strip()})

        # FIXME - Currently there is a bug in the `tls-certificates` lib where events are lost
        # This is due to too many units requesting at once, events get lost/exit without signing
        # This *should* work when that bug is resolved

        self.certificates.request_certificate_creation(certificate_signing_request=csr)

    def _on_certificate_available(self, event: CertificateAvailableEvent) -> None:
        """Handler for `certificates_available` event after provider updates signed certs."""
        if not self.cluster:
            logger.info("DEFERRING AVAILABLE CERT - NO CLUSTER")
            event.defer()
            return

        logger.info("SETTING CERT, CHAIN AND CA")
        self.cluster.data[self.charm.unit].update(
            {"certificate": event.certificate, "ca": event.ca, "chain": event.chain}
        )

        logger.info("SETTING STORES")
        self.set_truststore()
        self.set_p12_keystore()

    def _on_certificates_broken(self, _) -> None:
        """Handler for `certificates_relation_broken` event."""
        self.cluster.data[self.charm.unit].update(
            {"csr": "", "certificate": "", "ca": "", "chain": ""}
        )
        # remove all existing keystores from the unit so we don't preserve certs
        self.remove_stores()

        if not self.charm.unit.is_leader():
            return

        # if this event fired, trigger `upgrade` from ssl -> non-ssl
        # ideally trigger this before any other `certificates_*` step
        self.cluster.data[self.charm.app].update({"tls": "", "upgrading": "started"})

    def set_server_key(self) -> None:
        """Sets the unit private-key."""
        if not self.private_key:
            raise KeyNotFoundError(f"Private key missing for {self.charm.unit.name}")

        logger.info("SETTING SERVER KEY")
        safe_write_to_file(content=self.private_key, path=f"{SNAP_CONFIG_PATH}/server.key")

    def set_truststore(self) -> None:
        """Adds all available peer unit certs to the unit and creates JKS truststore."""
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
                logger.debug(f"SET {alias} TO TRUSTSTORE")
            except subprocess.CalledProcessError as e:
                logger.debug(e.output)
                if "already exists" in e.output:  # keytool non-0's if cert already added
                    continue
                else:
                    raise e

    def set_p12_keystore(self) -> None:
        """Creates and adds unit cert and private-key to a PCKS12 keystore."""
        try:
            subprocess.check_output(
                f"openssl pkcs12 -export -in {self.alias}.pem -inkey server.key -passin pass:{KEY_PASSWORD} -certfile {self.alias}.pem -out keystore.p12 -password pass:{KEY_PASSWORD}",
                stderr=subprocess.PIPE,
                shell=True,
                universal_newlines=True,
                cwd=SNAP_CONFIG_PATH,
            )
            logger.debug("SET KEYSTORE")
        except subprocess.CalledProcessError as e:
            logger.info(e.output)
            raise e

    def remove_stores(self) -> None:
        """Cleans up all keys/certs/stores on a unit."""
        try:
            subprocess.check_output(
                "rm -r *.pem *.key *.p12 *.jks",
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
        """Gets unique unit alias from unit name for a given unit.

        Args:
            unit: the `Unit` to get alias for

        Returns:
            String of `Unit` alias
        """
        return f"{unit.name.replace('/','-')}"

    @property
    def unit_aliases(self) -> Set[str]:
        """Gets unique unit aliases from peer units with signed certs.

        Returns:
            Set of `Unit` aliases. Empty if TLS is not enabled, or no units have signed certs
        """
        if not self.enabled:
            return set()

        aliases = set()
        for unit in getattr(self.charm, "cluster").peer_units:
            try:
                cert = self.cluster.data[unit].get("certificate", None)
            except KeyError:
                continue

            if cert:
                aliases.add(self._get_unit_alias(unit))

        return aliases

    @property
    def server_aliases(self) -> Set[str]:
        """Gets all cert aliases from current unit truststore.

        Returns:
            Set of aliases currently in the truststore. Empty set if TLS is not enabled
        """
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
