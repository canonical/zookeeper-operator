#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager for handling ZooKeeper TLS configuration."""

import base64
import logging
import os
import re
import socket
import subprocess
from typing import List, Optional

from charms.kafka.v0.kafka_snap import SNAP_CONFIG_PATH
from charms.tls_certificates_interface.v1.tls_certificates import (
    CertificateAvailableEvent,
    CertificateExpiringEvent,
    TLSCertificatesRequiresV1,
    generate_csr,
    generate_private_key,
)
from ops.charm import ActionEvent, RelationJoinedEvent
from ops.framework import Object
from ops.model import Relation

from literals import CERTS_REL_NAME, KEY_PASSWORD, PEER
from utils import safe_write_to_file

logger = logging.getLogger(__name__)


class TLSDataNotFoundError(Exception):
    """Missing required data for TLS."""


class ZooKeeperTLS(Object):
    """Handler for managing the client and unit TLS keys/certs."""

    def __init__(self, charm):
        super().__init__(charm, "tls")
        self.charm = charm
        self.certificates = TLSCertificatesRequiresV1(self.charm, "certificates")

        self.framework.observe(
            getattr(self.charm.on, "set_tls_private_key_action"), self._set_tls_private_key
        )

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
    def ca(self) -> Optional[str]:
        """The signed unit certificate from the provider relation.

        Returns:
            String of cert contents in PEM format
            None if cert not yet generated/signed
        """
        return self.cluster.data[self.charm.unit].get("ca", None)

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
        return bool(self.cluster.data[self.charm.app].get("upgrading", None) == "started")

    @property
    def all_units_unified(self) -> bool:
        """Flag to check whether all started units are currently running with `portUnification`.

        Returns:
            True if all units are running with `portUnification`. Otherwise False
        """
        if not self.charm.cluster.all_units_related:
            return False

        for unit in getattr(self.charm, "cluster").started_units:
            if not self.cluster.data[unit].get("unified", None):
                return False

        return True

    @property
    def all_units_certified(self) -> bool:
        """Flag to check whether all units have signed certs.

        Returns:
            True if all units have signed certs. Otherwise False
        """
        if not self.charm.cluster.all_units_related:
            return False

        for unit in self.charm.cluster.peer_units:
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
            event.defer()
            return

        # generate unit private key if not already created by action
        if not self.private_key:
            self.cluster.data[self.charm.unit].update(
                {"private-key": generate_private_key().decode("utf-8")}
            )

        self._request_certificate()

    def _on_certificate_available(self, event: CertificateAvailableEvent) -> None:
        """Handler for `certificates_available` event after provider updates signed certs."""
        if not self.cluster:
            event.defer()
            return

        if not event.certificate_signing_request == self.cluster.data[self.charm.unit].get(
            "csr", None
        ):
            logger.error("unknown certificate available")
            return

        # FIXME: `chain` is omitted, add later if needed
        self.cluster.data[self.charm.unit].update(
            {"certificate": event.certificate, "ca": event.ca}
        )

        self.set_server_key()
        self.set_truststore()
        self.set_p12_keystore()

    def _on_certificates_broken(self, _) -> None:
        """Handler for `certificates_relation_broken` event."""
        # FIXME: `chain` is omitted, add later if needed
        self.cluster.data[self.charm.unit].update({"csr": "", "certificate": "", "ca": ""})
        # remove all existing keystores from the unit so we don't preserve certs
        self.remove_stores()

        if not self.charm.unit.is_leader():
            return

        # if this event fired, trigger `upgrade` from ssl -> non-ssl
        # ideally trigger this before any other `certificates_*` step
        self.cluster.data[self.charm.app].update({"tls": "", "upgrading": "started"})

    def _on_certificate_expiring(self, event: CertificateExpiringEvent) -> None:
        if not event.certificate == self.cluster.data[self.charm.unit].get("certificate", None):
            logger.error("unknown certificate expiring")
            return

        if not self.private_key or not self.csr:
            logger.error("missing unit private key and/or old csr")
            return

        new_csr = generate_csr(
            private_key=self.private_key.encode("utf-8"),
            subject=os.uname()[1],
            sans=self._get_sans(),
        )

        self.certificates.request_certificate_renewal(
            old_certificate_signing_request=self.csr.encode("utf-8"),
            new_certificate_signing_request=new_csr,
        )

        self.cluster.data[self.charm.unit].update({"csr": new_csr.decode("utf-8")})

    def _set_tls_private_key(self, event: ActionEvent) -> None:
        """Handler for `set_tls_private_key` action."""
        private_key = self._parse_tls_file(event.params.get("internal-key", None))
        self.cluster.data[self.charm.unit].update({"private-key": private_key.strip()})
        self._request_certificate()

    def _request_certificate(self) -> None:
        """Generates and submits CSR to provider."""
        if not self.private_key:
            return

        csr = generate_csr(
            private_key=self.private_key.encode("utf-8"),
            subject=os.uname()[1],
            sans=self._get_sans(),
        )
        self.cluster.data[self.charm.unit].update({"csr": csr.decode("utf-8").strip()})

        self.certificates.request_certificate_creation(certificate_signing_request=csr)

    def set_server_key(self) -> None:
        """Sets the unit private-key."""
        if not self.private_key:
            raise TLSDataNotFoundError(f"Private key missing for {self.charm.unit.name}")

        safe_write_to_file(content=self.private_key, path=f"{SNAP_CONFIG_PATH}/server.key")

    def set_ca(self) -> None:
        """Sets the unit ca."""
        if not self.ca:
            raise TLSDataNotFoundError(f"CA missing for {self.charm.unit.name}")

        safe_write_to_file(content=self.ca, path=f"{SNAP_CONFIG_PATH}/ca.pem")

    def set_certificate(self) -> None:
        """Sets the unit private-key."""
        if not self.certificate:
            raise TLSDataNotFoundError(f"Signed Certificate missing for {self.charm.unit.name}")

        safe_write_to_file(content=self.certificate, path=f"{SNAP_CONFIG_PATH}/server.pem")

    def set_truststore(self) -> None:
        """Adds CA to single shared JKS truststore."""
        ca = self.cluster.data[self.charm.unit].get("ca", None)
        if not ca:
            logger.debug("CA not found for {unit.name}")
            return

        safe_write_to_file(content=ca, path=f"{SNAP_CONFIG_PATH}/ca.pem")

        try:
            subprocess.check_output(
                f"keytool -import -v -alias ca -file ca.pem -keystore truststore.jks -storepass {KEY_PASSWORD} -noprompt",
                stderr=subprocess.PIPE,
                shell=True,
                universal_newlines=True,
                cwd=SNAP_CONFIG_PATH,
            )
        except subprocess.CalledProcessError as e:
            logger.info(e.output)
            raise e

    def set_p12_keystore(self) -> None:
        """Creates and adds unit cert and private-key to a PCKS12 keystore."""
        try:
            subprocess.check_output(
                f"openssl pkcs12 -export -in server.pem -inkey server.key -passin pass:{KEY_PASSWORD} -certfile server.pem -out keystore.p12 -password pass:{KEY_PASSWORD}",
                stderr=subprocess.PIPE,
                shell=True,
                universal_newlines=True,
                cwd=SNAP_CONFIG_PATH,
            )
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
        except subprocess.CalledProcessError as e:
            logger.info(e.output)
            raise e

    @staticmethod
    def _parse_tls_file(raw_content: str) -> str:
        """Parse TLS files from both plain text or base64 format."""
        if re.match(r"(-+(BEGIN|END) [A-Z ]+-+)", raw_content):
            return raw_content
        return base64.b64decode(raw_content).decode("utf-8")

    def _get_sans(self) -> List[str]:
        """Create a list of DNS names for the unit."""
        unit_id = self.charm.unit.name.split("/")[1]
        return [
            f"{self.charm.app.name}-{unit_id}",
            socket.getfqdn(),
            self.cluster.data[self.charm.unit].get("private-address", None),
        ]
