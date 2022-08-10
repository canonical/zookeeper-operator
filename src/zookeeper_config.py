#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager for handling ZooKeeper auth configuration."""

import logging
from pathlib import Path

from charms.kafka.v0.kafka_snap import DATA_DIR, SNAP_CONFIG_PATH, safe_write_to_file

logger = logging.getLogger(__name__)

PEER = "cluster"
REL_NAME = "zookeeper"

ZOOKEEPER_AUTH_CONFIG_PATH = f"{SNAP_CONFIG_PATH}/zookeeper-jaas.cfg"
OPTS = [
    "-Dzookeeper.requireClientAuthScheme=sasl",
    "-Dzookeeper.superUser=super",
    f"-Djava.security.auth.login.config={ZOOKEEPER_AUTH_CONFIG_PATH}",
]

ZOOKEEPER_PROPERTIES = f"""
clientPort=2181
dataDir={DATA_DIR}
dataLogDir=/var/snap/kafka/common/log
dynamicConfigFile=/var/snap/kafka/common/zookeeper-dynamic.properties
maxClientCnxns=60
minSessionTimeout=4000
maxSessionTimeout=40000
autopurge.snapRetainCount=3
autopurge.purgeInterval=0
reconfigEnabled=true
standaloneEnabled=false
4lw.commands.whitelist=mntr,srvr
DigestAuthenticationProvider.digestAlg=SHA3-256
quorum.auth.enableSasl=true
quorum.auth.learnerRequireSasl=true
quorum.auth.serverRequireSasl=true
authProvider.sasl=org.apache.zookeeper.server.auth.SASLAuthenticationProvider
audit.enable=true"""

TLS_STORE_DIR = "/var/snap/kafka/common/certs"

TLS_TRUSTSTORE = "ca"

TLS_ZOOKEEPER_PROPERTIES = f"""
secureClientPort=2182
serverCnxnFactory=org.apache.zookeeper.server.NettyServerCnxnFactory
authProvider.x509=org.apache.zookeeper.server.auth.X509AuthenticationProvider
ssl.trustStore.location={TLS_STORE_DIR}/{TLS_TRUSTSTORE}
ssl.trustStore.type=PEM
ssl.quorum.trustStore.type=PEM
zookeeper.ssl.client.enable=true
zookeeper.set.acl=true
ssl.clientAuth=none"""


class ZooKeeperConfig:
    """Manager for handling ZooKeeper auth configuration."""

    def set_jaas_config(self, sync_password: str, super_password: str, users: str) -> None:
        """Sets the ZooKeeper JAAS config.

        Args:
            sync_password: the ZK cluster `sync_password` for inter-server auth
            super_password: the ZK cluster `super_password` for super-user auth
            users: the target related users to grant permissions to
        """
        auth_config = f"""
            QuorumServer {{
                org.apache.zookeeper.server.auth.DigestLoginModule required
                user_sync="{sync_password}";
            }};

            QuorumLearner {{
                org.apache.zookeeper.server.auth.DigestLoginModule required
                username="sync"
                password="{sync_password}";
            }};

            Server {{
                org.apache.zookeeper.server.auth.DigestLoginModule required
                {users}
                user_super="{super_password}";
            }};
        """
        safe_write_to_file(content=auth_config, path=ZOOKEEPER_AUTH_CONFIG_PATH, mode="w")

    @staticmethod
    def set_kafka_opts() -> None:
        """Sets the env-vars needed for SASL auth to /etc/environment on the unit."""
        opts_string = " ".join(OPTS)
        safe_write_to_file(content=f"KAFKA_OPTS={opts_string}", path="/etc/environment", mode="a")

    def create_properties(self, config):
        """Creates property file contents and inserts charm config values."""
        props = ZOOKEEPER_PROPERTIES
        keys = {
            "init-limit": "initLimit",
            "sync-limit": "syncLimit",
            "tick-time": "tickTime",
        }

        for k, v in keys.items():
            if config.get(k, None) is not None:
                props += f"\n{v}={config[k]}"

        if self.ssl_enabled():
            props += TLS_ZOOKEEPER_PROPERTIES

        return props

    def ssl_enabled(self):
        """Checks for the certificates needed for TLS."""
        truststore = Path(f"{TLS_STORE_DIR}/{TLS_TRUSTSTORE}")

        return truststore.is_file()
