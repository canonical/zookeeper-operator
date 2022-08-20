#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager for handling ZooKeeper auth configuration."""

import logging
from typing import List

from charms.kafka.v0.kafka_snap import SNAP_CONFIG_PATH
from ops.charm import CharmBase

from literals import PEER
from utils import safe_write_to_file

logger = logging.getLogger(__name__)

DEFAULT_PROPERTIES = """
clientPort=2181
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


class ZooKeeperConfig:
    """Manager for handling ZooKeeper auth configuration."""

    def __init__(self, charm: CharmBase):
        self.charm = charm
        self.default_config_path = SNAP_CONFIG_PATH
        self.properties_filepath = f"{self.default_config_path}/zookeeper.properties"
        self.dynamic_filepath = f"{self.default_config_path}/zookeeper-dynamic.properties"
        self.jaas_filepath = f"{self.default_config_path}/zookeeper-jaas.cfg"

    @property
    def kafka_opts(self) -> List[str]:
        """Builds necessary JVM config env vars for the Kafka snap."""
        return [
            "-Dzookeeper.requireClientAuthScheme=sasl",
            "-Dzookeeper.superUser=super",
            f"-Djava.security.auth.login.config={self.jaas_filepath}",
        ]

    @property
    def jaas_users(self) -> List[str]:
        """Builds the necessary user strings to add to ZK JAAS config files.

        Returns:
            Newline delimited string of JAAS users from relation data
        """
        jaas_users = []
        for relation in self.charm.model.get_relation("zookeeper"):
            username = relation.data[self.charm.app].get("username", None)
            password = relation.data[self.charm.app].get("password", None)

            if not (username and password):
                continue

            jaas_users.append(f'user_{username}="{password}"')

        return jaas_users

    @property
    def jaas_config(self) -> str:
        """Builds the JAAS config.

        Returns:
            String of JAAS config for super/user config
        """
        cluster = self.charm.model.get_relation(PEER)
        sync_password = cluster.data[self.charm.app].get("sync-password", None)
        super_password = cluster.data[self.charm.app].get("super-password", None)
        users = self.jaas_users

        return f"""
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

    @property
    def zookeeper_properties(self) -> List[str]:
        """Build the zookeeper.properties content.

        Returns:
            List of properties to be set to zookeeper.properties config file
        """
        return (
            [
                f"initLimit={self.charm.config['init-limit']}",
                f"syncLimit={self.charm.config['sync-limit']}",
                f"tickTime={self.charm.config['tick-time']}",
            ]
            + DEFAULT_PROPERTIES.split("\n")
            + [
                f"dataDir={self.default_config_path}/data",
                f"dataLogDir={self.default_config_path}/log",
                f"dynamicConfigFile={self.default_config_path}/zookeeper-dynamic.properties",
            ]
        )

    def set_jaas_config(self) -> None:
        """Sets the ZooKeeper JAAS config."""
        safe_write_to_file(content=self.jaas_config, path=self.jaas_filepath, mode="w")

    def set_kafka_opts(self) -> None:
        """Sets the env-vars needed for SASL auth to /etc/environment on the unit."""
        opts = " ".join(self.kafka_opts)
        safe_write_to_file(content=f"KAFKA_OPTS='{opts}'", path="/etc/environment", mode="w")

    def set_zookeeper_properties(self) -> None:
        """Writes built zookeeper.properties file."""
        safe_write_to_file(
            content="\n".join(self.zookeeper_properties),
            path=self.properties_filepath,
            mode="w",
        )

    def set_zookeeper_dynamic_properties(self, servers: str) -> None:
        """Writes zookeeper-dynamic.properties containing server connection strings."""
        safe_write_to_file(content=servers, path=self.dynamic_filepath, mode="w")

    def set_zookeeper_myid(self) -> None:
        """Writes ZooKeeper myid file to config/data."""
        safe_write_to_file(
            content=f"{int(self.charm.unit.name.split('/')[1]) + 1}",
            path=f"{self.default_config_path}/data/myid",
        )
