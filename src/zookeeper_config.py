#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager for handling ZooKeeper auth configuration."""

import logging

from charms.kafka.v0.kafka_snap import SNAP_CONFIG_PATH, safe_write_to_file

logger = logging.getLogger(__name__)

PEER = "cluster"
REL_NAME = "zookeeper"

ZOOKEEPER_AUTH_CONFIG_PATH = f"{SNAP_CONFIG_PATH}/zookeeper-jaas.cfg"
OPTS = [
    "-Dzookeeper.requireClientAuthScheme=sasl",
    "-Dzookeeper.superUser=super",
    f"-Djava.security.auth.login.config={ZOOKEEPER_AUTH_CONFIG_PATH}",
]


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
