#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager for handling ZooKeeper auth configuration."""

import logging
from typing import TYPE_CHECKING, List

from literals import JMX_PORT, METRICS_PROVIDER_PORT, REL_NAME
from utils import safe_get_file, safe_write_to_file, update_env

if TYPE_CHECKING:
    from charm import ZooKeeperCharm

logger = logging.getLogger(__name__)

DEFAULT_PROPERTIES = """
syncEnabled=true
maxClientCnxns=60
minSessionTimeout=4000
maxSessionTimeout=40000
autopurge.snapRetainCount=3
autopurge.purgeInterval=0
reconfigEnabled=true
standaloneEnabled=false
4lw.commands.whitelist=mntr,srvr,stat
DigestAuthenticationProvider.digestAlg=SHA3-256
quorum.auth.enableSasl=true
quorum.auth.learnerRequireSasl=true
quorum.auth.serverRequireSasl=true
authProvider.sasl=org.apache.zookeeper.server.auth.SASLAuthenticationProvider
audit.enable=true"""

TLS_PROPERTIES = """
secureClientPort=2182
ssl.clientAuth=none
ssl.quorum.clientAuth=none
ssl.client.enable=true
clientCnxnSocket=org.apache.zookeeper.ClientCnxnSocketNetty
serverCnxnFactory=org.apache.zookeeper.server.NettyServerCnxnFactory
ssl.trustStore.type=JKS
ssl.keyStore.type=PKCS12
"""


class ZooKeeperConfig:
    """Manager for handling ZooKeeper auth configuration."""

    def __init__(self, charm):
        self.charm: "ZooKeeperCharm" = charm
        self.properties_filepath = f"{self.charm.snap.conf_path}/zoo.cfg"
        self.log4j_properties_filepath = f"{self.charm.snap.conf_path}/log4j.properties"
        self.dynamic_filepath = f"{self.charm.snap.conf_path}/zookeeper-dynamic.properties"
        self.jaas_filepath = f"{self.charm.snap.conf_path}/zookeeper-jaas.cfg"
        self.keystore_filepath = f"{self.charm.snap.conf_path}/keystore.p12"
        self.truststore_filepath = f"{self.charm.snap.conf_path}/truststore.jks"
        self.jmx_prometheus_javaagent_filepath = (
            f"{self.charm.snap.binaries_path}/jmx_prometheus_javaagent.jar"
        )
        self.jmx_prometheus_config_filepath = f"{self.charm.snap.conf_path}/jmx_prometheus.yaml"

    @property
    def server_jvmflags(self) -> List[str]:
        """Builds necessary server JVM flag env-vars for the ZooKeeper Snap."""
        return [
            "-Dzookeeper.requireClientAuthScheme=sasl",
            "-Dzookeeper.superUser=super",
            f"-Djava.security.auth.login.config={self.jaas_filepath}",
            "-Djavax.net.debug=ssl:handshake:verbose:keymanager:trustmanager",
        ]

    @property
    def jmx_jvmflags(self) -> List[str]:
        """Builds necessary jmx flag env-vars for the ZooKeeper Snap."""
        return [
            "-Dcom.sun.management.jmxremote",
            f"-javaagent:{self.jmx_prometheus_javaagent_filepath}={JMX_PORT}:{self.jmx_prometheus_config_filepath}",
        ]

    @property
    def jaas_users(self) -> List[str]:
        """Builds the necessary user strings to add to ZK JAAS config files.

        Returns:
            Newline delimited string of JAAS users from relation data
        """
        client_relations = self.charm.model.relations[REL_NAME]
        if not client_relations:
            return []

        jaas_users = []
        for relation in client_relations:
            username = f"relation-{relation.id}"
            password = self.charm.app_peer_data.get(username, None)

            if not (username and password):
                continue

            jaas_users.append(f'user_{username}="{password}"')
        return jaas_users

    @property
    def metrics_exporter_config(self) -> list[str]:
        """Necessary config options for enabling built-in Prometheus metrics."""
        return [
            "metricsProvider.className=org.apache.zookeeper.metrics.prometheus.PrometheusMetricsProvider",
            f"metricsProvider.httpPort={METRICS_PROVIDER_PORT}",
        ]

    @property
    def jaas_config(self) -> str:
        """Builds the JAAS config.

        Returns:
            String of JAAS config for super/user config
        """
        sync_password = self.charm.app_peer_data.get("sync-password", None)
        super_password = self.charm.app_peer_data.get("super-password", None)
        users = "\n".join(self.jaas_users) or ""

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
        """Build the zoo.cfg content.

        Returns:
            List of properties to be set to zoo.cfg config file
        """
        properties = (
            [
                f"initLimit={self.charm.config['init-limit']}",
                f"syncLimit={self.charm.config['sync-limit']}",
                f"tickTime={self.charm.config['tick-time']}",
            ]
            + DEFAULT_PROPERTIES.split("\n")
            + [
                f"dataDir={self.charm.snap.data_path}",
                f"dataLogDir={self.charm.snap.logs_path}",
                f"{self.current_dynamic_config_file}",
            ]
            + self.metrics_exporter_config
        )

        if self.charm.tls.enabled:
            properties = (
                properties
                + TLS_PROPERTIES.split("\n")
                + [
                    f"ssl.quorum.keyStore.location={self.keystore_filepath}",
                    f"ssl.quorum.trustStore.location={self.truststore_filepath}",
                    f"ssl.keyStore.location={self.keystore_filepath}",
                    f"ssl.trustStore.location={self.truststore_filepath}",
                    f"ssl.keyStore.location={self.keystore_filepath}",
                    f"ssl.quorum.keyStore.password={self.charm.tls.keystore_password}",
                    f"ssl.quorum.trustStore.password={self.charm.tls.keystore_password}",
                    f"ssl.keyStore.password={self.charm.tls.keystore_password}",
                    f"ssl.trustStore.password={self.charm.tls.keystore_password}",
                ]
            )

        # `upgrading` and `quorum` field updates trigger rolling-restarts, which will modify config
        # https://zookeeper.apache.org/doc/r3.6.3/zookeeperAdmin.html#Upgrading+existing+nonTLS+cluster

        # non-ssl -> ssl cluster quorum, the required upgrade steps are:
        # 1. Add `portUnification`, rolling-restart
        # 2. Add `sslQuorum`, rolling-restart
        # 3. Remove `portUnification`, rolling-restart

        # ssl -> non-ssl cluster quorum, the required upgrade steps are:
        # 1. Add `portUnification`, rolling-restart
        # 2. Remove `sslQuorum`, rolling-restart
        # 3. Remove `portUnification`, rolling-restart

        if self.charm.tls.upgrading:
            properties = properties + ["portUnification=true"]

        if self.charm.cluster.quorum == "ssl":
            properties = properties + ["sslQuorum=true"]

        return properties

    @property
    def current_dynamic_config_file(self) -> str:
        """Gets current dynamicConfigFile property from live unit.

        When setting config dynamically, ZK creates a new properties file
            that keeps track of the current dynamic config version.
        When setting our config, we overwrite the file, losing the tracked version,
            so we can re-set it with this.

        Returns:
            String of current `dynamicConfigFile=<value>` for the running server
        """
        current_properties = safe_get_file(filepath=self.properties_filepath)

        if not current_properties:
            logger.debug("zoo.cfg file not found - using default dynamic path")
            return f"dynamicConfigFile={self.dynamic_filepath}"

        for current_property in current_properties:
            if "dynamicConfigFile" in current_property:
                return current_property

        logger.debug("dynamicConfigFile property missing - using default dynamic path")

        return f"dynamicConfigFile={self.dynamic_filepath}"

    @property
    def static_properties(self) -> List[str]:
        """Build the zoo.cfg content, without dynamic options.

        Returns:
            List of static properties to compared to current zoo.cfg
        """
        return self.build_static_properties(self.zookeeper_properties)

    def set_jaas_config(self) -> None:
        """Sets the ZooKeeper JAAS config."""
        safe_write_to_file(content=self.jaas_config, path=self.jaas_filepath, mode="w")

    def set_server_jvmflags(self) -> None:
        """Sets the env-vars needed for SASL auth to /etc/environment on the unit."""
        update_env(env={"SERVER_JVMFLAGS": " ".join(self.server_jvmflags + self.jmx_jvmflags)})

    def set_zookeeper_properties(self) -> None:
        """Writes built zoo.cfg file."""
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
            path=f"{self.charm.snap.data_path}/myid",
        )

    @staticmethod
    def build_static_properties(properties: List[str]) -> List[str]:
        """Removes dynamic config options from list of properties.

        Running ZooKeeper cluster with `reconfigEnabled` moves dynamic options
            to a dedicated dynamic file
        These options are `clientPort` and `secureClientPort`

        Args:
            properties: the properties to make static

        Returns:
            List of static properties
        """
        return [
            prop
            for prop in properties
            if ("clientPort" not in prop and "secureClientPort" not in prop)
        ]
