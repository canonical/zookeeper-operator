#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager for for handling configuration building + writing."""
import logging

from ops.model import ConfigData

from core.cluster import SUBSTRATES, ClusterState
from core.workload import WorkloadBase
from literals import DATA_DIR, DATALOG_DIR, JMX_PORT, METRICS_PROVIDER_PORT

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
quorum.auth.enableSasl=true
quorum.auth.learnerRequireSasl=true
quorum.auth.serverRequireSasl=true
authProvider.sasl=org.apache.zookeeper.server.auth.SASLAuthenticationProvider
audit.enable=true
"""

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


class ConfigManager:
    """Manager for for handling configuration building + writing."""

    def __init__(
        self,
        state: ClusterState,
        workload: WorkloadBase,
        substrate: SUBSTRATES,
        config: ConfigData,
    ):
        self.state = state
        self.workload = workload
        self.substrate = substrate
        self.config = config

    @property
    def log_level(self) -> str:
        """Return the Java-compliant logging level set by the user.

        Returns:
            String with these possible values: DEBUG, INFO, WARN, ERROR
        """
        # FIXME: use pydantic config models for this validation instead
        permitted_levels = ["DEBUG", "INFO", "WARNING", "ERROR"]
        config_log_level = self.config.get("log_level", "INFO")

        if config_log_level not in permitted_levels:
            logger.error(
                f"Invalid log-level config value of {config_log_level}. Must be one of {','.join(permitted_levels)}. Defaulting to 'INFO'"
            )
            config_log_level = "INFO"

        # Remapping to WARN that is generally used in Java applications based on log4j and logback.
        if config_log_level == "WARNING":
            return "WARN"

        return config_log_level

    @property
    def server_jvmflags(self) -> list[str]:
        """Builds necessary server JVM flag env-vars for the ZooKeeper Snap."""
        return [
            f"-Dcharmed.zookeeper.log.level={self.log_level}",
            "-Dzookeeper.requireClientAuthScheme=sasl",
            "-Dzookeeper.superUser=super",
            f"-Djava.security.auth.login.config={self.workload.paths.jaas}",
        ]

    @property
    def jmx_jvmflags(self) -> list[str]:
        """Builds necessary jmx flag env-vars for the ZooKeeper Snap."""
        return [
            "-Dcom.sun.management.jmxremote",
            f"-javaagent:{self.workload.paths.jmx_prometheus_javaagent}={JMX_PORT}:{self.workload.paths.jmx_prometheus_config}",
        ]

    @property
    def jaas_users(self) -> list[str]:
        """Builds the necessary user strings to add to ZK JAAS config files.

        Returns:
            Newline delimited string of JAAS users from relation data
        """
        jaas_users = []
        for client in self.state.clients:
            if not client.password:
                continue

            jaas_users.append(f'user_{client.username}="{client.password}"')

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
        users = "\n".join(self.jaas_users) or ""

        return f"""
            QuorumServer {{
                org.apache.zookeeper.server.auth.DigestLoginModule required
                user_sync="{self.state.cluster.internal_user_credentials.get('sync', '')}";
            }};

            QuorumLearner {{
                org.apache.zookeeper.server.auth.DigestLoginModule required
                username="sync"
                password="{self.state.cluster.internal_user_credentials.get('sync', '')}";
            }};

            Server {{
                org.apache.zookeeper.server.auth.DigestLoginModule required
                {users}
                user_super="{self.state.cluster.internal_user_credentials.get('super', '')}";
            }};
        """

    @property
    def zookeeper_properties(self) -> list[str]:
        """Build the zoo.cfg content.

        Returns:
            List of properties to be set to zoo.cfg config file
        """
        properties = (
            [
                f"initLimit={self.config['init-limit']}",
                f"syncLimit={self.config['sync-limit']}",
                f"tickTime={self.config['tick-time']}",
            ]
            + DEFAULT_PROPERTIES.split("\n")
            + [
                f"dataDir={self.workload.paths.data_path}/{DATA_DIR}",
                f"dataLogDir={self.workload.paths.data_path}/{DATALOG_DIR}",
                f"{self.current_dynamic_config_file}",
            ]
            + self.metrics_exporter_config
        )

        if self.state.cluster.tls:
            properties = (
                properties
                + TLS_PROPERTIES.split("\n")
                + [
                    f"ssl.keyStore.location={self.workload.paths.keystore}",
                    f"ssl.keyStore.password={self.state.unit_server.keystore_password}",
                    f"ssl.quorum.keyStore.location={self.workload.paths.keystore}",
                    f"ssl.quorum.keyStore.password={self.state.unit_server.keystore_password}",
                    f"ssl.trustStore.location={self.workload.paths.truststore}",
                    f"ssl.trustStore.password={self.state.unit_server.truststore_password}",
                    f"ssl.quorum.trustStore.location={self.workload.paths.truststore}",
                    f"ssl.quorum.trustStore.password={self.state.unit_server.truststore_password}",
                ]
            )

        # `switching-encryption` and `quorum` field updates trigger rolling-restarts, which will modify config
        # https://zookeeper.apache.org/doc/r3.6.3/zookeeperAdmin.html#Upgrading+existing+nonTLS+cluster

        # non-ssl -> ssl cluster quorum, the required upgrade steps are:
        # 1. Add `portUnification`, rolling-restart
        # 2. Add `sslQuorum`, rolling-restart
        # 3. Remove `portUnification`, rolling-restart

        # ssl -> non-ssl cluster quorum, the required upgrade steps are:
        # 1. Add `portUnification`, rolling-restart
        # 2. Remove `sslQuorum`, rolling-restart
        # 3. Remove `portUnification`, rolling-restart

        if self.state.cluster.switching_encryption:
            properties = properties + ["portUnification=true"]

        if self.state.cluster.quorum == "ssl":
            properties = properties + ["sslQuorum=true"]

        return properties

    @property
    def current_properties(self) -> list[str]:
        """The current configuration properties set to zoo.cfg."""
        return self.workload.read(self.workload.paths.properties)

    @property
    def current_jaas(self) -> list[str]:
        """The current JAAS configuration properties set to zookeeper-jaas.cfg."""
        return self.workload.read(self.workload.paths.jaas)

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
        current_properties = self.workload.read(path=self.workload.paths.properties)

        if not current_properties:
            logger.debug("zoo.cfg file not found - using default dynamic path")
            return f"dynamicConfigFile={self.workload.paths.dynamic}"

        for current_property in current_properties:
            if "dynamicConfigFile" in current_property:
                return current_property

        logger.debug("dynamicConfigFile property missing - using default dynamic path")

        return f"dynamicConfigFile={self.workload.paths.dynamic}"

    @property
    def static_properties(self) -> list[str]:
        """Build the zoo.cfg content, without dynamic options.

        Returns:
            List of static properties to compared to current zoo.cfg
        """
        return self.build_static_properties(self.zookeeper_properties)

    @property
    def etc_hosts_entries(self) -> list[str]:
        """Gets full `/etc/hosts` entry for resolving peer-related unit hosts.

        Returns:
            Multiline string of `/etc/hosts` entries
        """
        hosts_entries = []
        for server in self.state.servers:
            if not all([server.ip, server.hostname, server.fqdn]):
                return []

            hosts_entries.append(f"{server.ip} {server.fqdn} {server.hostname}")

        return hosts_entries

    def _update_environment(self, env: dict[str, str]) -> None:
        """Updates the /etc/environment for the workload.

        Args:
            env: dict of key env-var, value
        """
        raw_env = self.workload.read(path="/etc/environment")
        map_env = {}

        for var in raw_env:
            key = "".join(var.split("=", maxsplit=1)[0])
            value = "".join(var.split("=", maxsplit=1)[1:])
            if key:
                # only check for keys, as we can have an empty value for a variable
                map_env[key] = value

        updated_env = map_env | env
        content = "\n".join([f"{key}={value}" for key, value in updated_env.items()])

        self.workload.write(content=content, path="/etc/environment")

    def set_etc_hosts(self) -> None:
        """Writes to /etc/hosts with peer-related units."""
        self.workload.write(content="\n".join(self.etc_hosts_entries), path="/etc/hosts")

    def set_jaas_config(self) -> None:
        """Sets the ZooKeeper JAAS config."""
        self.workload.write(content=self.jaas_config, path=self.workload.paths.jaas)

    def set_server_jvmflags(self) -> None:
        """Sets the env-vars needed for SASL auth to /etc/environment on the unit."""
        self._update_environment(
            env={"SERVER_JVMFLAGS": " ".join(self.server_jvmflags + self.jmx_jvmflags)}
        )

    def set_zookeeper_properties(self) -> None:
        """Writes built zoo.cfg file."""
        self.workload.write(
            content="\n".join(self.zookeeper_properties),
            path=self.workload.paths.properties,
        )

    def set_zookeeper_dynamic_properties(self, servers: str) -> None:
        """Writes zookeeper-dynamic.properties containing server connection strings."""
        self.workload.write(content=servers, path=self.workload.paths.dynamic)

    def set_zookeeper_myid(self) -> None:
        """Writes ZooKeeper myid file to data dir."""
        self.workload.write(
            content=str(self.state.unit_server.server_id),
            path=f"{self.workload.paths.data_path}/{DATA_DIR}/myid",
        )

    @staticmethod
    def build_static_properties(properties: list[str]) -> list[str]:
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

    def config_changed(self) -> bool:
        """Compares expected vs actual config that would require a restart to apply."""
        server_properties = self.build_static_properties(self.current_properties)
        config_properties = self.static_properties

        properties_changed = set(server_properties) ^ set(config_properties)
        logger.debug(f"{properties_changed=}")

        jaas_config = self.current_jaas
        jaas_changed = set(jaas_config) ^ set(self.jaas_config.splitlines())

        if not (properties_changed or jaas_changed):
            return False

        if properties_changed:
            logger.info(
                (
                    f"Server.{self.state.unit_server.unit_id} updating properties - "
                    f"OLD PROPERTIES = {set(server_properties) - set(config_properties)}, "
                    f"NEW PROPERTIES = {set(config_properties) - set(server_properties)}"
                )
            )
            self.set_zookeeper_properties()

        if jaas_changed:
            clean_server_jaas = [conf.strip() for conf in jaas_config]
            clean_config_jaas = [conf.strip() for conf in self.jaas_config.splitlines()]
            logger.info(
                (
                    f"Server.{self.state.unit_server.unit_id} updating JAAS config - "
                    f"OLD JAAS = {set(clean_server_jaas) - set(clean_config_jaas)}, "
                    f"NEW JAAS = {set(clean_config_jaas) - set(clean_server_jaas)}"
                )
            )
            self.set_jaas_config()

        return True
