#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

from binascii import unhexlify
import logging
import re
from typing import Dict, Iterable, List, Optional, Set, Tuple
from kazoo.handlers.threading import KazooTimeoutError
from ops.charm import CharmBase
import json

from ops.model import (
    ActiveStatus,
    MaintenanceStatus,
    Relation,
    StatusBase,
    Unit,
)

from charms.zookeeper.v0.client import (
    MemberNotReadyError,
    MembersSyncingError,
    QuorumLeaderNotFoundError,
    ZooKeeperManager,
)

logger = logging.getLogger(__name__)

CHARM_KEY = "zookeeper"
PEER = "cluster"


class UnitNotFoundError(Exception):
    pass


class ZooKeeperCluster:
    """Handler for performing ZK cluster + peer relation commands."""

    def __init__(
        self,
        charm: CharmBase,
        client_port: int = 2181,
        server_port: int = 2888,
        election_port: int = 3888,
    ) -> None:
        self.charm = charm
        self.client_port = client_port
        self.server_port = server_port
        self.election_port = election_port
        self.status: StatusBase = MaintenanceStatus("performing cluster operation")

    @property
    def relation(self) -> Relation:
        return self.charm.model.get_relation(PEER)

    @property
    def units(self) -> Set[Unit]:
        units = [self.charm.unit]
        for item in self.relation.data:
            if not isinstance(item, Unit):
                continue
            else:
                units.append(item)

        return set(units)

    @property
    def planned_units(self) -> bool:
        return True if self.charm.app.planned_units() else False

    @property
    def servers(self) -> Dict[str, Dict[str, str]]:
        servers = {}
        for item in self.relation.data[self.charm.app]:
            result = json.loads(item)
            if result.get("state", None):
                servers = {**servers, **result}

        logger.info(f"{servers=}")
        return servers

    @property
    def init_finished(self) -> bool:
        for unit in self.units:
            if not self.relation.data[self.charm.app].get(unit.name, None):
                return False
        return True

    @staticmethod
    def get_unit_id(unit: Unit) -> int:
        return int(unit.name.split("/")[1])

    def generate_unit_config(
        self, unit_id: int, state: str, role: str = "participant"
    ) -> Dict[str, Dict[str, str]]:
        unit = None
        for relation_unit in self.units:
            if self.get_unit_id(relation_unit) == unit_id:
                unit = relation_unit

        if not unit:
            raise UnitNotFoundError

        server_id = int(unit.name.split("/")[1]) + 1
        host = self.relation.data[unit]["private-address"]
        server_string = f"server.{server_id}={host}:{self.server_port}:{self.election_port}:{role};0.0.0.0:{self.client_port}"

        return {
            str(unit.name): {
                "host": host,
                "server_string": server_string,
                "state": state,
                "server_id": str(server_id),
                "unit_id": str(unit_id),
            }
        }

    def server_string_to_server_config(
        self, server_string: str, state: str
    ) -> Dict[str, Dict[str, str]]:
        unit_id = int(re.findall(r"server.([1-9]*)", server_string)[0]) - 1
        return self.generate_unit_config(unit_id=unit_id, state=state)

    def update_cluster(self) -> Dict[str, Dict[str, str]]:
        # TODO: you were checking validity of units during startup

        active_hosts = [
            value["host"]
            for _, value in self.servers.items()
            if value.get("state", None) == "added"
        ]
        active_servers = {
            value["server_string"]
            for _, value in self.servers.items()
            if value.get("state", None) == "added"
        }

        try:
            zk = ZooKeeperManager(hosts=active_hosts, client_port=self.client_port)
            zk_members = zk.server_members  # the current members in the ZK quorum

            # remove units first, faster due to no startup/sync delay
            servers_to_remove = list(zk_members - active_servers)
            zk.remove_members(members=servers_to_remove)

            removed_servers = {}
            for removed_server in servers_to_remove:
                removed_servers = {
                    **removed_servers,
                    **self.server_string_to_server_config(removed_server, state="removed"),
                }

            # sorting units to ensure units are added in id order
            servers_to_add = sorted(active_servers - zk_members)
            zk.add_members(members=servers_to_add)

            added_servers = {}
            for added_server in servers_to_add:
                added_servers = {
                    **added_servers,
                    **self.server_string_to_server_config(added_server, state="added"),
                }

            self.status = ActiveStatus()

            return {**removed_servers, **added_servers}

        except (
            MembersSyncingError,
            MemberNotReadyError,
            QuorumLeaderNotFoundError,
            KazooTimeoutError,
        ) as e:
            self.status = MaintenanceStatus(str(e))
            return {}

    def _is_unit_turn(self, unit: Unit) -> bool:
        my_turn = True
        unit_id = self.get_unit_id(unit=unit)
        app_name = unit.name.split("/")[0]

        # looping through all app data, ensuring an item exists
        for myid in range(1, unit_id):
            # if it doesn't exist, it hasn't been added by the leader yet
            # i.e not ready
            if not self.servers.get(f"{app_name}/{myid}", None):
                my_turn = False

        return my_turn

    def _generate_init_units(self, unit_string: str) -> str:
        try:
            quorum_leader_config = self.generate_unit_config(
                unit_id=0, state="ready", role="participant"
            )
            quorum_leader_string = list(quorum_leader_config.values())[0]["server_string"]
        except UnitNotFoundError as e:  # leader unit not yet found, can't add
            logger.error(str(e))
            return ""

        return f"{unit_string}\n{quorum_leader_string}"

    def ready_to_start(self, unit: Unit) -> Tuple[bool, str, Dict]:
        unit_id = self.get_unit_id(unit=unit)
        servers = ""
        unit_config = self.generate_unit_config(unit_id=unit_id, state="ready", role="observer")
        unit_string = list(unit_config.values())[0]["server_string"]

        if unit_id == 0 and not self.app_started:
            unit_string = unit_string.replace("observer", "participant")
            unit_config[unit.name]["server_string"] = unit_string
            return True, unit_string.replace("observer", "participant"), unit_config

        if self.charm.app.planned_units():
            if not self._is_unit_turn(unit=unit):
                return False, "", {}

        if self.app_started:
            # populating with active servers
            for server in self.servers.values():
                if server.get("state", None) == "added":
                    servers = servers + "\n" + server.get("server_string", "")

            servers = f"{servers}\n{unit_string}"

        if not self.app_started:
            servers = self._generate_init_units(unit_string=unit_string)
            if not servers:
                return False, "", {}

        return True, servers, unit_config
