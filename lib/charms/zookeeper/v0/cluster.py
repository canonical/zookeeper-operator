#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
import re
from typing import Dict, List, Set, Tuple, Union
from kazoo.handlers.threading import KazooTimeoutError
from ops.charm import CharmBase

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

    def has_init_finished(self, unit) -> bool:
        logger.info("--------------HAS INIT FINISHED--------------")
        unit_id = self.get_unit_id(unit)
        logger.info(f"{unit_id=}")

        for myid in range(0, unit_id):
            logger.info(f"{myid=}")
            if self.relation.data[self.charm.app].get(str(myid), None) != ("added" or "started"):

                logger.info(f"FALSE")
                return False

        logger.info(f"TRUE")
        return True

    @property
    def peer_units(self) -> Set[Unit]:
        logger.info("--------------PEER UNITS--------------")
        logger.info(f"{set([self.charm.unit] + list(self.relation.units))=}")
        return set([self.charm.unit] + list(self.relation.units))

    @property
    def started_units(self) -> Set[Unit]:
        logger.info("--------------STARTED UNITS--------------")
        logger.info(f"{self.peer_units=}")
        started_units = set()
        for unit in self.peer_units:
            logger.info(f"{unit=}")
            logger.info(f"{self.relation.data[unit]=}")
            if self.relation.data[unit].get("state", None) == "started":
                started_units.add(unit)

        logger.info(f"{started_units=}")
        return started_units

    @staticmethod
    def get_unit_id(unit: Unit) -> int:
        return int(unit.name.split("/")[1])

    def get_unit_from_id(self, unit_id: int) -> Unit:
        for unit in self.peer_units:
            if int(unit.name.split("/")[1]) == unit_id:
                return unit

        raise UnitNotFoundError("could not find unit in peer relation")

    def unit_config(
        self, unit: Union[Unit, int], state: str = "ready", role: str = "participant"
    ) -> Dict[str, str]:
        logger.info("--------------UNIT CONFIG--------------")
        unit_id = None
        server_id = None
        if isinstance(unit, Unit):
            unit = unit
            unit_id = self.get_unit_id(unit=unit)
            server_id = unit_id + 1
        if isinstance(unit, int):
            unit_id = unit
            server_id = unit + 1
            unit = self.get_unit_from_id(unit)

        host = self.relation.data[unit]["private-address"]
        server_string = f"server.{server_id}={host}:{self.server_port}:{self.election_port}:{role};0.0.0.0:{self.client_port}"


        logger.info(f"{host=}")
        logger.info(f"{unit=}")
        logger.info(f"{unit_id=}")
        logger.info(f"{server_id=}")

        return {
            "host": host,
            "server_string": server_string,
            "server_id": str(server_id),
            "unit_id": str(unit_id),
            "unit_name": unit.name,
            "state": state,
        }

    def update_cluster(self) -> List:
        logger.info("--------------UPDATE CLUSTER--------------")
        active_hosts = []
        active_servers = set()

        logger.info(f"{self.started_units=}")
        for unit in self.started_units:
            logger.info(f"{unit=}")
            active_hosts.append(self.unit_config(unit=unit)["host"])
            active_servers.add(self.unit_config(unit=unit)["server_string"])


        logger.info(f"{active_hosts=}")
        logger.info(f"{active_servers=}")

        try:
            zk = ZooKeeperManager(hosts=active_hosts, client_port=self.client_port)
            zk_members = zk.server_members  # the current members in the ZK quorum

            # remove units first, faster due to no startup/sync delay
            servers_to_remove = list(zk_members - active_servers)
            zk.remove_members(members=servers_to_remove)

            # sorting units to ensure units are added in id order
            servers_to_add = sorted(active_servers - zk_members)
            zk.add_members(members=servers_to_add)

            self.status = ActiveStatus()

            updated_servers = []
            for server in servers_to_add:
                unit_id = str(int(re.findall(r"server.([1-9]+)", server)[0]) - 1)
                updated_servers.append({unit_id: "added"})

            updated_servers.append({"0": "added"})  # for during initial startup

            logger.info(f"{updated_servers=}")
            return updated_servers

        except (
            MembersSyncingError,
            MemberNotReadyError,
            QuorumLeaderNotFoundError,
            KazooTimeoutError,
            UnitNotFoundError,
        ) as e:
            self.status = MaintenanceStatus(str(e))
            return []

    def _is_unit_turn(self, unit: Unit) -> bool:
        logger.info("--------------IS UNIT TURN--------------")
        my_turn = True
        unit_id = self.get_unit_id(unit=unit)
        logger.info(f"{unit_id=}")

        # looping through all app data, ensuring an item exists
        for myid in range(0, unit_id):
            logger.info(f"{myid=}")
            logger.info(f"{self.relation.data[self.charm.app]=}")
            # if it doesn't exist, it hasn't been added by the leader yet
            # i.e not ready
            if not self.relation.data[self.charm.app].get(str(myid), None) != ("started", "added"):
                my_turn = False

        logger.info(f"{my_turn}")
        return my_turn

    def _generate_init_units(self, unit_string: str) -> str:
        logger.info("--------------GENERATE INIT UNITS--------------")
        try:
            quorum_leader_config = self.unit_config(unit=0, state="ready", role="participant")
            quorum_leader_string = quorum_leader_config["server_string"]
            return unit_string + "\n" + quorum_leader_string
        except UnitNotFoundError:
            return ""


    def _generate_units(self, unit_string: str) -> str:
        logger.info("--------------GENERATE UNITS--------------")
        try:
            servers = ""
            for unit_id in self.relation.data[self.charm.app]:
                server_string = self.unit_config(unit=int(unit_id))["server_string"]
                servers = servers + "\n" + server_string

            servers = servers + "\n" + unit_string
            logger.info(f"{servers=}")
            return servers
        except UnitNotFoundError:
            return ""


    def ready_to_start(self, unit: Unit) -> Tuple[bool, str, Dict]:
        servers = ""
        unit_config = self.unit_config(unit=unit, state="ready", role="observer")
        unit_string = unit_config["server_string"]
        unit_id = unit_config["unit_id"]

        if int(unit_id) == 0: 
            logger.info("LEADER")
            unit_string = unit_string.replace("observer", "participant")
            return True, unit_string.replace("observer", "participant"), unit_config

        if not self.has_init_finished(unit=unit):
            logger.info("INIT FOLLOWER")
            servers = self._generate_init_units(unit_string=unit_string)
            if not self._is_unit_turn(unit=unit) or not servers:
                return False, "", {}
            return True, servers, unit_config

        logger.info("FOLLOWER")
        servers = self._generate_units(unit_string=unit_string)
        if not self._is_unit_turn(unit=unit):
            return False, "", {}

        return True, servers, unit_config
