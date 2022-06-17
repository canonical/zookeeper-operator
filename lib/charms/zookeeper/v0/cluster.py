#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
import re
from typing import Dict, Iterable, List, Set, Tuple, Union
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
    """Generic exception for when a desired unit isn't yet found in the relation data."""

    pass

class NotUnitTurnError(Exception):
    """Generic exception for when a desired unit isn't next in line to start safely."""

    pass


class ZooKeeperCluster:
    """Handler for managing the ZK peer-relation.

    Mainly for managing scale-up/down orchestration
    """

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
        """Relation property to be used by both the instance and charm.

        Returns:
            relation (Relation): The peer relation
        """
        return self.charm.model.get_relation(PEER)

    def has_init_finished(self, unit: Unit) -> bool:
        """Checks whether any unit before the given unit has been added by the quorum leader.

        Returns:
            has_init_finished (bool): True if there are no units before the given unit pending. Otherwise False
        """
        unit_id = self.get_unit_id(unit)

        # when starting ZK units with auth, they need to join quorum in increasing order.
        # units should be added by the leader to app data before succeeding units can start
        for myid in range(0, unit_id):
            # TODO: this should probably be just "started"
            if self.relation.data[self.charm.app].get(str(myid), None) != ("added" or "started"):
                return False
        return True

    @property
    def peer_units(self) -> Iterable[Unit]:
        """Grabs all units in the current peer relation, including the running unit.

        Returns:
            peer_units (set(Unit)): Units in the current peer relation, including the running unit
        """
        return set([self.charm.unit] + list(self.relation.units))

    @property
    def started_units(self) -> Set[Unit]:
        """Checks peer relation units for whether they've started the ZK service.

        Returns:
            started_units (set(Unit)): The units who have started the service
        """
        started_units = set()
        for unit in self.peer_units:
            if self.relation.data[unit].get("state", None) == "started":
                started_units.add(unit)

        return started_units

    @staticmethod
    def get_unit_id(unit: Unit) -> int:
        """Grabs the unit's ID as definied by Juju.

        Args:
            unit (Unit): The target unit

        Returns:
            unit_id (int): The Juju unit ID for the unit
                e.g "zookeeper/0" -> 0
        """
        return int(unit.name.split("/")[1])

    def get_unit_from_id(self, unit_id: int) -> Unit:
        for unit in self.peer_units:
            if int(unit.name.split("/")[1]) == unit_id:
                return unit

        raise UnitNotFoundError("could not find unit in peer relation")

    def unit_config(
        self, unit: Union[Unit, int], state: str = "ready", role: str = "participant"
    ) -> Dict[str, str]:
        """Builds a collection of data useful for ZK for a given unit.

        Args:
            unit (Unit or int): The target unit, either directly or from it's Juju unit ID
            state (str): The desired output state.
                "ready" if installation is complete
                "started" if the ZK service has started running
            role (str): The ZK role for the unit. Default = "participant"

        Returns:
            unit_config (dict): The config for the given unit, with keys:
                "host" - the host address for the unit
                "server_string" - the built ZK server string. e.g "server.1:host:server_port:election_port:role:localhost:client_port"
                "server_id" - the ZK ID for the unit, equivalent to Juju unit ID + 1
                "unit_name" - the name of the unit
                "state" - the desired state of the unit

        Raises:
            UnitNotFoundError (from get_unit_from_id): When the target unit can't be found in the unit relation data, and as such cannot extract the private-address
        """
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

        try:
            host = self.relation.data[unit]["private-address"]
        except KeyError:
            raise UnitNotFoundError

        server_string = f"server.{server_id}={host}:{self.server_port}:{self.election_port}:{role};0.0.0.0:{self.client_port}"

        return {
            "host": host,
            "server_string": server_string,
            "server_id": str(server_id),
            "unit_id": str(unit_id),
            "unit_name": unit.name,
            "state": state,
        }

    def update_cluster(self) -> Dict[str, str]:
        """Adds and removes members from the current ZK quroum.

        To be ran by leader.

        After grabbing all the "started" units that the leader can see in the peer relation unit data, it removes members not in the quorum anymore (i.e relation_departed), and adds new members to the quorum (i.e relation_joined).

            Returns:
                updated_servers (dict) - a mapping of Juju unit IDs that were successfully added to the quorum, and their new state. To be used by the leader to update app data

        """
        active_hosts = []
        active_servers = set()

        for unit in self.started_units:
            active_hosts.append(self.unit_config(unit=unit)["host"])
            active_servers.add(self.unit_config(unit=unit)["server_string"])

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

            # extracts Juju unit ID from the newly removed servers
            updated_servers = {}
            for server in servers_to_add:
                unit_id = str(int(re.findall(r"server.([1-9]+)", server)[0]) - 1)
                updated_servers[unit_id:"removed"]

            # extracts Juju unit ID from the newly added servers
            for server in servers_to_add:
                unit_id = str(int(re.findall(r"server.([1-9]+)", server)[0]) - 1)
                updated_servers[unit_id:"added"]

            # for during initial startup, as otherwise wouldn't be set
            updated_servers["0"] = "added"

            return updated_servers
        
        # all errors relate to a unit/zk_server not yet being ready to change
        except (
            MembersSyncingError,
            MemberNotReadyError,
            QuorumLeaderNotFoundError,
            KazooTimeoutError,
            UnitNotFoundError,
        ) as e:
            self.status = MaintenanceStatus(str(e))
            return {}

    def _is_unit_turn(self, unit: Unit) -> bool:
        """Loops through the units in the app data confirming they are started,
        if they are, the unit is ready to start."""

        my_turn = True
        unit_id = self.get_unit_id(unit=unit)

        for myid in range(0, unit_id):
            # if it doesn't exist, it hasn't been added by the leader yet
            # i.e not ready
            # TODO: confirm "started" vs "added" here
            if not self.relation.data[self.charm.app].get(str(myid), None) != ("started", "added"):
                my_turn = False

        return my_turn

    def _generate_init_units(self, unit_string: str) -> str:
        """During initilisation of the cluster, populate target servers with at least the leader."""
        try:
            quorum_leader_config = self.unit_config(unit=0, state="ready", role="participant")
            quorum_leader_string = quorum_leader_config["server_string"]
            return unit_string + "\n" + quorum_leader_string
        except UnitNotFoundError:  # leader not yet in peer data
            return ""

    def _generate_units(self, unit_string: str) -> str:
        """After initilisation of the cluster, populate target servers with the active units from the app relation data."""
        try:
            servers = ""
            # TODO: Bug fix this, probably doesn't work
            for unit_id in self.relation.data[self.charm.app]:
                server_string = self.unit_config(unit=int(unit_id))["server_string"]
                servers = servers + "\n" + server_string

            servers = servers + "\n" + unit_string
            return servers
        except UnitNotFoundError:
            return ""

    def ready_to_start(self, unit: Unit) -> Tuple[str, Dict]:
        """Decides whether a unit should start, and with what configuration.
        
        Args:
            unit (Unit): the unit to validate

        Returns:
            server_config (str), unit_config (dict): 
                `server_config` - a new-line delimited string of servers to add to a config file
                `unit_config` - a mapping of configuration for the given unit to be added to unit data 

        """
        servers = ""
        unit_config = self.unit_config(unit=unit, state="ready", role="observer")
        unit_string = unit_config["server_string"]
        unit_id = unit_config["unit_id"]

        if int(unit_id) == 0:  # i.e is the initial leader unit, always a participant to start quorum
            unit_string = unit_string.replace("observer", "participant")
            return unit_string.replace("observer", "participant"), unit_config

        if not self.has_init_finished(unit=unit):
            servers = self._generate_init_units(unit_string=unit_string)
            if not self._is_unit_turn(unit=unit) or not servers:
                raise NotUnitTurnError("initialising - not unit turn")
            return servers, unit_config

        servers = self._generate_units(unit_string=unit_string)
        if not self._is_unit_turn(unit=unit) or not servers:
            raise NotUnitTurnError("not unit turn")

        return servers, unit_config
