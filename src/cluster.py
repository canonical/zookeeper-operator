#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
from typing import Any, Callable, Optional

from ops.charm import CharmBase, CharmEvents, RelationChangedEvent, RelationCreatedEvent
from ops.framework import EventBase, EventSource, Object
from ops.model import Relation

logger = logging.getLogger(__name__)

CLUSTER_KEY = "cluster"

# small decorator to ensure function is ran as leader
def leader_check(func: Callable) -> Optional[Any]:
    def check_unit_leader(*args, **kwargs):
        if not kwargs["charm"].unit.is_leader():
            return
        else:
            return func(*args, **kwargs)

    return check_unit_leader


class UnitsChangedEvent(EventBase):
    def __init__(self, handle, host, unit):
        super().__init__(handle)
        self.host = host
        self.unit = unit

    def snapshot(self):
        return {"host": self.host, "unit": self.unit}

    def restore(self, snapshot):
        self.host = snapshot["host"]
        self.unit = snapshot["unit"]

class UpdateServers(EventBase):
    def __init__(self, handle, msg):
        super().__init__(handle)
        self.msg = msg

    def snapshot(self):
        return {"msg": self.msg}

    def restore(self, snapshot):
        self.msg = snapshot["msg"]


class ZooKeeperClusterEvents(CharmEvents):
    update_servers = EventSource(UpdateServers)
    units_changed = EventSource(UnitsChangedEvent)


class ZooKeeperCluster(Object):
    def __init__(
        self,
        charm: CharmBase,
        client_port: int = 2181,
        server_port: int = 2888,
        election_port: int = 3888,
    ) -> None:
        super().__init__(charm, CLUSTER_KEY)
        self.charm = charm
        self.client_port = client_port
        self.server_port = server_port
        self.election_port = election_port
        self.relation: Relation = self.charm.model.get_relation(CLUSTER_KEY)

        self.framework.observe(
            getattr(self.charm.on, "cluster_relation_created"), self._on_cluster_relation_created
        )
        self.framework.observe(
            getattr(self.charm.on, "cluster_relation_departed"), self._on_cluster_relation_departed
        )

    def _on_cluster_relation_created(self, event: RelationCreatedEvent):
        # this event will be emitted once and only once on each unit after install
        # doing this avoids fragile if's confirming if the unit is actually up,
        # or if the event was just received early
        getattr(self.charm.on, "units_changed").emit(
            host=event.relation.data[event.unit]["private-address"], unit=event.unit
        )

    @leader_check
    def _on_units_changed(self, event: UnitsChangedEvent):
        live_units = self.relation.data[self.charm.model.app]["live_units"].copy()
        if event.unit.name in list(live_units.keys()):
            del live_units[event.unit.name]
        else:
            live_units[event.unit.name] = {"host": event.host}

        self.relation.data[self.charm.model.app]["live_units"].update(live_units)
        getattr(self.charm.on, "update_units").emit(msg="unit addresses updating")

    def _on_cluster_relation_departed(self, event):
        getattr(self.charm.on, "units_departed").emit(
            host=event.relation.data[event.unit]["private-address"], unit=event.unit
        )

    def _build_cluster_address(self, host: str) -> str:
        return f"{host}:{self.server_port}:{self.election_port}"

    def _build_client_address(self, host: str) -> str:
        return f"{host}:{self.client_port}"
