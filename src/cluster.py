#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

import json
import logging
from typing import Any, Callable, Optional

from ops.charm import (
    CharmBase,
    CharmEvents,
    RelationChangedEvent,
    RelationCreatedEvent,
    RelationEvent,
)
from ops.framework import EventBase, EventSource, Object
from ops.model import Relation

logger = logging.getLogger(__name__)

CLUSTER_KEY = "cluster"

# small decorator to ensure function is ran as leader
def leader_check(func: Callable) -> Any:
    def check_unit_leader(*args, **kwargs):
        if not kwargs["event"].unit.is_leader():
            return
        else:
            return func(*args, **kwargs)

    return check_unit_leader


class UnitsChangedEvent(RelationEvent):
    def __init__(self, handle, relation, unit, host, app=None):
        super().__init__(handle, relation, app=app, unit=unit)
        self.host = host

    def snapshot(self):
        snapshot = {"host": self.host}
        if self.app:
            snapshot["app_name"] = self.app.name
        if self.unit:
            snapshot["unit_name"] = self.unit.name
        return snapshot

    def restore(self, snapshot):
        self.host = snapshot["host"]
        
        app_name = snapshot.get('app_name')
        if app_name:
            self.app = getattr(self.framework, "model").get_app(app_name)
        else:
            self.app = None

        unit_name = snapshot.get('unit_name')
        if unit_name:
            self.unit = getattr(self.framework, "model").get_unit(unit_name)
        else:
            self.unit = None

class UpdateServersEvent(EventBase):
    def __init__(self, handle, msg):
        super().__init__(handle)
        self.msg = msg

    def snapshot(self):
        return {"msg": self.msg}

    def restore(self, snapshot):
        self.msg = snapshot["msg"]


class ZooKeeperClusterEvents(CharmEvents):
    update_servers = EventSource(UpdateServersEvent)
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

        # self.framework.observe(
        #     getattr(charm.on, "cluster_relation_joined"), self._on_cluster_relation_updated
        # )
        # self.framework.observe(
        #     getattr(charm.on, "cluster_relation_departed"), self._on_cluster_relation_updated
        # )
        self.framework.observe(
            getattr(charm.on, "cluster_relation_created"), self._on_cluster_relation_updated
        )

    def _on_cluster_relation_updated(self, event: RelationEvent):
        # this event will be emitted once and only once on each unit after install
        # doing this allows the leader to set their own data in the absence of a joined event
        getattr(self.charm.on, "units_changed").emit(
            relation=event.relation,
            unit=self.charm.unit,
            host=event.relation.data[self.charm.unit]["private-address"],
            app=self.charm.model.app
        )

    @leader_check
    def on_units_changed(self, event: UnitsChangedEvent):
        live_units = json.loads(self.relation.data[self.charm.model.app].get("live_units", "{}"))
        event_unit = getattr(event.unit, "name")

        if event_unit in list(live_units.keys()):
            del live_units[event_unit]
        else:
            live_units[event_unit] = {"host": event.host}

        changed_live_units = json.dumps(live_units, sort_keys=True)
        self.relation.data[self.charm.model.app].update({"live_units": changed_live_units})
        getattr(self.charm.on, "update_servers").emit(msg="unit addresses updating")

    def _on_cluster_relation_departed(self, event):
        getattr(self.charm.on, "units_departed").emit(
            host=event.relation.data[event.unit]["private-address"], unit=event.unit
        )

    def _build_cluster_address(self, host: str) -> str:
        return f"{host}:{self.server_port}:{self.election_port}"

    def _build_client_address(self, host: str) -> str:
        return f"{host}:{self.client_port}"
