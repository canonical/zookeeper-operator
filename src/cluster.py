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
    def __init__(self, handle, relation, unit, departing_unit=None, app=None):
        super().__init__(handle, relation, app=app, unit=unit)
        self._departing_unit = departing_unit

    @property
    def departing_unit(self):
        if not self._departing_unit:
            return None
        return getattr(self.framework, "model").get_unit(self._departing_unit.name)

    def snapshot(self):
        snapshot = {}
        if self.app:
            snapshot["app_name"] = self.app.name
        if self.unit:
            snapshot["unit_name"] = self.unit.name
        if self._departing_unit:
            snapshot["departing_unit_name"] = getattr(self._departing_unit, "name")
        return snapshot

    def restore(self, snapshot):
        app_name = snapshot.get('app_name')
        if app_name:
            self.app = getattr(self.framework, "model").get_app(app_name)
        else:
            self.app = None

        unit_name = snapshot.get('unit_name')
        if unit_name:
            self.unit = getattr(self.framework, "model").get_unit(unit_name)

        departing_unit_name = snapshot.get('departing_unit_name')
        if departing_unit_name:
            self._departing_unit = getattr(self.framework, "model").get_unit(departing_unit_name)
        else:
            self._departing_unit = None


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

        self.framework.observe(
            getattr(charm.on, "cluster_relation_created"), self._on_cluster_relation_updated
        )
        self.framework.observe(
            getattr(charm.on, "cluster_relation_joined"), self._on_cluster_relation_updated
        )
        self.framework.observe(
            getattr(charm.on, "cluster_relation_departed"), self._on_cluster_relation_updated
        )

    @property
    def relation(self):
        return self.charm.model.get_relation(CLUSTER_KEY)

    def _on_cluster_relation_updated(self, event: RelationEvent):
        # this event will be emitted once and only once on each unit after install
        # doing this allows the leader to set their own data in the absence of a joined event
        getattr(self.charm.on, "units_changed").emit(
            relation=event.relation,
            unit=self.charm.unit,
            app=self.charm.model.app,
            departing_unit=getattr(event, "departing_unit", None)
        )

    @leader_check
    def on_units_changed(self, event: UnitsChangedEvent):
        live_units = json.loads(self.relation.data[self.charm.model.app].get("live_units", "{}"))
        event_units = self.relation.units or {event.unit}

        for event_unit in event_units:
            host = self.relation.data[event_unit]["private-address"]
            live_units[event_unit.name] = {"host": host}

        if event.departing_unit:
            del live_units[event.departing_unit.name]

        changed_live_units = json.dumps(live_units, sort_keys=True)
        self.relation.data[self.charm.model.app].update({"live_units": changed_live_units})
        getattr(self.charm.on, "update_servers").emit(msg="unit addresses updating")

    def _build_cluster_address(self, host: str) -> str:
        return f"{host}:{self.server_port}:{self.election_port}"

    def _build_client_address(self, host: str) -> str:
        return f"{host}:{self.client_port}"
