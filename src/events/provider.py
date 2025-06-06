#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Event handler for related applications on the `zookeeper` relation interface."""
import logging
from typing import TYPE_CHECKING

from charms.data_platform_libs.v0.data_interfaces import DatabaseProviderEventHandlers
from charms.zookeeper.v0.client import (
    MemberNotReadyError,
    MembersSyncingError,
    QuorumLeaderNotFoundError,
)
from kazoo.handlers.threading import KazooTimeoutError
from ops.charm import RelationBrokenEvent, RelationEvent
from ops.framework import Object

from literals import REL_NAME

if TYPE_CHECKING:
    from charm import ZooKeeperCharm

logger = logging.getLogger(__name__)


class ProviderEvents(Object):
    """Event handlers for related applications on the `zookeeper` relation interface."""

    def __init__(self, charm):
        super().__init__(charm, "provider")
        self.charm: "ZooKeeperCharm" = charm

        self.provider_events = DatabaseProviderEventHandlers(
            self.charm, self.charm.state.client_provider_interface
        )

        self.framework.observe(
            self.charm.on[REL_NAME].relation_changed, self._on_client_relation_updated
        )
        self.framework.observe(
            self.charm.on[REL_NAME].relation_broken, self._on_client_relation_broken
        )

    def _on_client_relation_updated(self, event: RelationEvent) -> None:
        """Updates ACLs while handling `client_relation_joined` events.

        Once credentals and ACLs are added for the event username, sets them to relation data.
        Future `client_relation_changed` events called on non-leader units checks passwords before
            restarting.
        """
        # Update unit IP for this relation in peer data.
        self.charm.state.unit_server.update(
            {f"ip-{event.relation.id}": self.charm.state.get_relation_ip(event.relation)}
        )

        if not self.charm.unit.is_leader() or self.charm.state.cluster.is_restore_in_progress:
            return

        if not self.charm.state.stable:
            event.defer()
            return

        # ACLs created before passwords set to avoid restarting before successful adding
        # passing event here allows knowledge of broken app, removing it's chroot from ACL list
        try:
            self.charm.quorum_manager.update_acls(
                event=event if isinstance(event, RelationBrokenEvent) else None
            )
        except (
            MembersSyncingError,
            MemberNotReadyError,
            QuorumLeaderNotFoundError,
            KazooTimeoutError,
        ) as e:
            logger.warning(str(e))
            event.defer()
            return

        # FIXME: should be data-platform-libs
        # setting password for new users to relation data
        for client in self.charm.state.clients:
            if not client.database:
                continue

            if (
                isinstance(event, RelationBrokenEvent)
                and client.relation
                and client.relation.id == event.relation.id
            ):
                continue  # don't re-add passwords for broken events

            self.charm.state.cluster.update(
                {client.username: client.password or self.charm.workload.generate_password()}
            )

    def _on_client_relation_broken(self, event: RelationBrokenEvent) -> None:
        """Removes user from ZK app data on `client_relation_broken`.

        Args:
            event: used for passing `RelationBrokenEvent` to subsequent methods
        """
        # Don't remove anything if ZooKeeper is going down
        if self.charm.app.planned_units == 0 or not self.charm.unit.is_leader():
            return

        # clearing up broken application passwords
        self.charm.state.cluster.update({f"relation-{event.relation.id}": ""})
        self.charm.state.unit_server.update({f"ip-{event.relation.id}": ""})

        # call normal updated handler
        self._on_client_relation_updated(event=event)
