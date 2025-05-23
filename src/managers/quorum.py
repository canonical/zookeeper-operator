#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager for for handling quorum + ACL updates."""
import logging
import re
import socket
from dataclasses import dataclass
from functools import cached_property
from typing import Set

from charms.zookeeper.v0.client import (
    MemberNotReadyError,
    MembersSyncingError,
    QuorumLeaderNotFoundError,
    ZooKeeperManager,
)
from kazoo.exceptions import BadArgumentsError, ConnectionClosedError
from kazoo.handlers.threading import KazooTimeoutError
from kazoo.security import make_acl, make_digest_acl_credential
from ops.charm import RelationEvent

from core.cluster import ClusterState
from core.models import ZKServer
from literals import CLIENT_PORT

logger = logging.getLogger(__name__)


class QuorumManager:
    """Manager for for handling quorum + ACL updates."""

    def __init__(self, state: ClusterState):
        self.state = state

    @cached_property
    def client(self) -> ZooKeeperManager:
        """Cached client manager application for performing ZK commands."""
        admin_username = "super"
        admin_password = self.state.cluster.internal_user_credentials.get(admin_username, "")
        active_hosts = [server.internal_address for server in self.state.started_servers]

        return ZooKeeperManager(
            hosts=active_hosts,
            client_port=CLIENT_PORT,
            username=admin_username,
            password=admin_password,
            read_only=False,
        )

    @dataclass
    class SyncStatus:
        """Type for returning status of a syncing quorum."""

        passed: bool = False
        cause: str = ""

    def is_syncing(self) -> "QuorumManager.SyncStatus":
        """Checks if any server members are currently syncing data.

        To be used when evaluating whether a cluster can upgrade or not.
        """
        try:
            if not self.client.members_broadcasting or not len(self.client.server_members) == len(
                self.state.servers
            ):
                return self.SyncStatus(
                    cause="Not all application units are connected and broadcasting in the quorum"
                )

            if self.client.members_syncing:
                return self.SyncStatus(cause="Some quorum members are still syncing data")

            if not self.state.stable:
                return self.SyncStatus(cause="Charm has not finished initialising")

        except QuorumLeaderNotFoundError:
            return self.SyncStatus(cause="Quorum leader not found")

        except ConnectionClosedError:
            return self.SyncStatus(cause="Unable to connect to the cluster")

        except Exception as e:
            logger.error(str(e))
            return self.SyncStatus(cause="Unknown error")

        return self.SyncStatus(passed=True)

    def get_hostname_mapping(self) -> dict[str, str]:
        """Collects hostname mapping for current unit.

        Returns:
            Dict of string keys 'hostname', 'fqdn', 'ip' and their values
        """
        hostname = socket.gethostname()
        fqdn = socket.getfqdn()
        ip = self.state.get_relation_ip(self.state.peer_relation)

        return {"hostname": hostname, "fqdn": fqdn, "ip": ip}

    def _get_updated_servers(self, add: list[str]) -> dict[str, str]:
        """Simple wrapper for building `updated_servers` for passing to app data updates."""
        updated_servers = {}
        for server_string in add:
            unit_id = str(int(re.findall(r"server.([0-9]+)", server_string)[0]) - 1)
            if server_string in add:
                updated_servers[unit_id] = "added"

        # settings units to removed that were handled in relation-departed
        # also set here in case leader missed the event
        for added_id in self.state.cluster.added_unit_ids:
            if added_id not in [server.unit_id for server in self.state.servers]:
                updated_servers[str(added_id)] = "removed"

        return updated_servers

    def update_cluster(self) -> dict[str, str]:
        """Adds and removes members from the current ZK quorum.

        To be ran by the Juju leader.

        After grabbing all the "started" units that the leader can see in the peer relation
            unit data.
        Adds new members to the quorum (i.e `relation_joined` event).

        Returns:
            A mapping of Juju unit IDs and updated state for changed units
            To be used in updating the app data
                e.g {"0": "added", "1": "removed"}
        """
        # NOTE - BUG in Apache ZooKeeper - https://issues.apache.org/jira/browse/ZOOKEEPER-3577
        # This means that we cannot dynamically reconfigure without also having a PLAIN port open
        # Ideally, have a check here for `client_port=self.secure_client_port` if tls.enabled
        # Until then, we can just use the insecure port for convenience
        active_server_strings = {server.server_string for server in self.state.started_servers}

        try:
            # sorting units to ensure units are added in id order
            zk_members = self.client.server_members
            servers_to_add = sorted(active_server_strings - zk_members)
            self.client.add_members(members=servers_to_add)

            return self._get_updated_servers(add=servers_to_add)

        # caught errors relate to a unit/zk_server not yet being ready to change
        except (
            MembersSyncingError,
            MemberNotReadyError,
            QuorumLeaderNotFoundError,
            KazooTimeoutError,
            BadArgumentsError,
        ) as e:
            logger.warning(str(e))
            return {}

    def server_in_quorum(self, server: ZKServer) -> bool:
        """Checks if server is in current quorum.

        Args:
            server: the server to check

        Returns:
            True if server is found in the quorum. Otherwise False.
        """
        return server.server_string in self.client.server_members

    @staticmethod
    def _is_child_of(path: str, chroots: Set[str]) -> bool:
        """Checks if given path is a child znode from a set of chroot paths.

        Args:
            path: the desired znode path to check parenthood of
            chroots: the potential parent znode paths

        Returns:
            True if `path` is a child of a znode in `chroots`. Otherwise False.
        """
        for chroot in chroots:
            if path.startswith(chroot.rstrip("/") + "/"):
                return True

        return False

    def update_acls(self, event: RelationEvent | None = None) -> None:
        """Compares leader auth config to incoming relation config, applies add/remove actions.

        Args:
            event (optional): used for checking `RelationBrokenEvent`
        """
        leader_chroots = self.client.leader_znodes(path="/")
        logger.debug(f"{leader_chroots=}")

        requested_chroots = set()

        for client in self.state.clients:
            if not client.database:
                continue
            acls = {
                "read": "r" in client.extra_user_roles,
                "write": "w" in client.extra_user_roles,
                "create": "c" in client.extra_user_roles,
                "delete": "d" in client.extra_user_roles,
                "admin": "a" in client.extra_user_roles,
            }

            sasl_acl = make_acl(scheme="sasl", credential=client.username, **acls)
            digest_acl = make_acl(
                scheme="digest",
                credential=make_digest_acl_credential(client.username, client.password),
                **acls,
            )
            logger.debug(f"{sasl_acl=}")

            # FIXME: data-platform-libs should handle this when it's implemented
            if client.database:
                if event and client.relation and client.relation.id == event.relation.id:
                    continue  # skip broken chroots, so they're removed
                else:
                    requested_chroots.add(client.database)

            # Looks for newly related applications not in config yet
            if client.database not in leader_chroots:
                logger.info(f"CREATE CHROOT - {client.database}")
                self.client.create_znode_leader(path=client.database, acls=[sasl_acl, digest_acl])

            # Looks for existing related applications
            logger.debug(f"UPDATE CHROOT - {client.database}")
            self.client.set_acls_znode_leader(path=client.database, acls=[sasl_acl, digest_acl])

            subnodes = self.client.leader_znodes(path=client.database)
            for node in subnodes:
                self.client.set_acls_znode_leader(path=node, acls=[sasl_acl, digest_acl])
