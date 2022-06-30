# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

from collections import defaultdict
import logging
from typing import Dict, List, Optional, Set
from kazoo.handlers.threading import KazooTimeoutError
from kazoo.security import ACL, make_acl
from ops.charm import RelationBrokenEvent, RelationEvent

from ops.framework import Object
from ops.model import MaintenanceStatus, Relation

from charms.zookeeper.v0.client import (
    MemberNotReadyError,
    MembersSyncingError,
    QuorumLeaderNotFoundError,
    ZooKeeperManager,
)
from charms.zookeeper.v0.cluster import UnitNotFoundError, ZooKeeperCluster

logger = logging.getLogger(__name__)

REL_NAME = "database"
PEER = "cluster"


class ZooKeeperProvider(Object):
    def __init__(self, charm) -> None:
        super().__init__(charm, "client")

        self.charm = charm

        self.framework.observe(
            self.charm.on[REL_NAME].relation_joined, self._on_client_relation_updated
        )
        self.framework.observe(
            self.charm.on[REL_NAME].relation_changed, self._on_client_relation_updated
        )
        self.framework.observe(
            self.charm.on[REL_NAME].relation_broken, self._on_client_relation_broken
        )

    @property
    def app_relation(self) -> Relation:
        """Gets the current ZK peer relation."""
        return self.charm.model.get_relation(PEER)

    @property
    def client_relations(self) -> List[Relation]:
        """Gets the relations for all related client applications."""
        return self.charm.model.relations[REL_NAME]

    def relation_config(
        self, relation: Relation, event: Optional[RelationEvent] = None
    ) -> Optional[Dict[str, str]]:
        """Gets the auth config for a currently related application."""

        # If RelationBrokenEvent, skip, we don't want it in the live-data
        if isinstance(event, RelationBrokenEvent):
            return None

        # generating username
        relation_id = relation.id
        username = f"relation-{relation_id}"

        # Default to empty string in case passwords not set
        password = self.app_relation.data[self.charm.app].get(username, "")

        # Default to full permissions if not set by the app
        acl = relation.data[relation.app].get("chroot-acl", "cdrwa")

        # Attempt to default to `database` app value. Else None, it's unset
        chroot = relation.data[relation.app].get(
            "chroot", relation.data[relation.app].get("database", "")
        )

        # If chroot is unset, skip, we don't want it part of the config
        if not chroot:
            logger.info("CHROOT NOT SET")
            return None

        if not str(chroot).startswith("/"):
            chroot = f"/{chroot}"

        return {"username": username, "password": password, "chroot": chroot, "acl": acl}

    def relations_config(self, event: Optional[RelationEvent] = None) -> Dict[str, Dict[str, str]]:
        """Gets auth configs for all currently related applications."""
        relations_config = {}

        for relation in self.client_relations:
            config = self.relation_config(relation=relation, event=event)

            # in the case of RelationBroken or unset chroot
            if not config:
                continue

            relation_id: int = relation.id
            relations_config[str(relation_id)] = config

        return relations_config

    def build_acls(self) -> Dict[str, List[ACL]]:
        """Gets ACLs for all currently related applications."""
        acls = defaultdict(list)

        for _, relation_config in self.relations_config().items():
            chroot = relation_config["chroot"]
            generated_acl = make_acl(
                scheme="sasl",
                credential=relation_config["username"],
                read="r" in relation_config["acl"],
                write="w" in relation_config["acl"],
                create="c" in relation_config["acl"],
                delete="d" in relation_config["acl"],
                admin="a" in relation_config["acl"],
            )

            acls[chroot].append(generated_acl)

        return dict(acls)

    def relations_config_values_for_key(
        self, key: str, event: Optional[RelationEvent] = None
    ) -> Set[str]:
        """Grabs a specific auth config value from all related applications."""
        return {config.get(key, "") for config in self.relations_config(event=event).values()}

    def update_acls(self):
        """Compares leader auth config to incoming relation config, applies necessary add/update/remove actions."""
        super_password, _ = self.charm.cluster.passwords
        zk = ZooKeeperManager(
            hosts=self.charm.cluster.active_hosts, username="super", password=super_password
        )

        leader_chroots = zk.leader_znodes(path="/")
        logger.info(f"{leader_chroots=}")

        relation_chroots = self.relations_config_values_for_key("chroot")
        logger.info(f"{relation_chroots=}")

        acls = self.build_acls()
        logger.info(f"{acls=}")

        # Looks for newly related applications not in config yet
        for chroot in relation_chroots - leader_chroots:
            logger.info(f"CREATE CHROOT - {chroot}")
            zk.create_znode_leader(chroot, acls[chroot])

        # Looks for existing related applications
        for chroot in relation_chroots & leader_chroots:
            logger.info(f"UPDATE CHROOT - {chroot}")
            zk.set_acls_znode_leader(chroot, acls[chroot])

        # Looks for applications no longer in the relation but still in config
        for chroot in leader_chroots - relation_chroots:
            if not self._is_child_of(chroot, relation_chroots):
                logger.info(f"DROP CHROOT - {chroot}")
                zk.delete_znode_leader(chroot)

    @staticmethod
    def _is_child_of(path: str, chroots: Set[str]) -> bool:
        for chroot in chroots:
            if path.startswith(chroot.rstrip("/") + "/"):
                return True

        return False

    @staticmethod
    def build_uris(active_hosts: Set[str], chroot: str, client_port: int = 2181) -> List[str]:
        uris = []
        for host in active_hosts:
            uris.append(f"{host}:{client_port}{chroot}")

        return uris

    def _on_client_relation_updated(self, event: RelationEvent) -> None:
        if not self.charm.unit.is_leader():
            return

        try:
            self.update_acls()
        except (
            MembersSyncingError,
            MemberNotReadyError,
            QuorumLeaderNotFoundError,
            KazooTimeoutError,
            UnitNotFoundError,
        ) as e:
            logger.warning(str(e))
            self.charm.unit.status = MaintenanceStatus(str(e))
            return

        return

    def apply_relation_data(self) -> None:
        relations_config = self.relations_config()

        for relation_id, config in relations_config.items():
            hosts = self.charm.cluster.active_hosts

            relation_data = {}
            relation_data["username"] = config["username"]
            relation_data["password"] = config["password"] or ZooKeeperCluster.generate_password()
            relation_data["chroot"] = config["chroot"]
            relation_data["endpoints"] = ",".join(list(hosts))
            relation_data["uris"] = ",".join(
                [f"{host}:{self.charm.cluster.client_port}{config['chroot']}" for host in hosts]
            )

            self.app_relation.data[self.charm.app].update({config["username"]: config["password"]})

            self.charm.model.get_relation(REL_NAME, int(relation_id)).data[self.charm.app].update(
                relation_data
            )

    def _on_client_relation_broken(self, event: RelationBrokenEvent):
        if not self.charm.unit.is_leader():
            return

        # TODO: maybe remove departing app from event relation data?

        config = self.relation_config(relation=event.relation)
        username = config["username"] if config else ""

        if username in self.charm.cluster.relation.data[self.charm.app]:
            logger.info(f"DELETING - {username}")
            del self.charm.cluster.relation.data[self.charm.app][username]

        # call normal updated handler
        self._on_client_relation_updated(event=event)
