#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""Event handlers for creating and restoring backups."""
from __future__ import annotations

import json
import logging
from enum import Enum
from typing import TYPE_CHECKING, cast

from charms.data_platform_libs.v0.data_interfaces import (
    DataPeerData,
    DataPeerOtherUnitData,
    DataPeerUnitData,
)
from charms.data_platform_libs.v0.s3 import (
    CredentialsChangedEvent,
    CredentialsGoneEvent,
    S3Requirer,
)
from ops import (
    ActionEvent,
    Application,
    Relation,
    RelationEvent,
    Unit,
)
from ops.framework import Object

from core.models import SUBSTRATES, RelationState
from core.stubs import S3ConnectionInfo
from literals import RESTORE, S3_BACKUPS_PATH, S3_REL_NAME, SUBSTRATE, Status
from managers.backup import BackupManager

if TYPE_CHECKING:
    from charm import ZooKeeperCharm

logger = logging.getLogger(__name__)


class BackupEvents(Object):
    """Event handlers for creating and restoring backups."""

    def __init__(self, charm):
        super().__init__(charm, "backup")
        self.charm: ZooKeeperCharm = charm
        self.restore_state = RestoreState(self.charm, substrate=SUBSTRATE)
        self.s3_requirer = S3Requirer(self.charm, S3_REL_NAME)
        self.backup_manager = BackupManager(self.charm.state)

        self.framework.observe(
            self.s3_requirer.on.credentials_changed, self._on_s3_credentials_changed
        )
        self.framework.observe(self.s3_requirer.on.credentials_gone, self._on_s3_credentials_gone)

        self.framework.observe(self.charm.on.create_backup_action, self._on_create_backup_action)
        self.framework.observe(self.charm.on.list_backups_action, self._on_list_backups_action)
        self.framework.observe(self.charm.on.restore_action, self._on_restore_action)

        self.framework.observe(
            getattr(self.charm.on, "restore_relation_changed"), self._restore_event_dispatch
        )

    def _on_s3_credentials_changed(self, event: CredentialsChangedEvent):
        if not self.charm.unit.is_leader():
            return

        if not self.charm.state.peer_relation:
            self.charm._set_status(Status.NO_PEER_RELATION)
            event.defer()
            return

        s3_parameters = self.s3_requirer.get_s3_connection_info()
        required_parameters = [
            "bucket",
            "access-key",
            "secret-key",
        ]
        missing_required_parameters = [
            param for param in required_parameters if param not in s3_parameters
        ]
        if missing_required_parameters:
            logger.warning(
                f"Missing required S3 parameters in relation with S3 integrator: {missing_required_parameters}"
            )
            self.charm._set_status(Status.MISSING_S3_CONFIG)
            return

        s3_parameters = self.s3_requirer.get_s3_connection_info()

        s3_parameters.setdefault("endpoint", "https://s3.amazonaws.com")
        s3_parameters.setdefault("region", "")
        s3_parameters.setdefault("path", S3_BACKUPS_PATH)

        for key, value in s3_parameters.items():
            if isinstance(value, str):
                s3_parameters[key] = value.strip()

        s3_parameters = cast(S3ConnectionInfo, s3_parameters)
        if not self.backup_manager.create_bucket(s3_parameters):
            self.charm._set_status(Status.BUCKET_NOT_CREATED)
            return

        self.charm.state.cluster.update({"s3-credentials": json.dumps(s3_parameters)})

    def _on_s3_credentials_gone(self, event: CredentialsGoneEvent):
        if not self.charm.unit.is_leader():
            return

        self.charm.state.cluster.update({"s3-credentials": ""})

    def _on_create_backup_action(self, event: ActionEvent):
        failure_conditions = [
            (not self.charm.unit.is_leader(), "Action must be ran on the application leader"),
            (
                not self.charm.state.stable,
                "Cluster must be stable before making a backup",
            ),
            (
                not self.charm.state.cluster.s3_credentials,
                "Cluster needs an access to an object storage to make a backup",
            ),
        ]

        for check, msg in failure_conditions:
            if check:
                logging.error(msg)
                event.set_results({"error": msg})
                event.fail(msg)
                return

        backup_metadata = self.backup_manager.create_backup()

        output = self.backup_manager.format_backups_table(
            [backup_metadata], title="Backup created"
        )
        event.log(output)

    def _on_list_backups_action(self, event: ActionEvent):
        failure_conditions = [
            (not self.charm.unit.is_leader(), "Action must be ran on the application leader"),
            (
                not self.charm.state.cluster.s3_credentials,
                "Cluster needs an access to an object storage to make a backup",
            ),
        ]

        for check, msg in failure_conditions:
            if check:
                logging.error(msg)
                event.set_results({"error": msg})
                event.fail(msg)
                return

        backups_metadata = self.backup_manager.list_backups()
        output = self.backup_manager.format_backups_table(backups_metadata)
        event.log(output)
        event.set_results({"backups": json.dumps(backups_metadata)})

    def _on_restore_action(self, event: ActionEvent):
        """Restore a snapshot referenced by its id.

        Steps:
        - stop client traffic (still necessary if we stops the units?)
        - stop all units
        - backup local state so that we can rollback if anything goes wrong (manual op?)
        - wipe data folders
        - get snapshot from object storage, save in data folder
        - restart units
        - notify clients
        """
        failure_conditions = [
            (not self.charm.unit.is_leader(), "Action must be ran on the application leader"),
            (
                not self.charm.state.cluster.s3_credentials,
                "Cluster needs an access to an object storage to make a backup",
            ),
            (
                not (id_to_restore := event.params.get("backup-id", "")),
                "No backup id to restore provided",
            ),
            (
                not self.backup_manager.is_snapshot_in_bucket(id_to_restore),
                "Backup id not found in storage object",
            ),
            # TODO: check for ongoing restore
        ]

        for check, msg in failure_conditions:
            if check:
                logging.error(msg)
                event.set_results({"error": msg})
                event.fail(msg)
                return

        self.restore_state.cluster.update(
            {
                "id-to-restore": id_to_restore,
                "restore-instruction": RestoreStep.NOT_STARTED.value,
            }
        )
        event.log(f"Beggining restore flow for snapshot {id_to_restore}")

    def _restore_event_dispatch(self, event: RelationEvent):
        cluster_state = self.restore_state.cluster
        unit_state = self.restore_state.unit_server

        if not cluster_state.id_to_restore:
            self.restore_state.unit_server.update(
                {"restore-progress": RestoreStep.NOT_STARTED.value}
            )
            self.charm._set_status(self.charm.state.ready)
            return

        if self.charm.unit.is_leader():
            self._maybe_progress_step()

        match cluster_state.restore_instruction, unit_state.restore_progress:
            case RestoreStep.STOP_WORKFLOW, RestoreStep.NOT_STARTED:
                self._stop_workflow()
            case RestoreStep.RESTORE, RestoreStep.STOP_WORKFLOW:
                self._download_and_restore()
            case RestoreStep.RESTART, RestoreStep.RESTORE:
                self._restart_workflow()
            case RestoreStep.CLEAN, RestoreStep.RESTART:
                self._cleaning()
            case _:
                pass

    def _maybe_progress_step(self):
        """Check that all units are done with the current restore flow instruction and move to the next if applicable."""
        current_instruction = self.restore_state.cluster.restore_instruction
        next_instruction = current_instruction.next_step()

        if all(
            (unit.restore_progress is current_instruction for unit in self.restore_state.servers)
        ):
            payload = {"restore-instruction": next_instruction.value}
            if current_instruction is RestoreStep.CLEAN:
                payload = payload | {"id-to-restore": "", "to_restore": ""}
                # TODO: notify clients

            self.restore_state.cluster.update(payload)

    def _stop_workflow(self):
        self.charm._set_status(Status.ONGOING_RESTORE)
        logger.info("Restoring - stopping workflow")
        self.charm.workload.stop()
        self.restore_state.unit_server.update(
            {"restore-progress": RestoreStep.STOP_WORKFLOW.value}
        )

    def _download_and_restore(self):
        logger.info("Restoring - restore snapshot")
        # TODO
        self.backup_manager.restore_snapshot(
            self.restore_state.cluster.id_to_restore, self.charm.workload
        )
        self.restore_state.unit_server.update({"restore-progress": RestoreStep.RESTORE.value})

    def _restart_workflow(self):
        logger.info("Restoring - restarting workflow")
        self.charm.workload.restart()
        self.restore_state.unit_server.update({"restore-progress": RestoreStep.RESTART.value})

    def _cleaning(self):
        logger.info("Restoring - cleaning files")
        # TODO
        self.restore_state.unit_server.update({"restore-progress": RestoreStep.CLEAN.value})


class RestoreStep(str, Enum):
    """Represent restore flow step."""

    NOT_STARTED = ""
    STOP_WORKFLOW = "stop"
    RESTORE = "restore"
    RESTART = "restart"
    CLEAN = "clean"

    def next_step(self):
        """Get the next logical restore flow step."""
        match self:
            case RestoreStep.NOT_STARTED:
                return RestoreStep.STOP_WORKFLOW
            case RestoreStep.STOP_WORKFLOW:
                return RestoreStep.RESTORE
            case RestoreStep.RESTORE:
                return RestoreStep.RESTART
            case RestoreStep.RESTART:
                return RestoreStep.CLEAN
            case RestoreStep.CLEAN:
                return RestoreStep.NOT_STARTED


class ZKServerRestore(RelationState):
    """Restore-focused state collection metadata for a charm unit."""

    def __init__(
        self,
        relation: Relation | None,
        data_interface: DataPeerUnitData,
        component: Unit,
        substrate: SUBSTRATES,
    ):
        super().__init__(relation, data_interface, component, substrate)
        self.unit = component

    @property
    def restore_progress(self) -> RestoreStep:
        """Latest restore flow step the unit went through."""
        return RestoreStep(self.relation_data.get("restore-progress", ""))


class ZKClusterRestore(RelationState):
    """Restore-focused state collection metadata for the charm application."""

    def __init__(
        self,
        relation: Relation | None,
        data_interface: DataPeerData,
        component: Application,
        substrate: SUBSTRATES,
    ):
        super().__init__(relation, data_interface, component, substrate)
        self.data_interface = data_interface
        self.app = component

    @property
    def id_to_restore(self) -> str:
        """Backup id to restore."""
        return self.relation_data.get("id-to-restore", "")

    @property
    def restore_instruction(self) -> RestoreStep:
        """Current restore flow step to go through."""
        return RestoreStep(self.relation_data.get("restore-instruction", ""))


class RestoreState(Object):
    """Collection of global cluster state for Framework/Object."""

    def __init__(self, charm: Object, substrate: SUBSTRATES):
        super().__init__(parent=charm, key="restore_state")
        self.substrate: SUBSTRATES = substrate

        self.peer_app_interface = DataPeerData(self.model, relation_name=RESTORE)
        self.peer_unit_interface = DataPeerUnitData(self.model, relation_name=RESTORE)
        self._servers_data = {}

    @property
    def peer_relation(self) -> Relation | None:
        """The cluster peer relation."""
        return self.model.get_relation(RESTORE)

    # --- CORE COMPONENTS---

    @property
    def unit_server(self) -> ZKServerRestore:
        """The server state of the current running Unit."""
        return ZKServerRestore(
            relation=self.peer_relation,
            data_interface=self.peer_unit_interface,
            component=self.model.unit,
            substrate=self.substrate,
        )

    @property
    def peer_units_data_interfaces(self) -> dict[Unit, DataPeerOtherUnitData]:
        """The cluster peer relation."""
        if not self.peer_relation or not self.peer_relation.units:
            return {}

        for unit in self.peer_relation.units:
            if unit not in self._servers_data:
                self._servers_data[unit] = DataPeerOtherUnitData(
                    model=self.model, unit=unit, relation_name=RESTORE
                )
        return self._servers_data

    @property
    def cluster(self) -> ZKClusterRestore:
        """The cluster state of the current running App."""
        return ZKClusterRestore(
            relation=self.peer_relation,
            data_interface=self.peer_app_interface,
            component=self.model.app,
            substrate=self.substrate,
        )

    @property
    def servers(self) -> set[ZKServerRestore]:
        """Grabs all servers in the current peer relation, including the running unit server.

        Returns:
            Set of ZKServers in the current peer relation, including the running unit server.
        """
        if not self.peer_relation:
            return set()

        servers = set()
        for unit, data_interface in self.peer_units_data_interfaces.items():
            servers.add(
                ZKServerRestore(
                    relation=self.peer_relation,
                    data_interface=data_interface,
                    component=unit,
                    substrate=self.substrate,
                )
            )
        servers.add(self.unit_server)

        return servers
