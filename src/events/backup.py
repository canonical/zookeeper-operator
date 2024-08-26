#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""Event handlers for creating and restoring backups."""
import logging
from typing import TYPE_CHECKING

from charms.data_platform_libs.v0.s3 import (
    CredentialsChangedEvent,
    CredentialsGoneEvent,
    S3Requirer,
)
from ops.framework import Object

from literals import S3_REL_NAME

if TYPE_CHECKING:
    from charm import ZooKeeperCharm

logger = logging.getLogger(__name__)


class BackupEvents(Object):
    """Event handlers for creating and restoring backups."""

    def __init__(self, charm):
        super().__init__(charm, "backup")
        self.charm: "ZooKeeperCharm" = charm
        self.s3_client = S3Requirer(self.charm, S3_REL_NAME)

        self.framework.observe(
            self.s3_client.on.credentials_changed, self._on_s3_credentials_changed
        )
        self.framework.observe(self.s3_client.on.credentials_gone, self._on_s3_credentials_gone)

        self.framework.observe(self.charm.on.create_backup_action, self._on_create_backup_action)
        self.framework.observe(self.charm.on.list_backups_action, self._on_list_backups_action)
        self.framework.observe(self.charm.on.restore_action, self._on_restore_action)

    def _on_s3_credentials_changed(self, event: CredentialsChangedEvent):
        # TODO:
        # - store credentials
        # - create bucket if it does not exist

        if not self.charm.unit.is_leader():
            return

        _credentials = self.s3_client.get_s3_connection_info()

    def _on_s3_credentials_gone(self, event: CredentialsGoneEvent):
        # TODO: remove credentials from peer data
        if not self.charm.unit.is_leader():
            return

        pass

    def _on_create_backup_action(self, _):
        # TODO
        pass

    def _on_list_backups_action(self, _):
        # TODO
        pass

    def _on_restore_action(self, _):
        # TODO
        pass
