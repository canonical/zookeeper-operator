#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

"""ZooKeeper Snap class and methods."""
import logging

from charms.operator_libs_linux.v0 import apt
from charms.operator_libs_linux.v1 import snap
from literals import CHARMED_ZOOKEEPER_SNAP_REVISION

logger = logging.getLogger(__name__)


class ZooKeeperSnap:
    """Wrapper for performing common operations specific to the ZooKeeper Snap."""

    SNAP_NAME = "charmed-zookeeper"
    COMPONENT = "zookeeper"
    SNAP_SERVICE = "daemon"

    conf_path = f"/var/snap/{SNAP_NAME}/current/etc/{COMPONENT}"
    logs_path = f"/var/snap/{SNAP_NAME}/common/var/log/{COMPONENT}"
    data_path = f"/var/snap/{SNAP_NAME}/common/var/lib/{COMPONENT}"
    binaries_path = f"/snap/{SNAP_NAME}/current/opt/{COMPONENT}"

    def __init__(self) -> None:
        self.zookeeper = snap.SnapCache()[self.SNAP_NAME]

    def install(self) -> bool:
        """Loads the ZooKeeper snap from LP, returning a StatusBase for the Charm to set.

        Returns:
            True if successfully installed. False otherwise.
        """
        try:
            apt.update()
            apt.add_package(["snapd", "openjdk-17-jre-headless"])
            cache = snap.SnapCache()
            zookeeper = cache[self.SNAP_NAME]

            zookeeper.ensure(snap.SnapState.Present, revision=CHARMED_ZOOKEEPER_SNAP_REVISION)

            self.zookeeper = zookeeper
            self.zookeeper.hold()

            return True
        except (snap.SnapError, apt.PackageNotFoundError) as e:
            logger.error(str(e))
            return False

    def start_snap_service(self) -> bool:
        """Starts snap service process.

        Returns:
            True if service successfully starts. False otherwise.
        """
        try:
            self.zookeeper.start(services=[self.SNAP_SERVICE])
            return True
        except snap.SnapError as e:
            logger.exception(str(e))
            return False

    def stop_snap_service(self) -> bool:
        """Stops snap service process.

        Returns:
            True if service successfully stops. False otherwise.
        """
        try:
            self.zookeeper.stop(services=[self.SNAP_SERVICE])
            return True
        except snap.SnapError as e:
            logger.exception(str(e))
            return False

    def restart_snap_service(self) -> bool:
        """Restarts snap service process.

        Returns:
            True if service successfully restarts. False otherwise.
        """
        try:
            self.zookeeper.restart(services=[self.SNAP_SERVICE])
            return True
        except snap.SnapError as e:
            logger.exception(str(e))
            return False
