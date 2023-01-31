#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

"""ZooKeeper Snap class and methods."""
import logging

from charms.operator_libs_linux.v0 import apt
from charms.operator_libs_linux.v1 import snap

logger = logging.getLogger(__name__)

SNAP_CONFIG_PATH = "/var/snap/zookeeper/common/"


class ZooKeeperSnap:
    """Wrapper for performing common operations specific to the ZooKeeper Snap."""

    def __init__(self) -> None:
        self.snap_config_path = SNAP_CONFIG_PATH
        self.zookeeper = snap.SnapCache()["zookeeper"]

    def install(self) -> bool:
        """Loads the ZooKeeper snap from LP, returning a StatusBase for the Charm to set.

        Returns:
            True if successfully installed. False otherwise.
        """
        try:
            apt.update()
            apt.add_package(["snapd", "openjdk-17-jre-headless"])
            cache = snap.SnapCache()
            zookeeper = cache["zookeeper"]

            if not zookeeper.present:
                zookeeper.ensure(snap.SnapState.Latest, channel="3.6/edge")

            self.zookeeper = zookeeper
            return True
        except (snap.SnapError, apt.PackageNotFoundError) as e:
            logger.error(str(e))
            return False

    def start_snap_service(self, snap_service: str) -> bool:
        """Starts snap service process.

        Args:
            snap_service: The desired service to run on the unit

        Returns:
            True if service successfully starts. False otherwise.
        """
        try:
            self.zookeeper.start(services=[snap_service])
            return True
        except snap.SnapError as e:
            logger.exception(str(e))
            return False

    def stop_snap_service(self, snap_service: str) -> bool:
        """Stops snap service process.

        Args:
            snap_service: The desired service to stop on the unit

        Returns:
            True if service successfully stops. False otherwise.
        """
        try:
            self.zookeeper.stop(services=[snap_service])
            return True
        except snap.SnapError as e:
            logger.exception(str(e))
            return False

    def restart_snap_service(self, snap_service: str) -> bool:
        """Restarts snap service process.

        Args:
            snap_service: The desired service to run on the unit

        Returns:
            True if service successfully restarts. False otherwise.
        """
        try:
            self.zookeeper.restart(services=[snap_service])
            return True
        except snap.SnapError as e:
            logger.exception(str(e))
            return False
