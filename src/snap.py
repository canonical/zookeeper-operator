#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

"""ZooKeeper Snap class and methods."""
import logging

from charms.operator_libs_linux.v0 import apt
from charms.operator_libs_linux.v1 import snap
from literals import SNAP_COMMON_PATH

logger = logging.getLogger(__name__)


class ZooKeeperSnap:
    """Wrapper for performing common operations specific to the ZooKeeper Snap."""

    def __init__(self) -> None:
        self.config_path = f"{SNAP_COMMON_PATH}/conf"
        self.logs_path = f"{SNAP_COMMON_PATH}/logs"
        self.data_path = f"{SNAP_COMMON_PATH}/log-data"
        self.jvm_path = f"{SNAP_COMMON_PATH}/jvm"
        self.charm_opt_path = f"{SNAP_COMMON_PATH}/opt/charm"
        self.zookeeper_opt_path = f"{SNAP_COMMON_PATH}/opt/zookeeper"

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
