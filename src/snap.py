#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

"""ZooKeeper Snap class and methods."""
import logging

from charms.operator_libs_linux.v0 import apt
from charms.operator_libs_linux.v1 import snap

logger = logging.getLogger(__name__)


class ZooKeeperSnap:
    """Wrapper for performing common operations specific to the ZooKeeper Snap."""

    conf_path = "/var/snap/charmed-zookeeper/current/etc/zookeeper"
    logs_path = "/var/snap/charmed-zookeeper/common/var/log/zookeeper"
    data_path = "/var/snap/charmed-zookeeper/common/var/lib/zookeeper"
    binaries_path = "/snap/charmed-zookeeper/current/opt/zookeeper"

    def __init__(self) -> None:
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
            zookeeper = cache["charmed-zookeeper"]
            node_exporter = cache["node-exporter"]

            if not zookeeper.present:
                zookeeper.ensure(snap.SnapState.Latest, channel="3.6/edge")
            if not node_exporter.present:
                node_exporter.ensure(snap.SnapState.Latest, channel="edge")

            self.zookeeper = zookeeper

            [
                node_exporter.connect(plug=plug)
                for plug in [
                    "hardware-observe",
                    "network-observe",
                    "mount-observe",
                    "system-observe",
                ]
            ]

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
