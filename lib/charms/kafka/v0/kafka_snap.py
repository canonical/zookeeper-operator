# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

"""## Overview

`kafka_snap.py` provides a collection of common functions for managing snap installation and
config parsing common to both the Kafka and ZooKeeper charms

"""

import logging
import os
from typing import Dict, List

from ops.model import ActiveStatus, BlockedStatus, MaintenanceStatus

from charms.operator_libs_linux.v0 import apt
from charms.operator_libs_linux.v1 import snap

logger = logging.getLogger(__name__)

# The unique Charmhub library identifier, never change it
LIBID = "73d0f23286dd469596d358905406dcab"

# Increment this major API version when introducing breaking changes
LIBAPI = 0

# Increment this PATCH version before using `charmcraft publish-lib` or reset
# to 0 if you are raising the major API version
LIBPATCH = 4


SNAP_CONFIG_PATH = "/var/snap/kafka/common/"


def block_check(func):
    """Simple decorator for ensuring function does not run if object is blocked."""

    def check_if_blocked(*args, **kwargs):
        if args[0].blocked:
            return
        else:
            return func(*args, **kwargs)

    return check_if_blocked


def safe_write_to_file(content: str, path: str, mode: str = "w") -> None:
    """Ensures destination filepath exists before writing.

    args:
        content: The content to be written to a file
        path: The full destination filepath
        mode: The write mode. Usually "w" for write, or "a" for append. Default "w"
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, mode) as f:
        f.write(content)

    return


class KafkaSnap:
    """Wrapper for performing common operations specific to the Kafka Snap."""

    def __init__(self) -> None:
        self.snap_config_path = SNAP_CONFIG_PATH
        self.kafka = snap.SnapCache()["kafka"]
        self.blocked = False
        self.status = MaintenanceStatus("performing snap operation")

    @block_check
    def install_kafka_snap(self) -> None:
        """Loads the Kafka snap from LP, returning a StatusBase for the Charm to set.

        If fails with expected errors, it will block the KafkaSnap instance from executing
        additional non-idempotent methods.
        """
        try:
            apt.update()
            apt.add_package("snapd")
            cache = snap.SnapCache()
            kafka = cache["kafka"]

            if not kafka.present:
                kafka.ensure(snap.SnapState.Latest, channel="rock/edge")

            self.kafka = kafka
        except (snap.SnapError, apt.PackageNotFoundError) as e:
            logger.error(str(e))
            self.blocked = True
            self.status = BlockedStatus("failed to install kafka snap")
            return

    def get_kafka_apps(self) -> List:
        """Grabs apps from the snap property.

        Returns:
            List of apps declared by the installed snap
        """
        apps = self.kafka.apps

        return apps

    @block_check
    def start_snap_service(self, snap_service: str) -> None:
        """Starts snap service process.

        If fails with expected errors, it will block the KafkaSnap instance from executing
        additional non-idempotent methods.

        Args:
            snap_service: The desired service to run on the unit
                `kafka` or `zookeeper`
        """

        try:
            self.kafka.start(services=[snap_service])
            # TODO: check if the service is actually running (i.e not failed silently)
            self.status = ActiveStatus()
        except snap.SnapError as e:
            logger.error(str(e))
            self.blocked = True
            self.status = BlockedStatus(f"unable to start snap service: {snap_service}")
            return

    @block_check
    def write_properties(self, properties: str, property_label: str) -> None:
        """Writes to the expected config file location for the Kafka Snap.

        If fails with expected errors, it will block the KafkaSnap instance from executing
        additional non-idempotent methods.

        Args:
            properties: A multiline string containing the properties to be set
            property_label: The file prefix for the config file
                `server` for Kafka, `zookeeper` for ZooKeeper
            mode: The write mode. Usually "w" for write, or "a" for append. Default "w"
        """

        # TODO: Check if required properties are not set, update BlockedStatus
        path = f"{SNAP_CONFIG_PATH}/{property_label}.properties"
        safe_write_to_file(content=properties, path=path)
        logger.info(f"config successfully written to {path}")

    @block_check
    def write_zookeeper_myid(self, myid: int, property_label: str = "zookeeper") -> None:
        """Checks the *.properties file for dataDir, and writes ZooKeeper id to <data-dir>/myid.

        If fails with expected errors, it will block the KafkaSnap instance from executing
        additional non-idempotent methods.

        Args:
            myid: The desired ZooKeeper server id
                Expected to be (unit id + 1) to index from 1
            property_label: The file prefix for the config file. Default "zookeeper"
        """
        properties = self.get_properties(property_label=property_label)
        try:
            myid_path = f"{properties['dataDir']}/myid"
        except KeyError as e:
            logger.error(str(e))
            self.blocked = True
            self.status = BlockedStatus("required property is not set - 'dataDir'")
            return

        safe_write_to_file(content=str(myid), path=myid_path, mode="w")

    @block_check
    def get_properties(self, property_label: str) -> Dict[str, str]:
        """Grabs active config lines from *.properties.

        If fails with expected errors, it will block the KafkaSnap instance from executing
        additional non-idempotent methods.

        Returns:
            A maping of config properties and their values
        """
        path = f"{SNAP_CONFIG_PATH}/{property_label}.properties"
        config_map = {}

        try:
            with open(path, "r") as f:
                config = f.readlines()
        except FileNotFoundError as e:
            logger.error(str(e))
            self.blocked = True
            self.status = BlockedStatus(f"missing properties file: {path}")
            return {}

        for conf in config:
            if conf[0] != "#" and not conf.isspace():
                config_map[str(conf.split("=")[0])] = str(conf.split("=")[1].strip())

        return config_map
