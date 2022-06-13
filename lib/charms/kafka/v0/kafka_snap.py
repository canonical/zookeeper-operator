# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

"""## Overview

`kafka_snap.py` provides a collection of common functions for managing snap installation and
config parsing common to both the Kafka and ZooKeeper charms

"""

import logging
import os
from typing import Dict, List

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


class KafkaSnapError(Exception):
    def __init__(self, message: str) -> None:
        self.message = message


def safe_write_to_file(content: str, path: str, mode: str = "w") -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, mode) as f:
        f.write(content)


class KafkaSnap:
    def __init__(self) -> None:
        self.snap_config_path = SNAP_CONFIG_PATH
        self.kafka = snap.SnapCache()["kafka"]

    def install_kafka_snap(self) -> None:
        """Loads the Kafka snap from LP, returning a StatusBase for the Charm to set.

        Returns:
            MaintenanceStatus (StatusBase): If snap install was successful
            BlockedStatus (StatusBase): If snap install failed
        """
        try:
            apt.update()
            apt.add_package("snapd")
            cache = snap.SnapCache()
            kafka = cache["kafka"]

            if not kafka.present:
                kafka.ensure(snap.SnapState.Latest, channel="rock/edge")

            self.kafka = kafka
            logger.info("sucessfully installed kafka snap")
        except (snap.SnapError, apt.PackageNotFoundError) as e:
            logger.error(str(e))
            raise KafkaSnapError("failed to install kafka snap")

    def get_kafka_apps(self) -> List:
        """Grabs apps from the snap property.

        Returns:
            list: The apps declared by the installed snap
        """
        apps = self.kafka.apps

        return apps

    def start_snap_service(self, snap_service: str) -> None:
        """Starts snap service process

        Args:
            snap_service (str): The desired service to run on the unit
                `kafka` or `zookeeper`
        Returns:
            ActiveStatus (StatusBase): If service starts successfully
            BlockedStatus (StatusBase): If service fails to start
        """
        try:
            self.kafka.start(services=[snap_service])
            logger.info(f"successfully started snap service: {snap_service}")
        except snap.SnapError as e:
            logger.error(str(e))
            raise KafkaSnapError("unable to start snap service: {snap_service}")

    def write_default_properties(
        self, properties: str, property_label: str, mode: str = "w"
    ) -> None:
        path = f"{SNAP_CONFIG_PATH}/{property_label}.properties"
        safe_write_to_file(content=properties, path=path, mode=mode)
        logger.info(f"config successfully written to {path}")

    def write_zookeeper_myid(self, myid: int, property_label: str = "zookeeper"):
        # TODO: Check if properties not set, return BlockedStatus
        properties = self.get_properties(property_label=property_label)
        # TODO: Check if dataDir not set, return BlockedStatus
        myid_path = f"{properties['dataDir']}/myid"
        safe_write_to_file(content=str(myid), path=myid_path, mode="w")
        logger.info(f"succcessfully wrote file myid - {myid}")

    def get_properties(self, property_label: str) -> Dict[str, str]:
        """Grabs active config lines from *.properties.

        Returns:
            dict: A map of config properties and their values
        """
        path = f"{SNAP_CONFIG_PATH}/{property_label}.properties"
        try:
            with open(path, "r") as f:
                config = f.readlines()
        except FileNotFoundError as e:
            logger.error(str(e))
            raise KafkaSnapError(f"missing properties file: {path}")

        config_map = {}

        for conf in config:
            if conf[0] != "#" and not conf.isspace():
                logger.debug(conf.strip())
                config_map[str(conf.split("=")[0])] = str(conf.split("=")[1].strip())

        return config_map
