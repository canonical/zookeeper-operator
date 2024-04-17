#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

"""Collection of global literals for the ZooKeeper charm."""

from dataclasses import dataclass
from enum import Enum
from typing import Literal

from ops.model import ActiveStatus, BlockedStatus, MaintenanceStatus, StatusBase, WaitingStatus

CHARMED_ZOOKEEPER_SNAP_REVISION = 32

SUBSTRATE = "vm"
CHARM_KEY = "zookeeper"

PEER = "cluster"
REL_NAME = "zookeeper"
CONTAINER = "zookeeper"
CHARM_USERS = ["super", "sync"]
CERTS_REL_NAME = "certificates"
CLIENT_PORT = 2181
SECURE_CLIENT_PORT = 2182
SERVER_PORT = 2888
ELECTION_PORT = 3888
JMX_PORT = 9998
METRICS_PROVIDER_PORT = 7000
USER = "snap_daemon"
GROUP = "root"

DEPENDENCIES = {
    "service": {
        "dependencies": {},
        "name": "zookeeper",
        "upgrade_supported": "^3.5",
        "version": "3.8.4",
    },
}

PATHS = {
    "CONF": "/var/snap/charmed-zookeeper/current/etc/zookeeper",
    "DATA": "/var/snap/charmed-zookeeper/common/var/lib/zookeeper",
    "LOGS": "/var/snap/charmed-zookeeper/common/var/log/zookeeper",
    "BIN": "/snap/charmed-zookeeper/current/opt/zookeeper",
}

# --- TYPES ---

DebugLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR"]


@dataclass
class StatusLevel:
    status: StatusBase
    log_level: DebugLevel


class Status(Enum):
    ACTIVE = StatusLevel(ActiveStatus(), "DEBUG")
    NO_PEER_RELATION = StatusLevel(MaintenanceStatus("no peer relation yet"), "DEBUG")
    SERVICE_NOT_INSTALLED = StatusLevel(
        BlockedStatus("unable to install zookeeper service"), "ERROR"
    )
    SERVICE_NOT_RUNNING = StatusLevel(BlockedStatus("zookeeper service not running"), "ERROR")
    CONTAINER_NOT_CONNECTED = StatusLevel(
        MaintenanceStatus("zookeeper container not ready"), "DEBUG"
    )
    NO_PASSWORDS = StatusLevel(
        WaitingStatus("waiting for leader to create internal user credentials"), "DEBUG"
    )
    NOT_UNIT_TURN = StatusLevel(WaitingStatus("other units starting first"), "DEBUG")
    NOT_ALL_IP = StatusLevel(MaintenanceStatus("not all units registered IP"), "DEBUG")
    NO_CERT = StatusLevel(WaitingStatus("unit waiting for signed certificates"), "INFO")
    NOT_ALL_RELATED = StatusLevel(
        MaintenanceStatus("cluster not stable - not all units related"), "DEBUG"
    )
    STALE_QUORUM = StatusLevel(MaintenanceStatus("cluster not stable - quorum is stale"), "DEBUG")
    NOT_ALL_ADDED = StatusLevel(
        MaintenanceStatus("cluster not stable - not all units added to quorum"), "DEBUG"
    )
    NOT_ALL_QUORUM = StatusLevel(
        MaintenanceStatus("provider not ready - not all units using same encryption"), "DEBUG"
    )
    SWITCHING_ENCRYPTION = StatusLevel(
        MaintenanceStatus("provider not ready - switching quorum encryption"), "DEBUG"
    )
    ALL_UNIFIED = StatusLevel(
        MaintenanceStatus("provider not ready - portUnification not yet disabled"), "DEBUG"
    )
    SERVICE_UNHEALTHY = StatusLevel(
        BlockedStatus("zookeeper service is unreachable or not serving requests"), "ERROR"
    )
