#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

"""Collection of global literals for the ZooKeeper charm."""

CHARMED_ZOOKEEPER_SNAP_REVISION = 28

SUBSTRATE = "vm"
PEER = "cluster"
REL_NAME = "zookeeper"
CHARM_KEY = "zookeeper"
CHARM_USERS = ["super", "sync"]
CERTS_REL_NAME = "certificates"
CLIENT_PORT = 2181
SECURE_CLIENT_PORT = 2182
SERVER_PORT = 2888
ELECTION_PORT = 3888
JMX_PORT = 9998
METRICS_PROVIDER_PORT = 7000

DEPENDENCIES = {
    "service": {
        "dependencies": {},
        "name": "zookeeper",
        "upgrade_supported": "^3.5",
        "version": "3.8.2",
    },
}

DATA_DIR = "data"
DATALOG_DIR = "data-log"

PATHS = {
    "CONF": "/var/snap/charmed-zookeeper/current/etc/zookeeper",
    "DATA": "/var/snap/charmed-zookeeper/common/var/lib/zookeeper",
    "LOGS": "/var/snap/charmed-zookeeper/common/var/log/zookeeper",
    "BIN": "/snap/charmed-zookeeper/current/opt/zookeeper",
}
