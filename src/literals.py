#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

"""Collection of global literals for the ZooKeeper charm."""

CHARMED_ZOOKEEPER_SNAP_REVISION = 7

PEER = "cluster"
REL_NAME = "zookeeper"
STATE = "state"
CHARM_KEY = "zookeeper"
CHARM_USERS = ["super", "sync"]
CERTS_REL_NAME = "certificates"
JMX_PORT = 9998
METRICS_PROVIDER_PORT = 7000

DATA_DIR = "data"
DATALOG_DIR = "data-log"

DEPENDENCIES = {
    "charm": {
        "dependencies": {},
        "name": "zookeeper",
        "upgrade_supported": ">=99",
        "version": "100",
    },
    "snap": {
        "dependencies": {},
        "name": "charmed-zookeeper",
        "upgrade_supported": "^3.5",
        "version": "3.6.4",
    },
    "service": {
        "dependencies": {},
        "name": "zookeeper",
        "upgrade_supported": "^3.5",
        "version": "3.6.4",
    },
}
