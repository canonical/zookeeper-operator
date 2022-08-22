#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

from config import ZooKeeperConfig


def test_build_static_properties_removes_necessary_rows():
    properties = [
        "clientPort=2181",
        "authProvider.sasl=org.apache.zookeeper.server.auth.SASLAuthenticationProvider",
        "maxClientCnxns=60",
        "dynamicConfigFile=/var/snap/kafka/common/zookeeper.properties.dynamic.100000041",
    ]

    static = ZooKeeperConfig.build_static_properties(properties=properties)

    assert len(static) == 2
    assert "clientPort" not in "".join(static)
    assert "dynamicConfigFile" not in "".join(static)
