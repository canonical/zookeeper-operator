#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

import unittest
from unittest.mock import MagicMock, patch

from zookeeper_config import (
    TLS_ZOOKEEPER_PROPERTIES,
    ZOOKEEPER_PROPERTIES,
    ZooKeeperConfig,
)

FORMATTED_CONFIG = """
initLimit=5
syncLimit=2
tickTime=2000"""


class TestZookeeperConfig(unittest.TestCase):
    @patch("pathlib.Path.is_file", return_value=True)
    def test_ssl_enabled(self, mock_is_file):
        zk_config = ZooKeeperConfig()
        assert zk_config.ssl_enabled()
        mock_is_file.assert_called_once()

    @patch("pathlib.Path.is_file", return_value=False)
    def test_ssl_disabled(self, mock_is_file):
        zk_config = ZooKeeperConfig()
        assert not zk_config.ssl_enabled()
        mock_is_file.assert_called_once()

    def test_create_properties_ssl_disabled(self):
        zk_config = ZooKeeperConfig()
        config = {
            "init-limit": 5,
            "retain-certs": False,
            "sync-limit": 2,
            "tick-time": 2000,
        }
        zk_config.ssl_enabled = MagicMock(return_value=False)
        props = zk_config.create_properties(config)
        assert props == ZOOKEEPER_PROPERTIES + FORMATTED_CONFIG

    def test_create_properties_ssl_enabled(self):
        zk_config = ZooKeeperConfig()
        config = {
            "init-limit": 5,
            "retain-certs": False,
            "sync-limit": 2,
            "tick-time": 2000,
        }
        zk_config.ssl_enabled = MagicMock(return_value=True)
        props = zk_config.create_properties(config)
        assert props == ZOOKEEPER_PROPERTIES + FORMATTED_CONFIG + TLS_ZOOKEEPER_PROPERTIES
