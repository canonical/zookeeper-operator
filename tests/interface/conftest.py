from unittest.mock import PropertyMock, patch

import unittest
import pytest
from interface_tester.plugin import InterfaceTester

from charm import ZooKeeperCharm
from src.literals import Status


@pytest.fixture
def interface_tester(interface_tester: InterfaceTester):
    with (
        unittest.mock.patch("managers.quorum.QuorumManager.update_acls"),
        unittest.mock.patch(
            "managers.config.ConfigManager.current_jaas",
            new_callable=PropertyMock,
            return_value=["p@ssword"],
        ),
        unittest.mock.patch("workload.ZKWorkload.generate_password", return_value="p@ssword"),
        unittest.mock.patch("workload.ZKWorkload.write"),
        unittest.mock.patch(
            "core.cluster.ClusterState.all_installed",
            new_callable=PropertyMock,
            return_value=Status.ACTIVE,
        ),
        unittest.mock.patch(
            "core.cluster.ClusterState.stable",
            new_callable=PropertyMock,
            return_value=Status.ACTIVE,
        ),
    ):
        interface_tester.configure(
            charm_type=ZooKeeperCharm,
            repo="https://github.com/Batalex/charm-relation-interfaces",
            branch="feat/DPE-3737",
        )
        yield interface_tester
