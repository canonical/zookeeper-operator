# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

from unittest.mock import Mock, patch

import pytest
from ops import JujuVersion
from tests.unit.test_charm import PropertyMock

from literals import SUBSTRATE


@pytest.fixture(autouse=True)
def patched_idle(mocker, request):
    if "nopatched_idle" in request.keywords:
        yield
    else:
        yield mocker.patch(
            "events.upgrade.ZKUpgradeEvents.idle", new_callable=PropertyMock, return_value=True
        )


@pytest.fixture(autouse=True)
def patched_wait(mocker):
    mocker.patch("tenacity.nap.time")


@pytest.fixture(autouse=True)
def patched_set_rolling_update_partition(mocker):
    mocker.patch("events.upgrade.ZKUpgradeEvents._set_rolling_update_partition")


@pytest.fixture(autouse=True)
def patched_pebble_restart(mocker):
    mocker.patch("ops.model.Container.restart")


@pytest.fixture(autouse=True)
def patched_alive(mocker):
    mocker.patch("workload.ZKWorkload.alive", new_callable=PropertyMock, return_value=True)


@pytest.fixture(autouse=True)
def patched_healthy(mocker):
    mocker.patch("workload.ZKWorkload.healthy", new_callable=PropertyMock, return_value=True)


@pytest.fixture(autouse=True)
def patched_etc_hosts_environment():
    with (
        patch("managers.config.ConfigManager.set_etc_hosts"),
        patch("managers.config.ConfigManager.set_server_jvmflags"),
    ):
        yield


@pytest.fixture(autouse=True)
def juju_has_secrets(mocker):
    """Using Juju3 we should always have secrets available."""
    mocker.patch.object(JujuVersion, "has_secrets", new_callable=PropertyMock).return_value = True


@pytest.fixture(autouse=True)
def patched_k8s_client(monkeypatch):
    with monkeypatch.context() as m:
        m.setattr("lightkube.core.client.GenericSyncClient", Mock())
        yield


@pytest.fixture(autouse=True)
def patched_node_ip():
    if SUBSTRATE == "k8s":
        with patch(
            "core.models.ZKServer.node_ip",
            new_callable=PropertyMock,
            return_value="111.111.111.111",
        ) as patched_node_ip:
            yield patched_node_ip
    else:
        yield


@pytest.fixture(autouse=True)
def patched_node_port():
    if SUBSTRATE == "k8s":
        with patch(
            "managers.k8s.K8sManager.get_nodeport",
            return_value=30000,
        ) as patched_node_port:
            yield patched_node_port
    else:
        yield
