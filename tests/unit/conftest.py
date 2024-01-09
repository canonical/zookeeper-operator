# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

from unittest.mock import patch

import pytest


@pytest.fixture(autouse=True)
def patched_wait(mocker):
    mocker.patch("tenacity.nap.time")


@pytest.fixture(autouse=True)
def patched_etc_hosts_environment():
    with (
        patch("managers.config.ConfigManager.set_etc_hosts"),
        patch("managers.config.ConfigManager.set_server_jvmflags"),
    ):
        yield
