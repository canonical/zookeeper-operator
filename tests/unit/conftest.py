# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

import pytest

@pytest.fixture(autouse=True)
def patched_wait(mocker):
    mocker.patch("tenacity.nap.time")
