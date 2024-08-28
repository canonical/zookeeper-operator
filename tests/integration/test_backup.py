#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

import asyncio
import logging

import pytest
from pytest_operator.plugin import OpsTest

from .helpers import APP_NAME

logger = logging.getLogger(__name__)


@pytest.mark.abort_on_fail
async def test_deploy_active(ops_test: OpsTest, zk_charm):
    await asyncio.gather(
        ops_test.model.deploy(
            zk_charm,
            application_name=APP_NAME,
            num_units=3,
        )
    )

    async with ops_test.fast_forward():
        await ops_test.model.block_until(
            lambda: len(ops_test.model.applications[APP_NAME].units) == 3
        )
        await ops_test.model.wait_for_idle(
            apps=[APP_NAME], status="active", timeout=1000, idle_period=30
        )

    assert ops_test.model.applications[APP_NAME].status == "active"
