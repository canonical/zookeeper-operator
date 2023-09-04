#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

import asyncio
import logging
import time

import pytest
from pytest_operator.plugin import OpsTest

from .helpers import (
    APP_NAME,
    application_active,
    check_key,
    get_password,
    ping_servers,
    restart_unit,
    write_key,
)

logger = logging.getLogger(__name__)


@pytest.mark.abort_on_fail
async def test_deploy_active(ops_test: OpsTest):
    charm = await ops_test.build_charm(".")
    await ops_test.model.deploy(charm, application_name=APP_NAME, num_units=3)
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME], status="active", timeout=3600, idle_period=30, wait_for_exact_units=3
    )

    assert ops_test.model.applications[APP_NAME].status == "active"


@pytest.mark.abort_on_fail
async def test_simple_scale_up(ops_test: OpsTest):
    await ops_test.model.applications[APP_NAME].add_units(count=3)
    await ops_test.model.block_until(
        lambda: application_active(ops_test, expected_units=6), timeout=3600
    )
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME], status="active", timeout=3600, idle_period=30, wait_for_exact_units=6
    )
    assert ping_servers(ops_test)


@pytest.mark.abort_on_fail
async def test_simple_scale_down(ops_test: OpsTest):
    await ops_test.model.applications[APP_NAME].destroy_units(
        f"{APP_NAME}/5", f"{APP_NAME}/4", f"{APP_NAME}/3"
    )
    await ops_test.model.block_until(
        lambda: application_active(ops_test, expected_units=3), timeout=1000
    )
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME], status="active", timeout=1000, idle_period=30, wait_for_exact_units=3
    )
    assert ping_servers(ops_test)


@pytest.mark.abort_on_fail
async def test_scale_up_replication(ops_test: OpsTest):
    await ops_test.model.block_until(
        lambda: application_active(ops_test, expected_units=3), timeout=600
    )
    assert ping_servers(ops_test)

    host = ops_test.model.applications[APP_NAME].units[0].public_address
    model_full_name = ops_test.model_full_name
    password = get_password(model_full_name or "")
    write_key(host=host, password=password)

    await ops_test.model.applications[APP_NAME].add_units(count=1)
    await ops_test.model.block_until(
        lambda: application_active(ops_test, expected_units=4), timeout=3600
    )
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME], status="active", timeout=1000, idle_period=60, wait_for_exact_units=4
    )

    host = ops_test.model.applications[APP_NAME].units[3].public_address
    check_key(host=host, password=password)


@pytest.mark.abort_on_fail
async def test_kill_quorum_leader_remove(ops_test: OpsTest):
    """Gracefully removes ZK quorum leader using `juju remove`."""
    async with ops_test.fast_forward(fast_interval="1m"):
        await ops_test.model.applications[APP_NAME].destroy_units(f"{APP_NAME}/0")
        await ops_test.model.block_until(
            lambda: application_active(ops_test, expected_units=3), timeout=1000
        )
        await ops_test.model.wait_for_idle(
            apps=[APP_NAME], status="active", timeout=1000, idle_period=30, wait_for_exact_units=3
        )
        assert ping_servers(ops_test)


@pytest.mark.abort_on_fail
async def test_kill_juju_leader_remove(ops_test: OpsTest):
    """Gracefully removes Juju leader using `juju remove`."""
    async with ops_test.fast_forward(fast_interval="1m"):
        leader = None
        for unit in ops_test.model.applications[APP_NAME].units:
            if await unit.is_leader_from_status():
                leader = unit.name
                break

        if leader:
            await ops_test.model.applications[APP_NAME].destroy_units(leader)
            await ops_test.model.block_until(
                lambda: application_active(ops_test, expected_units=2), timeout=1000
            )
            await ops_test.model.wait_for_idle(
                apps=[APP_NAME],
                status="active",
                timeout=1000,
                idle_period=30,
                wait_for_exact_units=2,
            )
            assert ping_servers(ops_test)


@pytest.mark.abort_on_fail
async def test_kill_juju_leader_restart(ops_test: OpsTest):
    """Rudely removes Juju leader by restarting the LXD container."""
    async with ops_test.fast_forward(fast_interval="1m"):
        leader = None
        for unit in ops_test.model.applications[APP_NAME].units:
            if await unit.is_leader_from_status():
                leader = unit.name
                break

        if leader:
            # adding another unit to ensure minimum units for quorum
            await ops_test.model.applications[APP_NAME].add_units(count=1)
            await ops_test.model.block_until(
                lambda: application_active(ops_test, expected_units=3), timeout=3600
            )
            await ops_test.model.wait_for_idle(
                apps=[APP_NAME],
                status="active",
                timeout=1000,
                idle_period=30,
                wait_for_exact_units=3,
            )

            model_full_name = ops_test.model_full_name
            if model_full_name:
                restart_unit(model_full_name=model_full_name, unit=leader)
                time.sleep(10)
                assert ping_servers(ops_test)
            else:
                raise


@pytest.mark.abort_on_fail
async def test_same_model_application_deploys(ops_test: OpsTest):
    """Ensures that re-deployments of the charm starts on the same model."""
    await asyncio.gather(ops_test.model.applications[APP_NAME].remove())
    charm = await ops_test.build_charm(".")
    time.sleep(30)
    await ops_test.model.deploy(charm, application_name=APP_NAME, num_units=3)
    await ops_test.model.block_until(
        lambda: application_active(ops_test, expected_units=3), timeout=3600
    )
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME], status="active", timeout=3600, idle_period=30, wait_for_exact_units=3
    )

    assert ops_test.model.applications[APP_NAME].status == "active"
