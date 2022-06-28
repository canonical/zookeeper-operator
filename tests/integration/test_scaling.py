#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
import time
from pathlib import Path

import pytest
import yaml
from pytest_operator.plugin import OpsTest
from scaling_helpers import check_key, get_password, restart_unit, srvr, write_key

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]


async def ping_servers(ops_test: OpsTest):
    logger.info("pinging servers")
    for unit in ops_test.model.applications[APP_NAME].units:
        host = unit.public_address
        logger.info(f"{srvr(host)=}")
        mode = srvr(host)["Mode"]
        assert mode in ["leader", "follower"]


@pytest.mark.abort_on_fail
async def test_deploy_active(ops_test: OpsTest):
    charm = await ops_test.build_charm(".")
    await ops_test.model.deploy(charm, application_name=APP_NAME, num_units=3)
    await ops_test.model.block_until(lambda: len(ops_test.model.applications[APP_NAME].units) == 3)
    await ops_test.model.set_config({"update-status-hook-interval": "10s"})
    await ops_test.model.wait_for_idle(apps=[APP_NAME], status="active", timeout=1000)

    assert ops_test.model.applications[APP_NAME].status == "active"

    await ops_test.model.set_config({"update-status-hook-interval": "60m"})


@pytest.mark.abort_on_fail
async def test_simple_scale_up(ops_test: OpsTest):
    await ops_test.model.applications[APP_NAME].add_units(count=3)
    await ops_test.model.block_until(lambda: len(ops_test.model.applications[APP_NAME].units) == 6)
    await ops_test.model.wait_for_idle(apps=[APP_NAME], status="active", timeout=1000)
    await ping_servers(ops_test)


@pytest.mark.abort_on_fail
async def test_simple_scale_down(ops_test: OpsTest):
    await ops_test.model.applications[APP_NAME].destroy_units(
        f"{APP_NAME}/5", f"{APP_NAME}/4", f"{APP_NAME}/3"
    )
    await ops_test.model.block_until(lambda: len(ops_test.model.applications[APP_NAME].units) == 3)
    await ops_test.model.wait_for_idle(apps=[APP_NAME], status="active", timeout=1000)
    await ping_servers(ops_test)


@pytest.mark.abort_on_fail
async def test_scale_up_replication(ops_test: OpsTest):
    await ops_test.model.wait_for_idle(apps=[APP_NAME], status="active", timeout=1000)
    await ping_servers(ops_test)
    host = ops_test.model.applications[APP_NAME].units[0].public_address
    model_full_name = ops_test.model_full_name
    password = get_password(model_full_name or "")
    write_key(host=host, password=password)
    await ops_test.model.applications[APP_NAME].add_units(count=1)
    await ops_test.model.block_until(lambda: len(ops_test.model.applications[APP_NAME].units) == 4)
    await ops_test.model.wait_for_idle(apps=[APP_NAME], status="active", timeout=1000)
    check_key(host=host, password=password)


@pytest.mark.abort_on_fail
async def test_kill_quorum_leader_remove(ops_test: OpsTest):
    """Gracefully removes ZK quorum leader using `juju remove`."""
    await ops_test.model.applications[APP_NAME].destroy_units(f"{APP_NAME}/0")
    await ops_test.model.block_until(lambda: len(ops_test.model.applications[APP_NAME].units) == 3)
    await ops_test.model.wait_for_idle(apps=[APP_NAME], status="active", timeout=1000)
    await ping_servers(ops_test)


@pytest.mark.abort_on_fail
async def test_kill_juju_leader_remove(ops_test: OpsTest):
    """Gracefully removes Juju leader using `juju remove`."""
    leader = None
    for unit in ops_test.model.applications[APP_NAME].units:
        if await unit.is_leader_from_status():
            leader = unit.name
            break

    if leader:
        await ops_test.model.applications[APP_NAME].destroy_units(leader)
        await ops_test.model.block_until(
            lambda: len(ops_test.model.applications[APP_NAME].units) == 2
        )
        await ops_test.model.wait_for_idle(apps=[APP_NAME], status="active", timeout=1000)
        await ping_servers(ops_test)


@pytest.mark.abort_on_fail
async def test_kill_juju_leader_restart(ops_test: OpsTest):
    """Rudely removes Juju leader by restarting the LXD container."""
    leader = None
    for unit in ops_test.model.applications[APP_NAME].units:
        if await unit.is_leader_from_status():
            leader = unit.name
            break

    if leader:
        # adding another unit to ensure minimum units for quorum
        await ops_test.model.applications[APP_NAME].add_units(count=1)
        await ops_test.model.block_until(
            lambda: len(ops_test.model.applications[APP_NAME].units) == 3
        )
        await ops_test.model.wait_for_idle(apps=[APP_NAME], status="active", timeout=1000)

        model_full_name = ops_test.model_full_name
        if model_full_name:
            restart_unit(model_full_name=model_full_name, unit=leader)
            time.sleep(10)
            await ping_servers(ops_test)
        else:
            raise
