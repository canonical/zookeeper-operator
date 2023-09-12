#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

import asyncio
import logging
import time

import continuous_writes as cw
import pytest
from pytest_operator.plugin import OpsTest

from integration.ha.helpers import (
    APP_NAME,
    all_db_processes_down,
    check_key,
    get_hosts,
    get_leader_name,
    get_password,
    get_super_password,
    get_unit_host,
    patch_restart_delay,
    remove_restart_delay,
    send_control_signal,
    write_key,
)

logger = logging.getLogger(__name__)

RESTART_DELAY = 60


@pytest.fixture()
async def restart_delay(ops_test: OpsTest):
    for unit in ops_test.model.applications[APP_NAME].units:
        await patch_restart_delay(ops_test=ops_test, unit_name=unit.name, delay=60)
    yield
    for unit in ops_test.model.applications[APP_NAME].units:
        await remove_restart_delay(ops_test=ops_test, unit_name=unit.name)


@pytest.mark.abort_on_fail
async def test_deploy_active(ops_test: OpsTest):
    charm = await ops_test.build_charm(".")
    await ops_test.model.deploy(charm, application_name=APP_NAME, num_units=3)
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME],
        status="active",
        timeout=3600,
        idle_period=30,
        wait_for_exact_units=3,
    )
    assert ops_test.model.applications[APP_NAME].status == "active"


async def test_replication(ops_test: OpsTest):
    host_0 = ops_test.model.applications[APP_NAME].units[0].public_address
    host_1 = ops_test.model.applications[APP_NAME].units[1].public_address
    host_2 = ops_test.model.applications[APP_NAME].units[2].public_address
    model_full_name = ops_test.model_full_name
    password = get_password(model_full_name or "")

    write_key(host=host_0, password=password)
    time.sleep(1)

    check_key(host=host_0, password=password)
    check_key(host=host_1, password=password)
    check_key(host=host_2, password=password)


async def test_full_cluster_crash(ops_test: OpsTest, request, restart_delay):
    hosts = await get_hosts(ops_test)
    leader_name = await get_leader_name(ops_test)
    leader_host = await get_unit_host(ops_test, leader_name)
    username = "super"
    password = get_super_password(ops_test)
    parent = request.node.name
    non_leader_hosts = ",".join([host for host in hosts.split(",") if host != leader_host])

    logger.info("Starting continuous_writes...")
    cw.start_continuous_writes(parent=parent, hosts=hosts, username=username, password=password)
    await asyncio.sleep(10)

    logger.info("Counting writes are running at all...")
    assert cw.count_znodes(parent=parent, hosts=hosts, username=username, password=password)

    # kill all units "simultaneously"
    await asyncio.gather(
        *[
            send_control_signal(ops_test, unit.name, kill_code="SIGKILL")
            for unit in ops_test.model.applications[APP_NAME].units
        ]
    )

    # Check that all servers are down at the same time
    assert await all_db_processes_down(ops_test), "Not all units down at the same time."
    time.sleep(RESTART_DELAY * 2)

    logger.info("Checking writes are increasing...")
    writes = cw.count_znodes(
        parent=parent, hosts=non_leader_hosts, username=username, password=password
    )
    await asyncio.sleep(30)  # 3x client timeout
    new_writes = cw.count_znodes(
        parent=parent, hosts=non_leader_hosts, username=username, password=password
    )
    assert new_writes > writes, "writes not continuing to ZK"

    logger.info("Stopping continuous_writes...")
    cw.stop_continuous_writes()

    logger.info("Counting writes on surviving units...")
    last_write = cw.get_last_znode(
        parent=parent, hosts=non_leader_hosts, username=username, password=password
    )
    total_writes = cw.count_znodes(
        parent=parent, hosts=non_leader_hosts, username=username, password=password
    )
    assert last_write == total_writes
