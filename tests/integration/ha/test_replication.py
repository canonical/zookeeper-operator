#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

import asyncio
import logging

import continuous_writes as cw
import helpers
import pytest
from pytest_operator.plugin import OpsTest

logger = logging.getLogger(__name__)

USERNAME = "super"

CLIENT_TIMEOUT = 10


@pytest.mark.skip_if_deployed
@pytest.mark.abort_on_fail
async def test_deploy_active(ops_test: OpsTest):
    charm = await ops_test.build_charm(".")
    await ops_test.model.deploy(
        charm,
        application_name=helpers.APP_NAME,
        num_units=3,
        storage={"data": {"pool": "lxd-btrfs", "size": 10240}},
    )
    await helpers.wait_idle(ops_test)


async def test_replication(ops_test: OpsTest):
    host_0 = ops_test.model.applications[helpers.APP_NAME].units[0].public_address
    host_1 = ops_test.model.applications[helpers.APP_NAME].units[1].public_address
    host_2 = ops_test.model.applications[helpers.APP_NAME].units[2].public_address
    password = await helpers.get_password(ops_test=ops_test)

    helpers.write_key(host=host_0, password=password)
    await asyncio.sleep(1)

    helpers.check_key(host=host_0, password=password)
    helpers.check_key(host=host_1, password=password)
    helpers.check_key(host=host_2, password=password)


@pytest.mark.abort_on_fail
async def test_two_clusters_not_replicated(ops_test: OpsTest, request):
    """Confirms that writes to one cluster are not replicated to another."""
    zk_2 = f"{helpers.APP_NAME}2"

    logger.info("Deploying second cluster...")
    new_charm = await ops_test.build_charm(".")
    await ops_test.model.deploy(new_charm, application_name=zk_2, num_units=3)
    await helpers.wait_idle(ops_test, apps=[helpers.APP_NAME, zk_2])

    parent = request.node.name

    hosts_1 = helpers.get_hosts(ops_test)
    hosts_2 = helpers.get_hosts(ops_test, app_name=zk_2)
    password_1 = await helpers.get_password(ops_test)
    password_2 = await helpers.get_password(ops_test, app_name=zk_2)

    logger.info("Starting continuous_writes on original cluster...")
    cw.start_continuous_writes(
        parent=parent, hosts=hosts_1, username=USERNAME, password=password_1
    )
    await asyncio.sleep(CLIENT_TIMEOUT * 3)  # letting client set up and start writing

    logger.info("Checking writes are running at all...")
    assert cw.count_znodes(parent=parent, hosts=hosts_1, username=USERNAME, password=password_1)

    logger.info("Stopping continuous_writes...")
    cw.stop_continuous_writes()

    logger.info("Confirming writes on original cluster...")
    assert cw.count_znodes(parent=parent, hosts=hosts_1, username=USERNAME, password=password_1)

    logger.info("Confirming writes not replicated to new cluster...")
    with pytest.raises(Exception):
        cw.count_znodes(parent=parent, hosts=hosts_2, username=USERNAME, password=password_2)

    logger.info("Cleaning up old cluster...")
    await ops_test.model.applications[zk_2].remove()
    await helpers.wait_idle(ops_test)


@pytest.mark.abort_on_fail
async def test_scale_up_replication(ops_test: OpsTest, request):
    hosts = helpers.get_hosts(ops_test)
    password = await helpers.get_password(ops_test)
    parent = request.node.name

    logger.info("Starting continuous_writes...")
    cw.start_continuous_writes(parent=parent, hosts=hosts, username=USERNAME, password=password)
    await asyncio.sleep(CLIENT_TIMEOUT * 3)  # letting client set up and start writing

    logger.info("Checking writes are running at all...")
    assert cw.count_znodes(parent=parent, hosts=hosts, username=USERNAME, password=password)

    logger.info("Adding new unit...")
    await ops_test.model.applications[helpers.APP_NAME].add_units(count=1)
    await helpers.wait_idle(ops_test, units=4)

    original_hosts = set(hosts.split(","))
    new_hosts = set((helpers.get_hosts(ops_test)).split(","))
    new_host = max(new_hosts - original_hosts)
    new_unit_name = helpers.get_unit_name_from_host(ops_test, host=new_host)

    logger.info("Confirming writes replicated on new unit...")
    assert cw.count_znodes(parent=parent, hosts=new_host, username=USERNAME, password=password)

    logger.info("Stopping continuous_writes...")
    cw.stop_continuous_writes()

    logger.info("Cleaning up extraneous unit...")
    await ops_test.model.applications[helpers.APP_NAME].destroy_units(new_unit_name)
    await helpers.wait_idle(ops_test)
