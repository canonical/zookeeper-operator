import asyncio
import logging
from pathlib import Path

import continuous_writes as cw
import helpers
import pytest
import yaml
from pytest_operator.plugin import OpsTest

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]


@pytest.mark.abort_on_fail
async def test_deploy_active(ops_test: OpsTest):
    charm = await ops_test.build_charm(".")
    await ops_test.model.deploy(charm, application_name=APP_NAME, num_units=3)
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME], status="active", timeout=3600, idle_period=30, wait_for_exact_units=3
    )

    assert ops_test.model.applications[APP_NAME].status == "active"


@pytest.mark.abort_on_fail
async def test_kill_db_process(ops_test: OpsTest, request):
    """Gracefully removes ZK quorum leader using `juju remove`."""
    hosts = await helpers.get_hosts(ops_test)
    leader_name = await helpers.get_leader_name(ops_test)
    leader_host = await helpers.get_unit_host(ops_test, leader_name)
    username = "super"
    password = helpers.get_super_password(ops_test)
    parent = request.node.name
    non_leader_hosts = ",".join([host for host in hosts.split(",") if host != leader_host])

    logger.info("Starting continuous_writes...")
    cw.start_continuous_writes(parent=parent, hosts=hosts, username=username, password=password)
    await asyncio.sleep(10)

    logger.info("Counting writes are running at all...")
    assert cw.count_znodes(parent=parent, hosts=hosts, username=username, password=password)

    logger.info("Killing leader process...")
    await helpers.kill_unit_process(ops_test=ops_test, unit_name=leader_name, kill_code="SIGKILL")

    logger.info("Checking writes are increasing...")
    writes = cw.count_znodes(
        parent=parent, hosts=non_leader_hosts, username=username, password=password
    )
    await asyncio.sleep(30)  # 3x client timeout
    new_writes = cw.count_znodes(
        parent=parent, hosts=non_leader_hosts, username=username, password=password
    )
    assert new_writes > writes, "writes not continuing to ZK"

    logger.info("Checking leader re-election...")
    new_leader_name = await helpers.get_leader_name(ops_test)
    assert new_leader_name != leader_name

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

    logger.info("Checking old leader caught up...")
    last_write_leader = cw.get_last_znode(
        parent=parent, hosts=leader_host, username=username, password=password
    )
    total_writes_leader = cw.count_znodes(
        parent=parent, hosts=leader_host, username=username, password=password
    )
    assert last_write == last_write_leader
    assert total_writes == total_writes_leader


@pytest.mark.abort_on_fail
async def test_two_clusters_not_replicated(ops_test: OpsTest, request):
    """Confirms that writes to one cluster are not replicated to another."""
    zk_2 = f"{APP_NAME}2"

    logger.info("Deploying second cluster...")
    charm = await ops_test.build_charm(".")
    await ops_test.model.deploy(charm, application_name=zk_2, num_units=3)
    await ops_test.model.wait_for_idle(
        apps=[zk_2], status="active", timeout=3600, idle_period=30, wait_for_exact_units=3
    )

    assert ops_test.model.applications[zk_2].status == "active"

    parent = request.node.name
    username = "super"

    hosts_1 = await helpers.get_hosts(ops_test)
    hosts_2 = await helpers.get_hosts(ops_test, app_name=zk_2)
    password_1 = helpers.get_super_password(ops_test)
    password_2 = helpers.get_super_password(ops_test, app_name=zk_2)

    logger.info("Starting continuous_writes on original cluster...")
    cw.start_continuous_writes(
        parent=parent, hosts=hosts_1, username=username, password=password_1
    )
    await asyncio.sleep(10)

    logger.info("Counting writes are running at all...")
    assert cw.count_znodes(parent=parent, hosts=hosts_1, username=username, password=password_2)

    logger.info("Stopping continuous_writes...")
    cw.stop_continuous_writes()

    logger.info("Confirming writes on original cluster...")
    assert cw.count_znodes(parent=parent, hosts=hosts_1, username=username, password=password_1)

    with pytest.raises(Exception):
        cw.count_znodes(parent=parent, hosts=hosts_2, username=username, password=password_2)

    await ops_test.model.applications[zk_2].remove()
