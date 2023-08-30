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
USERNAME = "super"


@pytest.mark.abort_on_fail
async def test_deploy_active(ops_test: OpsTest):
    charm = await ops_test.build_charm(".")
    await ops_test.model.deploy(charm, application_name=APP_NAME, num_units=3)
    await helpers.wait_idle(ops_test)


@pytest.mark.abort_on_fail
async def test_kill_db_process(ops_test: OpsTest, request):
    """SIGKILLs leader process and checks recovery + re-election."""
    hosts = helpers.get_hosts(ops_test)
    leader_name = helpers.get_leader_name(ops_test, hosts)
    leader_host = helpers.get_unit_host(ops_test, leader_name)
    password = helpers.get_super_password(ops_test)
    parent = request.node.name
    non_leader_hosts = ",".join([host for host in hosts.split(",") if host != leader_host])

    logger.info("Starting continuous_writes...")
    cw.start_continuous_writes(parent=parent, hosts=hosts, username=USERNAME, password=password)
    await asyncio.sleep(30)  # letting client set up and start writing

    logger.info("Checking writes are running at all...")
    assert cw.count_znodes(parent=parent, hosts=hosts, username=USERNAME, password=password)

    logger.info("Killing leader process...")
    await helpers.kill_unit_process(ops_test=ops_test, unit_name=leader_name, kill_code="SIGKILL")

    logger.info("Checking writes are increasing...")
    writes = cw.count_znodes(
        parent=parent, hosts=non_leader_hosts, username=USERNAME, password=password
    )
    await asyncio.sleep(30)  # increasing writes
    new_writes = cw.count_znodes(
        parent=parent, hosts=non_leader_hosts, username=USERNAME, password=password
    )
    assert new_writes > writes, "writes not continuing to ZK"

    logger.info("Checking leader re-election...")
    new_leader_name = helpers.get_leader_name(ops_test, non_leader_hosts)
    assert new_leader_name != leader_name

    logger.info("Stopping continuous_writes...")
    cw.stop_continuous_writes()

    logger.info("Counting writes on surviving units...")
    last_write = cw.get_last_znode(
        parent=parent, hosts=non_leader_hosts, username=USERNAME, password=password
    )
    total_writes = cw.count_znodes(
        parent=parent, hosts=non_leader_hosts, username=USERNAME, password=password
    )
    assert last_write == total_writes

    logger.info("Checking old leader caught up...")
    last_write_leader = cw.get_last_znode(
        parent=parent, hosts=leader_host, username=USERNAME, password=password
    )
    total_writes_leader = cw.count_znodes(
        parent=parent, hosts=leader_host, username=USERNAME, password=password
    )
    assert last_write == last_write_leader
    assert total_writes == total_writes_leader


@pytest.mark.abort_on_fail
async def test_freeze_db_process(ops_test: OpsTest, request):
    """SIGSTOPs leader process and checks recovery + re-election after SIGCONT."""
    hosts = helpers.get_hosts(ops_test)
    leader_name = helpers.get_leader_name(ops_test, hosts)
    leader_host = helpers.get_unit_host(ops_test, leader_name)
    password = helpers.get_super_password(ops_test)
    parent = request.node.name
    non_leader_hosts = ",".join([host for host in hosts.split(",") if host != leader_host])

    logger.info("Starting continuous_writes...")
    cw.start_continuous_writes(parent=parent, hosts=hosts, username=USERNAME, password=password)
    await asyncio.sleep(30)  # letting client set up and start writing

    logger.info("Checking writes are running at all...")
    assert cw.count_znodes(parent=parent, hosts=hosts, username=USERNAME, password=password)

    logger.info("Stopping leader process...")
    await helpers.kill_unit_process(ops_test=ops_test, unit_name=leader_name, kill_code="SIGSTOP")
    await asyncio.sleep(30)  # to give time for re-election

    logger.info("Checking writes are increasing...")
    writes = cw.count_znodes(
        parent=parent, hosts=non_leader_hosts, username=USERNAME, password=password
    )
    await asyncio.sleep(30)  # increasing writes
    new_writes = cw.count_znodes(
        parent=parent, hosts=non_leader_hosts, username=USERNAME, password=password
    )
    assert new_writes > writes, "writes not continuing to ZK"

    logger.info("Checking leader re-election...")
    new_leader_name = helpers.get_leader_name(ops_test, non_leader_hosts)
    assert new_leader_name != leader_name

    logger.info("Continuing leader process...")
    await helpers.kill_unit_process(ops_test=ops_test, unit_name=leader_name, kill_code="SIGCONT")
    await asyncio.sleep(30)  # letting writes continue while unit rejoins

    logger.info("Stopping continuous_writes...")
    cw.stop_continuous_writes()
    await asyncio.sleep(10)  # buffer to ensure writes sync

    logger.info("Counting writes on surviving units...")
    last_write = cw.get_last_znode(
        parent=parent, hosts=non_leader_hosts, username=USERNAME, password=password
    )
    total_writes = cw.count_znodes(
        parent=parent, hosts=non_leader_hosts, username=USERNAME, password=password
    )
    assert last_write == total_writes

    logger.info("Checking old leader caught up...")
    last_write_leader = cw.get_last_znode(
        parent=parent, hosts=leader_host, username=USERNAME, password=password
    )
    total_writes_leader = cw.count_znodes(
        parent=parent, hosts=leader_host, username=USERNAME, password=password
    )
    assert last_write == last_write_leader
    assert total_writes == total_writes_leader


@pytest.mark.abort_on_fail
async def test_network_cut_self_heal(ops_test: OpsTest, request):
    """Cuts and restores network on leader, cluster self-heals after IP change."""
    hosts = helpers.get_hosts(ops_test)
    leader_name = helpers.get_leader_name(ops_test, hosts)
    leader_host = helpers.get_unit_host(ops_test, leader_name)
    leader_machine_name = await helpers.get_unit_machine_name(ops_test, leader_name)
    password = helpers.get_super_password(ops_test)
    parent = request.node.name
    non_leader_hosts = ",".join([host for host in hosts.split(",") if host != leader_host])

    logger.info("Starting continuous_writes...")
    cw.start_continuous_writes(parent=parent, hosts=hosts, username=USERNAME, password=password)
    await asyncio.sleep(30)  # letting client set up and start writing

    logger.info("Checking writes are running at all...")
    assert cw.count_znodes(parent=parent, hosts=hosts, username=USERNAME, password=password)

    logger.info("Cutting leader network...")
    helpers.cut_unit_network(machine_name=leader_machine_name)
    await asyncio.sleep(60)  # to give time for re-election, longer as network cut is weird

    logger.info("Checking writes are increasing...")
    writes = cw.count_znodes(
        parent=parent, hosts=non_leader_hosts, username=USERNAME, password=password
    )
    await asyncio.sleep(30)  # increasing writes
    new_writes = cw.count_znodes(
        parent=parent, hosts=non_leader_hosts, username=USERNAME, password=password
    )
    assert new_writes > writes, "writes not continuing to ZK"

    logger.info("Checking leader re-election...")
    new_leader_name = helpers.get_leader_name(ops_test, non_leader_hosts)
    assert new_leader_name != leader_name

    logger.info("Restoring leader network...")
    helpers.restore_unit_network(machine_name=leader_machine_name)

    logger.info("Waiting for Juju to detect new IP...")
    async with ops_test.fast_forward():  # to ensure update-status runs fast enough
        await ops_test.model.block_until(
            lambda: leader_host
            not in helpers.get_hosts_from_status(ops_test),  # ip changes after lxd config add
            timeout=900,
            wait_period=5,
        )

    logger.info("Stopping continuous_writes...")
    cw.stop_continuous_writes()

    logger.info("Counting writes on surviving units...")
    last_write = cw.get_last_znode(
        parent=parent, hosts=non_leader_hosts, username=USERNAME, password=password
    )
    total_writes = cw.count_znodes(
        parent=parent, hosts=non_leader_hosts, username=USERNAME, password=password
    )
    assert last_write == total_writes

    new_hosts = helpers.get_hosts_from_status(ops_test)
    new_leader_host = max(set(new_hosts) - set(hosts))

    logger.info("Checking old leader caught up...")
    last_write_leader = cw.get_last_znode(
        parent=parent, hosts=new_leader_host, username=USERNAME, password=password
    )
    total_writes_leader = cw.count_znodes(
        parent=parent, hosts=new_leader_host, username=USERNAME, password=password
    )
    assert last_write == last_write_leader
    assert total_writes == total_writes_leader


@pytest.mark.abort_on_fail
async def test_two_clusters_not_replicated(ops_test: OpsTest, request):
    """Confirms that writes to one cluster are not replicated to another."""
    zk_2 = f"{APP_NAME}2"

    logger.info("Deploying second cluster...")
    new_charm = await ops_test.build_charm(".")
    await ops_test.model.deploy(new_charm, application_name=zk_2, num_units=3)
    await helpers.wait_idle(ops_test, apps=[APP_NAME, zk_2])

    parent = request.node.name

    hosts_1 = helpers.get_hosts(ops_test)
    hosts_2 = helpers.get_hosts(ops_test, app_name=zk_2)
    password_1 = helpers.get_super_password(ops_test)
    password_2 = helpers.get_super_password(ops_test, app_name=zk_2)

    logger.info("Starting continuous_writes on original cluster...")
    cw.start_continuous_writes(
        parent=parent, hosts=hosts_1, username=USERNAME, password=password_1
    )
    await asyncio.sleep(30)  # letting client set up and start writing

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
    password = helpers.get_super_password(ops_test)
    parent = request.node.name

    logger.info("Starting continuous_writes...")
    cw.start_continuous_writes(parent=parent, hosts=hosts, username=USERNAME, password=password)
    await asyncio.sleep(30)  # letting client set up and start writing

    logger.info("Checking writes are running at all...")
    assert cw.count_znodes(parent=parent, hosts=hosts, username=USERNAME, password=password)

    logger.info("Adding new unit...")
    await ops_test.model.applications[APP_NAME].add_units(count=1)
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
    await ops_test.model.applications[APP_NAME].destroy_units(new_unit_name)
    await helpers.wait_idle(ops_test)


@pytest.mark.abort_on_fail
async def test_scale_down_storage_re_use(ops_test: OpsTest, request):
    hosts = helpers.get_hosts(ops_test)
    password = helpers.get_super_password(ops_test)
    parent = request.node.name

    logger.info("Starting continuous_writes...")
    cw.start_continuous_writes(parent=parent, hosts=hosts, username=USERNAME, password=password)
    await asyncio.sleep(30)  # letting client set up and start writing

    logger.info("Checking writes are running at all...")
    assert cw.count_znodes(parent=parent, hosts=hosts, username=USERNAME, password=password)

    logger.info("Stopping continuous_writes...")
    cw.stop_continuous_writes()
    await helpers.wait_idle(ops_test)

    logger.info("Scaling down and up and re-using storage...")
    new_host = await helpers.reuse_storage(ops_test)

    logger.info("Confirming writes replicated on new unit...")
    assert cw.count_znodes(parent=parent, hosts=new_host, username=USERNAME, password=password)
