#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
from pathlib import Path

import re
import time
from kazoo.client import KazooClient
from typing import Dict
import pytest
import yaml
from pytest_operator.plugin import OpsTest
from subprocess import PIPE, check_output

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]


def get_password(model_full_name):
    show_unit = check_output(
        f"JUJU_MODEL={model_full_name} juju show-unit {APP_NAME}/0",
        stderr=PIPE,
        shell=True,
        universal_newlines=True,
    )
    response = yaml.safe_load(show_unit)
    password = response[f"{APP_NAME}/0"]["relation-info"][0]["application-data"]["super_password"]
    logger.info(f"{password=}")
    return password


def restart_unit(model_full_name: str, unit: str):
    check_output(
        f"JUJU_MODEL={model_full_name} juju ssh {unit} -i sudo snap stop kafka.zookeeper",
        stderr=PIPE,
        shell=True,
        universal_newlines=True,
    )
    time.sleep(10)
    check_output(
        f"JUJU_MODEL={model_full_name} juju ssh {unit} -i sudo snap start kafka.zookeeper",
        stderr=PIPE,
        shell=True,
        universal_newlines=True,
    )


def write_key(host: str, password: str, username: str = "super"):
    kc = KazooClient(
        hosts=host,
        sasl_options={"mechanism": "DIGEST-MD5", "username": username, "password": password},
    )
    kc.start()
    kc.create_async("/legolas", b"hobbits")
    kc.stop()
    kc.close()


def check_key(host: str, password: str, username: str = "super"):
    kc = KazooClient(
        hosts=host,
        sasl_options={"mechanism": "DIGEST-MD5", "username": username, "password": password},
    )
    kc.start()
    assert kc.exists_async("/legolas")
    value, _ = kc.get_async("/legolas") or None, None
    assert value.get()[0] == b"hobbits"
    kc.stop()
    kc.close()


def srvr(host: str) -> Dict:
    response = check_output(
        f"echo srvr | nc {host} 2181", stderr=PIPE, shell=True, universal_newlines=True
    )

    result = {}
    for item in response.splitlines():
        k = re.split(": ", item)[0]
        v = re.split(": ", item)[1]
        result[k] = v

    return result


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
    for unit in range(3):
        assert ops_test.model.applications[APP_NAME].units[unit].workload_status == "active"
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
    password = get_password(model_full_name)
    write_key(host=host, password=password)
    await ops_test.model.applications[APP_NAME].add_units(count=1)
    await ops_test.model.block_until(lambda: len(ops_test.model.applications[APP_NAME].units) == 4)
    await ops_test.model.wait_for_idle(apps=[APP_NAME], status="active", timeout=1000)
    check_key(host=host, password=password)


@pytest.mark.abort_on_fail
async def test_kill_quorum_leader_remove(ops_test: OpsTest):
    await ops_test.model.applications[APP_NAME].destroy_units(f"{APP_NAME}/0")
    await ops_test.model.block_until(lambda: len(ops_test.model.applications[APP_NAME].units) == 3)
    await ops_test.model.wait_for_idle(apps=[APP_NAME], status="active", timeout=1000)
    await ping_servers(ops_test)


@pytest.mark.abort_on_fail
async def test_kill_juju_leader_remove(ops_test: OpsTest):
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
    leader = None
    for unit in ops_test.model.applications[APP_NAME].units:
        if await unit.is_leader_from_status():
            leader = unit.name
            break

    if leader:
        await ops_test.model.applications[APP_NAME].add_units(count=1)
        await ops_test.model.block_until(
            lambda: len(ops_test.model.applications[APP_NAME].units) == 3
        )
        await ops_test.model.wait_for_idle(apps=[APP_NAME], status="active", timeout=1000)

        model_full_name = ops_test.model_full_name
        if model_full_name:
            restart_unit(model_full_name=model_full_name, unit=leader)
            await ping_servers(ops_test)
        else:
            raise
