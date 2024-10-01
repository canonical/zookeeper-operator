#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

import json
import logging

import pytest
import requests
from pytest_operator.plugin import OpsTest

from literals import DEPENDENCIES, JMX_PORT, METRICS_PROVIDER_PORT

from .helpers import APP_NAME, count_lines_with, get_address

logger = logging.getLogger(__name__)


@pytest.mark.abort_on_fail
async def test_deploy_active(ops_test: OpsTest):
    charm = await ops_test.build_charm(".")
    await ops_test.model.create_storage_pool("test_pool", "lxd")
    await ops_test.model.deploy(
        charm,
        application_name=APP_NAME,
        num_units=3,
        storage={"data": {"pool": "test_pool", "size": 10240}},
    )

    async with ops_test.fast_forward():
        await ops_test.model.block_until(
            lambda: len(ops_test.model.applications[APP_NAME].units) == 3
        )
        await ops_test.model.wait_for_idle(
            apps=[APP_NAME], status="active", timeout=1000, idle_period=30
        )

    assert ops_test.model.applications[APP_NAME].status == "active"


@pytest.mark.abort_on_fail
async def test_consistency_between_workload_and_metadata(ops_test: OpsTest):
    application = ops_test.model.applications[APP_NAME]
    assert application.data.get("workload-version", "") == DEPENDENCIES["service"]["version"]


async def test_exporter_endpoints(ops_test: OpsTest):
    unit_address = await get_address(ops_test=ops_test)
    jmx_exporter_url = f"http://{unit_address}:{JMX_PORT}/metrics"
    jmx_resp = requests.get(jmx_exporter_url)

    metrics_url = f"http://{unit_address}:{METRICS_PROVIDER_PORT}/metrics"
    metrics_resp = requests.get(metrics_url)

    assert jmx_resp.ok, "jmx port not active"
    assert metrics_resp.ok, "metrics provider port not active"


@pytest.mark.abort_on_fail
@pytest.mark.log_level_change
async def test_log_level_change(ops_test: OpsTest):
    for unit in ops_test.model.applications[APP_NAME].units:
        assert (
            count_lines_with(
                ops_test.model_full_name,
                unit.name,
                "/var/snap/charmed-zookeeper/common/var/log/zookeeper/zookeeper.log",
                "DEBUG",
            )
            == 0
        )

    await ops_test.model.applications[APP_NAME].set_config({"log-level": "DEBUG"})

    await ops_test.model.wait_for_idle(
        apps=[APP_NAME], status="active", timeout=1000, idle_period=30
    )

    for unit in ops_test.model.applications[APP_NAME].units:
        assert (
            count_lines_with(
                ops_test.model_full_name,
                unit.name,
                "/var/snap/charmed-zookeeper/common/var/log/zookeeper/zookeeper.log",
                "DEBUG",
            )
            > 0
        )

    await ops_test.model.applications[APP_NAME].set_config({"log-level": "INFO"})

    await ops_test.model.wait_for_idle(
        apps=[APP_NAME], status="active", timeout=1000, idle_period=30
    )


@pytest.mark.abort_on_fait
async def test_deploy_with_existing_storage(ops_test: OpsTest):
    unit_to_remove, *_ = await ops_test.model.applications[APP_NAME].add_units(count=1)
    await ops_test.model.block_until(lambda: len(ops_test.model.applications[APP_NAME].units) == 4)
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME], status="active", timeout=1000, idle_period=30
    )

    _, stdout, _ = await ops_test.juju("storage", "--format", "json")
    storages = json.loads(stdout)["storage"]

    for data_storage_id, content in storages.items():
        units = content["attachments"]["units"].keys()
        if unit_to_remove.name not in units:
            continue
        break

    await unit_to_remove.remove(destroy_storage=False)
    await ops_test.model.block_until(lambda: len(ops_test.model.applications[APP_NAME].units) == 3)

    add_unit_cmd = f"add-unit {APP_NAME} --model={ops_test.model.info.name} --attach-storage={data_storage_id}".split()
    await ops_test.juju(*add_unit_cmd)
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME], status="active", timeout=1000, idle_period=30
    )
