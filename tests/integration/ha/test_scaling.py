#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

import logging

import helpers
import pytest
from pytest_operator.plugin import OpsTest

logger = logging.getLogger(__name__)


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
    await helpers.wait_idle(ops_test, units=3)


@pytest.mark.abort_on_fail
async def test_simple_scale_up(ops_test: OpsTest):
    await ops_test.model.applications[helpers.APP_NAME].add_units(count=3)
    await helpers.wait_idle(ops_test, units=6)


@pytest.mark.abort_on_fail
async def test_simple_scale_down(ops_test: OpsTest):
    await ops_test.model.applications[helpers.APP_NAME].destroy_units(
        f"{helpers.APP_NAME}/5", f"{helpers.APP_NAME}/4", f"{helpers.APP_NAME}/3"
    )
    await helpers.wait_idle(ops_test, units=3)

    # scaling back up
    await ops_test.model.applications[helpers.APP_NAME].add_units(count=3)
    await helpers.wait_idle(ops_test, units=6)


@pytest.mark.abort_on_fail
async def test_complex_scale_down(ops_test: OpsTest):
    hosts = helpers.get_hosts(ops_test)

    quorum_leader_name = helpers.get_leader_name(ops_test, hosts)
    charm_leader_name = None
    other_unit_name = None

    for unit in ops_test.model.applications[helpers.APP_NAME].units:
        if await unit.is_leader_from_status():
            charm_leader_name = unit.name

        if unit.name not in [charm_leader_name, quorum_leader_name]:
            other_unit_name = unit.name

    await ops_test.model.applications[helpers.APP_NAME].destroy_units(
        quorum_leader_name, charm_leader_name, other_unit_name
    )
    await helpers.wait_idle(ops_test, units=3)
