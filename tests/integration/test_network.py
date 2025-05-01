#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

import json
import logging
import secrets
import subprocess
from typing import Generator

import pytest
from pytest_operator.plugin import OpsTest

from literals import PEER

from .helpers import APP_NAME, get_relation_data

logger = logging.getLogger(__name__)


DEFAULT_NETWORK_CIDR = "10.0.0.0/8"
DEFAULT_SPACE = "alpha"
OTHER_NETWORK_CIDR = "172.30.0.1/24"
OTHER_SPACE = "test"
OTHER_ETH_DEVICE = "eth1"


@pytest.fixture(scope="module")
def other_network() -> Generator[str, None, None]:
    """Creates and/or returns a LXD network with `OTHER_NETWORK_CIDR` range of IPv4 addresses."""
    raw = subprocess.check_output(
        "lxc network list --format json", shell=True, stderr=subprocess.PIPE
    )
    networks_json = json.loads(raw)
    # Check if a network with the provided CIDR already exists:
    for network in networks_json:
        if network.get("config", {}).get("ipv4.address") == OTHER_NETWORK_CIDR:
            logger.info(
                f'Exisiting network {network["name"]} found with CIDR: {OTHER_NETWORK_CIDR}'
            )
            yield network["name"]
            return

    name = f"net-{secrets.token_hex(4)}"

    subprocess.check_output(
        f"lxc network create {name} ipv4.address={OTHER_NETWORK_CIDR} ipv6.address=none",
        shell=True,
        stderr=subprocess.PIPE,
    )
    yield name

    logger.info(f"Cleaning up {name} network...")
    try:
        subprocess.check_output(
            f"lxc network delete --force-local {name}",
            shell=True,
            stderr=subprocess.PIPE,
        )
    except Exception as e:
        logger.error(f"Network cleanup failed, details: {e}")
        logger.info(f"Try deleting the network manually using `lxc network delete {name}`")


@pytest.mark.abort_on_fail
@pytest.mark.skip_if_deployed
async def test_deploy_active(ops_test: OpsTest, zk_charm) -> None:
    """Deploys a cluster of ZK with 3 units and waits for `active|idle`."""
    await ops_test.model.deploy(
        zk_charm,
        application_name=APP_NAME,
        num_units=3,
    )

    async with ops_test.fast_forward():
        await ops_test.model.wait_for_idle(
            apps=[APP_NAME], status="active", timeout=1800, idle_period=30
        )

    assert ops_test.model.applications[APP_NAME].status == "active"


@pytest.mark.abort_on_fail
async def test_add_space(ops_test: OpsTest, other_network: str) -> None:
    """Adds `OTHER_SPACE` juju space and attaches the current units to the new network."""
    for machine in ops_test.model.machines.values():
        # attach the other network to the lxd machines.
        logging.info(f"Attaching {other_network} to {machine.hostname}...")
        ret, _, _ = await ops_test.run(
            "lxc", "network", "attach", other_network, f"{machine.hostname}"
        )
        assert not ret

    # reload subnets and move `OTHER_NETWORK_CIDR` subnet to the `OTHER_SPACE`
    await ops_test.juju("reload-spaces")
    await ops_test.juju("add-space", OTHER_SPACE)
    await ops_test.juju("move-to-space", OTHER_SPACE, OTHER_NETWORK_CIDR)

    for unit in ops_test.model.applications[APP_NAME].units:
        # set up the network interface on the machine.
        logging.info(f"Setting up {OTHER_ETH_DEVICE} on {unit.name}...")
        ret_1, _, _ = await ops_test.juju(
            "ssh", unit.name, f"sudo ip link set dev {OTHER_ETH_DEVICE} up"
        )
        ret_2, _, _ = await ops_test.juju("ssh", unit.name, f"sudo dhclient -v {OTHER_ETH_DEVICE}")
        assert not any([ret_1, ret_2])

    # force bind to new space
    await ops_test.juju("bind", "--force", APP_NAME, OTHER_SPACE)

    async with ops_test.fast_forward():
        await ops_test.model.wait_for_idle(
            apps=[APP_NAME], status="active", timeout=1000, idle_period=30
        )


@pytest.mark.abort_on_fail
async def test_endpoints_are_set_based_on_network_binds(ops_test: OpsTest) -> None:
    """Checks whether the `ip` field in the unit databag is correctly set based on juju binds."""
    # bind back to default space
    await ops_test.juju("bind", APP_NAME, DEFAULT_SPACE)

    async with ops_test.fast_forward():
        await ops_test.model.wait_for_idle(
            apps=[APP_NAME], status="active", timeout=1000, idle_period=30
        )

    # We should set `ip` from the `DEFAULT_NETWORK`
    for unit in ops_test.model.applications[APP_NAME].units:
        unit_data = get_relation_data(ops_test.model_full_name, unit.name, PEER, key="local-unit")
        ip = unit_data.get("data", {}).get("ip", "")
        logging.info(f"{unit.name}: {ip}")
        assert ip.startswith(DEFAULT_NETWORK_CIDR.split(".")[0])

    # now bind to the new space
    await ops_test.juju("bind", "--force", APP_NAME, OTHER_SPACE)

    async with ops_test.fast_forward():
        await ops_test.model.wait_for_idle(
            apps=[APP_NAME], status="active", timeout=1000, idle_period=30
        )

    # Now we should set `ip` from the `OTHER_NETWORK`
    for unit in ops_test.model.applications[APP_NAME].units:
        unit_data = get_relation_data(ops_test.model_full_name, unit.name, PEER, key="local-unit")
        ip = unit_data.get("data", {}).get("ip", "")
        logging.info(f"{unit.name}: {ip}")
        assert ip.startswith(OTHER_NETWORK_CIDR.split(".")[0])
