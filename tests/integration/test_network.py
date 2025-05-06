#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

import asyncio
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


TEST_APP = "app"
DEFAULT_NETWORK_CIDR = "10.0.0.0/8"
DEFAULT_SPACE = "alpha"
DEFAULT_NETWORK_NAME = "lxdbr0"
OTHER_NETWORK_CIDR = "172.30.0.1/24"
OTHER_SPACE = "test"
LXD_NETWORK_OPTIONS = [
    "ipv4.nat=true",
    "ipv6.address=none",
    "dns.mode=none",
]


@pytest.fixture(scope="module")
def other_network() -> Generator[str, None, None]:
    """Creates and/or returns a LXD network with `OTHER_NETWORK_CIDR` range of IPv4 addresses."""
    # We should set `dns.mode=none` for all LXD networks in this scenario.
    # This is required to avoid DNS name conflict issues
    # due to the fact that multiple NICs are connected to the same bridge network:
    # https://discuss.linuxcontainers.org/t/error-failed-start-validation-for-device-enp3s0f0-instance-dns-name-net17-nicole-munoz-marketing-already-used-on-network/15586/9?page=2
    subprocess.check_output(
        f"sudo lxc network set {DEFAULT_NETWORK_NAME} dns.mode=none",
        shell=True,
        stderr=subprocess.PIPE,
    )
    raw = subprocess.check_output(
        "sudo lxc network list --format json", shell=True, stderr=subprocess.PIPE
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
        f"sudo lxc network create {name} ipv4.address={OTHER_NETWORK_CIDR} {' '.join(LXD_NETWORK_OPTIONS)}",
        shell=True,
        stderr=subprocess.PIPE,
    )
    yield name

    logger.info(f"Cleaning up {name} network...")
    try:
        subprocess.check_output(
            f"sudo lxc network delete --force-local {name}",
            shell=True,
            stderr=subprocess.PIPE,
        )
    except Exception as e:
        logger.error(f"Network cleanup failed, details: {e}")
        logger.info(f"Try deleting the network manually using `lxc network delete {name}`")


@pytest.mark.abort_on_fail
async def test_add_space(ops_test: OpsTest, other_network: str) -> None:
    """Adds `OTHER_SPACE` juju space."""
    # reload subnets and move `OTHER_NETWORK_CIDR` subnet to the `OTHER_SPACE`
    await ops_test.juju("reload-spaces")
    await ops_test.juju("add-space", OTHER_SPACE)
    await ops_test.juju("move-to-space", OTHER_SPACE, OTHER_NETWORK_CIDR)


@pytest.mark.abort_on_fail
@pytest.mark.skip_if_deployed
async def test_deploy_active(ops_test: OpsTest, zk_charm) -> None:
    """Deploys a cluster of ZK with 3 units and a test app, waits for `active|idle`."""
    app_charm = await ops_test.build_charm("tests/integration/app-charm")

    await asyncio.gather(
        ops_test.model.deploy(
            zk_charm, application_name=APP_NAME, num_units=3, bind={"zookeeper": OTHER_SPACE}
        ),
        ops_test.model.deploy(
            app_charm,
            application_name=TEST_APP,
            num_units=1,
            bind={"cluster": DEFAULT_SPACE, "zookeeper": OTHER_SPACE},
        ),
    )
    await ops_test.model.add_relation(APP_NAME, TEST_APP)

    async with ops_test.fast_forward():
        await ops_test.model.wait_for_idle(
            apps=[APP_NAME, TEST_APP], timeout=1800, idle_period=30, status="active"
        )


@pytest.mark.abort_on_fail
async def test_endpoints_are_set_based_on_network_binds(ops_test: OpsTest) -> None:
    """Checks whether the `ip` field in the unit databag is correctly set based on juju binds."""
    relations = [
        rel for rel in ops_test.model.applications[TEST_APP].relations if "zookeeper" in rel.key
    ]
    if not relations:
        raise Exception(f"No client relations found for {TEST_APP}")

    client_relation = next(iter(relations))

    # We should have set `ip` from the `DEFAULT_NETWORK` and `ip-{relation.id}` from `OTHER_NETWORK`
    for unit in ops_test.model.applications[APP_NAME].units:
        unit_data = get_relation_data(ops_test.model_full_name, unit.name, PEER, key="local-unit")
        ip = unit_data.get("data", {}).get("ip", "")
        ip_client = unit_data.get("data", {}).get(f"ip-{client_relation.id}", "")
        logging.info(f"{unit.name}: {ip}, client: {ip_client}")
        assert ip.startswith(DEFAULT_NETWORK_CIDR.split(".")[0])
        assert ip_client.startswith(OTHER_NETWORK_CIDR.split(".")[0])

    # now bind to the new space
    await ops_test.juju("bind", APP_NAME, OTHER_SPACE)

    async with ops_test.fast_forward():
        await ops_test.model.wait_for_idle(
            apps=[APP_NAME], status="active", timeout=1000, idle_period=30
        )

    # Now we should set `ip` from the `OTHER_NETWORK`
    for unit in ops_test.model.applications[APP_NAME].units:
        unit_data = get_relation_data(ops_test.model_full_name, unit.name, PEER, key="local-unit")
        ip = unit_data.get("data", {}).get("ip", "")
        ip_client = unit_data.get("data", {}).get(f"ip-{client_relation.id}", "")
        logging.info(f"{unit.name}: {ip}, client: {ip_client}")
        assert ip.startswith(OTHER_NETWORK_CIDR.split(".")[0])
        assert ip_client.startswith(OTHER_NETWORK_CIDR.split(".")[0])
