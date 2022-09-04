#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

import asyncio
import logging
import time
from pathlib import Path

import pytest
import yaml
from pytest_operator.plugin import OpsTest

from tests.integration.helpers import (
    check_key,
    check_properties,
    get_password,
    ping_servers,
    restart_unit,
    write_key,
)

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]


@pytest.mark.abort_on_fail
async def test_deploy_ssl_quorum(ops_test: OpsTest):
    charm = await ops_test.build_charm(".")
    await asyncio.gather(
        ops_test.model.deploy(charm, application_name=APP_NAME, num_units=3),
        ops_test.model.deploy(
            "tls-certificates-operator",
            application_name="tls-certificates-operator",
            num_units=1,
            config={"generate-self-signed-certificates": "true", "ca-common-name": "zookeeper"},
        ),
    )
    await ops_test.model.block_until(lambda: len(ops_test.model.applications[APP_NAME].units) == 3)
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, "tls-certificates-operator"], status="active", timeout=1000
    )
    assert ops_test.model.applications[APP_NAME, "tls-certificates-operator"].status == "active"
    await ops_test.model.add_relation(APP_NAME, "tls-certificates-operator")
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, "tls-certificates-operator"], status="active", timeout=1000
    )
    assert ops_test.model.applications[APP_NAME, "tls-certificates-operator"].status == "active"

    assert ping_servers(ops_test)

    for unit in ops_test.model.applications[APP_NAME].units:
        assert "sslQuorum=true" in check_properties(
            model_full_name=ops_test.model_full_name, unit=unit.name
        )


@pytest.mark.abort_on_fail
async def test_remove_tls_provider(ops_test: OpsTest):
    await ops_test.model.remove_application("tls-certificates-operator", block_until_done=True)
    await ops_test.model.wait_for_idle(apps=[APP_NAME])
    assert ops_test.model.applications[APP_NAME].status == "active"

    assert ping_servers(ops_test)

    for unit in ops_test.model.applications[APP_NAME].units:
        assert "sslQuorum=true" not in check_properties(
            model_full_name=ops_test.model_full_name, unit=unit.name
        )


@pytest.mark.abort_on_fail
async def test_add_tls_provider(ops_test: OpsTest):
    await asyncio.gather(
        ops_test.model.deploy(
            "tls-certificates-operator",
            application_name="tls-certificates-operator",
            num_units=1,
            config={"generate-self-signed-certificates": "true", "ca-common-name": "zookeeper"},
        ),
    )
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, "tls-certificates-operator"], status="active", timeout=1000
    )
    assert ops_test.model.applications[APP_NAME, "tls-certificates-operator"].status == "active"
    await ops_test.model.add_relation(APP_NAME, "tls-certificates-operator")
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, "tls-certificates-operator"], status="active", timeout=1000
    )
    assert ops_test.model.applications[APP_NAME, "tls-certificates-operator"].status == "active"

    assert ping_servers(ops_test)

    for unit in ops_test.model.applications[APP_NAME].units:
        assert "sslQuorum=true" in check_properties(
            model_full_name=ops_test.model_full_name, unit=unit.name
        )
