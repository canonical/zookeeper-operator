#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

import asyncio
import logging
import socket

import pytest
import pytest_microceph
from pytest_operator.plugin import OpsTest

from .helpers import APP_NAME

logger = logging.getLogger(__name__)

S3_INTEGRATOR = "s3-integrator"
S3_CHANNEL = "latest/stable"


@pytest.fixture(scope="session")
def cloud_credentials(microceph: pytest_microceph.ConnectionInformation) -> dict[str, str]:
    """Read cloud credentials."""
    return {
        "access-key": microceph.access_key_id,
        "secret-key": microceph.secret_access_key,
    }


@pytest.fixture(scope="session")
def cloud_configs(microceph: pytest_microceph.ConnectionInformation):
    host_ip = socket.gethostbyname(socket.gethostname())
    return {
        "endpoint": f"http://{host_ip}",
        "bucket": microceph.bucket,
        "path": "mysql",
        "region": "",
    }


@pytest.mark.abort_on_fail
async def test_deploy_active(ops_test: OpsTest, zk_charm, cloud_configs, cloud_credentials):
    await asyncio.gather(
        ops_test.model.deploy(
            zk_charm,
            application_name=APP_NAME,
            num_units=3,
        ),
        ops_test.model.deploy(S3_INTEGRATOR, channel=S3_CHANNEL),
    )

    await ops_test.models.wait_for_idle(apps=[S3_INTEGRATOR], status="blocked", timeout=1000)

    logger.info("Syncing credentials")

    await ops_test.model.applications[S3_INTEGRATOR].set_config(cloud_configs)

    for unit in ops_test.model.applications[S3_INTEGRATOR].units:
        if await unit.is_leader_from_status():
            leader_unit = unit

    sync_action = await leader_unit.run_action(
        "sync-s3-credentials",
        **cloud_credentials,
    )
    await sync_action.wait()

    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, S3_INTEGRATOR],
        status="active",
        timeout=1000,
    )
