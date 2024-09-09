#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Implementation of WorkloadBase for running on VMs."""
import json
import logging
import os
import secrets
import shutil
import string
import subprocess
from subprocess import CalledProcessError

from charms.operator_libs_linux.v1 import snap
from ops.pebble import ExecError
from tenacity import retry, retry_if_result, stop_after_attempt, wait_fixed
from typing_extensions import override

from core.workload import WorkloadBase
from literals import ADMIN_SERVER_PORT, CHARMED_ZOOKEEPER_SNAP_REVISION

logger = logging.getLogger(__name__)


class ZKWorkload(WorkloadBase):
    """Implementation of WorkloadBase for running on VMs."""

    SNAP_NAME = "charmed-zookeeper"
    SNAP_SERVICE = "daemon"

    def __init__(self):
        self.zookeeper = snap.SnapCache()[self.SNAP_NAME]

    @override
    def start(self) -> None:
        try:
            self.zookeeper.start(services=[self.SNAP_SERVICE])
        except snap.SnapError as e:
            logger.exception(str(e))

    @override
    def stop(self) -> None:
        try:
            self.zookeeper.stop(services=[self.SNAP_SERVICE])
        except snap.SnapError as e:
            logger.exception(str(e))

    @override
    def restart(self) -> None:
        try:
            self.zookeeper.restart(services=[self.SNAP_SERVICE])
        except snap.SnapError as e:
            logger.exception(str(e))

    @override
    def read(self, path: str) -> list[str]:
        if not os.path.exists(path):
            return []
        else:
            with open(path) as f:
                content = f.read().split("\n")

        return content

    @override
    def write(self, content: str, path: str) -> None:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        shutil.chown(os.path.dirname(path), user="snap_daemon", group="root")

        with open(path, "w") as f:
            f.write(content)

        shutil.chown(path, user="snap_daemon", group="root")

    @override
    def exec(self, command: list[str], working_dir: str | None = None) -> str:
        return subprocess.check_output(
            command,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            cwd=working_dir,
        )

    @property
    @override
    @retry(
        wait=wait_fixed(1),
        stop=stop_after_attempt(5),
        retry=retry_if_result(lambda result: result is False),
        retry_error_callback=lambda _: False,
    )
    def alive(self) -> bool:
        try:
            return bool(self.zookeeper.services[self.SNAP_SERVICE]["active"])
        except KeyError:
            return False

    @property
    @override
    @retry(
        wait=wait_fixed(1),
        stop=stop_after_attempt(5),
        retry=retry_if_result(lambda result: result is False),
        retry_error_callback=lambda _: False,
    )
    def healthy(self) -> bool:
        """Flag to check if the unit service is reachable and serving requests."""
        if not self.alive:
            return False

        try:
            response = json.loads(
                self.exec(["curl", f"localhost:{ADMIN_SERVER_PORT}/commands/ruok", "-m", "10"])
            )

        except (ExecError, CalledProcessError, json.JSONDecodeError):
            return False

        if response.get("error", None):
            return False

        return True

    # --- ZK Specific ---

    def install(self) -> bool:
        """Loads the ZooKeeper snap from LP, returning a StatusBase for the Charm to set.

        Returns:
            True if successfully installed. False otherwise.
        """
        try:
            cache = snap.SnapCache()
            zookeeper = cache[self.SNAP_NAME]

            zookeeper.ensure(snap.SnapState.Present, revision=CHARMED_ZOOKEEPER_SNAP_REVISION)

            self.zookeeper = zookeeper
            self.zookeeper.hold()

            return True
        except snap.SnapError as e:
            logger.error(str(e))
            return False

    def generate_password(self) -> str:
        """Creates randomized string for use as app passwords.

        Returns:
            String of 32 randomized letter+digit characters
        """
        return "".join([secrets.choice(string.ascii_letters + string.digits) for _ in range(32)])

    @override
    def get_version(self) -> str:

        if not self.healthy:
            return ""

        try:
            response = json.loads(
                self.exec(["curl", f"localhost:{ADMIN_SERVER_PORT}/commands/srvr", "-m", "10"])
            )

        except (ExecError, CalledProcessError, json.JSONDecodeError):
            return ""

        if not (full_version := response.get("version", "")):
            return full_version
        else:
            return full_version.split("-")[0]
