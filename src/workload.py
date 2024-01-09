#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Implementation of WorkloadBase for running on VMs."""
import logging
import os
import secrets
import shutil
import string
import subprocess

from charms.operator_libs_linux.v0 import apt
from charms.operator_libs_linux.v1 import snap
from tenacity import retry
from tenacity.retry import retry_if_not_result
from tenacity.stop import stop_after_attempt
from tenacity.wait import wait_fixed
from typing_extensions import override

from core.workload import WorkloadBase
from literals import CHARMED_ZOOKEEPER_SNAP_REVISION

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

    @override
    @retry(
        wait=wait_fixed(1),
        stop=stop_after_attempt(5),
        retry_error_callback=lambda state: state.outcome.result(),  # type: ignore
        retry=retry_if_not_result(lambda result: True if result else False),
    )
    def alive(self) -> bool:
        try:
            return bool(self.zookeeper.services[self.SNAP_SERVICE]["active"])
        except KeyError:
            return False

    @override
    def healthy(self) -> bool:
        return self.alive()

    # --- ZK Specific ---

    def install(self) -> bool:
        """Loads the ZooKeeper snap from LP, returning a StatusBase for the Charm to set.

        Returns:
            True if successfully installed. False otherwise.
        """
        try:
            apt.update()
            apt.add_package(["snapd"])
            cache = snap.SnapCache()
            zookeeper = cache[self.SNAP_NAME]

            zookeeper.ensure(snap.SnapState.Present, revision=CHARMED_ZOOKEEPER_SNAP_REVISION)

            self.zookeeper = zookeeper
            self.zookeeper.hold()

            return True
        except (snap.SnapError, apt.PackageNotFoundError) as e:
            logger.error(str(e))
            return False

    def generate_password(self) -> str:
        """Creates randomized string for use as app passwords.

        Returns:
            String of 32 randomized letter+digit characters
        """
        return "".join([secrets.choice(string.ascii_letters + string.digits) for _ in range(32)])