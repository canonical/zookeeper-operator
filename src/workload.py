#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Implementation of WorkloadBase for running on VMs."""
import logging
import os
import re
import secrets
import shutil
import string
import subprocess
from subprocess import CalledProcessError

from charms.operator_libs_linux.v0 import apt
from charms.operator_libs_linux.v1 import snap
from ops.pebble import ExecError
from tenacity import retry
from tenacity.retry import retry_if_not_result
from tenacity.stop import stop_after_attempt
from tenacity.wait import wait_fixed
from typing_extensions import override

from core.workload import WorkloadBase
from literals import CHARMED_ZOOKEEPER_SNAP_REVISION, CLIENT_PORT

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

    @property
    @override
    @retry(
        wait=wait_fixed(2),
        stop=stop_after_attempt(5),
        retry=retry_if_not_result(lambda result: True if result else False),
        retry_error_callback=(lambda state: state.outcome.result()),  # type: ignore
    )
    def healthy(self) -> bool:
        """Flag to check if the unit service is reachable and serving requests."""
        # netcat isn't a default utility, so can't guarantee it's on the charm containers
        # this ugly hack avoids needing netcat
        bash_netcat = (
            f"echo '4lw' | (exec 3<>/dev/tcp/localhost/{CLIENT_PORT}; cat >&3; cat <&3; exec 3<&-)"
        )
        ruok = [bash_netcat.replace("4lw", "ruok")]
        srvr = [bash_netcat.replace("4lw", "srvr")]

        # timeout needed as it can sometimes hang forever if there's a problem
        # for example when the endpoint is unreachable
        timeout = ["timeout", "10s", "bash", "-c"]

        try:
            ruok_response = self.exec(command=timeout + ruok)
            if not ruok_response or "imok" not in ruok_response:
                return False

            srvr_response = self.exec(command=timeout + srvr)
            if not srvr_response or "not currently serving requests" in srvr_response:
                return False
        except (ExecError, CalledProcessError):
            return False

        return True

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

    @override
    def get_version(self) -> str:

        if not self.healthy:
            return ""

        stat = [
            f"echo 'stat' | (exec 3<>/dev/tcp/localhost/{CLIENT_PORT}; cat >&3; cat <&3; exec 3<&-)"
        ]

        # timeout needed as it can sometimes hang forever if there's a problem
        # for example when the endpoint is unreachable
        timeout = ["timeout", "10s", "bash", "-c"]

        try:
            stat_response = self.exec(command=timeout + stat)
            if not stat_response:
                return ""

            matcher = re.search(r"(?P<version>\d\.\d\.\d)", stat_response)
            version = matcher.group("version") if matcher else ""

        except (ExecError, CalledProcessError):
            return ""

        return version
