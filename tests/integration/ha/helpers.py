#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

import json
import logging
import subprocess
from pathlib import Path
from typing import Dict, Optional

import yaml
from kazoo.client import KazooClient
from pytest_operator.plugin import OpsTest
from tenacity import RetryError, Retrying, retry, stop_after_attempt, wait_fixed

from literals import ADMIN_SERVER_PORT

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]
PROCESS = "org.apache.zookeeper.server.quorum.QuorumPeerMain"
SERVICE_DEFAULT_PATH = "/etc/systemd/system/snap.charmed-zookeeper.daemon.service"
PEER = "cluster"


class ProcessError(Exception):
    """Raised when a process fails."""


class ProcessRunningError(Exception):
    """Raised when a process is running when it is not expected to be."""


async def wait_idle(ops_test, apps: list[str] = [APP_NAME], units: int = 3) -> None:
    """Waits for active/idle on specified application.

    Args:
        ops_test: OpsTest
        apps: list of application names to wait for
        units: integer number of units to wait for for each application
    """
    await ops_test.model.wait_for_idle(
        apps=apps, status="active", timeout=3600, idle_period=30, wait_for_exact_units=units
    )
    assert ops_test.model.applications[APP_NAME].status == "active"


@retry(
    wait=wait_fixed(5),
    stop=stop_after_attempt(60),
    reraise=True,
)
def srvr(model_full_name: str, unit: str) -> dict:
    """Calls srvr 4lw command to specified unit.

    Args:
        model_full_name: Current test model
        unit: ZooKeeper unit to issue srvr 4lw command to

    Returns:
        Dict of srvr command output key/values
    """
    response = subprocess.check_output(
        f"JUJU_MODEL={model_full_name} juju ssh {unit} sudo -i 'curl localhost:{ADMIN_SERVER_PORT}/commands/srvr -m 10'",
        stderr=subprocess.PIPE,
        shell=True,
        universal_newlines=True,
    )

    assert response, "ZooKeeper not running"

    return json.loads(response)


def get_hosts_from_status(
    ops_test: OpsTest, app_name: str = APP_NAME, port: int = 2181
) -> list[str]:
    """Manually calls `juju status` and grabs the host addresses from there for a given application.

    Needed as after an ip change (e.g network cut test), OpsTest does not recognise the new address.

    Args:
        ops_test: OpsTest
        app_name: the Juju application to get hosts from
            Defaults to `zookeeper`
        port: the desired ZooKeeper port.
            Defaults to `2181`

    Returns:
        List of ZooKeeper server addresses and ports
    """
    ips = subprocess.check_output(
        f"JUJU_MODEL={ops_test.model_full_name} juju status {app_name} --format json | jq '.. .\"public-address\"? // empty' | xargs | tr -d '\"'",
        shell=True,
        universal_newlines=True,
    ).split()

    return [f"{ip}:{port}" for ip in ips]


def get_hosts(ops_test: OpsTest, app_name: str = APP_NAME, port: int = 2181) -> str:
    """Gets all ZooKeeper server addresses for a given application.

    Args:
        ops_test: OpsTest
        app_name: the Juju application to get hosts from
            Defaults to `zookeeper`
        port: the desired ZooKeeper port.
            Defaults to `2181`

    Returns:
        Comma-delimited string of ZooKeeper server addresses and ports
    """
    return ",".join(
        [
            f"{unit.public_address}:{str(port)}"
            for unit in ops_test.model.applications[app_name].units
        ]
    )


def get_unit_host(
    ops_test: OpsTest, unit_name: str, app_name: str = APP_NAME, port: int = 2181
) -> str:
    """Gets ZooKeeper server address for a given unit name.

    Args:
        ops_test: OpsTest
        unit_name: the Juju unit to get host from
        app_name: the Juju application the unit belongs to
            Defaults to `zookeeper`
        port: the desired ZooKeeper port.
            Defaults to `2181`

    Returns:
        String of ZooKeeper server address and port
    """
    return [
        f"{unit.public_address}:{str(port)}"
        for unit in ops_test.model.applications[app_name].units
        if unit.name == unit_name
    ][0]


def get_unit_name_from_host(ops_test: OpsTest, host: str, app_name: str = APP_NAME) -> str:
    """Gets unit name for a given ZooKeeper server address.

    Args:
        ops_test: OpsTest
        host: the ZooKeeper ip address and port
        app_name: the Juju application the ZooKeeper server belongs to
            Defaults to `zookeeper`

    Returns:
        String of unit name
    """
    return [
        unit.name
        for unit in ops_test.model.applications[app_name].units
        if unit.public_address == host.split(":")[0]
    ][0]


def get_leader_name(ops_test: OpsTest, hosts: str, app_name: str = APP_NAME) -> str:
    """Gets current ZooKeeper quorum leader for a given application.

    Args:
        ops_test: OpsTest
        hosts: comma-delimited list of ZooKeeper ip addresses and ports
        app_name: the Juju application the unit belongs to
            Defaults to `zookeeper`

    Returns:
        String of unit name of the ZooKeeper quorum leader
    """
    for host in hosts.split(","):
        unit_name = get_unit_name_from_host(ops_test, host, app_name)
        try:
            mode = (
                srvr(ops_test.model_full_name, unit_name)
                .get("server_stats", {})
                .get("server_state", "")
            )
        except subprocess.CalledProcessError:  # unit is down
            continue
        if mode == "leader":
            return unit_name

    return ""


async def get_unit_machine_name(ops_test: OpsTest, unit_name: str) -> str:
    """Gets current LXD machine name for a given unit name.

    Args:
        ops_test: OpsTest
        unit_name: the Juju unit name to get from

    Returns:
        String of LXD machine name
            e.g juju-123456-0
    """
    _, raw_hostname, _ = await ops_test.juju("ssh", unit_name, "hostname")
    return raw_hostname.strip()


def disable_lxd_dnsmasq() -> None:
    """Disables DNS resolution in LXD."""
    disable_dnsmasq_cmd = "lxc network set lxdbr0 dns.mode=none"
    subprocess.check_call(disable_dnsmasq_cmd.split())


def enable_lxd_dnsmasq() -> None:
    """Disables DNS resolution in LXD."""
    enable_dnsmasq = "lxc network unset lxdbr0 dns.mode"
    subprocess.check_call(enable_dnsmasq.split())


def cut_unit_network(machine_name: str) -> None:
    """Cuts network access for a given LXD container.

    Args:
        machine_name: the LXD machine name to cut network for
            e.g `juju-123456-0`
    """
    cut_network_command = f"lxc config device add {machine_name} eth0 none"
    subprocess.check_call(cut_network_command.split())


def network_throttle(machine_name: str) -> None:
    """Cut network from a lxc container (without causing the change of the unit IP address).

    Args:
        machine_name: lxc container hostname
    """
    override_command = f"lxc config device override {machine_name} eth0"
    try:
        subprocess.check_call(override_command.split())
    except subprocess.CalledProcessError:
        # Ignore if the interface was already overridden.
        pass
    limit_set_command = f"lxc config device set {machine_name} eth0 limits.egress=0kbit"
    subprocess.check_call(limit_set_command.split())
    limit_set_command = f"lxc config device set {machine_name} eth0 limits.ingress=1kbit"
    subprocess.check_call(limit_set_command.split())
    limit_set_command = f"lxc config device set {machine_name} eth0 limits.priority=10"
    subprocess.check_call(limit_set_command.split())


def network_release(machine_name: str) -> None:
    """Restore network from a lxc container (without causing the change of the unit IP address).

    Args:
        machine_name: lxc container hostname
    """
    limit_set_command = f"lxc config device set {machine_name} eth0 limits.priority="
    subprocess.check_call(limit_set_command.split())
    restore_unit_network(machine_name=machine_name)


def restore_unit_network(machine_name: str) -> None:
    """Restores network access for a given LXD container.

    Args:
        machine_name: the LXD machine name to restore network for
            e.g `juju-123456-0`
    """
    restore_network_command = f"lxc config device remove {machine_name} eth0"
    subprocess.check_call(restore_network_command.split())


def get_storage_id(ops_test, unit_name: str) -> str:
    """Gets the current Juju storage ID for a given unit name.

    Args:
        ops_test: OpsTest
        unit_name: the Juju unit name to get storage ID of

    Returns:
        String of Juju storage ID
    """
    proc = subprocess.check_output(
        f"JUJU_MODEL={ops_test.model_full_name} juju storage --format yaml",
        stderr=subprocess.PIPE,
        shell=True,
        universal_newlines=True,
    )
    response = yaml.safe_load(proc)
    storages = response["storage"]

    for storage_id, storage in storages.items():
        if unit_name in storage["attachments"]["units"]:
            return storage_id

    raise Exception(f"storage id not found for {unit_name}")


async def reuse_storage(ops_test, unit_storage_id: str, app_name: str = APP_NAME) -> None:
    """Removes and adds back a unit, reusing it's storage.

    Args:
        ops_test: OpsTest
        unit_storage_id: the Juju storage id to be reused
        app_name: the Juju application ZooKeeper belongs to
            Defaults to `zookeeper`
    """
    logger.info("Adding new unit with old unit's storage...")
    subprocess.check_output(
        f"JUJU_MODEL={ops_test.model_full_name} juju add-unit {app_name} --attach-storage={unit_storage_id}",
        stderr=subprocess.PIPE,
        shell=True,
        universal_newlines=True,
    )
    await wait_idle(ops_test, apps=[app_name])


async def send_control_signal(
    ops_test: OpsTest, unit_name: str, signal: str, app_name: str = APP_NAME
) -> None:
    """Issues given job control signals to a ZooKeeper process on a given Juju unit.

    Args:
        ops_test: OpsTest
        unit_name: the Juju unit running the ZooKeeper process
        signal: the signal to issue
            e.g `SIGKILL`, `SIGSTOP`, `SIGCONT` etc
        app_name: the ZooKeeper Juju application
    """
    if len(ops_test.model.applications[app_name].units) < 3:
        await ops_test.model.applications[app_name].add_unit(count=1)
        await ops_test.model.wait_for_idle(apps=[app_name], status="active", timeout=1000)

    kill_cmd = f"exec --unit {unit_name} -- pkill --signal {signal} -f {PROCESS}"
    return_code, stdout, stderr = await ops_test.juju(*kill_cmd.split())

    if return_code != 0:
        raise Exception(
            f"Expected kill command {kill_cmd} to succeed instead it failed: {return_code}, {stdout}, {stderr}"
        )


async def get_password(
    ops_test, user: Optional[str] = "super", app_name: Optional[str] = None
) -> str:
    if not app_name:
        app_name = APP_NAME
    secret_data = await get_secret_by_label(ops_test, f"{PEER}.{app_name}.app", app_name)
    return secret_data.get(f"{user}-password")


async def get_secret_by_label(ops_test, label: str, owner: Optional[str] = None) -> Dict[str, str]:
    secrets_meta_raw = await ops_test.juju("list-secrets", "--format", "json")
    secrets_meta = json.loads(secrets_meta_raw[1])

    for secret_id in secrets_meta:
        if owner and not secrets_meta[secret_id]["owner"] == owner:
            continue
        if secrets_meta[secret_id]["label"] == label:
            break

    secret_data_raw = await ops_test.juju("show-secret", "--format", "json", "--reveal", secret_id)
    secret_data = json.loads(secret_data_raw[1])
    return secret_data[secret_id]["content"]["Data"]


def write_key(host: str, password: str, username: str = "super") -> None:
    """Write a key to the ZooKeeper server.

    Args:
        host: host to connect to
        username: user for ZooKeeper
        password: password of the user
    """
    kc = KazooClient(
        hosts=host,
        sasl_options={"mechanism": "DIGEST-MD5", "username": username, "password": password},
    )
    kc.start()
    kc.create_async("/legolas", b"hobbits")
    kc.stop()
    kc.close()


def check_key(host: str, password: str, username: str = "super") -> None:
    """Assert that a key is read on a ZooKeeper server.

    Args:
        host: host to connect to
        username: user for ZooKeeper
        password: password of the user
    """
    kc = KazooClient(
        hosts=host,
        sasl_options={"mechanism": "DIGEST-MD5", "username": username, "password": password},
    )
    kc.start()
    assert kc.exists_async("/legolas")
    value, _ = kc.get_async("/legolas") or None, None

    stored_value = ""
    if value:
        stored_value = value.get()
    if stored_value:
        assert stored_value[0] == b"hobbits"
        return

    raise KeyError


async def is_down(ops_test: OpsTest, unit: str) -> bool:
    """Check if a unit zookeeper process is down."""
    try:
        for attempt in Retrying(stop=stop_after_attempt(10), wait=wait_fixed(5)):
            with attempt:
                search_db_process = f"exec --unit {unit} pgrep -x java"
                _, processes, _ = await ops_test.juju(*search_db_process.split())
                # splitting processes by "\n" results in one or more empty lines, hence we
                # need to process these lines accordingly.
                processes = [proc for proc in processes.split("\n") if len(proc) > 0]
                if len(processes) > 0:
                    raise ProcessRunningError
    except RetryError:
        return False

    return True


async def all_db_processes_down(ops_test: OpsTest) -> bool:
    """Verifies that all units of the charm do not have the DB process running."""
    try:
        for attempt in Retrying(stop=stop_after_attempt(10), wait=wait_fixed(5)):
            with attempt:
                for unit in ops_test.model.applications[APP_NAME].units:
                    search_db_process = f"exec --unit {unit.name} pgrep -x java"
                    _, processes, _ = await ops_test.juju(*search_db_process.split())
                    # splitting processes by "\n" results in one or more empty lines, hence we
                    # need to process these lines accordingly.
                    processes = [proc for proc in processes.split("\n") if len(proc) > 0]
                    if len(processes) > 0:
                        raise ProcessRunningError
    except RetryError:
        return False

    return True


async def patch_restart_delay(ops_test: OpsTest, unit_name: str, delay: int) -> None:
    """Adds a restart delay in the DB service file.

    When the DB service fails it will now wait for `delay` number of seconds.
    """
    add_delay_cmd = (
        f"exec --unit {unit_name} -- "
        f"sudo sed -i -e '/^[Service]/a RestartSec={delay}' "
        f"{SERVICE_DEFAULT_PATH}"
    )
    await ops_test.juju(*add_delay_cmd.split(), check=True)

    # reload the daemon for systemd to reflect changes
    reload_cmd = f"exec --unit {unit_name} -- sudo systemctl daemon-reload"
    await ops_test.juju(*reload_cmd.split(), check=True)


async def remove_restart_delay(ops_test: OpsTest, unit_name: str) -> None:
    """Removes the restart delay from the service."""
    remove_delay_cmd = (
        f"exec --unit {unit_name} -- sed -i -e '/^RestartSec=.*/d' {SERVICE_DEFAULT_PATH}"
    )
    await ops_test.juju(*remove_delay_cmd.split(), check=True)

    # reload the daemon for systemd to reflect changes
    reload_cmd = f"exec --unit {unit_name} -- sudo systemctl daemon-reload"
    await ops_test.juju(*reload_cmd.split(), check=True)


def ping_servers(ops_test: OpsTest) -> bool:
    """Pings srvr to all ZooKeeper units, ensuring they're in quorum.

    Args:
        ops_test: OpsTest

    Returns:
        True if all units are in quorum. Otherwise False
    """
    for unit in ops_test.model.applications[APP_NAME].units:
        srvr_response = srvr(ops_test.model_full_name, unit.name)

        if srvr_response.get("error", None):
            return False

        mode = srvr_response.get("server_stats", {}).get("server_state", "")
        if mode not in ["leader", "follower"]:
            return False

    return True
