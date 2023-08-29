import logging
import re
import subprocess
from pathlib import Path

import yaml
from pytest_operator.plugin import OpsTest
from tenacity import retry, stop_after_attempt, wait_fixed

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]

PROCESS = "org.apache.zookeeper.server.quorum.QuorumPeerMain"


async def wait_idle(ops_test, apps: list[str] = [APP_NAME], units: int = 3):
    await ops_test.model.wait_for_idle(
        apps=apps, status="active", timeout=3600, idle_period=30, wait_for_exact_units=units
    )
    assert ops_test.model.applications[APP_NAME].status == "active"


@retry(
    wait=wait_fixed(5),
    stop=stop_after_attempt(3),
    reraise=True,
)
def srvr(host: str) -> dict:
    response = subprocess.check_output(
        f"echo srvr | nc {host} 2181", stderr=subprocess.PIPE, shell=True, universal_newlines=True
    )

    assert response, "ZooKeeper not running"

    result = {}
    for item in response.splitlines():
        k = re.split(": ", item)[0]
        v = re.split(": ", item)[1]
        result[k] = v

    return result


def get_hosts_from_status(ops_test: OpsTest, app_name: str = APP_NAME, port: int = 2181):
    ips = subprocess.check_output(
        f"JUJU_MODEL={ops_test.model_full_name} juju status {app_name} --format json | jq '.. .\"public-address\"? // empty' | xargs | tr -d '\"'",
        shell=True,
        universal_newlines=True,
    ).split()

    return [f"{ip}:{port}" for ip in ips]


def get_hosts(ops_test: OpsTest, app_name: str = APP_NAME, port: int = 2181) -> str:
    return ",".join(
        [
            f"{unit.public_address}:{str(port)}"
            for unit in ops_test.model.applications[app_name].units
        ]
    )


def get_unit_host(
    ops_test: OpsTest, unit_name: str, app_name: str = APP_NAME, port: int = 2181
) -> str:
    return [
        f"{unit.public_address}:{str(port)}"
        for unit in ops_test.model.applications[app_name].units
        if unit.name == unit_name
    ][0]


def get_unit_name_from_host(ops_test: OpsTest, host: str, app_name: str = APP_NAME) -> str:
    return [
        unit.name
        for unit in ops_test.model.applications[app_name].units
        if unit.public_address == host.split(":")[0]
    ][0]


def get_leader_name(ops_test: OpsTest, hosts: str, app_name: str = APP_NAME) -> str:
    for host in hosts.split(","):
        try:
            mode = srvr(host.split(":")[0])["Mode"]
        except subprocess.CalledProcessError:  # unit is down
            continue
        if mode == "leader":
            leader_name = get_unit_name_from_host(ops_test, host, app_name)
            return leader_name

    return ""


async def get_unit_machine_name(ops_test: OpsTest, unit_name: str) -> str:
    _, raw_hostname, _ = await ops_test.juju("ssh", unit_name, "hostname")
    return raw_hostname.strip()


def cut_unit_network(machine_name: str) -> None:
    cut_network_command = f"lxc config device add {machine_name} eth0 none"
    subprocess.check_call(cut_network_command.split())


def restore_unit_network(machine_name: str) -> None:
    restore_network_command = f"lxc config device remove {machine_name} eth0"
    subprocess.check_call(restore_network_command.split())


def app_is_rootfs(ops_test) -> bool:
    proc = subprocess.check_output(
        f"JUJU_MODEL={ops_test.model_full_name} juju storage --format yaml",
        stderr=subprocess.PIPE,
        shell=True,
        universal_newlines=True,
    )
    response = yaml.safe_load(proc)
    storages = response["filesystems"]

    for fs in storages.values():
        if fs["pool"] == "rootfs":
            return True

    return False


def get_storage_id(ops_test, unit_name: str) -> str:
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


async def reuse_storage(ops_test, app_name: str = APP_NAME) -> str:
    """Removes and adds back a unit, reusing it's storage.

    Returns:
        String of new unit host
    """
    logger.info("Scaling down unit...")
    unit_to_remove = ops_test.model.applications[APP_NAME].units[0]
    unit_storage_id = get_storage_id(ops_test, unit_name=unit_to_remove.name)
    await ops_test.model.applications[APP_NAME].destroy_units(unit_to_remove.name)
    await wait_idle(ops_test, units=2)

    old_units = ops_test.model.applications[app_name].units

    logger.info("Adding new unit with old unit's storage...")
    subprocess.check_output(
        f"JUJU_MODEL={ops_test.model_full_name} juju add-unit {app_name} --attach-storage={unit_storage_id}",
        stderr=subprocess.PIPE,
        shell=True,
        universal_newlines=True,
    )
    await wait_idle(ops_test, apps=[app_name])

    new_units = ops_test.model.applications[app_name].units
    added_unit = list(set(new_units) - set(old_units))[0]

    logger.info("Verifying storage re-use...")
    assert get_storage_id(ops_test, unit_name=added_unit.name) == unit_storage_id

    return get_unit_host(ops_test, added_unit.name)


def get_super_password(ops_test: OpsTest, app_name: str = APP_NAME) -> str:
    for unit in ops_test.model.applications[app_name].units:
        show_unit = subprocess.check_output(
            f"JUJU_MODEL={ops_test.model_full_name} juju show-unit {unit.name}",
            stderr=subprocess.PIPE,
            shell=True,
            universal_newlines=True,
        )
        response = yaml.safe_load(show_unit)
        relations_info = response[f"{unit.name}"]["relation-info"]

        password = None
        for info in relations_info:
            if info["endpoint"] == "cluster":
                password = info["application-data"]["super-password"]
                return password

        if not password:
            raise Exception("no relations found")


async def kill_unit_process(
    ops_test: OpsTest, unit_name: str, kill_code: str, app_name: str = APP_NAME
) -> None:
    if len(ops_test.model.applications[app_name].units) < 3:
        await ops_test.model.applications[app_name].add_unit(count=1)
        await ops_test.model.wait_for_idle(apps=[app_name], status="active", timeout=1000)

    kill_cmd = f"run --unit {unit_name} -- pkill --signal {kill_code} -f {PROCESS}"
    return_code, _, _ = await ops_test.juju(*kill_cmd.split())

    if return_code != 0:
        raise Exception(
            f"Expected kill command {kill_cmd} to succeed instead it failed: {return_code}"
        )
