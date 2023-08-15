from pytest_operator.plugin import OpsTest
import logging
import yaml
from pathlib import Path
import re
import subprocess

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]

PROCESS = "org.apache.zookeeper.server.quorum.QuorumPeerMain"


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

async def get_hosts(ops_test: OpsTest, app_name: str = APP_NAME, port: int = 2181) -> str:
    return ",".join([f"{unit.public_address}:{str(port)}" for unit in ops_test.model.applications[app_name].units])

async def get_unit_host(ops_test: OpsTest, unit_name: str, app_name: str = APP_NAME, port: int = 2181) -> str:
    return [f"{unit.public_address}:{str(port)}" for unit in ops_test.model.applications[app_name].units if unit.name == unit_name][0]
    

async def get_leader_name(ops_test: OpsTest, app_name: str = APP_NAME) -> str:
    for unit in ops_test.model.applications[app_name].units:
        host = unit.public_address
        try:
            mode = srvr(host)["Mode"]
        except subprocess.CalledProcessError:  # unit is down
            continue
        if mode == "leader":
            return unit.name

    raise Exception("Leader not found")

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

async def kill_unit_process(ops_test: OpsTest, unit_name: str, kill_code: str, app_name: str = APP_NAME) -> None:
    if len(ops_test.model.applications[app_name].units) < 3:
        await ops_test.model.applications[app_name].add_unit(count=1)
        await ops_test.model.wait_for_idle(apps=[app_name], status="active", timeout=1000)

    kill_cmd = f"run --unit {unit_name} -- pkill --signal {kill_code} -f {PROCESS}"
    return_code, _, _ = await ops_test.juju(*kill_cmd.split())

    if return_code != 0:
        raise Exception(
            f"Expected kill command {kill_cmd} to succeed instead it failed: {return_code}"
        )


