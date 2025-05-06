#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

import json
import logging
import re
import tempfile
from pathlib import Path
from subprocess import PIPE, CalledProcessError, check_output
from typing import Dict, List, Literal

import yaml
from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError
from pytest_operator.plugin import OpsTest

from core.workload import ZKPaths
from literals import ADMIN_SERVER_PORT

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]
PEER = "cluster"

logger = logging.getLogger(__name__)


def application_active(ops_test: OpsTest, expected_units: int) -> bool:
    units = ops_test.model.applications[APP_NAME].units

    if len(units) != expected_units:
        return False

    for unit in units:
        if unit.workload_status != "active":
            return False

    return True


async def get_password(ops_test) -> str:
    secret_data = await get_secret_by_label(ops_test, f"{PEER}.{APP_NAME}.app")
    return secret_data.get("super-password")


async def get_secret_by_label(ops_test, label: str, owner: str = APP_NAME) -> Dict[str, str]:
    secrets_meta_raw = await ops_test.juju("list-secrets", "--format", "json")
    secrets_meta = json.loads(secrets_meta_raw[1])

    for secret_id in secrets_meta:
        if secrets_meta[secret_id]["label"] == label and secrets_meta[secret_id]["owner"] == owner:
            break

    secret_data_raw = await ops_test.juju("show-secret", "--format", "json", "--reveal", secret_id)
    secret_data = json.loads(secret_data_raw[1])
    return secret_data[secret_id]["content"]["Data"]


async def get_user_password(ops_test: OpsTest, user: str, num_unit=0) -> str:
    """Use the charm action to retrieve the password for user.

    Return:
        String with the password stored on the peer relation databag.
    """
    action = await ops_test.model.units.get(f"{APP_NAME}/{num_unit}").run_action(
        f"get-{user}-password"
    )
    password = await action.wait()
    return password.results[f"{user}-password"]


async def set_password(ops_test: OpsTest, username="super", password=None, num_unit=0) -> str:
    """Use the charm action to start a password rotation."""
    params = {"username": username}
    if password:
        params["password"] = password

    action = await ops_test.model.units.get(f"{APP_NAME}/{num_unit}").run_action(
        "set-password", **params
    )
    password = await action.wait()
    return password.results


def restart_unit(model_full_name: str, unit: str) -> None:
    # getting juju id
    machine_id = check_output(
        f"JUJU_MODEL={model_full_name} juju status | grep {unit} | awk '{{ print $4 }}'",
        stderr=PIPE,
        shell=True,
        universal_newlines=True,
    )

    # getting lxc machine name
    machine_name = check_output(
        f"JUJU_MODEL={model_full_name} juju machines | grep awk '{{print $4}}' | grep -e '-{machine_id}'| head -1",
        stderr=PIPE,
        shell=True,
        universal_newlines=True,
    )
    _ = check_output(
        f"lxc restart {machine_name}",
        stderr=PIPE,
        shell=True,
        universal_newlines=True,
    )


def write_key(host: str, password: str, username: str = "super") -> None:
    kc = KazooClient(
        hosts=host,
        sasl_options={"mechanism": "DIGEST-MD5", "username": username, "password": password},
    )
    kc.start()
    kc.create_async("/legolas", b"hobbits")
    kc.stop()
    kc.close()


def check_key(host: str, password: str, username: str = "super") -> None:
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


def srvr(model_full_name: str, unit: str) -> dict:
    """Retrieves attributes returned from the 'srvr' 4lw command.

    Specifically for this test, we are interested in the "Mode" of the ZK server,
    which allows checking quorum leadership and follower active status.
    """
    response = check_output(
        f"JUJU_MODEL={model_full_name} juju ssh {unit} sudo -i 'curl localhost:{ADMIN_SERVER_PORT}/commands/srvr -m 10'",
        stderr=PIPE,
        shell=True,
        universal_newlines=True,
    )

    assert response, "ZooKeeper not running"

    return json.loads(response)


async def ping_servers(ops_test: OpsTest) -> bool:
    for unit in ops_test.model.applications[APP_NAME].units:
        srvr_response = srvr(ops_test.model_full_name, unit.name)

        if srvr_response.get("error", None):
            return False

        mode = srvr_response.get("server_stats", {}).get("server_state", "")
        if mode not in ["leader", "follower"]:
            return False

    return True


async def correct_version_running(ops_test: OpsTest, expected_version: str) -> bool:
    for unit in ops_test.model.applications[APP_NAME].units:
        srvr_response = srvr(ops_test.model_full_name, unit.name)

        if expected_version not in srvr_response.get("version", ""):
            return False

    return True


def check_jaas_config(model_full_name: str, unit: str):
    config = check_output(
        f"JUJU_MODEL={model_full_name} juju ssh {unit} sudo -i 'cat {ZKPaths().jaas}'",
        stderr=PIPE,
        shell=True,
        universal_newlines=True,
    )

    user_lines = {}
    for line in config.splitlines():
        matched = re.search(pattern=r"user_([a-zA-Z\-\d]+)=\"([a-zA-Z0-9]+)\"", string=line)
        if matched:
            user_lines[matched[1]] = matched[2]

    return user_lines


async def get_address(ops_test: OpsTest, app_name=APP_NAME, unit_num=0) -> str:
    """Get the address for a unit."""
    status = await ops_test.model.get_status()  # noqa: F821
    address = status["applications"][app_name]["units"][f"{app_name}/{unit_num}"]["public-address"]
    return address


def _get_show_unit_json(model_full_name: str, unit: str) -> Dict:
    """Retrieve the show-unit result in json format."""
    show_unit_res = check_output(
        f"JUJU_MODEL={model_full_name} juju show-unit {unit} --format json",
        stderr=PIPE,
        shell=True,
        universal_newlines=True,
    )

    try:
        show_unit_res_dict = json.loads(show_unit_res)
        return show_unit_res_dict
    except json.JSONDecodeError:
        raise ValueError


def check_properties(model_full_name: str, unit: str):
    properties = check_output(
        f"JUJU_MODEL={model_full_name} juju ssh {unit} sudo -i 'cat {ZKPaths().properties}'",
        stderr=PIPE,
        shell=True,
        universal_newlines=True,
    )
    return properties.splitlines()


def check_acl_permission(host: str, password: str, folder: str, username: str = "super") -> None:
    """Checks the existence of ACL permission of a given folder."""
    kc = KazooClient(
        hosts=host,
        timeout=30.0,
        sasl_options={"mechanism": "DIGEST-MD5", "username": username, "password": f"{password}"},
    )

    kc.start()
    try:
        value, _ = kc.get_acls_async(f"/{folder}") or None, None
        stored_value = None
        if value:
            stored_value = value.get()
        if stored_value:
            assert stored_value is not None
            return
    except NoNodeError:
        raise Exception("No ACL permission found!")
    finally:
        kc.stop()
        kc.close()


def get_relation_id(model_full_name: str, unit: str, app_name: str):
    show_unit = _get_show_unit_json(model_full_name=model_full_name, unit=unit)
    d_relations = show_unit[unit]["relation-info"]
    for relation in d_relations:
        if relation["endpoint"] == app_name:
            relation_id = relation["relation-id"]
            return relation_id
    raise Exception("No relation found!")


def get_relation_data(
    model_full_name: str,
    unit: str,
    endpoint: str,
    key: Literal["application-data", "local-unit"] = "application-data",
):
    show_unit = _get_show_unit_json(model_full_name=model_full_name, unit=unit)
    d_relations = show_unit[unit]["relation-info"]
    for relation in d_relations:
        if relation["endpoint"] == endpoint:
            return relation[key]
    raise Exception("No relation found!")


async def get_application_hosts(ops_test: OpsTest, app_name: str, units: List[str]) -> List[str]:
    """Retrieves the ip addresses of the containers."""
    hosts = []
    status = await ops_test.model.get_status()  # noqa: F821
    for unit in units:
        hosts.append(status["applications"][app_name]["units"][f"{unit}"]["public-address"])
    return hosts


def count_lines_with(model_full_name: str, unit: str, file: str, pattern: str) -> int:
    result = check_output(
        f"JUJU_MODEL={model_full_name} juju ssh {unit} sudo -i 'grep \"{pattern}\" {file} | wc -l'",
        stderr=PIPE,
        shell=True,
        universal_newlines=True,
    )

    return int(result)


def sign_manual_certs(ops_test: OpsTest, manual_app: str = "manual-tls-certificates") -> None:
    delim = "-----BEGIN CERTIFICATE REQUEST-----"

    csrs_cmd = f"JUJU_MODEL={ops_test.model_full_name} juju run {manual_app}/0 get-outstanding-certificate-requests --format=json | jq -r '.[\"{manual_app}/0\"].results.result' | jq '.[].csr' | sed 's/\\\\n/\\n/g' | sed 's/\\\"//g'"
    csrs = check_output(csrs_cmd, stderr=PIPE, universal_newlines=True, shell=True).split(delim)

    for i, csr in enumerate(csrs):
        if not csr:
            continue

        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp)
            csr_file = tmp_dir / f"csr{i}"
            csr_file.write_text(delim + csr)

            cert_file = tmp_dir / f"{i}.crt"

            try:
                sign_cmd = f"openssl x509 -req -in {csr_file} -CAkey tests/integration/data/inter.key -CA tests/integration/data/inter.crt -days 100 -CAcreateserial -out {cert_file} -copy_extensions copyall --passin pass:password"
                provide_cmd = f'JUJU_MODEL={ops_test.model_full_name} juju run {manual_app}/0 provide-certificate ca-certificate="$(base64 -w0 tests/integration/data/inter.crt)" ca-chain="$(base64 -w0 tests/integration/data/chain)" certificate="$(base64 -w0 {cert_file})" certificate-signing-request="$(base64 -w0 {csr_file})"'

                check_output(sign_cmd, stderr=PIPE, universal_newlines=True, shell=True)
                check_output(provide_cmd, stderr=PIPE, universal_newlines=True, shell=True)
            except CalledProcessError as e:
                logger.error(f"{e.stdout=}, {e.stderr=}, {e.output=}")
                raise e


async def list_truststore_aliases(ops_test: OpsTest, unit: str = f"{APP_NAME}/0") -> list[str]:
    secret_data = await get_secret_by_label(
        ops_test=ops_test, label=f"{PEER}.{APP_NAME}.unit", owner=unit
    )
    truststore_password = secret_data.get("truststore-password")

    result = check_output(
        f"JUJU_MODEL={ops_test.model_full_name} juju ssh {unit} sudo -i 'charmed-zookeeper.keytool -list -keystore /var/snap/charmed-zookeeper/current/etc/zookeeper/truststore.jks -storepass {truststore_password}'",
        stderr=PIPE,
        shell=True,
        universal_newlines=True,
    )

    trusted_aliases = []
    for line in result.splitlines():
        if "trustedCertEntry" not in line:
            continue

        trusted_aliases.append(line.split(",")[0])

    return trusted_aliases
