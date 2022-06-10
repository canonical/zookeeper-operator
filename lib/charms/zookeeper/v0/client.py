import logging
import re
from typing import Any, Dict, Iterable, List, Set
from kazoo.client import KazooClient

logger = logging.getLogger(__name__)


class MembersSyncingError(Exception):
    pass


class MemberNotReadyError(Exception):
    pass


class ZooKeeperManager:
    def __init__(self, hosts: List[str], client_port: int = 2181):
        self.hosts = hosts
        self.client_port = client_port
        self.leader = ""

        for host in self.hosts:
            with ZooKeeperClient(host=host, client_port=client_port) as zk:
                response = zk.srvr
                if response.get("Mode") == "leader":
                    self.leader = host

    @property
    def server_members(self) -> Set[str]:
        with ZooKeeperClient(host=self.leader, client_port=self.client_port) as zk:
            members, _ = zk.config

        return set(members)

    @property
    def version(self) -> int:
        with ZooKeeperClient(host=self.leader, client_port=self.client_port) as zk:
            _, version = zk.config

        return version

    @property
    def members_syncing(self) -> bool:
        with ZooKeeperClient(host=self.leader, client_port=self.client_port) as zk:
            result = zk.mntr
        if (
            result.get("zk_peer_state", "") == "leading - broadcast"
            and result["zk_pending_syncs"] == "0"
        ):
            return False
        return True

    def add_members(self, members: Iterable[str]):
        if self.members_syncing:
            raise MembersSyncingError("Unable to add members - some members are syncing")

        for member in members:
            host = member.split("=")[1].split(":")[0]
            with ZooKeeperClient(host=host, client_port=self.client_port) as zk:
                if not zk.is_ready:
                    raise MemberNotReadyError(f"Server is not ready: {host}")

                data, stat = zk.client.reconfig(
                    joining=member, leaving=None, new_members=None, from_config=self.version
                )
                logger.debug(f"ZooKeeper reconfig response: {stat}, {data}")

    def remove_members(self, members: Iterable[str]):
        if self.members_syncing:
            raise MembersSyncingError("Unable to remove members - some members are syncing")

        for member in members:
            member_id = re.findall(r"server.([1-9]*)", member)[0]
            with ZooKeeperClient(host=self.leader, client_port=self.client_port) as zk:
                data, stat = zk.client.reconfig(
                    joining=None, leaving=member_id, new_members=None, from_config=self.version
                )
                logger.debug(f"ZooKeeper reconfig response: {stat}, {data}")


class ZooKeeperClient:
    def __init__(self, host, client_port):
        self.host = host
        self.client_port = client_port
        self.client = KazooClient(hosts=f"{host}:{client_port}")

    def __enter__(self):
        return self

    def __exit__(self):
        self.client.close()

    def _run_4lw_command(self, command: str):
        return self.client.command(command.encode())

    @property
    def config(self):
        response = self.client.get("/zookeeper/config")
        if response:
            result = str(response[0].decode("utf-8")).splitlines()
            version = int(result.pop(-1).split("=")[1], base=16)
        else:
            raise

        return result, version

    @property
    def srvr(self) -> Dict[str, Any]:
        response = self._run_4lw_command("srvr")
        result = {}
        for item in response.splitlines():
            k = re.split(": ", item)[0]
            v = re.split(": ", item)[1]
            result[k] = v

        return result

    @property
    def mntr(self) -> Dict[str, Any]:
        response = self._run_4lw_command("mntr")
        result = {}
        for item in response.splitlines():
            if re.search("=|\\t", item):
                k = re.split("=|\\t", item)[0]
                v = re.split("=|\\t", item)[1]
                result[k] = v
            else:
                result[item] = ""

        return result

    @property
    def is_ready(self) -> bool:
        if self.client.connected:
            return "broadcast" in self.mntr.get("zk_peer_state", "")
        return False
