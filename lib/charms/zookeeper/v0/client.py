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
        logger.info("---------- ZooKeeperManager ----------")
        self.hosts = hosts
        logger.info(f"{hosts=}")
        self.client_port = client_port
        logger.info(f"{client_port=}")
        self.leader = ""

        for host in self.hosts:
            logger.info(f"{host=}")
            with ZooKeeperClient(host=host, client_port=client_port) as zk:
                response = zk.srvr
                logger.info(f"{response=}")
                if response.get("Mode") == "leader":
                    self.leader = host

    @property
    def server_members(self) -> Set[str]:
        logger.info("---------- server_members ----------")
        with ZooKeeperClient(host=self.leader, client_port=self.client_port) as zk:
            members, _ = zk.config
            logger.info(f"{members=}")

        return set(members)

    @property
    def version(self) -> int:
        logger.info("---------- version ----------")
        with ZooKeeperClient(host=self.leader, client_port=self.client_port) as zk:
            _, version = zk.config
            logger.info(f"{version=}")

        return version

    @property
    def members_syncing(self) -> bool:
        logger.info("---------- members_syncing ----------")
        with ZooKeeperClient(host=self.leader, client_port=self.client_port) as zk:
            result = zk.mntr
            logger.info(f"{result=}")
        if (
            result.get("zk_peer_state", "") == "leading - broadcast"
            and result["zk_pending_syncs"] == "0"
        ):
            return False
        return True

    def add_members(self, members: Iterable[str]):
        logger.info("---------- add_members ----------")
        if self.members_syncing:
            raise MembersSyncingError("Unable to add members - some members are syncing")

        for member in members:
            logger.info(f"{member=}")
            host = member.split("=")[1].split(":")[0]
            logger.info(f"{host=}")
            with ZooKeeperClient(host=host, client_port=self.client_port) as zk:
                if not zk.is_ready:
                    raise MemberNotReadyError(f"Server is not ready: {host}")

                data, stat = zk.client.reconfig(
                    joining=member, leaving=None, new_members=None, from_config=self.version
                )
                logger.info(f"ZooKeeper reconfig response: {stat}, {data}")

    def remove_members(self, members: Iterable[str]):
        logger.info("---------- remove_members ----------")
        if self.members_syncing:
            raise MembersSyncingError("Unable to remove members - some members are syncing")

        for member in members:
            logger.info(f"{member=}")
            member_id = re.findall(r"server.([1-9]*)", member)[0]
            logger.info(f"{member_id=}")
            with ZooKeeperClient(host=self.leader, client_port=self.client_port) as zk:
                data, stat = zk.client.reconfig(
                    joining=None, leaving=member_id, new_members=None, from_config=self.version
                )
                logger.info(f"ZooKeeper reconfig response: {stat}, {data}")


class ZooKeeperClient:
    def __init__(self, host, client_port):
        logger.info("---------- ZooKeeperClient ----------")
        self.host = host
        self.client_port = client_port
        self.client = KazooClient(hosts=f"{host}:{client_port}")
        self.client.start()

    def __enter__(self):
        return self

    def __exit__(self):
        self.client.close()

    def _run_4lw_command(self, command: str):
        return self.client.command(command.encode())

    @property
    def config(self):
        logger.info("---------- config ----------")
        response = self.client.get("/zookeeper/config")
        logger.info(f"{response=}")
        if response:
            result = str(response[0].decode("utf-8")).splitlines()
            logger.info(f"{result=}")
            version = int(result.pop(-1).split("=")[1], base=16)
            logger.info(f"{version=}")
        else:
            raise

        return result, version

    @property
    def srvr(self) -> Dict[str, Any]:
        logger.info("---------- srvr ----------")
        response = self._run_4lw_command("srvr")
        logger.info(f"{response=}")
        result = {}
        for item in response.splitlines():
            logger.info(f"{item=}")
            k = re.split(": ", item)[0]
            logger.info(f"{k=}")
            v = re.split(": ", item)[1]
            logger.info(f"{v=}")
            result[k] = v

        logger.info(f"{result=}")
        return result

    @property
    def mntr(self) -> Dict[str, Any]:
        logger.info("---------- mntr ----------")
        response = self._run_4lw_command("mntr")
        logger.info(f"{response=}")
        result = {}
        for item in response.splitlines():
            logger.info(f"{item=}")
            if re.search("=|\\t", item):
                k = re.split("=|\\t", item)[0]
                logger.info(f"{k=}")
                v = re.split("=|\\t", item)[1]
                logger.info(f"{v=}")
                result[k] = v
            else:
                result[item] = ""

        logger.info(f"{result=}")
        return result

    @property
    def is_ready(self) -> bool:
        logger.info("---------- is_ready ----------")
        if self.client.connected:
            return "broadcast" in self.mntr.get("zk_peer_state", "")
        return False
