import logging
import re
import time
from typing import Any, Dict, Iterable, List, Set, Tuple
from kazoo.client import KazooClient
from kazoo.handlers.threading import KazooTimeoutError

logger = logging.getLogger(__name__)

# Kazoo logs are unbearably chatty
logging.getLogger("kazoo.client").disabled = True


class MembersSyncingError(Exception):
    """Generic exception for when quorum members are syncing data."""

    pass


class MemberNotReadyError(Exception):
    """Generic exception for when a zk unit can't be connected to or is not broadcasting."""

    pass


class QuorumLeaderNotFoundError(Exception):
    """Generic exception for when there are no zk leaders in the app."""

    pass


class ZooKeeperManager:
    """Handler for performing ZK commands."""

    def __init__(
        self,
        hosts: List[str],
        username: str,
        password: str,
        client_port: int = 2181,
    ):
        self.hosts = hosts
        self.username = username
        self.password = password
        self.client_port = client_port
        self.leader = ""

        # iterate through all hosts to find current leader
        retries = 2
        for _ in range(retries):
            for host in self.hosts:
                try:
                    with ZooKeeperClient(
                        host=host, client_port=client_port, username=username, password=password
                    ) as zk:
                        response = zk.srvr
                        if response.get("Mode") == "leader":
                            self.leader = host
                            break
                except KazooTimeoutError:
                    logger.info(f"TIMEOUT - {host}")
                    continue
            if not self.leader:
                logger.info(f"RETRYING - quorum leader not found")
                time.sleep(3)
                continue

        if not self.leader:
            raise QuorumLeaderNotFoundError("quorum leader not found, probably not ready yet")

    @property
    def server_members(self) -> Set[str]:
        """The current members within the ZooKeeper quorum.

        Returns:
            A set of ZK member strings
                e.g {"server.1=10.141.78.207:2888:3888:participant;0.0.0.0:2181"}
        """
        with ZooKeeperClient(
            host=self.leader,
            client_port=self.client_port,
            username=self.username,
            password=self.password,
        ) as zk:
            members, _ = zk.config

        return set(members)

    @property
    def config_version(self) -> int:
        """The current config version for ZooKeeper.

        Returns:
            The zookeeper config version decoded from base16
        """
        with ZooKeeperClient(
            host=self.leader,
            client_port=self.client_port,
            username=self.username,
            password=self.password,
        ) as zk:
            _, version = zk.config

        return version

    @property
    def members_syncing(self) -> bool:
        """Flag to check if any quorum members are currently syncing data.

        Returns:
            True if any members are syncing. Otherwise False.
        """
        with ZooKeeperClient(
            host=self.leader,
            client_port=self.client_port,
            username=self.username,
            password=self.password,
        ) as zk:
            result = zk.mntr
        if (
            result.get("zk_peer_state", "") == "leading - broadcast"
            and result["zk_pending_syncs"] == "0"
        ):
            return False
        return True

    def add_members(self, members: Iterable[str]) -> None:
        """Adds new members to the members' dynamic config.

        Raises:
            MembersSyncingError: if any members are busy syncing data
            MemberNotReadyError: if any members are not yet broadcasting
        """
        if self.members_syncing:
            raise MembersSyncingError("Unable to add members - some members are syncing")

        for member in members:
            host = member.split("=")[1].split(":")[0]
            
            try:
                # individual connections to each server
                with ZooKeeperClient(
                    host=host,
                    client_port=self.client_port,
                    username=self.username,
                    password=self.password,
                ) as zk:
                    if not zk.is_ready:
                        raise MemberNotReadyError(f"Server is not ready: {host}")
            except KazooTimeoutError as e:
                logger.warning(str(e))
                continue

            # specific connection to leader
            with ZooKeeperClient(
                host=self.leader,
                client_port=self.client_port,
                username=self.username,
                password=self.password,
            ) as zk:
                zk.client.reconfig(
                    joining=member, leaving=None, new_members=None, from_config=self.config_version
                )

    def remove_members(self, members: Iterable[str]):
        """Removes members from the members' dynamic config.

        Raises:
            MembersSyncingError: if any members are busy syncing data
        """
        if self.members_syncing:
            raise MembersSyncingError("Unable to remove members - some members are syncing")

        for member in members:
            member_id = re.findall(r"server.([1-9]+)", member)[0]
            with ZooKeeperClient(
                host=self.leader,
                client_port=self.client_port,
                username=self.username,
                password=self.password,
            ) as zk:
                zk.client.reconfig(
                    joining=None,
                    leaving=member_id,
                    new_members=None,
                    from_config=self.config_version,
                )


class ZooKeeperClient:
    """Handler for ZooKeeper connections and running 4lw client commands."""

    def __init__(self, host: str, client_port: int, username: str, password: str):
        self.host = host
        self.client_port = client_port
        self.username = username
        self.password = password
        self.client = KazooClient(
            hosts=f"{host}:{client_port}",
            timeout=1.0,
            sasl_options={"mechanism": "DIGEST-MD5", "username": username, "password": password},
        )
        self.client.start()

    def __enter__(self):
        return self

    def __exit__(self, object_type, value, traceback):
        self.client.stop()

    def _run_4lw_command(self, command: str):
        return self.client.command(command.encode())

    @property
    def config(self) -> Tuple[List[str], int]:
        """Retreives the dynamic config for a ZooKeeper service.

        Returns:
            Tuple of the decoded config list, and decoded config version
        """
        response = self.client.get("/zookeeper/config")
        if response:
            result = str(response[0].decode("utf-8")).splitlines()
            version = int(result.pop(-1).split("=")[1], base=16)
        else:
            raise

        return result, version

    @property
    def srvr(self) -> Dict[str, Any]:
        """Retreives attributes returned from the 'srvr' 4lw command.

        Returns:
            Mapping of field and setting returned from `mntr`
        """
        response = self._run_4lw_command("srvr")

        result = {}
        for item in response.splitlines():
            k = re.split(": ", item)[0]
            v = re.split(": ", item)[1]
            result[k] = v

        return result

    @property
    def mntr(self) -> Dict[str, Any]:
        """Retreives attributes returned from the 'mntr' 4lw command.

        Returns:
            Mapping of field and setting returned from `mntr`
        """
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
        """Flag to confirm connected ZooKeeper server is connected and broadcasting.

        Returns:
            True if server is broadcasting. Otherwise False.
        """
        if self.client.connected:
            return "broadcast" in self.mntr.get("zk_peer_state", "")
        return False
