import logging
import re
from typing import Any, Dict, Iterable, List, Set, Tuple
from kazoo.client import KazooClient
from kazoo.handlers.threading import KazooTimeoutError
from tenacity import RetryError, retry
from tenacity.retry import retry_if_not_result
from tenacity.stop import stop_after_attempt
from tenacity.wait import wait_fixed
from kazoo.client import ACL, KazooClient

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

        try:
            self.leader = self.get_leader()
        except RetryError:
            raise QuorumLeaderNotFoundError("quorum leader not found")

    @retry(
        wait=wait_fixed(3),
        stop=stop_after_attempt(2),
        retry=retry_if_not_result(lambda result: True if result else False),
    )
    def get_leader(self) -> str:
        """Attempts to find the current ZK quorum leader.

        In the case when there is a leadership election, this may fail.
        When this happens, we attempt 1 retry after 3 seconds.

        Returns:
            String of the host for the quorum leader

        Raises:
            tenacity.RetryError: if the leader can't be found during the retry conditions
        """
        leader = None
        for host in self.hosts:
            try:
                with ZooKeeperClient(
                    host=host,
                    client_port=self.client_port,
                    username=self.username,
                    password=self.password,
                ) as zk:
                    response = zk.srvr
                    if response.get("Mode") == "leader":
                        leader = host
                        break
            except KazooTimeoutError:  # in the case of having a dead unit in relation data
                logger.debug(f"TIMEOUT - {host}")
                continue

        return leader or ""

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
            except KazooTimeoutError as e:  # for when units are departing
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

    def leader_znodes(self, path: str) -> Set[str]:
        with ZooKeeperClient(
            host=self.leader,
            client_port=self.client_port,
            username=self.username,
            password=self.password,
        ) as zk:
            all_znode_children = zk.get_all_znode_children(path=path)

        return all_znode_children

    def create_znode_leader(self, path: str, acls: List[ACL]) -> None:
        with ZooKeeperClient(
            host=self.leader,
            client_port=self.client_port,
            username=self.username,
            password=self.password,
        ) as zk:
            zk.create_znode(path=path, acls=acls)

    def set_acls_znode_leader(self, path: str, acls: List[ACL]) -> None:
        with ZooKeeperClient(
            host=self.leader,
            client_port=self.client_port,
            username=self.username,
            password=self.password,
        ) as zk:
            zk.set_acls(path=path, acls=acls)

    def delete_znode_leader(self, path: str) -> None:
        with ZooKeeperClient(
            host=self.leader,
            client_port=self.client_port,
            username=self.username,
            password=self.password,
        ) as zk:
            zk.delete_znode(path=path)


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
        """Retrieves the dynamic config for a ZooKeeper service.

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
        """Retrieves attributes returned from the 'srvr' 4lw command.

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
        """Retrieves attributes returned from the 'mntr' 4lw command.

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

    def get_all_znode_children(self, path: str) -> Set[str]:
        """Recursively gets all children for a given parent znode path.

        Args:
            path: the desired parent znode path to recurse

        Returns:
            Set of all nested children znode paths for the given parent
        """
        children = self.client.get_children(path) or []

        result = set()
        for child in children:
            if path + child != "/zookeeper":
                result.update(self.get_all_znode_children(path.rstrip("/") + "/" + child))
        if path != "/":
            result.add(path)

        return result

    def delete_znode(self, path: str) -> None:
        """Drop znode and all it's children from ZK tree.

        Args:
            path: the desired znode path to delete
        """
        if not self.client.exists(path):
            return
        self.client.delete(path, recursive=True)

    def create_znode(self, path: str, acls: List[ACL]) -> None:
        """Create new znode.

        Args:
            path: the desired znode path to create
            acls: the acls for the new znode
        """
        self.client.create(path, acl=acls, makepath=True)

    def get_acls(self, path: str) -> List[ACL]:
        """Gets acls for a desired znode path.

        Args:
            path: the desired znode path

        Returns:
            List of the acls set for the given znode
        """
        acl_list = self.client.get_acls(path)

        return acl_list if acl_list else []

    def set_acls(self, path: str, acls: List[ACL]) -> None:
        """Sets acls for a desired znode path.

        Args:
            path: the desired znode path
            acls: the acls to set to the given znode
        """
        self.client.set_acls(path, acls)
