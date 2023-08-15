import subprocess
import sys
import time
import logging
from kazoo.retry import KazooRetry

from kazoo.client import KazooClient


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

def continous_writes(parent: str, hosts: str, username: str, password: str):
    count = 0

    while True:
        client = KazooClient(
            hosts=hosts,
            timeout=10,
            sasl_options={"mechanism": "DIGEST-MD5", "username": username, "password": password},
            connection_retry=KazooRetry(max_tries=10, delay=1),
            command_retry=KazooRetry(max_tries=10, delay=1)
        )
        client.start()

        client.create(f"{parent}/{count}", acl=[], makepath=True)

        client.stop()

        time.sleep(1)
        count += 1


def start_continuous_writes(parent: str, hosts: str, username: str, password: str):
    subprocess.Popen(
        [
            "python3",
            "tests/integration/ha/continuous_writes.py",
            parent,
            hosts,
            username,
            password,
        ]
    )

def stop_continuous_writes():
    proc = subprocess.Popen(["pkill", "-9", "-f", "continous_writes.py"])
    proc.communicate()

def get_last_znode(parent: str, hosts: str, username: str, password: str) -> int:
    client = KazooClient(
        hosts=hosts,
        timeout=5,
        sasl_options={"mechanism": "DIGEST-MD5", "username": username, "password": password},
        connection_retry=KazooRetry(max_tries=10, delay=1),
        command_retry=KazooRetry(max_tries=10, delay=1),
        read_only=True,
    )
    client.start()

    znodes = client.get_children(parent) 
    assert znodes

    last_znode = sorted(list(map(lambda x: int(x), znodes)))[-1]

    client.stop()

    return last_znode

def count_znodes(parent: str, hosts: str, username: str, password: str) -> int:
    client = KazooClient(
        hosts=hosts,
        timeout=5,
        sasl_options={"mechanism": "DIGEST-MD5", "username": username, "password": password},
        connection_retry=KazooRetry(max_tries=10, delay=1),
        command_retry=KazooRetry(max_tries=10, delay=1),
        read_only=True,
    )
    client.start()

    znodes = client.get_children(parent) 

    assert znodes

    client.stop()

    return len(znodes) - 1  # to account for the parent node

def main():
    parent = sys.argv[1]
    hosts = sys.argv[2]
    username = sys.argv[3]
    password = sys.argv[4]

    continous_writes(parent, hosts, username, password)


if __name__ == "__main__":
    main()
