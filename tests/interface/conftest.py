import pytest
from charm import ZooKeeperCharm
from interface_tester.plugin import InterfaceTester


@pytest.fixture
def interface_tester(interface_tester: InterfaceTester):
    interface_tester.configure(
        charm_type=ZooKeeperCharm,
        repo="https://github.com/Batalex/charm-relation-interfaces",
        branch="feat/DPE-3737",
    )
    yield interface_tester
