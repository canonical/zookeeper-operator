import pytest
from charm import ZooKeeperCharm
from interface_tester.plugin import InterfaceTester


@pytest.fixture
def interface_tester(interface_tester: InterfaceTester):
    interface_tester.configure(charm_type=ZooKeeperCharm)
    yield interface_tester
