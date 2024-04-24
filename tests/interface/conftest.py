import pytest
from charm import ZookeeperCharm
from interface_tester.plugin import InterfaceTester


@pytest.fixture
def interface_tester(interface_tester: InterfaceTester):
    interface_tester.configure(charm_type=ZookeeperCharm)
    yield interface_tester
