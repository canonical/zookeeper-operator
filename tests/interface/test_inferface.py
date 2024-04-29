from interface_tester.plugin import InterfaceTester


def test_interface(interface_tester: InterfaceTester):
    interface_tester.configure(interface_name="zookeeper", interface_version=0)
    interface_tester.run()
