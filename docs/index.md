# Apache ZooKeeper charmed operator

Apache ZooKeeper charmed operator is a [Juju charm](https://canonical-juju.readthedocs-hosted.com/en/latest/user/reference/charm/) that provides automated operations management for Apache ZooKeeper, facilitating highly reliable distributed coordination.

Apache ZooKeeper is a free, open-source software project by the Apache Software Foundation. Users can find out more at the [Apache ZooKeeper project page](https://zookeeper.apache.org/).

<!-- Apache ZooKeeper is a distributed coordination service widely used for managing configuration information, naming, synchronisation, and group services in distributed systems. -->

Apache ZooKeeper charmed operator automates the deployment, scaling, and maintenance of Apache ZooKeeper clusters, ensuring reliable coordination for distributed applications. It manages leader election, configuration storage, and synchronisation while handling security features like authentication and access control. The operator enables seamless horizontal scaling, service discovery, and automated recovery, reducing manual intervention. It utilises [Juju](https://juju.is/) for simplified life cycle management across cloud, VM, and bare-metal environments.

```{note}
This charm operates Apache ZooKeeper in [machine](https://canonical-juju.readthedocs-hosted.com/en/latest/user/reference/machine/) Juju environments: physical systems, Virtual Machines (VMs), and a wide range of cloud and cloud-like environments, including AWS, Azure, OpenStack, and VMware.
For Kubernetes, see Apache ZooKeeper K8s charmed operator.
```
<!-- TODO add a link to the K8s charm above -->

Apache ZooKeeper charmed operator simplifies the deployment and management of Apache ZooKeeper clusters, ensuring high availability, security, and fault tolerance with minimal effort. Designed for production environments, it streamlines and unifies coordination for distributed applications like Apache Kafka.

The charm is useful for DevOps teams, platform engineers, and organisations running distributed systems that require reliable coordination. Teams looking to reduce operational overhead, enhance security, and simplify cluster scaling will find it especially useful. 

## In this documentation

| | |
|--|--|
|  [Tutorial](content/tutorial/index.md) </br>  Get started - a hands-on introduction to Apache ZooKeeper charmed operator for new users </br> |  [How-to guides](content/how-to/index.md) </br> Step-by-step guides covering key operations and common tasks |
|  Explanation </br> Concepts - discussion and clarification of key topics, architecture | [Reference](content/reference/index.md) </br> Technical information and reference materials | 

## Project and community

Apache ZooKeeper charmed operator is a distribution of Apache ZooKeeper. It’s an open-source project that welcomes community contributions, suggestions, fixes and constructive feedback.

- [Read our Code of Conduct](https://ubuntu.com/community/code-of-conduct)
- [Join the Discourse forum](https://discourse.charmhub.io/tag/kafka)
- [Contribute](https://github.com/canonical/zookeeper-operator/blob/main/CONTRIBUTING.md) and report [issues](https://github.com/canonical/zookeeper-operator/issues/new)
- Explore [Canonical Data Fabric solutions](https://canonical.com/data)
- [Contact us](https://discourse.charmhub.io/t/13107) for all further questions

Apache®, Apache ZooKeeper, ZooKeeper™, Apache Kafka, Kafka®, and the Apache Kafka logo are either registered trademarks or trademarks of the Apache Software Foundation in the United States and/or other countries.

```{toctree}
:hidden:
Overview<self>
Tutorial <content/tutorial/index.md>
How To guides <content/how-to/index.md>
Reference <content/reference/index.md>
```
