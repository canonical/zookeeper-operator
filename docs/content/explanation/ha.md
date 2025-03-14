# High availability

Apache ZooKeeper is replicated over a set of hosts called an ensemble.

Ensemble
: the full set of peer servers in a Apache ZooKeeper cluster

Quorum
: the minimum number of servers available in a cluster for it to serve requests. By default, it's a simple majority (more than a half) of all servers in ensemble.

If Apache ZooKeeper cluster looses quorum (has less than a half of available servers), then it can't elect a new leader and effectively stops serving requests as it can't guarantee a consistent view of the system.

For Apache ZooKeeper implementation details, see the [official documentation](https://zookeeper.apache.org/doc/r3.8.2/zookeeperOver.html).

## Number of servers

For Apache ZooKeeper charmed operator the number of servers in a cluster can be easily adjusted by [adding](https://canonical-juju.readthedocs-hosted.com/en/latest/user/howto/manage-units/#add-a-unit) or [removing](https://canonical-juju.readthedocs-hosted.com/en/latest/user/howto/manage-units/#remove-a-unit) Juju [units](https://canonical-juju.readthedocs-hosted.com/en/latest/user/reference/unit/) from the application.

A minimum of three servers/units are required in a cluster for high availability (HA). A cluster with just two servers is inherently less stable than a single server, because there are two single points of failure. If one of them fails, there are not enough machines to form a majority quorum.

We recommend using at least five servers/units of Apache ZooKeeper for production environments. That way, one of the servers/units can be taken down for maintenance without losing HA.

Always use an odd number of servers in an Apache ZooKeeper cluster/ensemble for optimal performance and HA.

## Availability zones

For best high availability, use at least three availability zones (AZ) to deploy an Apache ZooKeeper cluster.

With only two AZ, if the availability zone with the majority number of Apache ZooKeeper servers/units fails, cluster will not have a quorum and will not be available.
