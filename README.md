# ZooKeeper Operator

## Description

The ZooKeeper Operator deploys, operates and manages [Apache ZooKeeper](https://zookeeper.apache.org/index.html) on machine clusters using [Juju](https://juju.is) and the [Charmed Operator SDK](https://juju.is/docs/sdk).

It uses the latest release of the [Kafka Snap](https://snapcraft.io/kafka) distrubted by Canonical, which tracks and builds the upstream ZooKeeper binaries released by the The Apache Software Foundation that come with [Apache Kafka](https://github.com/apache/kafka).

Manual, Day 2 operations like scaling-up/retiring servers, updating users and distibuting new ACL permissions are handled automatically using the [Juju Operator Lifecycle Manager](https://juju.is/docs/olm).

Server-Server and Client-Server authentication is enabled by default using the SASL mechanism and `DIGEST-MD5` as the authentication scheme, and access control management supports user-provided ACL lists.

## Usage

This charm is still in active development. If you would like to contribute, please refer to [CONTRIBUTING.md](https://github.com/canonical/zookeeper-operator/blob/main/CONTRIBUTING.md)
