# Apache ZooKeeper charm tutorial

The Apache ZooKeeper charm simplifies the deployment, management, and scaling of Apache ZooKeeper clusters on your Juju environment.

In this tutorial, we will:

* Set up your environment using Multipass, LXD and Juju.
* Deploy Apache ZooKeeper using charm.
* Integrate Apache ZooKeeper and Apache Kafka charms.

While this tutorial intends to teach you to use Apache ZooKeeper charm, it will be most beneficial if you already have a familiarity with:

* Basic terminal commands
* [Apache Zookeeper](https://zookeeper.apache.org/) concepts

## Minimum system requirements

Before we start, make sure your machine meets the following requirements:

* Ubuntu 22.04 (Focal) or later.
* 8 GB of RAM.
* 4 CPU cores.
* At least 50 GB of available storage.
* Access to the internet for downloading the required snaps and charms.

<!-- We can't go below 50 GB, as that's the minimum for `charm-dev` -->

```{toctree}
:hidden:

Setup<setup>
deploy
integrate
cleanup
```
