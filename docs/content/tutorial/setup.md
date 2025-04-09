# Setup the environment

For this tutorial, we’ll set up a Multipass VM with two key components:

* LXD, a simple and lightweight system container provider
* Juju, which we’ll use to deploy and manage Apache ZooKeeper and related applications

## Multipass

Multipass is a quick and easy way to launch virtual machines running Ubuntu.
It uses “cloud-init” standard to install and configure all the necessary parts automatically.

Let’s install Multipass from a snap and launch a new VM using “charm-dev” cloud-init configuration:

```bash
sudo snap install multipass && \
multipass launch --cpus 4 --memory 8G --disk 50G --name zk-vm charm-dev
```

```{note}
See also: `multipass launch` command [reference](https://multipass.run/docs/launch-command).
```

Wait for the VM to start and open its shell:

```bash
multipass shell zk-vm
```

For the rest of the tutorial we will work inside this virtual environment.

## LXD

The fastest way to deploy an Apache ZooKeeper charm locally is by setting up a local [LXD](https://canonical.com/lxd) cloud.
LXD is a system container and virtual machine manager—Apache ZooKeeper will run inside one of these containers and be managed by Juju.

While this tutorial covers the basics of LXD, you can [explore more about LXD](https://linuxcontainers.org/lxd/getting-started-cli/). LXD comes pre-installed on Ubuntu 20.04 LTS and later.
Verify that LXD is installed:

```bash
which lxd
```

The output should be:

```text
/usr/sbin/lxd
```

Even if LXD is already installed, we need to run `lxd init` to perform post-installation tasks.
For this tutorial, the default parameters are preferred and the network bridge should be set to have no IPv6 addresses:

```bash
lxd init --auto
lxc network set lxdbr0 ipv6.address none
```

You can list all LXD containers by entering the `lxc list` command.
However, at this point of the tutorial, none should exist and you'll only see an empty list:

```text
+------+-------+------+------+------+-----------+
| NAME | STATE | IPV4 | IPV6 | TYPE | SNAPSHOTS |
+------+-------+------+------+------+-----------+
```

## Juju

[Juju](https://juju.is/) is an orchestration engine for clouds, bare metal, LXD or Kubernetes.
We will be using it to deploy and manage Apache ZooKeeper charm.
We need to install it locally to be able to use CLI commands.
As with LXD, Juju is installed from a snap package:

```bash
sudo snap install juju
```

Juju already has built-in knowledge of LXD and how it works, so there is no additional setup or configuration needed.
A controller will be used to deploy and control Charmed Apache ZooKeeper.
All we need to do is run the following command to bootstrap a Juju controller named `overlord` to LXD.
This bootstrapping process can take several minutes:

```bash
juju bootstrap localhost overlord
```

The Juju controller should exist within an LXD container.
You can verify this by entering the `lxc list` command:

```text
+---------------+---------+-----------------------+------+-----------+-----------+
|     NAME      |  STATE  |         IPV4          | IPV6 |   TYPE    | SNAPSHOTS |
+---------------+---------+-----------------------+------+-----------+-----------+
| juju-<id>     | RUNNING | 10.105.164.235 (eth0) |      | CONTAINER | 0         |
+---------------+---------+-----------------------+------+-----------+-----------+
```

where `<id>` is a unique combination of numbers and letters such as `9d7e4e-0`

A controller can work with multiple different models to group applications such as Apache ZooKeeper.
Add a new model for this tutorial named `tutorial`:

```shell
juju add-model tutorial
```

You can now check the status of the model:

```bash
juju status
```

You should see an output similar to the following:

```text
Model    Controller  Cloud/Region         Version  SLA          Timestamp
tutorial overlord    localhost/localhost  3.1.6    unsupported  23:20:53Z

Model "admin/tutorial" is empty.
```

## Next step

After finishing the setup, continue to the [deploy](deploy) page of the tutorial.
