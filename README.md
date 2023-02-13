# Charmed ZooKeeper Operator

## Overview

The Charmed ZooKeeper Operator delivers automated operations management from day 0 to day 2 on the [Apache ZooKeeper](https://zookeeper.apache.org/) server which enables highly reliable distributed coordination, deployed on top of a [Kubernetes cluster](https://kubernetes.io/). It is an open source, end-to-end, production ready data platform on top of cloud native technologies.

This operator charm comes with features such as:
- Horizontal scaling for high-availability out-of-the-box
- Server-Server and Client-Server authentication both enabled by default
- Access control management supported with user-provided ACL lists.

The ZooKeeper Operator uses the latest upstream ZooKeeper binaries released by the The Apache Software Foundation that come with Kafka, made available using the [`zookeeper` snap](https://snapcraft.io/zookeeper) distributed by Canonical.

## Requirements

For production environments, it is recommended to deploy at least 5 nodes for Zookeeper.
While the following requirements are meant to be for production, the charm can be deployed in smaller environments.

- 4-8GB of RAM
- 2-4 cores
- 1 storage device, 64GB

## Config options

To get a description of all config options available, please refer to the [`config.yaml`](https://github.com/canonical/zookeeper-operator/blob/main/config.yaml) file.

Options can be changed by using the `juju config` command:
```shell
juju config zookeeper <config_option_1>=<value> [<config_option_2>=<value>]
```
## Usage
### Basic usage

The ZooKeeper operator may be deployed using the Juju command line as follows:

```bash
$ juju deploy zookeeper -n 5
```

To watch the process, `juju status` can be used. Once all the units show as `active|idle` the credentials to access the admin user can be queried with:
```shell
juju run-action zookeeper/leader get-super-password --wait 
```

### Replication
#### Scaling up
The charm can be scaled up using `juju add-unit` command.
```shell
juju add-unit zookeeper
```

To add a specific number of servers, an extra argument is needed:
```shell
juju add-unit zookeeper -n <num_servers_to_add>
```

#### Scaling down
To scale down the charm, use `juju remove-unit` command.
```shell
juju remove-unit <unit_name>
```

### Password rotation
#### Internal users
The Charmed ZooKeeper Operator has two internal users:
- `super`: admin user for the cluster. Used mainly with the Kafka operator.
- `sync`: specific to the internal quorum handling. 

The `set-password` action can be used to rotate the password of one of them. If no username is passed, it will default to the `super` user.
```shell
# to set a specific password for the sync user
juju run-action zookeeper/leader set-password username=sync password=<password> --wait

# to randomly generate a password for the super user
juju run-action zookeeper/leader set-password --wait
```

## Relations

Supported [relations](https://juju.is/docs/olm/relations):

#### `tls-certificates` interface:

The `tls-certificates` interface is used with the `tls-certificates-operator` charm.

To enable TLS:

```shell
# deploy the TLS charm 
juju deploy tls-certificates-operator --channel=edge
# add the necessary configurations for TLS
juju config tls-certificates-operator generate-self-signed-certificates="true" ca-common-name="Test CA" 
# to enable TLS relate the application 
juju relate tls-certificates-operator zookeeper
```

Updates to private keys for certificate signing requests (CSR) can be made via the `set-tls-private-key` action.
```shell
# Updates can be done with auto-generated keys with
juju run-action zookeeper/0 set-tls-private-key --wait
juju run-action zookeeper/1 set-tls-private-key --wait
juju run-action zookeeper/2 set-tls-private-key --wait
```

Passing keys to internal keys should *only be done with* `base64 -w0` *not* `cat`. With three servers this schema should be followed:
```shell
# generate shared internal key
openssl genrsa -out internal-key.pem 3072
# apply keys on each unit
juju run-action zookeeper/0 set-tls-private-key "internal-key=$(base64 -w0 internal-key.pem)"  --wait
juju run-action zookeeper/1 set-tls-private-key "internal-key=$(base64 -w0 internal-key.pem)"  --wait
juju run-action zookeeper/2 set-tls-private-key "internal-key=$(base64 -w0 internal-key.pem)"  --wait
```

To disable TLS remove the relation
```shell
juju remove-relation zookeeper tls-certificates-operator
```

Note: The TLS settings here are for self-signed-certificates which are not recommended for production clusters, the `tls-certificates-operator` charm offers a variety of configurations, read more on the TLS charm [here](https://charmhub.io/tls-certificates-operator)


## Security
Security issues in the Charmed ZooKeeper Operator can be reported through [LaunchPad](https://wiki.ubuntu.com/DebuggingSecurity#How%20to%20File). Please do not file GitHub issues about security issues.


## Contributing

Please see the [Juju SDK docs](https://juju.is/docs/sdk) for guidelines on enhancements to this charm following best practice guidelines, and [CONTRIBUTING.md](https://github.com/canonical/zookeeper-operator/blob/main/CONTRIBUTING.md) for developer guidance.


## License
The Charmed ZooKeeper Operator is free software, distributed under the Apache Software License, version 2.0. See [LICENSE](https://github.com/canonical/zookeeper-operator/blob/main/LICENSE) for more information.

