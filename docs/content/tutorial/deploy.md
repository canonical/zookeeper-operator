# Deploy

This is the part of the Apache ZooKeeper charmed operator tutorial about deployment and configuration. Make sure to complete instruction from the [Setup](setup) page before reading further.

## Deploy the charm

Apache ZooKeeper charmed operator can be deployed as any other charm via `juju deploy` command:

```
juju deploy zookeeper -n 5
```

```{note}
See command reference: [juju deploy](https://canonical-juju.readthedocs-hosted.com/en/latest/user/reference/juju-cli/list-of-juju-cli-commands/deploy/)
```

This will deploy five [units](https://canonical-juju.readthedocs-hosted.com/en/latest/user/reference/unit/#unit) of Apache ZooKeeper charm.

Check the status of the deployment:

```
juju status --watch 1s
```

Wait until all units have `active` and `idle` status.

```{note}
See command reference: [juju status](https://canonical-juju.readthedocs-hosted.com/en/latest/user/reference/juju-cli/list-of-juju-cli-commands/status/)
```

## Scale the cluster

```{warning}
Apache ZooKeeper requires an odd number of nodes for quorum. Make sure to deploy a sufficient number of units.
```

You can change the number of units in the Apache ZooKeeper cluster. For example, to specify the desired number of units for the Apache ZooKeeper application deployed earlier:

```
juju scale-application zookeeper 3
```

```{note}
See also: [juju scale-application](https://canonical-juju.readthedocs-hosted.com/en/latest/user/reference/juju-cli/list-of-juju-cli-commands/scale-application/), [juju add-unit](https://canonical-juju.readthedocs-hosted.com/en/latest/user/reference/juju-cli/list-of-juju-cli-commands/add-unit/), [juju remove-unit](https://canonical-juju.readthedocs-hosted.com/en/latest/user/reference/juju-cli/list-of-juju-cli-commands/remove-unit/).
```

## Configure Apache ZooKeeper

Configure the Apache ZooKeeper cluster by setting the `tick-time` configuration option via the `juju config` command:

To configure these settings, use the following command:

```
juju config zookeeper tick-time=2000
```

The `tick-time` option sets the basic time unit in milliseconds used by Apache ZooKeeper for heartbeats.

## Next step

After finishing the deployment and configuration, continue to the [deploy](deploy) page of the tutorial.
