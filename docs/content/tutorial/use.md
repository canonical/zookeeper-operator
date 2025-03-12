# Use

This is the part of the Apache ZooKeeper charmed operator tutorial about using Apache ZooKeeper with other charms. Make sure to complete instruction from the [Deploy](deploy) page before reading further.

## Deploy Apache Kafka

Deploy Apache Kafka charm to the same model:

```
juju deploy kafka --channel 3/stable -n 3 --trust
```

Make sure Apache Kafka cluster is deployed by checking its status:

```
juju status --watch 1s
```

Wait until all units have `active` and `idle` status.

Integrate the Apache ZooKeeper and Apache Kafka applications:

```
juju integrate kafka zookeeper
```

<!-- Add something here to show the ZooKeeper's role and/or process. -->

## Next step

We finished most of the tutorial, continue to the [Cleanup](cleanup) page of the tutorial.
