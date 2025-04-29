# Clean up

This is the last part of the Apache ZooKeeper charm tutorial.
After finishing all previous parts, make sure to free up all resources used by the tutorial's environment.

## Destroy the Apache ZooKeeper application

Destroy the entire Apache ZooKeeper application:

```bash
juju remove-application zookeeper
```

This will remove Apache ZooKeeper from your Juju model.

## Destroy other applications

Let's say we are not sure what other applications we have in our model.
Use `juju status` command:

```bash
juju status
```

Now, remove the applications mentioned in the output:

```bash
juju remove-application kafka
```

Finally, make sure that deletion is complete with `juju status` and remove the Juju model:

```bash
juju destroy-model tutorial
```

## Destroy the VM

Exit the VM shell by pressing `Ctrl + D` and destroy the entire Multipass VM created for this tutorial:

```bash
multipass stop zk-vm
multipass delete --purge zk-vm
```

This concludes the clean up process and the Apache ZooKeeper charm tutorial.
