# Clean up

This is the last part of the Apache ZooKeeper charm tutorial.
After finishing all previous parts, make sure to free up all resources used by the tutorial's environment.

## Destroy the VM

Destroy the entire Multipass VM created for this tutorial:

```bash
multipass stop zk-vm
multipass delete --purge zk-vm
```

This concludes the clean up process and the Apache ZooKeeper charm tutorial.
