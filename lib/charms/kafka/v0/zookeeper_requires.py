import logging

from typing import Optional
from ops.charm import CharmBase, CharmEvents, RelationChangedEvent, RelationEvent
from ops.framework import EventSource, Object
from ops.model import Relation


logger = logging.getLogger(__name__)


class ZooKeeperAddressChangedEvent(RelationEvent):
    """Event emitted when ZooKeeper's address changes."""


class ZooKeeperEvents(CharmEvents):
    zookeeper_address_changed = EventSource(ZooKeeperAddressChangedEvent)


class ZooKeeperRequires(Object):
    def __init__(self, charm: CharmBase, relation_name: str = "zookeeper") -> None:
        super().__init__(charm, relation_name)
        self.charm = charm
        self.relation: Relation = self.charm.model.get_relation(relation_name)

        self.framework.observe(
            charm.on[self.relation.name].relation_changed, self._on_relation_changed
        )

    @property
    def address(self) -> Optional[str]:
        """Get zookeeper address."""
        return self._get_relation_data("address")

    def _get_relation_data(self, key: str) -> Optional[str]:
        """Retrieves data from relation.

        Args:
            key (str): Key to retrieve the data from the relation.
        Returns:
            str/None: A string value stored in the relation data bag for
                the specified key. None if key does not found
        """
        return self.relation.data[self.relation.app].get(key)

    def _on_relation_changed(self, event: RelationChangedEvent) -> None:
        if not self.charm.unit.is_leader():
            return
        else:
            getattr(self.charm.on, "zookeeper_address_changed").emit(event.relation)
