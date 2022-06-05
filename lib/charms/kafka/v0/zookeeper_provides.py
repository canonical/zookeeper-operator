import logging

from typing import Dict
from ops.charm import CharmBase
from ops.framework import Object
from ops.model import Relation


logger = logging.getLogger(__name__)


class ZooKeeperProvides(Object):
    def __init__(self, charm: CharmBase, relation_name: str = "zookeeeper") -> None:
        super().__init__(charm, relation_name)
        self.charm = charm
        self.relation: Relation = self.charm.model.get_relation(relation_name)

    def _update_relation_data(self, data: Dict[str, str]) -> None:
        """Updates a set of key-value pairs in the relation.

        This function writes in the application data bag, therefore
        only the leader unit can call it

        Args:
            data (dict): key-value pairs to be updated in the relation
        """
        if not self.charm.unit.is_leader():
            return
        else:
            self.relation.data[self.relation.app].update(data)

    def set_address(self, address: str) -> None:
        """Set ZooKeeper address.

        Args:
            address (str): comma seperated list of addresses
                e.g localhost:2181,localhost:2182
        """
        self._update_relation_data({"address": address})
