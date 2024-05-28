#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

"""Application charm that connects to database charms.

This charm is meant to be used only for testing
of the libraries in this repository.
"""

import logging
import random

from charms.data_platform_libs.v0.data_interfaces import DatabaseRequires
from ops.charm import CharmBase, RelationEvent
from ops.main import main
from ops.model import ActiveStatus

logger = logging.getLogger(__name__)


CHARM_KEY = "app"
PEER = "cluster"
REL_NAME = "zookeeper"


class ApplicationCharm(CharmBase):
    """Application charm that connects to database charms."""

    def __init__(self, *args):
        super().__init__(*args)
        self.name = CHARM_KEY

        self.requires_interface = DatabaseRequires(self, REL_NAME, "/myapp")

        self.framework.observe(getattr(self.on, "start"), self._on_start)
        self.framework.observe(self.on[REL_NAME].relation_changed, self._log)
        self.framework.observe(self.on[REL_NAME].relation_broken, self._log)
        self.framework.observe(self.on[REL_NAME].relation_joined, self._set_data)

    @property
    def relation(self):
        return self.model.get_relation(REL_NAME)

    def _on_start(self, _) -> None:
        self.unit.status = ActiveStatus()

    def _set_data(self, _) -> None:
        if not self.unit.is_leader():
            return

        # reasonable confidence there won't be conflicting chroots
        self.relation.data[self.app].update(
            {
                "database": f"{CHARM_KEY}_{random.randrange(1,99)}",
                "requested-secrets": """["username","password","tls","tls-ca","uris"]""",
            }
        )

    def _log(self, event: RelationEvent):
        return


if __name__ == "__main__":
    main(ApplicationCharm)
