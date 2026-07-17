#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Logging extensions & filters."""

import logging
from abc import ABC, abstractmethod
from collections.abc import Iterable


class WithSensitiveValues(ABC):
    """Interface for an object carrying sensitive values."""

    @property
    @abstractmethod
    def sensitive_values(self) -> Iterable[str]:
        """Return an iterable of sensitive values held by this object."""
        ...


class RedactionFilter(logging.Filter):
    """Logging filter that redacts a set of secret strings from log messages."""

    REDACTED = "[redacted]"

    def __init__(self, sensitive_object: WithSensitiveValues):
        self._obj = sensitive_object
        super().__init__()

    def filter(self, record: logging.LogRecord) -> bool:
        """Replaces any configured secret found in the record's message with '[redacted]'."""
        message = record.getMessage()
        for secret in self._obj.sensitive_values:
            if secret:
                message = message.replace(str(secret), self.REDACTED)

        record.msg = message
        record.args = ()
        return True
