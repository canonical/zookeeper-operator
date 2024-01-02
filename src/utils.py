#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

"""General purpose helper functions for manaing common charm functions."""

import logging
import os
import secrets
import shutil
import string
from typing import List, Optional

logger = logging.getLogger(__name__)


def safe_make_dir(path: str) -> None:
    """Ensures destination directory.

    Args:
        path: the full directory filepath to create
    """
    os.makedirs(path, exist_ok=True)
    shutil.chown(path, user="snap_daemon", group="root")


def safe_write_to_file(content: str, path: str, mode: str = "w") -> None:
    """Ensures destination filepath exists before writing.

    Args:
        content: the content to be written to a file
        path: the full destination filepath
        mode: the write mode. Usually "w" for write, or "a" for append. Default "w"
    """
    safe_make_dir(path=os.path.dirname(path))
    with open(path, mode) as f:
        f.write(content)

    shutil.chown(path, user="snap_daemon", group="root")

    return


def safe_get_file(filepath: str) -> Optional[List[str]]:
    """Load file contents from charm workload.

    Args:
        filepath: the filepath to load data from

    Returns:
        List of file content lines
        None if file does not exist
    """
    if not os.path.exists(filepath):
        return None
    else:
        with open(filepath) as f:
            content = f.read().split("\n")

    return content


def generate_password() -> str:
    """Creates randomized string for use as app passwords.

    Returns:
        String of 32 randomized letter+digit characters
    """
    return "".join([secrets.choice(string.ascii_letters + string.digits) for _ in range(32)])


def map_env(env: list[str]) -> dict[str, str]:
    """Builds environment map for arbitrary env-var strings.

    Returns:
        Dict of env-var and value
    """
    map_env = {}
    for var in env:
        key = "".join(var.split("=", maxsplit=1)[0])
        value = "".join(var.split("=", maxsplit=1)[1:])
        if key:
            # only check for keys, as we can have an empty value for a variable
            map_env[key] = value

    return map_env


def get_env() -> dict[str, str]:
    """Builds map of current basic environment for all processes.

    Returns:
        Dict of env-var and value
    """
    raw_env = safe_get_file("/etc/environment") or []
    return map_env(env=raw_env)


def update_env(env: dict[str, str]) -> None:
    """Updates /etc/environment file."""
    current_env = get_env()
    if not env or current_env == env:
        return

    updated_env = current_env | env
    content = "\n".join([f'{key}=\"{value}' for key, value in updated_env.items() if value])
    safe_write_to_file(content=content, path="/etc/environment", mode="w")
