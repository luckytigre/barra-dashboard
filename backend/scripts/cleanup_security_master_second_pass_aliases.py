#!/usr/bin/env python3
"""Retired entrypoint for the legacy physical security_master alias cleanup tool."""

from __future__ import annotations

import sys


RETIRED_MESSAGE = (
    "cleanup_security_master_second_pass_aliases has been retired. "
    "Physical security_master cleanup is no longer a supported runtime path. "
    "If historical context is needed, see "
    "backend/scripts/_archive/cleanup_security_master_second_pass_aliases.py "
    "and docs/archive/one-time-protocols/SECURITY_MASTER_ALIAS_CLEANUP.md."
)


def main() -> int:
    print(RETIRED_MESSAGE, file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
