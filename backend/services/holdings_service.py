"""Compatibility shim for cUSE4 holdings read/mutation semantics.

Prefer importing ``backend.services.cuse4_holdings_service`` from the default
cUSE4 holdings and portfolio routes. This module remains only for older callers
and direct service tests that still import the legacy path.
"""

from __future__ import annotations

from backend.services.cuse4_holdings_service import (
    IMPORT_MODES,
    HoldingsDependencies,
    get_holdings_dependencies,
    load_holdings_accounts,
    load_holdings_positions,
    logger,
    record_holdings_dirty,
    run_holdings_import,
    run_position_remove,
    run_position_upsert,
    run_whatif_apply,
    trigger_light_refresh_if_requested,
)

__all__ = [
    "IMPORT_MODES",
    "HoldingsDependencies",
    "get_holdings_dependencies",
    "load_holdings_accounts",
    "load_holdings_positions",
    "logger",
    "record_holdings_dirty",
    "run_holdings_import",
    "run_position_remove",
    "run_position_upsert",
    "run_whatif_apply",
    "trigger_light_refresh_if_requested",
]
