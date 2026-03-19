"""Explicit cUSE4 alias for operator-status payload semantics."""

from __future__ import annotations

from backend.services import operator_status_service as _legacy


config = _legacy.config
core_reads = _legacy.core_reads
get_holdings_sync_state = _legacy.get_holdings_sync_state
get_refresh_status = _legacy.get_refresh_status
job_runs = _legacy.job_runs
profile_catalog = _legacy.profile_catalog
runtime_state = _legacy.runtime_state
sqlite = _legacy.sqlite

build_operator_status_payload = _legacy.build_operator_status_payload

__all__ = [
    "build_operator_status_payload",
    "config",
    "core_reads",
    "get_holdings_sync_state",
    "get_refresh_status",
    "job_runs",
    "profile_catalog",
    "runtime_state",
    "sqlite",
]
