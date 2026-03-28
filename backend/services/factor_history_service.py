"""Compatibility shim for cUSE4 factor-history route semantics.

Prefer importing ``backend.services.cuse4_factor_history_service`` from the
default cUSE4 route family. This module remains only for older callers and
tests that still import the legacy path directly.
"""

from __future__ import annotations

from backend.services.cuse4_factor_history_service import (
    FactorHistoryDependencies,
    FactorHistoryNotReady,
    cache_get,
    config,
    get_factor_history_dependencies,
    load_factor_history_response,
    load_factor_return_history,
    load_runtime_payload,
    resolve_factor_history_factor,
    resolve_factor_identifier,
)

__all__ = [
    "FactorHistoryNotReady",
    "FactorHistoryDependencies",
    "cache_get",
    "config",
    "get_factor_history_dependencies",
    "load_factor_history_response",
    "load_factor_return_history",
    "load_runtime_payload",
    "resolve_factor_history_factor",
    "resolve_factor_identifier",
]
