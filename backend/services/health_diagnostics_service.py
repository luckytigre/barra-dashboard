"""Compatibility shim for cUSE4 health-diagnostics route semantics.

Prefer importing ``backend.services.cuse4_health_diagnostics_service`` from
the default cUSE4 route family. This module remains only for older callers and
tests that still import the legacy path directly.
"""

from __future__ import annotations

from backend.services.cuse4_health_diagnostics_service import (
    HealthDiagnosticsNotReady,
    HealthDiagnosticsReaders,
    cache_get,
    get_health_diagnostics_readers,
    load_health_diagnostics_payload,
    load_runtime_payload,
)

__all__ = [
    "HealthDiagnosticsNotReady",
    "HealthDiagnosticsReaders",
    "cache_get",
    "get_health_diagnostics_readers",
    "load_health_diagnostics_payload",
    "load_runtime_payload",
]
