"""Explicit cUSE4 alias for default dashboard payload assembly."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from backend.services import dashboard_payload_service as _legacy


DashboardPayloadNotReady = _legacy.DashboardPayloadNotReady
cache_get = _legacy.cache_get
load_runtime_payload = _legacy.load_runtime_payload


def load_exposures_response(
    *,
    mode: str,
    payload_loader: Callable[..., Any] | None = None,
    fallback_loader=None,
) -> dict[str, Any]:
    return _legacy.load_exposures_response(
        mode=mode,
        payload_loader=payload_loader or load_runtime_payload,
        fallback_loader=fallback_loader or cache_get,
    )


def load_risk_response(
    *,
    payload_loader: Callable[..., Any] | None = None,
    fallback_loader=None,
) -> dict[str, Any]:
    return _legacy.load_risk_response(
        payload_loader=payload_loader or load_runtime_payload,
        fallback_loader=fallback_loader or cache_get,
    )


def load_portfolio_response(
    *,
    position_normalizer=None,
    payload_loader: Callable[..., Any] | None = None,
    fallback_loader=None,
) -> dict[str, Any]:
    return _legacy.load_portfolio_response(
        position_normalizer=position_normalizer,
        payload_loader=payload_loader or load_runtime_payload,
        fallback_loader=fallback_loader or cache_get,
    )


__all__ = [
    "DashboardPayloadNotReady",
    "cache_get",
    "load_exposures_response",
    "load_portfolio_response",
    "load_risk_response",
    "load_runtime_payload",
]
