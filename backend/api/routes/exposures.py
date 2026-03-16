"""GET /api/exposures?mode= — per-factor values with position-level drilldown."""

from __future__ import annotations

import math

from fastapi import APIRouter, Query

from backend import config
from backend.api.routes.readiness import raise_cache_not_ready
from backend.data.history_queries import load_factor_return_history, resolve_factor_history_factor
from backend.data.serving_outputs import load_runtime_payload
from backend.data.sqlite import cache_get
from backend.services.dashboard_payload_service import (
    DashboardPayloadNotReady,
    load_exposures_response,
)

router = APIRouter()


def _resolve_factor_name(factor_id: str) -> tuple[str, str]:
    clean = str(factor_id or "").strip()
    if not clean:
        return "", ""
    payload_names = ("risk", "universe_factors", "universe_loadings")
    for payload_name in payload_names:
        payload = load_runtime_payload(payload_name, fallback_loader=cache_get)
        catalog = (payload or {}).get("factor_catalog") if isinstance(payload, dict) else None
        if not isinstance(catalog, list):
            continue
        for entry in catalog:
            if not isinstance(entry, dict):
                continue
            entry_id = str(entry.get("factor_id") or "").strip()
            entry_name = str(entry.get("factor_name") or "").strip()
            if clean == entry_id or clean == entry_name:
                return entry_id or clean, entry_name or clean
    return resolve_factor_history_factor(
        config.SQLITE_PATH,
        factor_token=clean,
    )

@router.get("/exposures")
async def get_exposures(mode: str = Query("raw", pattern="^(raw|sensitivity|risk_contribution)$")):
    try:
        return load_exposures_response(
            mode=mode,
            payload_loader=load_runtime_payload,
            fallback_loader=cache_get,
        )
    except DashboardPayloadNotReady as exc:
        raise_cache_not_ready(
            cache_key=exc.cache_key,
            message=exc.message,
            refresh_profile=exc.refresh_profile,
        )


@router.get("/exposures/history")
async def get_exposure_history(
    factor_id: str = Query(..., min_length=1),
    years: int = Query(5, ge=1, le=10),
):
    resolved_factor_id, factor_name = _resolve_factor_name(factor_id)
    latest, rows = load_factor_return_history(
        config.SQLITE_PATH,
        factor=str(factor_name),
        years=int(years),
    )
    if latest is None:
        raise_cache_not_ready(
            cache_key="daily_factor_returns",
            message="Historical factor returns are not available yet.",
            refresh_profile="cold-core",
        )

    if not rows:
        return {"factor_id": resolved_factor_id, "factor_name": factor_name, "years": years, "points": [], "_cached": True}

    points = []
    cumulative = 1.0
    for dt, raw_ret in rows:
        r = float(raw_ret or 0.0)
        if not math.isfinite(r):
            r = 0.0
        cumulative *= (1.0 + r)
        points.append({
            "date": str(dt),
            "factor_return": round(r, 8),
            "cum_return": round(cumulative - 1.0, 8),
        })

    return {"factor_id": resolved_factor_id, "factor_name": factor_name, "years": years, "points": points, "_cached": True}
