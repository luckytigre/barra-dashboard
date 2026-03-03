"""GET /api/health/diagnostics — model health diagnostics payload."""

from __future__ import annotations

from fastapi import APIRouter

from db.sqlite import cache_get
from routes.readiness import raise_cache_not_ready

router = APIRouter()


@router.get("/health/diagnostics")
async def get_health_diagnostics():
    data = cache_get("health_diagnostics")
    if data is not None:
        return {**data, "_cached": True}
    raise_cache_not_ready(
        cache_key="health_diagnostics",
        message="Health diagnostics are not ready yet. Run a full refresh.",
        refresh_mode="full",
    )
