"""GET /api/risk — variance decomposition, factor details, covariance matrix."""

from fastapi import APIRouter
from db.sqlite import cache_get
from routes.readiness import raise_cache_not_ready

router = APIRouter()


@router.get("/risk")
async def get_risk():
    data = cache_get("risk")
    if data is None:
        raise_cache_not_ready(
            cache_key="risk",
            message="Risk cache is not ready yet. Run refresh and try again.",
            refresh_mode="light",
        )
    sanity = cache_get("model_sanity") or {"status": "no-data", "warnings": [], "checks": {}}
    return {**data, "model_sanity": sanity, "_cached": True}
