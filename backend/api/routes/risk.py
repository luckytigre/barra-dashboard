"""GET /api/risk — variance decomposition, factor details, covariance matrix."""

from fastapi import APIRouter
from backend.api.routes.readiness import raise_cache_not_ready
from backend.data.serving_outputs import load_runtime_payload
from backend.data.sqlite import cache_get
from backend.services.dashboard_payload_service import (
    DashboardPayloadNotReady,
    load_risk_response,
)

router = APIRouter()


@router.get("/risk")
async def get_risk():
    try:
        return load_risk_response(
            payload_loader=load_runtime_payload,
            fallback_loader=cache_get,
        )
    except DashboardPayloadNotReady as exc:
        raise_cache_not_ready(
            cache_key=exc.cache_key,
            message=exc.message,
            refresh_profile=exc.refresh_profile,
        )
