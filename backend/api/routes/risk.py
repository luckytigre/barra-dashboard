"""GET /api/risk — variance decomposition, factor details, covariance matrix."""

from fastapi import APIRouter
from backend import config
from backend.api.routes.readiness import raise_cache_not_ready
from backend.data.serving_outputs import load_current_payload
from backend.data.sqlite import cache_get

router = APIRouter()


def _risk_payload_complete(data) -> bool:
    if not isinstance(data, dict):
        return False
    cov = data.get("cov_matrix") if isinstance(data, dict) else {}
    factors = cov.get("factors") if isinstance(cov, dict) else []
    correlation = cov.get("correlation") if isinstance(cov, dict) else []
    matrix = cov.get("matrix") if isinstance(cov, dict) else []
    cov_rows = correlation if isinstance(correlation, list) and correlation else matrix
    risk_engine = data.get("risk_engine") if isinstance(data, dict) else {}
    specific_count = int((risk_engine or {}).get("specific_risk_ticker_count") or 0)
    return bool(
        isinstance(factors, list)
        and factors
        and isinstance(cov_rows, list)
        and cov_rows
        and specific_count > 0
    )


@router.get("/risk")
async def get_risk():
    data = load_current_payload("risk")
    if data is None and not config.cloud_mode():
        data = cache_get("risk")
    if data is None:
        raise_cache_not_ready(
            cache_key="risk",
            message="Risk cache is not ready yet. Run refresh and try again.",
            refresh_mode="light",
        )
    if not _risk_payload_complete(data):
        raise_cache_not_ready(
            cache_key="risk",
            message="Risk cache exists but is incomplete. Run a core refresh and try again.",
            refresh_mode="full",
        )
    sanity = load_current_payload("model_sanity")
    if sanity is None and not config.cloud_mode():
        sanity = cache_get("model_sanity")
    if sanity is None:
        sanity = {"status": "no-data", "warnings": [], "checks": {}}
    return {**data, "model_sanity": sanity, "_cached": True}
