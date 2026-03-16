"""GET /api/risk — variance decomposition, factor details, covariance matrix."""

from fastapi import APIRouter
from backend.api.routes.readiness import raise_cache_not_ready
from backend.data.serving_outputs import load_runtime_payload
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


def _normalize_systematic_shares(shares):
    if not isinstance(shares, dict):
        return shares
    clean = dict(shares)
    if "market" not in clean and "country" in clean:
        clean["market"] = clean.get("country")
    clean.pop("country", None)
    return clean


def _normalize_factor_details(rows):
    if not isinstance(rows, list):
        return []
    out = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        clean = dict(row)
        factor_token = str(
            clean.get("factor_id")
            or clean.get("factor")
            or clean.get("factor_name")
            or ""
        ).strip()
        if factor_token:
            clean["factor_id"] = factor_token
            clean.setdefault("factor_name", factor_token)
        if str(clean.get("category") or "").strip().lower() == "country":
            clean["category"] = "market"
        out.append(clean)
    return out


def _normalize_risk_payload(data):
    if not isinstance(data, dict):
        return data
    clean = dict(data)
    clean["risk_shares"] = _normalize_systematic_shares(clean.get("risk_shares"))
    clean["component_shares"] = _normalize_systematic_shares(clean.get("component_shares"))
    clean["factor_details"] = _normalize_factor_details(clean.get("factor_details"))
    return clean


@router.get("/risk")
async def get_risk():
    data = load_runtime_payload("risk", fallback_loader=cache_get)
    if data is None:
        raise_cache_not_ready(
            cache_key="risk",
            message="Risk cache is not ready yet. Run refresh and try again.",
        )
    if not _risk_payload_complete(data):
        raise_cache_not_ready(
            cache_key="risk",
            message="Risk cache exists but is incomplete. Run a core refresh and try again.",
            refresh_profile="cold-core",
        )
    sanity = load_runtime_payload("model_sanity", fallback_loader=cache_get)
    if sanity is None:
        sanity = {"status": "no-data", "warnings": [], "checks": {}}
    normalized = _normalize_risk_payload(data)
    return {**normalized, "model_sanity": sanity, "_cached": True}
