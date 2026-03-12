"""Portfolio serving + what-if preview routes."""

from typing import Any

from fastapi import APIRouter
from fastapi import HTTPException
from pydantic import BaseModel, Field

from backend import config
from backend.api.routes.presenters import normalize_trbc_sector_fields
from backend.api.routes.readiness import raise_cache_not_ready
from backend.data.serving_outputs import load_current_payload
from backend.data.sqlite import cache_get
from backend.services.portfolio_whatif import preview_portfolio_whatif

router = APIRouter()


class WhatIfScenarioRow(BaseModel):
    account_id: str
    quantity: float
    ticker: str | None = None
    ric: str | None = None
    source: str | None = None


class WhatIfPreviewRequest(BaseModel):
    scenario_rows: list[WhatIfScenarioRow] = Field(default_factory=list)


@router.get("/portfolio")
async def get_portfolio():
    data = load_current_payload("portfolio")
    if data is None and not config.cloud_mode():
        data = cache_get("portfolio")
    if data is None:
        raise_cache_not_ready(
            cache_key="portfolio",
            message="Portfolio cache is empty. Run refresh to build positions.",
            refresh_mode="light",
        )
    positions = []
    for raw in data.get("positions", []):
        positions.append(normalize_trbc_sector_fields(raw))
    return {**data, "positions": positions, "_cached": True}


@router.post("/portfolio/whatif")
async def post_portfolio_whatif(payload: WhatIfPreviewRequest):
    try:
        out = preview_portfolio_whatif(
            scenario_rows=[dict(row) for row in payload.model_dump().get("scenario_rows", [])],
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"What-if preview failed: {exc}") from exc

    def _normalize_positions(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [normalize_trbc_sector_fields(dict(row)) for row in rows]

    out["current"]["positions"] = _normalize_positions(out["current"].get("positions", []))
    out["hypothetical"]["positions"] = _normalize_positions(out["hypothetical"].get("positions", []))
    return out
