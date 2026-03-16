"""Portfolio serving + what-if preview routes."""

from typing import Any

from fastapi import APIRouter
from fastapi import Header
from fastapi import HTTPException
from pydantic import BaseModel, Field, FiniteFloat

from backend import config
from backend.api.auth import require_role
from backend.api.routes.presenters import normalize_trbc_sector_fields
from backend.api.routes.readiness import raise_cache_not_ready
from backend.data.serving_outputs import load_runtime_payload
from backend.data.sqlite import cache_get
from backend.services import holdings_service
from backend.services.portfolio_whatif import preview_portfolio_whatif

router = APIRouter()
MAX_WHATIF_SCENARIO_ROWS = 100


class WhatIfScenarioRow(BaseModel):
    account_id: str
    quantity: FiniteFloat
    ticker: str
    ric: str | None = None
    source: str | None = None


class WhatIfPreviewRequest(BaseModel):
    scenario_rows: list[WhatIfScenarioRow] = Field(default_factory=list)


class WhatIfApplyRequest(BaseModel):
    scenario_rows: list[WhatIfScenarioRow] = Field(default_factory=list)
    requested_by: str | None = None
    default_source: str = "what_if"


@router.get("/portfolio")
async def get_portfolio():
    data = load_runtime_payload("portfolio", fallback_loader=cache_get)
    if data is None:
        raise_cache_not_ready(
            cache_key="portfolio",
            message="Portfolio cache is empty. Run refresh to build positions.",
        )
    positions = []
    for raw in data.get("positions", []):
        positions.append(normalize_trbc_sector_fields(raw))
    return {**data, "positions": positions, "_cached": True}


@router.post("/portfolio/whatif")
async def post_portfolio_whatif(
    payload: WhatIfPreviewRequest,
    x_operator_token: str | None = Header(default=None, alias="X-Operator-Token"),
    authorization: str | None = Header(default=None),
):
    if config.cloud_mode():
        require_role(
            "operator",
            x_operator_token=x_operator_token,
            authorization=authorization,
        )
    scenario_rows = [dict(row) for row in payload.model_dump().get("scenario_rows", [])]
    if len(scenario_rows) > MAX_WHATIF_SCENARIO_ROWS:
        raise HTTPException(status_code=400, detail=f"Too many what-if rows. Max {MAX_WHATIF_SCENARIO_ROWS}.")
    try:
        out = preview_portfolio_whatif(
            scenario_rows=scenario_rows,
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


@router.post("/portfolio/whatif/apply")
async def post_portfolio_whatif_apply(
    payload: WhatIfApplyRequest,
    x_editor_token: str | None = Header(default=None, alias="X-Editor-Token"),
    x_operator_token: str | None = Header(default=None, alias="X-Operator-Token"),
    authorization: str | None = Header(default=None),
):
    require_role(
        "editor",
        x_editor_token=x_editor_token,
        x_operator_token=x_operator_token,
        authorization=authorization,
    )
    scenario_rows = [dict(row) for row in payload.model_dump().get("scenario_rows", [])]
    if len(scenario_rows) > MAX_WHATIF_SCENARIO_ROWS:
        raise HTTPException(status_code=400, detail=f"Too many what-if rows. Max {MAX_WHATIF_SCENARIO_ROWS}.")
    try:
        out = holdings_service.run_whatif_apply(
            scenario_rows=scenario_rows,
            requested_by=payload.requested_by,
            default_source=payload.default_source,
            dry_run=False,
        )
        out["refresh"] = None
        return out
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=503, detail=f"What-if apply failed: {exc}") from exc
