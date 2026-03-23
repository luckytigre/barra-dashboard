"""Holdings management API (Neon-backed)."""

from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, Header, HTTPException, Query
from pydantic import BaseModel, Field

from backend.api.auth import require_role
from backend.services import cuse4_holdings_service as holdings_service

router = APIRouter()


class HoldingsImportRow(BaseModel):
    account_id: str | None = None
    ric: str | None = None
    ticker: str | None = None
    quantity: float
    source: str | None = None


class HoldingsImportRequest(BaseModel):
    account_id: str
    mode: Literal["replace_account", "upsert_absolute", "increment_delta"]
    rows: list[HoldingsImportRow] = Field(default_factory=list)
    filename: str | None = None
    requested_by: str | None = None
    notes: str | None = None
    default_source: str = "csv_upload"
    dry_run: bool = False
    trigger_refresh: bool = True


class HoldingsPositionEditRequest(BaseModel):
    account_id: str
    quantity: float
    ric: str | None = None
    ticker: str | None = None
    source: str = "ui_edit"
    requested_by: str | None = None
    notes: str | None = None
    dry_run: bool = False
    trigger_refresh: bool = True


class HoldingsPositionRemoveRequest(BaseModel):
    account_id: str
    ric: str | None = None
    ticker: str | None = None
    requested_by: str | None = None
    notes: str | None = None
    dry_run: bool = False
    trigger_refresh: bool = True


@router.get("/holdings/modes")
async def get_holdings_modes():
    return {
        "modes": sorted(holdings_service.IMPORT_MODES),
        "default": "upsert_absolute",
    }


@router.get("/holdings/accounts")
async def get_holdings_accounts():
    try:
        rows = holdings_service.load_holdings_accounts()
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=503, detail=f"Neon not available: {exc}") from exc
    return {"accounts": rows}


@router.get("/holdings/positions")
async def get_holdings_positions(account_id: str | None = Query(default=None)):
    try:
        rows = holdings_service.load_holdings_positions(account_id=account_id)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=503, detail=f"Neon not available: {exc}") from exc
    return {
        "positions": rows,
        "account_id": account_id,
        "count": int(len(rows)),
    }


@router.post("/holdings/import")
async def post_holdings_import(
    payload: HoldingsImportRequest,
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
    try:
        return holdings_service.run_holdings_import(
            account_id=payload.account_id,
            mode=str(payload.mode),
            rows=[r.model_dump() for r in payload.rows],
            filename=payload.filename,
            requested_by=payload.requested_by,
            notes=payload.notes,
            default_source=payload.default_source,
            dry_run=bool(payload.dry_run),
            trigger_refresh=bool(payload.trigger_refresh),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=503, detail=f"Neon not available: {exc}") from exc


@router.post("/holdings/position")
async def post_holdings_position(
    payload: HoldingsPositionEditRequest,
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
    try:
        return holdings_service.run_position_upsert(
            account_id=payload.account_id,
            quantity=payload.quantity,
            ric=payload.ric,
            ticker=payload.ticker,
            source=payload.source,
            requested_by=payload.requested_by,
            notes=payload.notes,
            dry_run=bool(payload.dry_run),
            trigger_refresh=bool(payload.trigger_refresh),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=503, detail=f"Neon not available: {exc}") from exc


@router.post("/holdings/position/remove")
async def post_holdings_position_remove(
    payload: HoldingsPositionRemoveRequest,
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
    try:
        return holdings_service.run_position_remove(
            account_id=payload.account_id,
            ric=payload.ric,
            ticker=payload.ticker,
            requested_by=payload.requested_by,
            notes=payload.notes,
            dry_run=bool(payload.dry_run),
            trigger_refresh=bool(payload.trigger_refresh),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=503, detail=f"Neon not available: {exc}") from exc
