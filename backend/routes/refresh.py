"""POST /api/refresh — refresh caches and weekly-gated risk-engine state."""

from fastapi import APIRouter
from fastapi import Query
from fastapi.responses import JSONResponse

from services.refresh_manager import get_refresh_status, start_refresh

router = APIRouter()


@router.post("/refresh", status_code=202)
async def refresh(
    force_risk_recompute: bool = Query(False),
    mode: str = Query("full"),
):
    clean_mode = str(mode or "full").strip().lower()
    if clean_mode not in {"full", "light"}:
        clean_mode = "full"
    started, state = start_refresh(
        force_risk_recompute=bool(force_risk_recompute),
        mode=clean_mode,
    )
    if not started:
        return JSONResponse(
            status_code=409,
            content={
                "status": "busy",
                "message": "A refresh is already running.",
                "refresh": state,
            },
        )
    return {
        "status": "accepted",
        "message": "Refresh started in background.",
        "refresh": state,
    }


@router.get("/refresh/status")
async def refresh_status():
    return {
        "status": "ok",
        "refresh": get_refresh_status(),
    }
