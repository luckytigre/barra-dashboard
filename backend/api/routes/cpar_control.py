"""POST /api/cpar/build — operator-only cPAR package-build dispatch."""

from __future__ import annotations

from fastapi import APIRouter, Header, HTTPException, Query, status

from backend import config
from backend.api.auth import require_role

router = APIRouter()


@router.post("/cpar/build", status_code=202)
def dispatch_cpar_build(
    profile: str = Query("cpar-weekly"),
    as_of_date: str | None = Query(None),
    x_operator_token: str | None = Header(default=None, alias="X-Operator-Token"),
    authorization: str | None = Header(default=None),
):
    require_role(
        "operator",
        x_operator_token=x_operator_token,
        authorization=authorization,
    )

    from backend.services.cpar_build_service import dispatch_cpar_build as _dispatch

    try:
        ok, result = _dispatch(profile=profile, as_of_date=as_of_date)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc
    if not ok:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=result,
        )
    return result
