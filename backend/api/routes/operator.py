"""GET /api/operator/status — operator-facing run-lane summary and recency."""

from __future__ import annotations

from fastapi import APIRouter, Header

from backend import config
from backend.api.auth import require_role
from backend.services import operator_status_service

router = APIRouter()
profile_catalog = operator_status_service.profile_catalog
job_runs = operator_status_service.job_runs
core_reads = operator_status_service.core_reads
runtime_state = operator_status_service.runtime_state
sqlite = operator_status_service.sqlite
get_refresh_status = operator_status_service.get_refresh_status
get_holdings_sync_state = operator_status_service.get_holdings_sync_state
_now_iso = operator_status_service._now_iso
_today_session_date = operator_status_service._today_session_date
_risk_recompute_due = operator_status_service._risk_recompute_due
_load_authoritative_operator_source_dates = operator_status_service._load_authoritative_operator_source_dates
_load_local_archive_source_dates = operator_status_service._load_local_archive_source_dates


def build_operator_status_payload():
    return operator_status_service.build_operator_status_payload()


@router.get("/operator/status")
def get_operator_status(
    x_operator_token: str | None = Header(default=None, alias="X-Operator-Token"),
    x_refresh_token: str | None = Header(default=None, alias="X-Refresh-Token"),
    authorization: str | None = Header(default=None),
):
    if config.cloud_mode():
        require_role(
            "operator",
            x_operator_token=x_operator_token,
            x_refresh_token=x_refresh_token,
            authorization=authorization,
        )
    return build_operator_status_payload()
