"""Application-facing refresh control surface for routes and control clients."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from backend import config
from backend.ops import cloud_run_jobs
from backend.services.holdings_runtime_state import mark_refresh_finished
from backend.services.holdings_runtime_state import mark_refresh_started
from backend.services.refresh_request_policy import resolve_refresh_request
from backend.services.refresh_status_service import load_persisted_refresh_status
from backend.services.refresh_status_service import persist_refresh_status
from backend.services.refresh_status_service import try_claim_refresh_status


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _persist_state(**updates: Any) -> dict[str, Any]:
    state = load_persisted_refresh_status()
    state.update(updates)
    persist_refresh_status(state)
    return state


def start_refresh(
    *,
    force_risk_recompute: bool,
    profile: str | None = None,
    refresh_scope: str | None = None,
    as_of_date: str | None = None,
    resume_run_id: str | None = None,
    from_stage: str | None = None,
    to_stage: str | None = None,
    force_core: bool = False,
) -> tuple[bool, dict[str, Any]]:
    if not config.serve_refresh_cloud_job_configured():
        from backend.services.refresh_manager import start_refresh as _start_refresh

        return _start_refresh(
            force_risk_recompute=force_risk_recompute,
            profile=profile,
            refresh_scope=refresh_scope,
            as_of_date=as_of_date,
            resume_run_id=resume_run_id,
            from_stage=from_stage,
            to_stage=to_stage,
            force_core=force_core,
        )

    request = resolve_refresh_request(
        profile=profile,
        from_stage=from_stage,
        to_stage=to_stage,
        force_core=force_core,
        force_risk_recompute=force_risk_recompute,
    )
    job_id = uuid.uuid4().hex[:12]
    pipeline_run_id = (str(resume_run_id).strip() if resume_run_id else f"crj_{job_id}")
    now = _now_iso()
    claimed, running_state = try_claim_refresh_status(
        {
            "status": "running",
            "job_id": job_id,
            "pipeline_run_id": pipeline_run_id,
            "profile": request["profile"],
            "requested_profile": (str(profile).strip().lower() if profile else None),
            "mode": request["mode"],
            "refresh_scope": (str(refresh_scope).strip().lower() if refresh_scope else None),
            "as_of_date": (str(as_of_date).strip() if as_of_date else None),
            "resume_run_id": (str(resume_run_id).strip() if resume_run_id else None),
            "from_stage": request["from_stage"],
            "to_stage": request["to_stage"],
            "force_core": bool(request["force_core"]),
            "force_risk_recompute": bool(force_risk_recompute),
            "current_stage": "dispatch",
            "current_stage_message": "Dispatching serve-refresh to Cloud Run Job.",
            "current_stage_heartbeat_at": now,
            "requested_at": now,
            "started_at": now,
            "finished_at": None,
            "result": None,
            "error": None,
            "dispatch_backend": "cloud_run_job",
            "dispatch_id": None,
        }
    )
    if not claimed:
        return False, running_state

    try:
        mark_refresh_started(profile=request["profile"], run_id=pipeline_run_id)
    except Exception:
        pass
    try:
        dispatch = cloud_run_jobs.dispatch_serve_refresh(
            pipeline_run_id=pipeline_run_id,
            profile=request["profile"],
            as_of_date=(str(as_of_date).strip() if as_of_date else None),
            from_stage=request["from_stage"],
            to_stage=request["to_stage"],
            force_core=bool(request["force_core"]),
            refresh_scope=(str(refresh_scope).strip().lower() if refresh_scope else None),
        )
    except Exception as exc:  # noqa: BLE001
        try:
            mark_refresh_finished(
                profile=request["profile"],
                run_id=pipeline_run_id,
                status="failed",
                message=str(exc),
                clear_pending=False,
            )
        except Exception:
            pass
        failed_state = _persist_state(
            status="failed",
            finished_at=_now_iso(),
            current_stage=None,
            current_stage_message=None,
            error={"type": type(exc).__name__, "message": str(exc)},
            dispatch_backend="cloud_run_job",
            dispatch_id=None,
        )
        return False, failed_state

    dispatched_state = _persist_state(
        dispatch_backend="cloud_run_job",
        dispatch_id=str(dispatch.get("execution_name") or "").strip() or None,
        current_stage="dispatch",
        current_stage_message="Refresh dispatched to Cloud Run Job.",
        current_stage_heartbeat_at=_now_iso(),
    )
    return True, dispatched_state


def get_refresh_status() -> dict[str, Any]:
    if config.serve_refresh_cloud_job_configured():
        return load_persisted_refresh_status()

    from backend.services.refresh_manager import get_refresh_status as _get_refresh_status

    return _get_refresh_status()
