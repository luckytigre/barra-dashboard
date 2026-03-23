"""Single-process refresh manager with background execution and status tracking."""

from __future__ import annotations

import logging
import threading
import traceback
import uuid
from datetime import datetime, timezone
from typing import Any

from backend import config
from backend.data.sqlite import cache_get, cache_set
from backend.orchestration.refresh_execution import run_refresh_execution
from backend.services.holdings_runtime_state import mark_refresh_started
from backend.services.holdings_runtime_state import mark_refresh_finished
from backend.services.refresh_request_policy import _resolve_profile as _resolve_profile_request
from backend.services.refresh_request_policy import resolve_refresh_request
from backend.services.refresh_status_service import default_refresh_status_state
from backend.services.refresh_status_service import load_persisted_refresh_status
from backend.services.refresh_status_service import persist_refresh_status

logger = logging.getLogger(__name__)

_RUN_LOCK = threading.Lock()
_STATE_LOCK = threading.Lock()
_ACTIVE_WORKER: threading.Thread | None = None
_STATE_LOADED = False


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _default_state() -> dict[str, Any]:
    return default_refresh_status_state()


def _persist_state(state: dict[str, Any]) -> dict[str, Any]:
    return persist_refresh_status(state, fallback_writer=cache_set)


def _load_initial_state() -> dict[str, Any]:
    base = load_persisted_refresh_status(fallback_loader=cache_get)
    if base.get("status") == "running":
        # After process restart, a previously running task cannot be resumed.
        base["status"] = "unknown"
        base["error"] = {
            "type": "process_restart",
            "message": "Refresh state was running before process restart.",
        }
        base["finished_at"] = _now_iso()
    return base


_STATE = default_refresh_status_state()


def _resolve_profile(profile: str | None) -> str:
    """Compatibility shim for legacy tests and local callers."""
    return _resolve_profile_request(profile)


def _ensure_state_loaded() -> None:
    global _STATE_LOADED, _STATE
    with _STATE_LOCK:
        if _STATE_LOADED:
            return
    loaded = _load_initial_state()
    with _STATE_LOCK:
        if _STATE_LOADED:
            return
        _STATE = loaded
        _STATE_LOADED = True


def _snapshot() -> dict[str, Any]:
    _ensure_state_loaded()
    with _STATE_LOCK:
        return dict(_STATE)


def _set_active_worker(worker: threading.Thread | None) -> None:
    global _ACTIVE_WORKER
    with _STATE_LOCK:
        _ACTIVE_WORKER = worker


def _get_active_worker() -> threading.Thread | None:
    with _STATE_LOCK:
        return _ACTIVE_WORKER


def _set_state(**updates: Any) -> dict[str, Any]:
    suppress_persist_errors = bool(updates.pop("suppress_persist_errors", False))
    _ensure_state_loaded()
    with _STATE_LOCK:
        _STATE.update(updates)
        snap = dict(_STATE)
    try:
        persist_refresh_status(snap, fallback_writer=cache_set)
    except Exception:
        if not suppress_persist_errors:
            raise
        logger.exception("Failed to persist refresh status to runtime_state")
    return snap


def _reconcile_orphaned_running_state() -> dict[str, Any]:
    snap = _snapshot()
    if str(snap.get("status") or "") != "running":
        return snap
    worker = _get_active_worker()
    if worker is not None and worker.is_alive():
        return snap
    if _RUN_LOCK.locked():
        try:
            _RUN_LOCK.release()
        except RuntimeError:
            logger.exception("Failed to release orphaned refresh lock")
    try:
        mark_refresh_finished(
            profile=str(snap.get("profile") or "") or None,
            run_id=str(snap.get("pipeline_run_id") or "") or None,
            status="unknown",
            message="Refresh worker is no longer running; state reconciled locally.",
            clear_pending=False,
        )
    except Exception:
        logger.exception("Failed to reconcile holdings state for orphaned refresh")
    reconciled = _set_state(
        status="unknown",
        finished_at=_now_iso(),
        current_stage=None,
        current_stage_substage=None,
        current_stage_substage_status=None,
        current_stage_diagnostics_section=None,
        stage_started_at=None,
        current_stage_message=None,
        current_stage_progress_pct=None,
        current_stage_items_processed=None,
        current_stage_items_total=None,
        current_stage_unit=None,
        current_stage_heartbeat_at=None,
        error={
            "type": "refresh_worker_missing",
            "message": "Refresh state was running but no background worker is alive.",
        },
        suppress_persist_errors=True,
    )
    _set_active_worker(None)
    return reconciled


def get_refresh_status() -> dict[str, Any]:
    """Return current or most recent refresh status."""
    return _reconcile_orphaned_running_state()


def _run_in_background(
    *,
    job_id: str,
    pipeline_run_id: str,
    profile: str,
    mode: str,
    as_of_date: str | None,
    resume_run_id: str | None,
    from_stage: str | None,
    to_stage: str | None,
    force_core: bool,
    refresh_scope: str | None = None,
) -> None:
    try:
        run_refresh_execution(
            job_id=job_id,
            pipeline_run_id=pipeline_run_id,
            profile=profile,
            mode=mode,
            as_of_date=as_of_date,
            resume_run_id=resume_run_id,
            from_stage=from_stage,
            to_stage=to_stage,
            force_core=force_core,
            refresh_scope=refresh_scope,
            snapshot_state=_snapshot,
            set_state=_set_state,
        )
    finally:
        _set_active_worker(None)
        _RUN_LOCK.release()
        logger.info("Background refresh %s finished", job_id)


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
    """Start refresh in a background thread. Returns (started, status)."""
    request = resolve_refresh_request(
        profile=profile,
        from_stage=from_stage,
        to_stage=to_stage,
        force_core=force_core,
        force_risk_recompute=force_risk_recompute,
    )
    resolved_profile = str(request["profile"])
    mode = str(request["mode"])
    stage_from = request["from_stage"]
    stage_to = request["to_stage"]
    force_core_effective = bool(request["force_core"])

    if not _RUN_LOCK.acquire(blocking=False):
        reconciled = _reconcile_orphaned_running_state()
        if not _RUN_LOCK.acquire(blocking=False):
            return False, reconciled

    job_id = uuid.uuid4().hex[:12]
    pipeline_run_id = (str(resume_run_id).strip() if resume_run_id else f"api_{job_id}")
    now = _now_iso()
    running_state = _set_state(
        status="running",
        job_id=job_id,
        pipeline_run_id=pipeline_run_id,
        profile=resolved_profile,
        requested_profile=(str(profile).strip().lower() if profile else None),
        mode=mode,
        refresh_scope=(str(refresh_scope).strip().lower() if refresh_scope else None),
        as_of_date=(str(as_of_date).strip() if as_of_date else None),
        resume_run_id=(str(resume_run_id).strip() if resume_run_id else None),
        from_stage=stage_from,
        to_stage=stage_to,
        force_core=bool(force_core_effective),
        force_risk_recompute=bool(force_risk_recompute),
        current_stage=None,
        current_stage_substage=None,
        current_stage_substage_status=None,
        current_stage_diagnostics_section=None,
        stage_index=None,
        stage_count=None,
        stage_started_at=None,
        current_stage_message=None,
        current_stage_progress_pct=None,
        current_stage_items_processed=None,
        current_stage_items_total=None,
        current_stage_unit=None,
        current_stage_heartbeat_at=None,
        serving_publish_completed_at=None,
        serving_publish_snapshot_id=None,
        serving_publish_run_id=None,
        serving_publish_payload_count=None,
        requested_at=now,
        started_at=now,
        finished_at=None,
        result=None,
        error=None,
    )
    try:
        mark_refresh_started(profile=resolved_profile, run_id=pipeline_run_id)
    except Exception:
        logger.exception("Failed to mark holdings refresh start state")

    worker = threading.Thread(
        target=_run_in_background,
        kwargs={
            "job_id": job_id,
            "pipeline_run_id": pipeline_run_id,
            "profile": resolved_profile,
            "mode": str(mode),
            "refresh_scope": (str(refresh_scope).strip().lower() if refresh_scope else None),
            "as_of_date": (str(as_of_date).strip() if as_of_date else None),
            "resume_run_id": (str(resume_run_id).strip() if resume_run_id else None),
            "from_stage": stage_from,
            "to_stage": stage_to,
            "force_core": bool(force_core_effective),
        },
        name=f"refresh-{job_id}",
        daemon=True,
    )
    try:
        worker.start()
        _set_active_worker(worker)
    except Exception as exc:  # noqa: BLE001
        _set_active_worker(None)
        _RUN_LOCK.release()
        try:
            mark_refresh_finished(
                profile=resolved_profile,
                run_id=pipeline_run_id,
                status="failed",
                message=str(exc),
                clear_pending=False,
            )
        except Exception:
            logger.exception("Failed to mark holdings refresh worker-start failure state")
        failed_state = _set_state(
            status="failed",
            finished_at=_now_iso(),
            error={"type": type(exc).__name__, "message": str(exc)},
            suppress_persist_errors=True,
        )
        return False, failed_state
    return True, running_state
