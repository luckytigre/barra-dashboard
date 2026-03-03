"""Single-process refresh manager with background execution and status tracking."""

from __future__ import annotations

import logging
import threading
import traceback
import uuid
from datetime import datetime, timezone
from typing import Any

from analytics.pipeline import run_refresh
from db.sqlite import cache_get, cache_set

logger = logging.getLogger(__name__)

_STATUS_CACHE_KEY = "refresh_status"
_RUN_LOCK = threading.Lock()
_STATE_LOCK = threading.Lock()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _default_state() -> dict[str, Any]:
    return {
        "status": "idle",
        "job_id": None,
        "mode": None,
        "force_risk_recompute": False,
        "requested_at": None,
        "started_at": None,
        "finished_at": None,
        "result": None,
        "error": None,
    }


def _load_initial_state() -> dict[str, Any]:
    cached = cache_get(_STATUS_CACHE_KEY)
    if isinstance(cached, dict):
        base = _default_state()
        base.update(cached)
        if base.get("status") == "running":
            # After process restart, a previously running task cannot be resumed.
            base["status"] = "unknown"
            base["error"] = {
                "type": "process_restart",
                "message": "Refresh state was running before process restart.",
            }
            base["finished_at"] = _now_iso()
        return base
    return _default_state()


_STATE = _load_initial_state()


def _snapshot() -> dict[str, Any]:
    with _STATE_LOCK:
        return dict(_STATE)


def _set_state(**updates: Any) -> dict[str, Any]:
    with _STATE_LOCK:
        _STATE.update(updates)
        snap = dict(_STATE)
    cache_set(_STATUS_CACHE_KEY, snap)
    return snap


def get_refresh_status() -> dict[str, Any]:
    """Return current or most recent refresh status."""
    return _snapshot()


def _run_in_background(job_id: str, mode: str, force_risk_recompute: bool) -> None:
    try:
        result = run_refresh(mode=mode, force_risk_recompute=force_risk_recompute)
        _set_state(
            status="ok",
            finished_at=_now_iso(),
            result=result,
            error=None,
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("Background refresh failed")
        _set_state(
            status="failed",
            finished_at=_now_iso(),
            error={
                "type": type(exc).__name__,
                "message": str(exc),
                "traceback": traceback.format_exc(limit=12),
            },
        )
    finally:
        _RUN_LOCK.release()
        logger.info("Background refresh %s finished", job_id)


def start_refresh(mode: str, force_risk_recompute: bool) -> tuple[bool, dict[str, Any]]:
    """Start refresh in a background thread. Returns (started, status)."""
    if not _RUN_LOCK.acquire(blocking=False):
        return False, _snapshot()

    job_id = uuid.uuid4().hex[:12]
    now = _now_iso()
    running_state = _set_state(
        status="running",
        job_id=job_id,
        mode=mode,
        force_risk_recompute=bool(force_risk_recompute),
        requested_at=now,
        started_at=now,
        finished_at=None,
        result=None,
        error=None,
    )

    worker = threading.Thread(
        target=_run_in_background,
        args=(job_id, mode, bool(force_risk_recompute)),
        name=f"refresh-{job_id}",
        daemon=True,
    )
    try:
        worker.start()
    except Exception as exc:  # noqa: BLE001
        _RUN_LOCK.release()
        failed_state = _set_state(
            status="failed",
            finished_at=_now_iso(),
            error={"type": type(exc).__name__, "message": str(exc)},
        )
        return False, failed_state
    return True, running_state
