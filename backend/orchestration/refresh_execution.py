"""Shared refresh execution flow for local threads and Cloud Run Jobs."""

from __future__ import annotations

import logging
import traceback
from datetime import datetime, timezone
from typing import Any
from typing import Callable

from backend.orchestration.run_model_pipeline import run_model_pipeline
from backend.services.holdings_runtime_state import mark_refresh_finished

logger = logging.getLogger(__name__)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def run_refresh_execution(
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
    refresh_scope: str | None,
    snapshot_state: Callable[[], dict[str, Any]],
    set_state: Callable[..., dict[str, Any]],
) -> dict[str, Any]:
    try:
        def _stage_callback(event: dict[str, Any]) -> None:
            snap = snapshot_state()
            set_state(
                current_stage=event.get("stage"),
                current_stage_substage=event.get("refresh_substage"),
                current_stage_substage_status=event.get("substage_status"),
                current_stage_diagnostics_section=event.get("diagnostics_section"),
                stage_index=event.get("stage_index"),
                stage_count=event.get("stage_count"),
                stage_started_at=event.get("started_at"),
                current_stage_message=event.get("message"),
                current_stage_progress_pct=event.get("progress_pct"),
                current_stage_items_processed=event.get("items_processed"),
                current_stage_items_total=event.get("items_total"),
                current_stage_unit=event.get("unit"),
                current_stage_heartbeat_at=_now_iso(),
                serving_publish_completed_at=(
                    event.get("published_at")
                    if bool(event.get("publish_complete"))
                    else snap.get("serving_publish_completed_at")
                ),
                serving_publish_snapshot_id=(
                    event.get("published_snapshot_id")
                    if bool(event.get("publish_complete"))
                    else snap.get("serving_publish_snapshot_id")
                ),
                serving_publish_run_id=(
                    event.get("published_run_id")
                    if bool(event.get("publish_complete"))
                    else snap.get("serving_publish_run_id")
                ),
                serving_publish_payload_count=(
                    event.get("published_payload_count")
                    if bool(event.get("publish_complete"))
                    else snap.get("serving_publish_payload_count")
                ),
            )

        result = run_model_pipeline(
            profile=profile,
            as_of_date=as_of_date,
            run_id=(None if resume_run_id else pipeline_run_id),
            resume_run_id=resume_run_id,
            from_stage=from_stage,
            to_stage=to_stage,
            force_core=bool(force_core),
            refresh_scope=refresh_scope,
            stage_callback=_stage_callback,
        )
        terminal = "ok" if str(result.get("status") or "") == "ok" else "failed"
        message = None if terminal == "ok" else "Orchestrated pipeline returned failed status."
        try:
            mark_refresh_finished(
                profile=profile,
                run_id=str(result.get("run_id") or pipeline_run_id).strip() or None,
                status=terminal,
                message=message,
                clear_pending=False,
            )
        except Exception:
            logger.exception("Failed to mark holdings refresh terminal state")
        set_state(
            status=terminal,
            pipeline_run_id=result.get("run_id"),
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
            result=result,
            error=None if terminal == "ok" else {
                "type": "pipeline_failed",
                "message": message,
            },
        )
        return {"status": terminal, "result": result}
    except Exception as exc:  # noqa: BLE001
        logger.exception("Refresh execution failed")
        try:
            mark_refresh_finished(
                profile=profile,
                run_id=str(pipeline_run_id).strip() or None,
                status="failed",
                message=str(exc),
                clear_pending=False,
            )
        except Exception:
            logger.exception("Failed to mark holdings refresh failure state")
        error = {
            "type": type(exc).__name__,
            "message": str(exc),
            "traceback": traceback.format_exc(limit=12),
        }
        set_state(
            status="failed",
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
            error=error,
        )
        return {"status": "failed", "error": error}
