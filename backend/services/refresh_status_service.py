"""Read-only refresh-status persistence helpers."""

from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
from typing import Any

from psycopg.rows import dict_row

from backend import config
from backend.data import runtime_state
from backend.data.neon import connect, resolve_dsn

_STATUS_CACHE_KEY = "refresh_status"
_CLAIM_LOCK = threading.Lock()


def default_refresh_status_state() -> dict[str, Any]:
    return {
        "status": "idle",
        "job_id": None,
        "pipeline_run_id": None,
        "profile": None,
        "requested_profile": None,
        "mode": None,
        "as_of_date": None,
        "resume_run_id": None,
        "from_stage": None,
        "to_stage": None,
        "refresh_scope": None,
        "force_core": False,
        "force_risk_recompute": False,
        "current_stage": None,
        "current_stage_substage": None,
        "current_stage_substage_status": None,
        "current_stage_diagnostics_section": None,
        "stage_index": None,
        "stage_count": None,
        "stage_started_at": None,
        "current_stage_message": None,
        "current_stage_progress_pct": None,
        "current_stage_items_processed": None,
        "current_stage_items_total": None,
        "current_stage_unit": None,
        "current_stage_heartbeat_at": None,
        "serving_publish_completed_at": None,
        "serving_publish_snapshot_id": None,
        "serving_publish_run_id": None,
        "serving_publish_payload_count": None,
        "requested_at": None,
        "started_at": None,
        "finished_at": None,
        "result": None,
        "error": None,
        "dispatch_backend": None,
        "dispatch_id": None,
    }


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _merge_refresh_status(payload: Any) -> dict[str, Any]:
    state = default_refresh_status_state()
    if isinstance(payload, dict):
        state.update(payload)
    return state


def load_persisted_refresh_status(*, fallback_loader=None) -> dict[str, Any]:
    cached = runtime_state.load_runtime_state(
        _STATUS_CACHE_KEY,
        fallback_loader=fallback_loader,
    )
    return _merge_refresh_status(cached)


def read_persisted_refresh_status(*, fallback_loader=None) -> dict[str, Any]:
    raw = runtime_state.read_runtime_state(
        _STATUS_CACHE_KEY,
        fallback_loader=fallback_loader,
    )
    state = _merge_refresh_status(raw.get("value"))
    return {
        **raw,
        "value": state,
    }


def persist_refresh_status(state: dict[str, Any], *, fallback_writer=None) -> dict[str, Any]:
    return runtime_state.persist_runtime_state(
        _STATUS_CACHE_KEY,
        state,
        fallback_writer=fallback_writer,
    )


def try_claim_refresh_status(
    claim_updates: dict[str, Any],
    *,
    fallback_loader=None,
    fallback_writer=None,
) -> tuple[bool, dict[str, Any]]:
    if config.runtime_state_primary_reads_enabled() and config.neon_surface_enabled(runtime_state.SURFACE_NAME):
        return _try_claim_refresh_status_neon(claim_updates)
    return _try_claim_refresh_status_fallback(
        claim_updates,
        fallback_loader=fallback_loader,
        fallback_writer=fallback_writer,
    )


def _try_claim_refresh_status_fallback(
    claim_updates: dict[str, Any],
    *,
    fallback_loader=None,
    fallback_writer=None,
) -> tuple[bool, dict[str, Any]]:
    with _CLAIM_LOCK:
        current = load_persisted_refresh_status(fallback_loader=fallback_loader)
        if str(current.get("status") or "") == "running":
            return False, current
        next_state = _merge_refresh_status(current)
        next_state.update(claim_updates)
        persist_refresh_status(next_state, fallback_writer=fallback_writer)
        return True, next_state


def _try_claim_refresh_status_neon(claim_updates: dict[str, Any]) -> tuple[bool, dict[str, Any]]:
    initial_state = default_refresh_status_state()
    initial_json = json.dumps(initial_state, default=str, sort_keys=True, separators=(",", ":"))
    conn = connect(dsn=resolve_dsn(None), autocommit=False)
    try:
        runtime_state._ensure_postgres_schema(conn)
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                INSERT INTO runtime_state_current (state_key, value_json, updated_at)
                VALUES (%s, %s::jsonb, %s::timestamptz)
                ON CONFLICT (state_key) DO NOTHING
                """,
                (_STATUS_CACHE_KEY, initial_json, _now_iso()),
            )
            cur.execute(
                """
                SELECT value_json
                FROM runtime_state_current
                WHERE state_key = %s
                FOR UPDATE
                """,
                (_STATUS_CACHE_KEY,),
            )
            row = cur.fetchone() or {}
            current = _merge_refresh_status(row.get("value_json"))
            if str(current.get("status") or "") == "running":
                conn.commit()
                return False, current
            next_state = _merge_refresh_status(current)
            next_state.update(claim_updates)
            cur.execute(
                """
                UPDATE runtime_state_current
                SET value_json = %s::jsonb,
                    updated_at = %s::timestamptz
                WHERE state_key = %s
                """,
                (
                    json.dumps(next_state, default=str, sort_keys=True, separators=(",", ":")),
                    _now_iso(),
                    _STATUS_CACHE_KEY,
                ),
            )
        conn.commit()
        return True, next_state
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
