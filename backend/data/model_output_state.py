"""State readers for durable model-output persistence."""

from __future__ import annotations

import json
import sqlite3
from typing import Any


def latest_date(conn: sqlite3.Connection, *, table: str, col: str) -> str | None:
    row = conn.execute(f"SELECT MAX({col}) FROM {table}").fetchone()
    if not row or row[0] is None:
        return None
    return str(row[0])


def latest_risk_engine_state(conn: sqlite3.Connection) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT risk_engine_state_json
        FROM model_run_metadata
        WHERE status = 'ok'
        ORDER BY completed_at DESC, updated_at DESC
        LIMIT 1
        """
    ).fetchone()
    if not row or row[0] is None:
        return {}
    try:
        decoded = json.loads(str(row[0]))
    except Exception:
        return {}
    return decoded if isinstance(decoded, dict) else {}


def factor_returns_load_start_from_state(
    *,
    latest_persisted_date: str | None,
    previous_state: dict[str, Any],
    as_of_date: str,
    risk_engine_state: dict[str, Any],
) -> tuple[str | None, str]:
    if not latest_persisted_date:
        return None, "full"

    previous_method_version = str(previous_state.get("method_version") or "").strip()
    current_method_version = str(risk_engine_state.get("method_version") or "").strip()
    if current_method_version and previous_method_version and current_method_version != previous_method_version:
        return None, "full"
    if current_method_version and not previous_method_version:
        return None, "full"

    if as_of_date and as_of_date < latest_persisted_date:
        return str(as_of_date), "incremental"
    return str(latest_persisted_date), "incremental"


def factor_returns_load_start_sqlite(
    conn: sqlite3.Connection,
    *,
    as_of_date: str,
    risk_engine_state: dict[str, Any],
) -> tuple[str | None, str]:
    return factor_returns_load_start_from_state(
        latest_persisted_date=latest_date(conn, table="model_factor_returns_daily", col="date"),
        previous_state=latest_risk_engine_state(conn),
        as_of_date=as_of_date,
        risk_engine_state=risk_engine_state,
    )


def pg_latest_date(pg_conn, *, table: str, col: str) -> str | None:
    with pg_conn.cursor() as cur:
        cur.execute(f"SELECT MAX({col})::text FROM {table}")
        row = cur.fetchone()
    if not row or row[0] is None:
        return None
    return str(row[0])


def pg_latest_risk_engine_state(pg_conn) -> dict[str, Any]:
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            SELECT risk_engine_state_json
            FROM model_run_metadata
            WHERE status = 'ok'
            ORDER BY completed_at DESC, updated_at DESC
            LIMIT 1
            """
        )
        row = cur.fetchone()
    if not row or row[0] is None:
        return {}
    try:
        decoded = json.loads(str(row[0]))
    except Exception:
        return {}
    return decoded if isinstance(decoded, dict) else {}


def factor_returns_load_start_postgres(
    pg_conn,
    *,
    as_of_date: str,
    risk_engine_state: dict[str, Any],
) -> tuple[str | None, str]:
    return factor_returns_load_start_from_state(
        latest_persisted_date=pg_latest_date(pg_conn, table="model_factor_returns_daily", col="date"),
        previous_state=pg_latest_risk_engine_state(pg_conn),
        as_of_date=as_of_date,
        risk_engine_state=risk_engine_state,
    )
