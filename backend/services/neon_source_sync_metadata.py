from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any


def require_source_sync_metadata_tables(
    pg_conn,
    *,
    table_exists_pg: Callable[[Any, str], bool],
) -> None:
    required_tables = (
        "source_sync_runs",
        "source_sync_watermarks",
        "security_source_status_current",
    )
    missing = [table for table in required_tables if not table_exists_pg(pg_conn, table)]
    if missing:
        raise RuntimeError(
            "Neon registry-first sync requires metadata tables to exist before publication: "
            + ", ".join(sorted(missing))
        )


def record_source_sync_run_start(
    pg_conn,
    *,
    table_exists_pg: Callable[[Any, str], bool],
    sync_run_id: str,
    mode: str,
    sqlite_path: Path,
    selected_tables: list[str],
    started_at: str,
) -> None:
    if not table_exists_pg(pg_conn, "source_sync_runs"):
        return
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO source_sync_runs (
                sync_run_id,
                mode,
                sqlite_path,
                selected_tables_json,
                table_results_json,
                status,
                started_at,
                completed_at,
                error_type,
                error_message,
                updated_at
            ) VALUES (%s, %s, %s, %s::jsonb, %s::jsonb, %s, %s, NULL, NULL, NULL, %s)
            ON CONFLICT (sync_run_id) DO UPDATE SET
                mode = EXCLUDED.mode,
                sqlite_path = EXCLUDED.sqlite_path,
                selected_tables_json = EXCLUDED.selected_tables_json,
                table_results_json = EXCLUDED.table_results_json,
                status = EXCLUDED.status,
                started_at = EXCLUDED.started_at,
                completed_at = EXCLUDED.completed_at,
                error_type = EXCLUDED.error_type,
                error_message = EXCLUDED.error_message,
                updated_at = EXCLUDED.updated_at
            """,
            (
                str(sync_run_id),
                str(mode),
                str(sqlite_path),
                json.dumps(list(selected_tables), sort_keys=True),
                json.dumps({}, sort_keys=True),
                "running",
                str(started_at),
                str(started_at),
            ),
        )
    pg_conn.commit()


def finalize_source_sync_run(
    pg_conn,
    *,
    table_exists_pg: Callable[[Any, str], bool],
    sync_run_id: str,
    status: str,
    table_results: dict[str, Any],
    updated_at: str,
    error_type: str | None = None,
    error_message: str | None = None,
) -> None:
    if not table_exists_pg(pg_conn, "source_sync_runs"):
        return
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            UPDATE source_sync_runs
            SET
                table_results_json = %s::jsonb,
                status = %s,
                completed_at = %s,
                error_type = %s,
                error_message = %s,
                updated_at = %s
            WHERE sync_run_id = %s
            """,
            (
                json.dumps(table_results, sort_keys=True),
                str(status),
                str(updated_at),
                error_type,
                error_message,
                str(updated_at),
                str(sync_run_id),
            ),
        )


def materialize_security_source_status_current_pg(
    pg_conn,
    *,
    table_exists_pg: Callable[[Any, str], bool],
    sync_run_id: str,
    updated_at: str,
) -> int:
    if not table_exists_pg(pg_conn, "security_source_status_current"):
        return 0
    with pg_conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE security_source_status_current")
        cur.execute(
            """
            WITH latest_obs AS (
                SELECT
                    ric,
                    as_of_date,
                    classification_ready,
                    has_price_history_as_of_date,
                    has_fundamentals_history_as_of_date,
                    has_classification_history_as_of_date,
                    latest_price_date,
                    latest_fundamentals_as_of_date,
                    latest_classification_as_of_date
                FROM (
                    SELECT
                        UPPER(TRIM(ric)) AS ric,
                        as_of_date,
                        classification_ready,
                        has_price_history_as_of_date,
                        has_fundamentals_history_as_of_date,
                        has_classification_history_as_of_date,
                        latest_price_date,
                        latest_fundamentals_as_of_date,
                        latest_classification_as_of_date,
                        ROW_NUMBER() OVER (
                            PARTITION BY UPPER(TRIM(ric))
                            ORDER BY as_of_date DESC, updated_at DESC
                        ) AS rn
                    FROM security_source_observation_daily
                    WHERE ric IS NOT NULL
                      AND TRIM(ric) <> ''
                ) ranked
                WHERE rn = 1
            )
            INSERT INTO security_source_status_current (
                ric,
                ticker,
                tracking_status,
                instrument_kind,
                vehicle_structure,
                model_home_market_scope,
                is_single_name_equity,
                classification_ready,
                price_ingest_enabled,
                pit_fundamentals_enabled,
                pit_classification_enabled,
                allow_cuse_native_core,
                allow_cuse_fundamental_projection,
                allow_cuse_returns_projection,
                allow_cpar_core_target,
                allow_cpar_extended_target,
                observation_as_of_date,
                has_price_history_as_of_date,
                has_fundamentals_history_as_of_date,
                has_classification_history_as_of_date,
                latest_price_date,
                latest_fundamentals_as_of_date,
                latest_classification_as_of_date,
                source_sync_run_id,
                updated_at
            )
            SELECT
                UPPER(TRIM(reg.ric)) AS ric,
                UPPER(TRIM(COALESCE(reg.ticker, ''))) AS ticker,
                COALESCE(NULLIF(TRIM(reg.tracking_status), ''), 'active') AS tracking_status,
                tax.instrument_kind,
                tax.vehicle_structure,
                tax.model_home_market_scope,
                COALESCE(tax.is_single_name_equity, 0) AS is_single_name_equity,
                COALESCE(obs.classification_ready, tax.classification_ready, 0) AS classification_ready,
                COALESCE(pol.price_ingest_enabled, 1) AS price_ingest_enabled,
                COALESCE(pol.pit_fundamentals_enabled, 0) AS pit_fundamentals_enabled,
                COALESCE(pol.pit_classification_enabled, 0) AS pit_classification_enabled,
                COALESCE(pol.allow_cuse_native_core, 0) AS allow_cuse_native_core,
                COALESCE(pol.allow_cuse_fundamental_projection, 0) AS allow_cuse_fundamental_projection,
                COALESCE(pol.allow_cuse_returns_projection, 0) AS allow_cuse_returns_projection,
                COALESCE(pol.allow_cpar_core_target, 0) AS allow_cpar_core_target,
                COALESCE(pol.allow_cpar_extended_target, 0) AS allow_cpar_extended_target,
                obs.as_of_date AS observation_as_of_date,
                COALESCE(obs.has_price_history_as_of_date, 0) AS has_price_history_as_of_date,
                COALESCE(obs.has_fundamentals_history_as_of_date, 0) AS has_fundamentals_history_as_of_date,
                COALESCE(obs.has_classification_history_as_of_date, 0) AS has_classification_history_as_of_date,
                obs.latest_price_date,
                obs.latest_fundamentals_as_of_date,
                obs.latest_classification_as_of_date,
                %s AS source_sync_run_id,
                %s AS updated_at
            FROM security_registry reg
            LEFT JOIN security_policy_current pol
              ON UPPER(TRIM(pol.ric)) = UPPER(TRIM(reg.ric))
            LEFT JOIN security_taxonomy_current tax
              ON UPPER(TRIM(tax.ric)) = UPPER(TRIM(reg.ric))
            LEFT JOIN latest_obs obs
              ON obs.ric = UPPER(TRIM(reg.ric))
            WHERE reg.ric IS NOT NULL
              AND TRIM(reg.ric) <> ''
              AND COALESCE(NULLIF(TRIM(reg.tracking_status), ''), 'active') <> 'disabled'
            """,
            (str(sync_run_id), str(updated_at)),
        )
        return int(cur.rowcount or 0)


def upsert_source_sync_watermarks(
    sqlite_conn: sqlite3.Connection,
    pg_conn,
    *,
    selected_cfgs: Sequence[Any],
    table_results: dict[str, Any],
    sync_run_id: str,
    updated_at: str,
    sqlite_table_exists: Callable[[sqlite3.Connection, str], bool],
    table_exists_pg: Callable[[Any, str], bool],
    profile_sqlite_table: Callable[[sqlite3.Connection, Any], dict[str, Any]],
    profile_pg_table: Callable[[Any, Any], dict[str, Any]],
) -> int:
    if not table_exists_pg(pg_conn, "source_sync_watermarks"):
        return 0
    rows_written = 0
    with pg_conn.cursor() as cur:
        for cfg in selected_cfgs:
            table = cfg.name
            result = table_results.get(table) or {}
            if str(result.get("status") or "").startswith("skipped"):
                continue
            if not sqlite_table_exists(sqlite_conn, table) or not table_exists_pg(pg_conn, table):
                continue
            source = profile_sqlite_table(sqlite_conn, cfg)
            target = profile_pg_table(pg_conn, cfg)
            cur.execute(
                """
                INSERT INTO source_sync_watermarks (
                    table_name,
                    sync_run_id,
                    source_min_value,
                    source_max_value,
                    target_min_value,
                    target_max_value,
                    row_count,
                    updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (table_name) DO UPDATE SET
                    sync_run_id = EXCLUDED.sync_run_id,
                    source_min_value = EXCLUDED.source_min_value,
                    source_max_value = EXCLUDED.source_max_value,
                    target_min_value = EXCLUDED.target_min_value,
                    target_max_value = EXCLUDED.target_max_value,
                    row_count = EXCLUDED.row_count,
                    updated_at = EXCLUDED.updated_at
                """,
                (
                    str(table),
                    str(sync_run_id),
                    source.get("min_date"),
                    source.get("max_date"),
                    target.get("min_date"),
                    target.get("max_date"),
                    int(target.get("row_count") or 0),
                    str(updated_at),
                ),
            )
            rows_written += 1
    return rows_written
