"""Stage-2 Neon migration helpers (schema apply, sync, parity audit)."""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

logger = logging.getLogger(__name__)

from psycopg import sql

import backend.services.neon_source_sync_metadata as neon_source_sync_metadata
import backend.services.neon_source_sync_transfer as neon_source_sync_transfer

from backend.data.neon import connect, resolve_dsn


@dataclass(frozen=True)
class TableConfig:
    name: str
    pk_cols: tuple[str, ...]
    date_col: str | None = None
    entity_col: str | None = None
    identifier_history_backfill: bool = False
    overlap_days: int = 7
    sync_mode: str = "replace"  # replace | upsert


TABLE_CONFIGS: dict[str, TableConfig] = {
    "security_registry": TableConfig(
        name="security_registry",
        pk_cols=("ric",),
    ),
    "security_taxonomy_current": TableConfig(
        name="security_taxonomy_current",
        pk_cols=("ric",),
    ),
    "security_policy_current": TableConfig(
        name="security_policy_current",
        pk_cols=("ric",),
    ),
    "security_source_observation_daily": TableConfig(
        name="security_source_observation_daily",
        pk_cols=("as_of_date", "ric"),
        date_col="as_of_date",
        entity_col="ric",
        overlap_days=31,
    ),
    "security_ingest_runs": TableConfig(
        name="security_ingest_runs",
        pk_cols=("job_run_id",),
        date_col="started_at",
        overlap_days=31,
        sync_mode="upsert",
    ),
    "security_ingest_audit": TableConfig(
        name="security_ingest_audit",
        pk_cols=("job_run_id", "ric", "artifact_name"),
        entity_col="ric",
        overlap_days=31,
        sync_mode="upsert",
    ),
    "security_master_compat_current": TableConfig(
        name="security_master_compat_current",
        pk_cols=("ric",),
    ),
    "security_prices_eod": TableConfig(
        name="security_prices_eod",
        pk_cols=("ric", "date"),
        date_col="date",
        entity_col="ric",
        identifier_history_backfill=True,
        overlap_days=10,
    ),
    "security_fundamentals_pit": TableConfig(
        name="security_fundamentals_pit",
        pk_cols=("ric", "as_of_date", "stat_date"),
        date_col="as_of_date",
        entity_col="ric",
        identifier_history_backfill=True,
        overlap_days=62,
    ),
    "security_classification_pit": TableConfig(
        name="security_classification_pit",
        pk_cols=("ric", "as_of_date"),
        date_col="as_of_date",
        entity_col="ric",
        identifier_history_backfill=True,
        overlap_days=62,
    ),
    "estu_membership_daily": TableConfig(
        name="estu_membership_daily",
        pk_cols=("date", "ric"),
        date_col="date",
        entity_col="ric",
        overlap_days=31,
    ),
    "universe_cross_section_snapshot": TableConfig(
        name="universe_cross_section_snapshot",
        pk_cols=("ric", "as_of_date"),
        date_col="as_of_date",
        entity_col="ric",
        overlap_days=31,
    ),
    "barra_raw_cross_section_history": TableConfig(
        name="barra_raw_cross_section_history",
        pk_cols=("ric", "as_of_date"),
        date_col="as_of_date",
        entity_col="ric",
        overlap_days=14,
    ),
    "model_factor_returns_daily": TableConfig(
        name="model_factor_returns_daily",
        pk_cols=("date", "factor_name"),
        date_col="date",
        overlap_days=14,
    ),
    "model_factor_covariance_daily": TableConfig(
        name="model_factor_covariance_daily",
        pk_cols=("as_of_date", "factor_name", "factor_name_2"),
        date_col="as_of_date",
        overlap_days=14,
    ),
    "model_specific_risk_daily": TableConfig(
        name="model_specific_risk_daily",
        pk_cols=("as_of_date", "ric"),
        date_col="as_of_date",
        entity_col="ric",
        overlap_days=14,
    ),
    "model_run_metadata": TableConfig(
        name="model_run_metadata",
        pk_cols=("run_id",),
        date_col="completed_at",
        overlap_days=31,
    ),
    "projected_instrument_loadings": TableConfig(
        name="projected_instrument_loadings",
        pk_cols=("ric", "as_of_date", "factor_name"),
        date_col="as_of_date",
        entity_col="ric",
        overlap_days=14,
    ),
    "projected_instrument_meta": TableConfig(
        name="projected_instrument_meta",
        pk_cols=("ric", "as_of_date"),
        date_col="as_of_date",
        entity_col="ric",
        overlap_days=14,
    ),
    "serving_payload_current": TableConfig(
        name="serving_payload_current",
        pk_cols=("payload_name",),
    ),
}


def canonical_tables() -> list[str]:
    return [
        "security_registry",
        "security_taxonomy_current",
        "security_policy_current",
        "security_source_observation_daily",
        "security_ingest_runs",
        "security_ingest_audit",
        "security_master_compat_current",
        "security_prices_eod",
        "security_fundamentals_pit",
        "security_classification_pit",
        "estu_membership_daily",
        "universe_cross_section_snapshot",
        "barra_raw_cross_section_history",
        "model_factor_returns_daily",
        "model_factor_covariance_daily",
        "model_specific_risk_daily",
        "model_run_metadata",
        "projected_instrument_loadings",
        "projected_instrument_meta",
        "serving_payload_current",
    ]


def _sqlite_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return [str(r[1]) for r in rows]


def _sqlite_column_defs(conn: sqlite3.Connection, table: str) -> list[dict[str, Any]]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return [
        {
            "name": str(r[1]),
            "type": str(r[2] or ""),
            "notnull": bool(r[3]),
            "default": r[4],
            "pk": bool(r[5]),
        }
        for r in rows
    ]


def _pg_columns(pg_conn, table: str) -> list[str]:
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = %s
            ORDER BY ordinal_position
            """,
            (table,),
        )
        return [str(r[0]) for r in cur.fetchall()]


def _cursor_fetchone(cur):
    if hasattr(cur, "fetchone"):
        return cur.fetchone()
    if not hasattr(cur, "fetchall"):
        return None
    rows = cur.fetchall()
    return rows[0] if rows else None


def _pg_table_has_rows(pg_conn, table: str) -> bool:
    if not _table_exists_pg(pg_conn, table):
        return False
    with pg_conn.cursor() as cur:
        cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table)))
        row = _cursor_fetchone(cur)
        return bool(row and int(row[0] or 0) > 0)


def _sqlite_table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type='table' AND name=?
        LIMIT 1
        """,
        (str(table),),
    ).fetchone()
    return row is not None


def _sqlite_table_has_rows(conn: sqlite3.Connection, table: str) -> bool:
    if not _sqlite_table_exists(conn, table):
        return False
    row = conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()
    return bool(row and int(row[0] or 0) > 0)


def _require_sqlite_tables(
    sqlite_conn: sqlite3.Connection,
    *,
    required_tables: list[str] | tuple[str, ...],
    required_nonempty_tables: list[str] | tuple[str, ...] = (),
) -> None:
    missing = [table for table in required_tables if not _sqlite_table_exists(sqlite_conn, str(table))]
    empty = [table for table in required_nonempty_tables if not _sqlite_table_has_rows(sqlite_conn, str(table))]
    if missing or empty:
        parts: list[str] = []
        if missing:
            parts.append("missing SQLite source tables: " + ", ".join(sorted(str(table) for table in missing)))
        if empty:
            parts.append("empty required SQLite source tables: " + ", ".join(sorted(str(table) for table in empty)))
        raise RuntimeError("; ".join(parts))


def _sqlite_declared_type_to_pg(column_name: str, declared_type: str | None) -> str:
    clean_name = str(column_name or "").strip().lower()
    clean_type = str(declared_type or "").strip().upper()
    if clean_name == "payload_json":
        return "JSONB"
    if clean_name in {"date", "as_of_date", "stat_date", "period_end_date"}:
        return "DATE"
    if clean_name.endswith("_at"):
        return "TIMESTAMPTZ"
    if "BOOL" in clean_type:
        return "BOOLEAN"
    if "INT" in clean_type:
        return "INTEGER"
    if any(token in clean_type for token in ("REAL", "FLOA", "DOUB", "NUM")):
        return "DOUBLE PRECISION"
    return "TEXT"


def ensure_target_columns_from_sqlite(
    sqlite_conn: sqlite3.Connection,
    pg_conn,
    *,
    source_table: str,
    target_table: str | None = None,
) -> dict[str, Any]:
    target = str(target_table or source_table)
    if not _table_exists_pg(pg_conn, target):
        raise RuntimeError(f"target table missing in Neon: {target}")

    source_cols = _sqlite_column_defs(sqlite_conn, source_table)
    if not source_cols:
        raise RuntimeError(f"source table missing in SQLite: {source_table}")
    target_cols = set(_pg_columns(pg_conn, target))
    added_columns: list[dict[str, str]] = []
    for col in source_cols:
        name = str(col.get("name") or "").strip()
        if not name or name in target_cols:
            continue
        pg_type = _sqlite_declared_type_to_pg(name, str(col.get("type") or ""))
        with pg_conn.cursor() as cur:
            cur.execute(
                sql.SQL("ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} {}").format(
                    sql.Identifier(target),
                    sql.Identifier(name),
                    sql.SQL(pg_type),
                )
            )
        target_cols.add(name)
        added_columns.append(
            {
                "column": name,
                "pg_type": pg_type,
                "sqlite_type": str(col.get("type") or ""),
            }
        )
    return {
        "status": "ok",
        "source_table": str(source_table),
        "target_table": target,
        "added_columns": added_columns,
    }


def _table_exists_pg(pg_conn, table: str) -> bool:
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name = %s
            LIMIT 1
            """,
            (table,),
        )
        return _cursor_fetchone(cur) is not None


def _record_source_sync_run_start(
    pg_conn,
    *,
    sync_run_id: str,
    mode: str,
    sqlite_path: Path,
    selected_tables: list[str],
    started_at: str,
) -> None:
    neon_source_sync_metadata.record_source_sync_run_start(
        pg_conn,
        table_exists_pg=_table_exists_pg,
        sync_run_id=sync_run_id,
        mode=mode,
        sqlite_path=sqlite_path,
        selected_tables=selected_tables,
        started_at=started_at,
    )


def _require_source_sync_metadata_tables(pg_conn) -> None:
    neon_source_sync_metadata.require_source_sync_metadata_tables(
        pg_conn,
        table_exists_pg=_table_exists_pg,
    )


def _finalize_source_sync_run(
    pg_conn,
    *,
    sync_run_id: str,
    status: str,
    table_results: dict[str, Any],
    updated_at: str,
    error_type: str | None = None,
    error_message: str | None = None,
) -> None:
    neon_source_sync_metadata.finalize_source_sync_run(
        pg_conn,
        table_exists_pg=_table_exists_pg,
        sync_run_id=sync_run_id,
        status=status,
        table_results=table_results,
        updated_at=updated_at,
        error_type=error_type,
        error_message=error_message,
    )


def _materialize_security_source_status_current_pg(
    pg_conn,
    *,
    sync_run_id: str,
    updated_at: str,
) -> int:
    return neon_source_sync_metadata.materialize_security_source_status_current_pg(
        pg_conn,
        table_exists_pg=_table_exists_pg,
        sync_run_id=sync_run_id,
        updated_at=updated_at,
    )


def _upsert_source_sync_watermarks(
    sqlite_conn: sqlite3.Connection,
    pg_conn,
    *,
    selected_cfgs: list[TableConfig],
    table_results: dict[str, Any],
    sync_run_id: str,
    updated_at: str,
    profile_sqlite_table,
    profile_pg_table,
) -> int:
    return neon_source_sync_metadata.upsert_source_sync_watermarks(
        sqlite_conn,
        pg_conn,
        selected_cfgs=selected_cfgs,
        table_results=table_results,
        sync_run_id=sync_run_id,
        updated_at=updated_at,
        sqlite_table_exists=_sqlite_table_exists,
        table_exists_pg=_table_exists_pg,
        profile_sqlite_table=profile_sqlite_table,
        profile_pg_table=profile_pg_table,
    )


def _sync_table_from_sqlite_to_neon(
    sqlite_conn: sqlite3.Connection,
    pg_conn,
    *,
    cfg: TableConfig,
    columns: list[str],
    mode: str,
    batch_size: int,
) -> neon_source_sync_transfer.SourceSyncTableTransferResult:
    return neon_source_sync_transfer.sync_table_from_sqlite_to_neon(
        sqlite_conn,
        pg_conn,
        cfg=cfg,
        columns=columns,
        mode=mode,
        batch_size=batch_size,
        deps=neon_source_sync_transfer.SourceSyncTransferDeps(
            pg_count_table=_pg_count_table,
            upsert_table_on_pk=_upsert_table_on_pk,
            pg_max_date=_pg_max_date,
            parse_iso_date=_parse_iso_date,
            format_iso_date=_format_iso_date,
            pg_min_date=_pg_min_date,
            sqlite_entity_min_dates=_sqlite_entity_min_dates,
            pg_entity_min_dates=_pg_entity_min_dates,
            delete_pg_rows_for_entities=_delete_pg_rows_for_entities,
            sqlite_count=_sqlite_count,
            copy_into_postgres=_copy_into_postgres,
            copy_into_postgres_idempotent=_copy_into_postgres_idempotent,
            sqlite_select_rows=_sqlite_select_rows,
            sqlite_select_rows_for_entities_before_date=_sqlite_select_rows_for_entities_before_date,
            assert_post_load_row_counts=_assert_post_load_row_counts,
        ),
    )


def _parse_iso_date(value: str | None) -> date | None:
    if not value:
        return None
    txt = str(value).strip()
    if not txt:
        return None
    try:
        return date.fromisoformat(txt[:10])
    except ValueError:
        return None


def _format_iso_date(value: date | None, fallback: str | None = None) -> str | None:
    if value is None:
        return fallback
    return value.isoformat()


def _canonical_temporal_text(value: Any) -> str | None:
    txt = str(value or "").strip()
    if not txt:
        return None
    if "T" not in txt and " " not in txt:
        try:
            return date.fromisoformat(txt[:10]).isoformat()
        except ValueError:
            return txt
    try:
        return datetime.fromisoformat(txt.replace("Z", "+00:00")).isoformat()
    except ValueError:
        try:
            return date.fromisoformat(txt[:10]).isoformat()
        except ValueError:
            return txt


def _sqlite_count(conn: sqlite3.Connection, table: str, where_sql: str = "", params: tuple[Any, ...] = ()) -> int:
    row = conn.execute(f"SELECT COUNT(*) FROM {table} {where_sql}", params).fetchone()
    return int(row[0] or 0) if row else 0


def _sqlite_iterated_row_count(
    conn: sqlite3.Connection,
    *,
    table: str,
    columns: list[str],
    where_sql: str = "",
    params: tuple[Any, ...] = (),
    batch_size: int = 25_000,
) -> int:
    count = 0
    for _row in _sqlite_select_rows(
        conn,
        table=table,
        columns=columns,
        where_sql=where_sql,
        params=params,
        batch_size=batch_size,
    ):
        count += 1
    return count


def _sqlite_select_rows(
    conn: sqlite3.Connection,
    *,
    table: str,
    columns: list[str],
    where_sql: str = "",
    params: tuple[Any, ...] = (),
    batch_size: int = 25_000,
):
    cols_sql = ", ".join(f'"{c}"' for c in columns)
    cur = conn.execute(f"SELECT {cols_sql} FROM {table} {where_sql}", params)
    while True:
        rows = cur.fetchmany(batch_size)
        if not rows:
            break
        for row in rows:
            yield tuple(row)


def _sqlite_entity_min_dates(
    conn: sqlite3.Connection,
    *,
    table: str,
    entity_col: str,
    date_col: str,
) -> dict[str, str]:
    cur = conn.execute(
        f"""
        SELECT "{entity_col}", MIN("{date_col}")
        FROM {table}
        WHERE "{entity_col}" IS NOT NULL
        GROUP BY "{entity_col}"
        """
    )
    out: dict[str, str] = {}
    for row in cur.fetchall():
        if not row or row[0] is None or row[1] is None:
            continue
        out[str(row[0])] = str(row[1])
    return out


def _sqlite_select_rows_for_entities_before_date(
    conn: sqlite3.Connection,
    *,
    table: str,
    columns: list[str],
    entity_col: str,
    entities: list[str],
    date_col: str,
    from_date: str | None = None,
    before_date: str,
    batch_size: int = 25_000,
):
    if not entities:
        return
    placeholders = ", ".join("?" for _ in entities)
    where_parts = [f'"{entity_col}" IN ({placeholders})']
    params_list: list[Any] = list(entities)
    if from_date is not None:
        where_parts.append(f'"{date_col}" >= ?')
        params_list.append(from_date)
    where_parts.append(f'"{date_col}" < ?')
    params_list.append(before_date)
    where_sql = "WHERE " + " AND ".join(where_parts)
    params: tuple[Any, ...] = tuple(params_list)
    yield from _sqlite_select_rows(
        conn,
        table=table,
        columns=columns,
        where_sql=where_sql,
        params=params,
        batch_size=batch_size,
    )


def _inspect_security_prices_eod_source_integrity(
    conn: sqlite3.Connection,
    *,
    run_sqlite_integrity_check: bool,
    batch_size: int = 25_000,
) -> dict[str, Any]:
    out: dict[str, Any] = {
        "table": "security_prices_eod",
        "status": "ok",
        "issues": [],
    }
    if not _sqlite_table_exists(conn, "security_prices_eod"):
        out["status"] = "skipped_missing_source"
        out["issues"] = ["missing_table:security_prices_eod"]
        return out

    columns = _sqlite_columns(conn, "security_prices_eod")
    count_all = _sqlite_count(conn, "security_prices_eod")
    iterated_row_count = _sqlite_iterated_row_count(
        conn,
        table="security_prices_eod",
        columns=columns,
        batch_size=batch_size,
    )
    distinct_pk_row_count = _sqlite_count(
        conn,
        "(SELECT ric, date FROM security_prices_eod GROUP BY ric, date) AS deduped_prices",
    )
    out["count_all"] = int(count_all)
    out["iterated_row_count"] = int(iterated_row_count)
    out["distinct_pk_row_count"] = int(distinct_pk_row_count)
    if int(count_all) != int(iterated_row_count):
        out["issues"].append(
            f"count_iter_mismatch:security_prices_eod:{count_all}!={iterated_row_count}"
        )
    if int(count_all) != int(distinct_pk_row_count):
        out["issues"].append(
            f"count_distinct_mismatch:security_prices_eod:{count_all}!={distinct_pk_row_count}"
        )
    if int(iterated_row_count) != int(distinct_pk_row_count):
        out["issues"].append(
            "iterated_distinct_mismatch:security_prices_eod"
        )

    if run_sqlite_integrity_check:
        quick_check = str(conn.execute("PRAGMA quick_check").fetchone()[0])
        integrity_check = str(conn.execute("PRAGMA integrity_check").fetchone()[0])
        out["quick_check"] = quick_check
        out["integrity_check"] = integrity_check
        if quick_check != "ok":
            out["issues"].append(f"quick_check_failed:security_prices_eod:{quick_check}")
        if integrity_check != "ok":
            out["issues"].append(f"integrity_check_failed:security_prices_eod:{integrity_check}")

    if out["issues"]:
        out["status"] = "failed"
    return out


def _inspect_sqlite_source_integrity(
    conn: sqlite3.Connection,
    *,
    selected_tables: list[str],
    run_sqlite_integrity_check: bool,
    batch_size: int = 25_000,
) -> dict[str, Any]:
    checks: dict[str, Any] = {}
    issues: list[str] = []
    if "security_prices_eod" in set(selected_tables):
        prices_check = _inspect_security_prices_eod_source_integrity(
            conn,
            run_sqlite_integrity_check=run_sqlite_integrity_check,
            batch_size=batch_size,
        )
        checks["security_prices_eod"] = prices_check
        issues.extend(list(prices_check.get("issues") or []))
    return {
        "status": "ok" if not issues else "failed",
        "tables": checks,
        "issues": issues,
        "run_sqlite_integrity_check": bool(run_sqlite_integrity_check),
    }


def inspect_sqlite_source_integrity(
    *,
    sqlite_path: Path,
    selected_tables: list[str] | None = None,
    run_sqlite_integrity_check: bool = False,
    batch_size: int = 25_000,
) -> dict[str, Any]:
    db = Path(sqlite_path).expanduser().resolve()
    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row
    try:
        out = _inspect_sqlite_source_integrity(
            conn,
            selected_tables=list(selected_tables or canonical_tables()),
            run_sqlite_integrity_check=run_sqlite_integrity_check,
            batch_size=batch_size,
        )
    finally:
        conn.close()
    out["sqlite_path"] = str(db)
    return out


def _pg_count_table(pg_conn, table: str) -> int:
    with pg_conn.cursor() as cur:
        cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table)))
        row = cur.fetchone()
    return int(row[0] or 0) if row else 0


def _assert_post_load_row_counts(
    pg_conn,
    *,
    table: str,
    action: str,
    target_rows_before: int,
    deleted_overlap_rows: int,
    identifier_backfill_deleted: int,
    rows_loaded: int,
) -> dict[str, Any]:
    action_norm = str(action or "").strip()
    expected_target_rows_after: int | None
    if action_norm in {"truncate_and_reload", "target_empty_truncate_and_reload"}:
        expected_target_rows_after = int(rows_loaded)
    elif action_norm in {"incremental_overlap_reload", "incremental_overlap_plus_identifier_backfill"}:
        expected_target_rows_after = int(
            target_rows_before - deleted_overlap_rows - identifier_backfill_deleted + rows_loaded
        )
    else:
        return {
            "status": "skipped",
            "reason": f"unsupported_action:{action_norm or 'unknown'}",
        }
    target_rows_after = _pg_count_table(pg_conn, table)
    if int(target_rows_after) != int(expected_target_rows_after):
        raise RuntimeError(
            f"target row mismatch for {table}: expected {expected_target_rows_after} row(s) in Neon "
            f"after {action_norm}, found {target_rows_after}"
        )
    return {
        "status": "ok",
        "target_rows_before": int(target_rows_before),
        "target_rows_after": int(target_rows_after),
        "expected_target_rows_after": int(expected_target_rows_after),
        "deleted_overlap_rows": int(deleted_overlap_rows),
        "identifier_backfill_deleted": int(identifier_backfill_deleted),
    }


def _pg_max_date(pg_conn, *, table: str, date_col: str) -> str | None:
    with pg_conn.cursor() as cur:
        cur.execute(
            sql.SQL("SELECT MAX({})::text FROM {}")
            .format(sql.Identifier(date_col), sql.Identifier(table))
        )
        row = cur.fetchone()
        if not row or row[0] is None:
            return None
        return str(row[0])


def _pg_min_date(pg_conn, *, table: str, date_col: str) -> str | None:
    with pg_conn.cursor() as cur:
        cur.execute(
            sql.SQL("SELECT MIN({})::text FROM {}")
            .format(sql.Identifier(date_col), sql.Identifier(table))
        )
        row = cur.fetchone()
        if not row or row[0] is None:
            return None
        return str(row[0])


def _pg_entity_min_dates(
    pg_conn,
    *,
    table: str,
    entity_col: str,
    date_col: str,
) -> dict[str, str]:
    with pg_conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                SELECT {}, MIN({})::text
                FROM {}
                WHERE {} IS NOT NULL
                GROUP BY {}
                """
            ).format(
                sql.Identifier(entity_col),
                sql.Identifier(date_col),
                sql.Identifier(table),
                sql.Identifier(entity_col),
                sql.Identifier(entity_col),
            ),
        )
        out: dict[str, str] = {}
        for row in cur.fetchall():
            if not row or row[0] is None or row[1] is None:
                continue
            out[str(row[0])] = str(row[1])
        return out


def _chunked_strings(values: list[str], chunk_size: int = 1_000):
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")
    for idx in range(0, len(values), chunk_size):
        yield values[idx : idx + chunk_size]


def _delete_pg_rows_for_entities(
    pg_conn,
    *,
    table: str,
    entity_col: str,
    entities: list[str],
) -> int:
    if not entities:
        return 0
    deleted = 0
    with pg_conn.cursor() as cur:
        for chunk in _chunked_strings(entities):
            cur.execute(
                sql.SQL("DELETE FROM {} WHERE {} = ANY(%s)").format(
                    sql.Identifier(table),
                    sql.Identifier(entity_col),
                ),
                (chunk,),
            )
            deleted += int(cur.rowcount or 0)
    return deleted


def _copy_into_postgres(
    pg_conn,
    *,
    table: str,
    columns: list[str],
    rows,
) -> int:
    copied = 0
    with pg_conn.cursor() as cur:
        copy_sql = sql.SQL("COPY {} ({}) FROM STDIN")
        copy_sql = copy_sql.format(
            sql.Identifier(table),
            sql.SQL(", ").join(sql.Identifier(c) for c in columns),
        )
        with cur.copy(copy_sql) as cp:
            for row in rows:
                cp.write_row(row)
                copied += 1
    return copied


def _copy_into_postgres_idempotent(
    pg_conn,
    *,
    table: str,
    columns: list[str],
    pk_cols: list[str],
    rows,
) -> int:
    copied = 0
    temp_name = f"_sync_{table}_{uuid4().hex[:8]}"
    table_ident = sql.Identifier(table)
    temp_ident = sql.Identifier(temp_name)
    cols_sql = sql.SQL(", ").join(sql.Identifier(c) for c in columns)
    pk_sql = sql.SQL(", ").join(sql.Identifier(c) for c in pk_cols)
    order_sql = sql.SQL(", ").join(sql.Identifier(c) for c in pk_cols)
    non_key = [c for c in columns if c not in set(pk_cols)]
    if non_key:
        conflict_sql = sql.SQL("ON CONFLICT ({}) DO UPDATE SET {}").format(
            pk_sql,
            sql.SQL(", ").join(
                sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))
                for c in non_key
            ),
        )
    else:
        conflict_sql = sql.SQL("ON CONFLICT ({}) DO NOTHING").format(pk_sql)

    with pg_conn.cursor() as cur:
        cur.execute(
            sql.SQL("CREATE TEMP TABLE {} (LIKE {} INCLUDING DEFAULTS) ON COMMIT DROP").format(
                temp_ident,
                table_ident,
            )
        )
        copy_sql = sql.SQL("COPY {} ({}) FROM STDIN").format(temp_ident, cols_sql)
        with cur.copy(copy_sql) as cp:
            for row in rows:
                cp.write_row(row)
                copied += 1
        cur.execute(
            sql.SQL(
                """
                INSERT INTO {} ({})
                SELECT DISTINCT ON ({}) {}
                FROM {}
                ORDER BY {}
                {}
                """
            ).format(
                table_ident,
                cols_sql,
                pk_sql,
                cols_sql,
                temp_ident,
                order_sql,
                conflict_sql,
            )
        )
    return copied


def _upsert_table_on_pk(
    sqlite_conn: sqlite3.Connection,
    pg_conn,
    *,
    table: str,
    columns: list[str],
    pk_cols: list[str],
    batch_size: int,
) -> int:
    missing_pk_cols = [col for col in pk_cols if col not in columns]
    if missing_pk_cols:
        raise ValueError(
            f"{table} upsert requires declared pk column(s): {', '.join(missing_pk_cols)}"
        )

    non_key = [c for c in columns if c not in set(pk_cols)]
    insert_sql = sql.SQL("INSERT INTO {} ({}) VALUES ({}) ").format(
        sql.Identifier(table),
        sql.SQL(", ").join(sql.Identifier(c) for c in columns),
        sql.SQL(", ").join(sql.Placeholder() for _ in columns),
    )
    pk_sql = sql.SQL(", ").join(sql.Identifier(c) for c in pk_cols)
    if non_key:
        insert_sql += sql.SQL("ON CONFLICT ({}) DO UPDATE SET {}").format(
            pk_sql,
            sql.SQL(", ").join(
                sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))
                for c in non_key
            ),
        )
    else:
        insert_sql += sql.SQL("ON CONFLICT ({}) DO NOTHING").format(pk_sql)

    src_rows = _sqlite_select_rows(
        sqlite_conn,
        table=table,
        columns=columns,
        batch_size=batch_size,
    )

    col_idx = {c: i for i, c in enumerate(columns)}

    def _to_flag(value: Any) -> int:
        if isinstance(value, bool):
            return 1 if value else 0
        if isinstance(value, int):
            return 1 if value != 0 else 0
        if isinstance(value, float):
            return 1 if value != 0.0 else 0
        txt = str(value or "").strip().lower()
        if txt in {"1", "true", "yes", "y"}:
            return 1
        if txt in {"0", "false", "no", "n", ""}:
            return 0
        try:
            return 1 if float(txt) != 0.0 else 0
        except ValueError:
            return 0

    def _normalize_row(row: tuple[Any, ...]) -> tuple[Any, ...]:
        out = list(row)
        if "classification_ok" in col_idx:
            out[col_idx["classification_ok"]] = _to_flag(
                out[col_idx["classification_ok"]]
            )
        if "is_equity_eligible" in col_idx:
            out[col_idx["is_equity_eligible"]] = _to_flag(
                out[col_idx["is_equity_eligible"]]
            )
        if "updated_at" in col_idx:
            raw = out[col_idx["updated_at"]]
            txt = str(raw or "").strip()
            if txt:
                try:
                    datetime.fromisoformat(txt.replace("Z", "+00:00"))
                except ValueError:
                    txt = ""
            if not txt:
                txt = datetime.now(timezone.utc).isoformat()
            out[col_idx["updated_at"]] = txt
        return tuple(out)

    loaded = 0
    chunk: list[tuple[Any, ...]] = []
    with pg_conn.cursor() as cur:
        for row in src_rows:
            chunk.append(_normalize_row(row))
            if len(chunk) >= batch_size:
                cur.executemany(insert_sql, chunk)
                loaded += len(chunk)
                chunk = []
        if chunk:
            cur.executemany(insert_sql, chunk)
            loaded += len(chunk)
    return loaded


def sync_from_sqlite_to_neon(
    *,
    sqlite_path: Path,
    dsn: str | None = None,
    tables: list[str] | None = None,
    mode: str = "incremental",
    batch_size: int = 25_000,
    required_tables: list[str] | None = None,
    required_nonempty_tables: list[str] | None = None,
    verify_source_integrity: bool = False,
    run_sqlite_integrity_check: bool = False,
) -> dict[str, Any]:
    db = Path(sqlite_path).expanduser().resolve()
    if not db.exists():
        raise FileNotFoundError(f"sqlite db not found: {db}")

    selected = tables or canonical_tables()
    unknown = [t for t in selected if t not in TABLE_CONFIGS]
    if unknown:
        raise ValueError(f"unknown table(s): {', '.join(sorted(unknown))}")

    selected_cfgs = [TABLE_CONFIGS[t] for t in selected]
    mode_norm = str(mode).strip().lower()
    if mode_norm not in {"full", "incremental"}:
        raise ValueError("mode must be one of: full, incremental")

    out: dict[str, Any] = {
        "status": "ok",
        "mode": mode_norm,
        "sqlite_path": str(db),
        "sync_run_id": f"source_sync_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')}_{uuid4().hex[:8]}",
        "tables": {},
    }
    sync_started_at = datetime.now(timezone.utc).isoformat()

    sqlite_conn = sqlite3.connect(str(db))
    sqlite_conn.row_factory = sqlite3.Row
    pg_conn = connect(dsn=resolve_dsn(dsn), autocommit=False)

    try:
        if required_tables or required_nonempty_tables:
            _require_sqlite_tables(
                sqlite_conn,
                required_tables=list(required_tables or []),
                required_nonempty_tables=list(required_nonempty_tables or []),
            )
        _require_source_sync_metadata_tables(pg_conn)
        _record_source_sync_run_start(
            pg_conn,
            sync_run_id=str(out["sync_run_id"]),
            mode=mode_norm,
            sqlite_path=db,
            selected_tables=list(selected),
            started_at=sync_started_at,
        )
        if verify_source_integrity:
            out["source_integrity"] = _inspect_sqlite_source_integrity(
                sqlite_conn,
                selected_tables=list(selected),
                run_sqlite_integrity_check=bool(run_sqlite_integrity_check),
                batch_size=max(1_000, int(batch_size)),
            )
            if str(out["source_integrity"].get("status") or "") != "ok":
                raise RuntimeError(
                    "source integrity check failed: "
                    + "; ".join(list(out["source_integrity"].get("issues") or []))
                )
        for cfg in selected_cfgs:
            table = cfg.name

            # Check source table existence before schema-sync to handle
            # lazily-created tables (e.g., projected_instrument_* tables
            # are created on first projection run).
            src_cols = _sqlite_columns(sqlite_conn, table)
            if not src_cols:
                logger.info("Skipping table %s: not yet created in SQLite", table)
                out["tables"][table] = {"status": "skipped_missing_source"}
                continue

            schema_update = ensure_target_columns_from_sqlite(
                sqlite_conn,
                pg_conn,
                source_table=table,
                target_table=table,
            )

            tgt_cols = _pg_columns(pg_conn, table)
            if not tgt_cols:
                raise RuntimeError(f"target table has no columns in Neon: {table}")

            cols = [c for c in src_cols if c in set(tgt_cols)]
            missing_in_target = [c for c in src_cols if c not in set(tgt_cols)]
            if missing_in_target:
                raise RuntimeError(
                    f"target table {table} missing source columns: {', '.join(missing_in_target)}"
                )

            table_result = _sync_table_from_sqlite_to_neon(
                sqlite_conn,
                pg_conn,
                cfg=cfg,
                columns=cols,
                mode=mode_norm,
                batch_size=int(batch_size),
            )
            table_out: dict[str, Any] = {
                "status": table_result.status,
                "action": table_result.action,
                "source_rows": table_result.source_rows,
                "rows_loaded": table_result.rows_loaded,
                "schema_update": schema_update,
            }
            if table_result.action != "upsert":
                table_out["where_sql"] = table_result.where_sql
                table_out["where_params"] = list(table_result.where_params)
                table_out["identifier_backfill"] = table_result.identifier_backfill
                table_out["target_row_validation"] = table_result.target_row_validation
            out["tables"][table] = table_out
        metadata_updated_at = datetime.now(timezone.utc).isoformat()
        out["watermark_rows_updated"] = _upsert_source_sync_watermarks(
            sqlite_conn,
            pg_conn,
            selected_cfgs=selected_cfgs,
            table_results=dict(out["tables"]),
            sync_run_id=str(out["sync_run_id"]),
            updated_at=metadata_updated_at,
            profile_sqlite_table=_profile_sqlite_table,
            profile_pg_table=_profile_pg_table,
        )
        out["security_source_status_current_rows"] = _materialize_security_source_status_current_pg(
            pg_conn,
            sync_run_id=str(out["sync_run_id"]),
            updated_at=metadata_updated_at,
        )
        _finalize_source_sync_run(
            pg_conn,
            sync_run_id=str(out["sync_run_id"]),
            status="ok",
            table_results=dict(out["tables"]),
            updated_at=metadata_updated_at,
        )
        pg_conn.commit()
    except Exception as exc:
        pg_conn.rollback()
        try:
            failed_at = datetime.now(timezone.utc).isoformat()
            _finalize_source_sync_run(
                pg_conn,
                sync_run_id=str(out["sync_run_id"]),
                status="failed",
                table_results=dict(out["tables"]),
                updated_at=failed_at,
                error_type=type(exc).__name__,
                error_message=str(exc),
            )
            pg_conn.commit()
        except Exception:
            pg_conn.rollback()
        raise
    finally:
        sqlite_conn.close()
        pg_conn.close()

    return out


def _profile_sqlite_table(conn: sqlite3.Connection, cfg: TableConfig) -> dict[str, Any]:
    table = cfg.name
    out: dict[str, Any] = {
        "row_count": _sqlite_count(conn, table),
    }
    if cfg.date_col:
        row = conn.execute(
            f"SELECT MIN({cfg.date_col}), MAX({cfg.date_col}) FROM {table}"
        ).fetchone()
        out["min_date"] = (str(row[0]) if row and row[0] is not None else None)
        out["max_date"] = (str(row[1]) if row and row[1] is not None else None)
        max_date = out["max_date"]
        if max_date:
            if "ric" in _sqlite_columns(conn, table):
                drow = conn.execute(
                    f"SELECT COUNT(DISTINCT ric) FROM {table} WHERE {cfg.date_col} = ?",
                    (max_date,),
                ).fetchone()
                out["latest_distinct_ric"] = int(drow[0] or 0) if drow else 0
    return out


def _profile_pg_table(pg_conn, cfg: TableConfig) -> dict[str, Any]:
    table = cfg.name
    out: dict[str, Any] = {}
    with pg_conn.cursor() as cur:
        cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table)))
        out["row_count"] = int(cur.fetchone()[0] or 0)
        if cfg.date_col:
            cur.execute(
                sql.SQL("SELECT MIN({})::text, MAX({})::text FROM {}")
                .format(
                    sql.Identifier(cfg.date_col),
                    sql.Identifier(cfg.date_col),
                    sql.Identifier(table),
                )
            )
            row = cur.fetchone()
            out["min_date"] = (str(row[0]) if row and row[0] is not None else None)
            out["max_date"] = (str(row[1]) if row and row[1] is not None else None)
            if out["max_date"] and _has_ric_column_pg(pg_conn, table):
                cur.execute(
                    sql.SQL("SELECT COUNT(DISTINCT ric) FROM {} WHERE {} = %s")
                    .format(sql.Identifier(table), sql.Identifier(cfg.date_col)),
                    (out["max_date"],),
                )
                out["latest_distinct_ric"] = int(cur.fetchone()[0] or 0)
    return out


def _duplicate_group_count_sqlite(conn: sqlite3.Connection, cfg: TableConfig) -> int:
    group_cols = ", ".join(cfg.pk_cols)
    row = conn.execute(
        f"SELECT COUNT(*) FROM (SELECT {group_cols}, COUNT(*) c FROM {cfg.name} GROUP BY {group_cols} HAVING c > 1)"
    ).fetchone()
    return int(row[0] or 0) if row else 0


def _duplicate_group_count_pg(pg_conn, cfg: TableConfig) -> int:
    group_cols = sql.SQL(", ").join(sql.Identifier(c) for c in cfg.pk_cols)
    query = sql.SQL(
        "SELECT COUNT(*) FROM (SELECT {}, COUNT(*) c FROM {} GROUP BY {} HAVING COUNT(*) > 1) q"
    ).format(group_cols, sql.Identifier(cfg.name), group_cols)
    with pg_conn.cursor() as cur:
        cur.execute(query)
        row = cur.fetchone()
    return int(row[0] or 0) if row else 0


def _sqlite_count_from_date(
    conn: sqlite3.Connection,
    *,
    table: str,
    date_col: str,
    min_date: str,
) -> int:
    row = conn.execute(
        f"SELECT COUNT(*) FROM {table} WHERE {date_col} >= ?",
        (min_date,),
    ).fetchone()
    return int(row[0] or 0) if row else 0


def _expected_retention_gap(
    conn: sqlite3.Connection,
    *,
    cfg: TableConfig,
    source: dict[str, Any],
    target: dict[str, Any],
) -> dict[str, Any] | None:
    if not cfg.date_col:
        return None
    source_min = str(_canonical_temporal_text(source.get("min_date")) or "").strip()
    target_min = str(_canonical_temporal_text(target.get("min_date")) or "").strip()
    source_max = str(_canonical_temporal_text(source.get("max_date")) or "").strip()
    target_max = str(_canonical_temporal_text(target.get("max_date")) or "").strip()
    if not source_min or not target_min or not source_max or not target_max:
        return None
    if target_min <= source_min:
        return None
    if source_max != target_max:
        return None

    source_rows_in_target_window = _sqlite_count_from_date(
        conn,
        table=cfg.name,
        date_col=str(cfg.date_col),
        min_date=target_min,
    )
    target_row_count = int(target.get("row_count") or 0)
    status = "ok" if source_rows_in_target_window == target_row_count else "mismatch"
    return {
        "status": status,
        "source_archive_min_date": source_min,
        "target_retained_min_date": target_min,
        "source_rows_in_target_window": int(source_rows_in_target_window),
        "target_row_count": target_row_count,
    }


def _resolve_orphan_anchor_sqlite(conn: sqlite3.Connection) -> tuple[str | None, str | None]:
    for table, alias in (
        ("security_registry", "reg"),
        ("security_master_compat_current", "compat"),
    ):
        if _sqlite_table_has_rows(conn, table):
            return table, alias
    return None, None


def _resolve_orphan_anchor_pg(pg_conn) -> tuple[str | None, str | None]:
    for table, alias in (
        ("security_registry", "reg"),
        ("security_master_compat_current", "compat"),
    ):
        if _pg_table_has_rows(pg_conn, table):
            return table, alias
    return None, None


def _orphan_ric_sqlite(conn: sqlite3.Connection, table: str, *, anchor_table: str | None = None) -> int:
    if not anchor_table:
        anchor_table, _ = _resolve_orphan_anchor_sqlite(conn)
    if not anchor_table:
        return 0
    anchor_alias = "reg" if anchor_table == "security_registry" else "compat"
    row = conn.execute(
        f"""
        SELECT COUNT(*)
        FROM {table} x
        LEFT JOIN {anchor_table} {anchor_alias}
          ON {anchor_alias}.ric = x.ric
        WHERE {anchor_alias}.ric IS NULL
        """
    ).fetchone()
    return int(row[0] or 0) if row else 0


def _orphan_ric_pg(pg_conn, table: str, *, anchor_table: str | None = None) -> int:
    if not anchor_table:
        anchor_table, _ = _resolve_orphan_anchor_pg(pg_conn)
    if not anchor_table:
        return 0
    anchor_alias = sql.Identifier("reg" if anchor_table == "security_registry" else "compat")
    with pg_conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                SELECT COUNT(*)
                FROM {} x
                LEFT JOIN {} {}
                  ON {}.ric = x.ric
                WHERE {}.ric IS NULL
                """
            ).format(
                sql.Identifier(table),
                sql.Identifier(anchor_table),
                anchor_alias,
                anchor_alias,
                anchor_alias,
            )
        )
        row = cur.fetchone()
    return int(row[0] or 0) if row else 0


def _has_ric_column_sqlite(conn: sqlite3.Connection, table: str) -> bool:
    return "ric" in set(_sqlite_columns(conn, table))


def _has_ric_column_pg(pg_conn, table: str) -> bool:
    return "ric" in set(_pg_columns(pg_conn, table))


def run_parity_audit(
    *,
    sqlite_path: Path,
    dsn: str | None = None,
    tables: list[str] | None = None,
) -> dict[str, Any]:
    db = Path(sqlite_path).expanduser().resolve()
    if not db.exists():
        raise FileNotFoundError(f"sqlite db not found: {db}")

    selected = tables or canonical_tables()
    unknown = [t for t in selected if t not in TABLE_CONFIGS]
    if unknown:
        raise ValueError(f"unknown table(s): {', '.join(sorted(unknown))}")

    selected_cfgs = [TABLE_CONFIGS[t] for t in selected]
    out: dict[str, Any] = {
        "status": "ok",
        "sqlite_path": str(db),
        "tables": {},
        "issues": [],
        "notes": [],
    }

    sqlite_conn = sqlite3.connect(str(db))
    sqlite_conn.row_factory = sqlite3.Row
    pg_conn = connect(dsn=resolve_dsn(dsn), autocommit=False)
    source_anchor_table, _ = _resolve_orphan_anchor_sqlite(sqlite_conn)
    target_anchor_table, _ = _resolve_orphan_anchor_pg(pg_conn)
    orphan_anchor_note_added = False

    try:
        for cfg in selected_cfgs:
            if not _table_exists_pg(pg_conn, cfg.name):
                out["issues"].append(f"missing_target_table:{cfg.name}")
                out["tables"][cfg.name] = {
                    "source": _profile_sqlite_table(sqlite_conn, cfg),
                    "target": None,
                    "duplicate_groups": {
                        "source": _duplicate_group_count_sqlite(sqlite_conn, cfg),
                        "target": None,
                    },
                }
                continue

            src = _profile_sqlite_table(sqlite_conn, cfg)
            tgt = _profile_pg_table(pg_conn, cfg)
            dup_src = _duplicate_group_count_sqlite(sqlite_conn, cfg)
            dup_tgt = _duplicate_group_count_pg(pg_conn, cfg)

            table_out: dict[str, Any] = {
                "source": src,
                "target": tgt,
                "duplicate_groups": {
                    "source": int(dup_src),
                    "target": int(dup_tgt),
                },
            }
            retention_gap = _expected_retention_gap(
                sqlite_conn,
                cfg=cfg,
                source=src,
                target=tgt,
            )
            expected_retention_gap = bool(
                isinstance(retention_gap, dict)
                and str(retention_gap.get("status") or "") == "ok"
            )
            if retention_gap is not None:
                table_out["retention_gap"] = retention_gap
                if expected_retention_gap:
                    out["notes"].append(
                        f"expected_retention_gap:{cfg.name}:{retention_gap['source_archive_min_date']}->{retention_gap['target_retained_min_date']}"
                    )

            if _has_ric_column_sqlite(sqlite_conn, cfg.name) and _has_ric_column_pg(pg_conn, cfg.name):
                if not source_anchor_table or not target_anchor_table:
                    if not orphan_anchor_note_added:
                        out["notes"].append("orphan_checks_skipped:no_anchor_table")
                        orphan_anchor_note_added = True
                elif cfg.name != source_anchor_table and cfg.name != target_anchor_table:
                    orphan_src = _orphan_ric_sqlite(sqlite_conn, cfg.name, anchor_table=source_anchor_table)
                    orphan_tgt = _orphan_ric_pg(pg_conn, cfg.name, anchor_table=target_anchor_table)
                    table_out["orphan_ric_rows"] = {
                        "source": int(orphan_src),
                        "target": int(orphan_tgt),
                    }
                    if orphan_src != orphan_tgt:
                        out["issues"].append(f"orphan_mismatch:{cfg.name}:{orphan_src}!={orphan_tgt}")

            if int(src.get("row_count") or 0) != int(tgt.get("row_count") or 0) and not expected_retention_gap:
                out["issues"].append(
                    f"row_count_mismatch:{cfg.name}:{src.get('row_count')}!={tgt.get('row_count')}"
                )
            if cfg.date_col:
                src_min = _canonical_temporal_text(src.get("min_date"))
                tgt_min = _canonical_temporal_text(tgt.get("min_date"))
                if src_min != tgt_min and not expected_retention_gap:
                    out["issues"].append(
                        f"min_date_mismatch:{cfg.name}:{src.get('min_date')}!={tgt.get('min_date')}"
                    )
                for key in ("max_date", "latest_distinct_ric"):
                    src_value = _canonical_temporal_text(src.get(key)) if key == "max_date" else src.get(key)
                    tgt_value = _canonical_temporal_text(tgt.get(key)) if key == "max_date" else tgt.get(key)
                    if src_value != tgt_value:
                        out["issues"].append(
                            f"{key}_mismatch:{cfg.name}:{src.get(key)}!={tgt.get(key)}"
                        )
            if dup_src != dup_tgt:
                out["issues"].append(f"duplicate_group_mismatch:{cfg.name}:{dup_src}!={dup_tgt}")

            out["tables"][cfg.name] = table_out

        out["status"] = "ok" if not out["issues"] else "mismatch"
        return out
    finally:
        sqlite_conn.close()
        pg_conn.close()


def apply_sql_file(pg_conn, *, sql_path: Path) -> dict[str, Any]:
    path = Path(sql_path).expanduser().resolve()
    if not path.exists():
        raise FileNotFoundError(f"SQL file not found: {path}")
    script = path.read_text(encoding="utf-8")
    with pg_conn.cursor() as cur:
        cur.execute(script)
    pg_conn.commit()
    return {
        "status": "ok",
        "sql_path": str(path),
        "bytes": len(script.encode("utf-8")),
    }
