"""Post-refresh Neon mirror sync/parity/prune workflow."""

from __future__ import annotations

import sqlite3
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from psycopg import sql

from backend.data.neon import connect, resolve_dsn
from backend.services.neon_stage2 import canonical_tables, sync_from_sqlite_to_neon


def _cutoff_iso(*, years: int, as_of: date | None = None) -> str:
    base = as_of or datetime.now(timezone.utc).date()
    return (base - timedelta(days=365 * max(1, int(years)))).isoformat()


def _pg_table_exists(pg_conn, table: str) -> bool:
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
        return cur.fetchone() is not None


def prune_neon_history(
    *,
    dsn: str | None = None,
    source_years: int = 10,
    analytics_years: int = 5,
) -> dict[str, Any]:
    source_cutoff = _cutoff_iso(years=source_years)
    analytics_cutoff = _cutoff_iso(years=analytics_years)

    source_specs = [
        ("security_prices_eod", "date"),
        ("security_fundamentals_pit", "as_of_date"),
        ("security_classification_pit", "as_of_date"),
    ]
    analytics_specs = [
        ("barra_raw_cross_section_history", "as_of_date"),
        ("model_factor_returns_daily", "date"),
        ("model_factor_covariance_daily", "as_of_date"),
        ("model_specific_risk_daily", "as_of_date"),
    ]

    out: dict[str, Any] = {
        "status": "ok",
        "source_cutoff": source_cutoff,
        "analytics_cutoff": analytics_cutoff,
        "tables": {},
    }

    pg_conn = connect(dsn=resolve_dsn(dsn), autocommit=False)
    try:
        with pg_conn.cursor() as cur:
            for table, col in source_specs:
                exists = _pg_table_exists(pg_conn, table)
                if not exists:
                    out["tables"][table] = {
                        "exists": False,
                        "deleted": 0,
                    }
                    continue
                cur.execute(
                    sql.SQL("DELETE FROM {} WHERE {} < %s").format(
                        sql.Identifier(table),
                        sql.Identifier(col),
                    ),
                    (source_cutoff,),
                )
                out["tables"][table] = {
                    "exists": True,
                    "deleted": int(cur.rowcount or 0),
                    "cutoff": source_cutoff,
                }

            for table, col in analytics_specs:
                exists = _pg_table_exists(pg_conn, table)
                if not exists:
                    out["tables"][table] = {
                        "exists": False,
                        "deleted": 0,
                    }
                    continue
                cur.execute(
                    sql.SQL("DELETE FROM {} WHERE {} < %s").format(
                        sql.Identifier(table),
                        sql.Identifier(col),
                    ),
                    (analytics_cutoff,),
                )
                out["tables"][table] = {
                    "exists": True,
                    "deleted": int(cur.rowcount or 0),
                    "cutoff": analytics_cutoff,
                }
        pg_conn.commit()
    except Exception:
        pg_conn.rollback()
        raise
    finally:
        pg_conn.close()

    return out


def _sqlite_count_window(
    conn: sqlite3.Connection,
    *,
    table: str,
    date_col: str | None,
    cutoff: str | None,
) -> dict[str, Any]:
    params: list[Any] = []
    where = ""
    if date_col and cutoff:
        where = f" WHERE {date_col} >= ?"
        params.append(cutoff)
    row = conn.execute(f"SELECT COUNT(*) FROM {table}{where}", params).fetchone()
    count = int(row[0] or 0) if row else 0

    min_date = max_date = None
    latest_distinct_ric = None
    if date_col:
        row = conn.execute(
            f"SELECT MIN({date_col}), MAX({date_col}) FROM {table}{where}",
            params,
        ).fetchone()
        if row:
            min_date = str(row[0]) if row[0] is not None else None
            max_date = str(row[1]) if row[1] is not None else None
        if max_date:
            latest_row = conn.execute(
                f"SELECT COUNT(DISTINCT ric) FROM {table} WHERE {date_col} = ?",
                (max_date,),
            ).fetchone()
            latest_distinct_ric = int(latest_row[0] or 0) if latest_row else 0

    return {
        "row_count": count,
        "min_date": min_date,
        "max_date": max_date,
        "latest_distinct_ric": latest_distinct_ric,
    }


def _pg_count_window(
    pg_conn,
    *,
    table: str,
    date_col: str | None,
    cutoff: str | None,
) -> dict[str, Any]:
    params: list[Any] = []
    where_sql = sql.SQL("")
    if date_col and cutoff:
        where_sql = sql.SQL(" WHERE {} >= %s").format(sql.Identifier(date_col))
        params.append(cutoff)

    with pg_conn.cursor() as cur:
        cur.execute(
            sql.SQL("SELECT COUNT(*) FROM {}{}")
            .format(sql.Identifier(table), where_sql),
            params,
        )
        count = int(cur.fetchone()[0] or 0)

        min_date = max_date = None
        latest_distinct_ric = None
        if date_col:
            cur.execute(
                sql.SQL("SELECT MIN({})::text, MAX({})::text FROM {}{}")
                .format(sql.Identifier(date_col), sql.Identifier(date_col), sql.Identifier(table), where_sql),
                params,
            )
            row = cur.fetchone()
            if row:
                min_date = str(row[0]) if row[0] is not None else None
                max_date = str(row[1]) if row[1] is not None else None
            if max_date:
                cur.execute(
                    sql.SQL("SELECT COUNT(DISTINCT ric) FROM {} WHERE {} = %s")
                    .format(sql.Identifier(table), sql.Identifier(date_col)),
                    (max_date,),
                )
                latest_distinct_ric = int(cur.fetchone()[0] or 0)

    return {
        "row_count": count,
        "min_date": min_date,
        "max_date": max_date,
        "latest_distinct_ric": latest_distinct_ric,
    }


def run_bounded_parity_audit(
    *,
    sqlite_path: Path,
    dsn: str | None = None,
    source_years: int = 10,
    analytics_years: int = 5,
) -> dict[str, Any]:
    source_cutoff = _cutoff_iso(years=source_years)
    analytics_cutoff = _cutoff_iso(years=analytics_years)

    table_specs = [
        ("security_master", None, None),
        ("security_prices_eod", "date", source_cutoff),
        ("security_fundamentals_pit", "as_of_date", source_cutoff),
        ("security_classification_pit", "as_of_date", source_cutoff),
        ("barra_raw_cross_section_history", "as_of_date", analytics_cutoff),
    ]

    sqlite_db = Path(sqlite_path).expanduser().resolve()
    if not sqlite_db.exists():
        raise FileNotFoundError(f"sqlite db not found: {sqlite_db}")

    out: dict[str, Any] = {
        "status": "ok",
        "source_cutoff": source_cutoff,
        "analytics_cutoff": analytics_cutoff,
        "sqlite_path": str(sqlite_db),
        "tables": {},
        "issues": [],
    }

    sqlite_conn = sqlite3.connect(str(sqlite_db))
    pg_conn = connect(dsn=resolve_dsn(dsn), autocommit=False)
    try:
        for table, date_col, cutoff in table_specs:
            if date_col is None:
                srow = sqlite_conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                source = {"row_count": int(srow[0] or 0)}
                with pg_conn.cursor() as cur:
                    cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table)))
                    target = {"row_count": int(cur.fetchone()[0] or 0)}
            else:
                source = _sqlite_count_window(
                    sqlite_conn,
                    table=table,
                    date_col=date_col,
                    cutoff=cutoff,
                )
                target = _pg_count_window(
                    pg_conn,
                    table=table,
                    date_col=date_col,
                    cutoff=cutoff,
                )

            mismatch = source != target
            if mismatch:
                out["issues"].append(f"mismatch:{table}")
            out["tables"][table] = {
                "source": source,
                "target": target,
                "cutoff": cutoff,
                "status": "ok" if not mismatch else "mismatch",
            }

        out["status"] = "ok" if not out["issues"] else "mismatch"
        return out
    finally:
        sqlite_conn.close()
        pg_conn.close()


def run_neon_mirror_cycle(
    *,
    sqlite_path: Path,
    dsn: str | None = None,
    mode: str = "incremental",
    tables: list[str] | None = None,
    batch_size: int = 25_000,
    parity_enabled: bool = True,
    prune_enabled: bool = True,
    source_years: int = 10,
    analytics_years: int = 5,
) -> dict[str, Any]:
    selected_tables = tables or canonical_tables()
    out: dict[str, Any] = {
        "status": "ok",
        "mode": str(mode),
        "tables": selected_tables,
        "sync": None,
        "prune": None,
        "parity": None,
    }

    out["sync"] = sync_from_sqlite_to_neon(
        sqlite_path=Path(sqlite_path),
        dsn=dsn,
        tables=selected_tables,
        mode=str(mode),
        batch_size=int(batch_size),
    )

    if prune_enabled:
        out["prune"] = prune_neon_history(
            dsn=dsn,
            source_years=int(source_years),
            analytics_years=int(analytics_years),
        )

    if parity_enabled:
        out["parity"] = run_bounded_parity_audit(
            sqlite_path=Path(sqlite_path),
            dsn=dsn,
            source_years=int(source_years),
            analytics_years=int(analytics_years),
        )
        if str(out["parity"].get("status")) != "ok":
            out["status"] = "mismatch"

    return out
