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


def _parse_iso_date(value: str | None) -> date | None:
    txt = str(value or "").strip()
    if not txt:
        return None
    try:
        return date.fromisoformat(txt[:10])
    except ValueError:
        return None


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


def _sqlite_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return [str(row[1]) for row in rows]


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
        return [str(row[0]) for row in cur.fetchall()]


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


def sync_factor_returns_to_neon(
    *,
    cache_path: Path,
    dsn: str | None = None,
    mode: str = "incremental",
    overlap_days: int = 14,
    batch_size: int = 25_000,
    analytics_years: int = 5,
) -> dict[str, Any]:
    cache_db = Path(cache_path).expanduser().resolve()
    if not cache_db.exists():
        raise FileNotFoundError(f"cache db not found: {cache_db}")

    table = "model_factor_returns_daily"
    source_table = "daily_factor_returns"
    mode_norm = str(mode or "incremental").strip().lower()
    if mode_norm not in {"full", "incremental"}:
        mode_norm = "incremental"
    sync_cutoff = _cutoff_iso(years=analytics_years)

    sqlite_conn = sqlite3.connect(str(cache_db))
    pg_conn = connect(dsn=resolve_dsn(dsn), autocommit=False)
    try:
        if not _sqlite_table_exists(sqlite_conn, source_table):
            return {
                "status": "skipped",
                "reason": f"missing_source_table:{source_table}",
                "cache_path": str(cache_db),
                "table": table,
            }
        if not _pg_table_exists(pg_conn, table):
            raise RuntimeError(f"target table missing in Neon: {table}")

        where_sql = ""
        where_params: tuple[Any, ...] = ()
        action = "truncate_and_reload"
        source_count_sql = f"SELECT COUNT(*) FROM {source_table}"
        source_count_params: tuple[Any, ...] = ()
        if mode_norm == "full":
            with pg_conn.cursor() as cur:
                cur.execute(sql.SQL("TRUNCATE TABLE {}").format(sql.Identifier(table)))
            where_sql = ""
            where_params = ()
        else:
            with pg_conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        "SELECT COUNT(*), MAX(date)::text FROM {}"
                    ).format(sql.Identifier(table)),
                )
                row = cur.fetchone()
                target_count = int(row[0] or 0) if row else 0
                max_date_txt = str(row[1]) if row and row[1] is not None else ""
            src_count_row = sqlite_conn.execute(source_count_sql, source_count_params).fetchone()
            source_window_rows = int(src_count_row[0] or 0) if src_count_row else 0
            max_date = _parse_iso_date(max_date_txt)
            if max_date is None:
                with pg_conn.cursor() as cur:
                    cur.execute(sql.SQL("TRUNCATE TABLE {}").format(sql.Identifier(table)))
                action = "target_empty_truncate_and_reload"
                where_sql = ""
                where_params = ()
            elif target_count != source_window_rows:
                with pg_conn.cursor() as cur:
                    cur.execute(sql.SQL("TRUNCATE TABLE {}").format(sql.Identifier(table)))
                where_sql = ""
                where_params = ()
                action = "bounded_full_reload_after_count_mismatch"
            else:
                cutoff = max_date - timedelta(days=max(0, int(overlap_days)))
                cutoff_txt = max(cutoff.isoformat(), sync_cutoff)
                with pg_conn.cursor() as cur:
                    cur.execute(
                        sql.SQL("DELETE FROM {} WHERE date >= %s").format(
                            sql.Identifier(table)
                        ),
                        (cutoff_txt,),
                    )
                where_sql = "WHERE date >= ?"
                where_params = (cutoff_txt,)
                action = "incremental_overlap_reload"

        src_count = sqlite_conn.execute(
            f"SELECT COUNT(*) FROM {source_table} {where_sql}",
            where_params,
        ).fetchone()
        source_rows = int(src_count[0] or 0) if src_count else 0

        select_sql = f"""
            SELECT
                date,
                factor_name,
                factor_return,
                COALESCE(robust_se, 0.0) AS robust_se,
                COALESCE(t_stat, 0.0) AS t_stat,
                r_squared,
                residual_vol,
                cross_section_n,
                eligible_n,
                coverage
            FROM {source_table}
            {where_sql}
            ORDER BY date, factor_name
        """
        now_iso = datetime.now(timezone.utc).isoformat()
        insert_sql = sql.SQL(
            """
            INSERT INTO {} (
                date,
                factor_name,
                factor_return,
                robust_se,
                t_stat,
                r_squared,
                residual_vol,
                cross_section_n,
                eligible_n,
                coverage,
                updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date, factor_name) DO UPDATE SET
                factor_return = EXCLUDED.factor_return,
                robust_se = EXCLUDED.robust_se,
                t_stat = EXCLUDED.t_stat,
                r_squared = EXCLUDED.r_squared,
                residual_vol = EXCLUDED.residual_vol,
                cross_section_n = EXCLUDED.cross_section_n,
                eligible_n = EXCLUDED.eligible_n,
                coverage = EXCLUDED.coverage,
                updated_at = EXCLUDED.updated_at
            """
        ).format(sql.Identifier(table))

        loaded = 0
        chunk: list[tuple[Any, ...]] = []
        with pg_conn.cursor() as cur:
            for row in sqlite_conn.execute(select_sql, where_params):
                chunk.append(
                    (
                        row[0],
                        row[1],
                        row[2],
                        row[3],
                        row[4],
                        row[5],
                        row[6],
                        row[7],
                        row[8],
                        row[9],
                        now_iso,
                    )
                )
                if len(chunk) >= max(500, int(batch_size)):
                    cur.executemany(insert_sql, chunk)
                    loaded += len(chunk)
                    chunk = []
            if chunk:
                cur.executemany(insert_sql, chunk)
                loaded += len(chunk)
        pg_conn.commit()

        out: dict[str, Any] = {
            "status": "ok",
            "mode": mode_norm,
            "cache_path": str(cache_db),
            "table": table,
            "source_table": source_table,
            "action": action,
            "source_rows": int(source_rows),
            "rows_loaded": int(loaded),
        }
        if where_sql:
            out["where_sql"] = where_sql
            out["where_params"] = [str(v) for v in where_params]
        return out
    except Exception:
        pg_conn.rollback()
        raise
    finally:
        sqlite_conn.close()
        pg_conn.close()


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
    distinct_col: str | None = "ric",
) -> dict[str, Any]:
    params: list[Any] = []
    where = ""
    if date_col and cutoff:
        where = f" WHERE {date_col} >= ?"
        params.append(cutoff)
    row = conn.execute(f"SELECT COUNT(*) FROM {table}{where}", params).fetchone()
    count = int(row[0] or 0) if row else 0

    min_date = max_date = None
    latest_distinct = None
    if date_col:
        row = conn.execute(
            f"SELECT MIN({date_col}), MAX({date_col}) FROM {table}{where}",
            params,
        ).fetchone()
        if row:
            min_date = str(row[0]) if row[0] is not None else None
            max_date = str(row[1]) if row[1] is not None else None
        if max_date and distinct_col:
            latest_row = conn.execute(
                f"SELECT COUNT(DISTINCT {distinct_col}) FROM {table} WHERE {date_col} = ?",
                (max_date,),
            ).fetchone()
            latest_distinct = int(latest_row[0] or 0) if latest_row else 0

    return {
        "row_count": count,
        "min_date": min_date,
        "max_date": max_date,
        "latest_distinct": latest_distinct,
    }


def _pg_count_window(
    pg_conn,
    *,
    table: str,
    date_col: str | None,
    cutoff: str | None,
    distinct_col: str | None = "ric",
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
        latest_distinct = None
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
            if max_date and distinct_col:
                cur.execute(
                    sql.SQL("SELECT COUNT(DISTINCT {}) FROM {} WHERE {} = %s").format(
                        sql.Identifier(str(distinct_col)),
                        sql.Identifier(table),
                        sql.Identifier(date_col),
                    ),
                    (max_date,),
                )
                latest_distinct = int(cur.fetchone()[0] or 0)

    return {
        "row_count": count,
        "min_date": min_date,
        "max_date": max_date,
        "latest_distinct": latest_distinct,
    }


def _sqlite_non_null_counts(
    conn: sqlite3.Connection,
    *,
    table: str,
    columns: list[str],
    where_sql: str = "",
    params: tuple[Any, ...] = (),
) -> dict[str, int]:
    if not columns:
        return {}
    select_sql = ", ".join(
        f'SUM(CASE WHEN "{col}" IS NOT NULL THEN 1 ELSE 0 END) AS "{col}"'
        for col in columns
    )
    row = conn.execute(f"SELECT {select_sql} FROM {table} {where_sql}", params).fetchone()
    if not row:
        return {str(col): 0 for col in columns}
    return {str(col): int(row[idx] or 0) for idx, col in enumerate(columns)}


def _pg_non_null_counts(
    pg_conn,
    *,
    table: str,
    columns: list[str],
    date_col: str | None = None,
    cutoff: str | None = None,
) -> dict[str, int]:
    if not columns:
        return {}
    where_sql = sql.SQL("")
    params: list[Any] = []
    if date_col and cutoff:
        where_sql = sql.SQL(" WHERE {} >= %s").format(sql.Identifier(date_col))
        params.append(cutoff)
    select_sql = sql.SQL(", ").join(
        sql.SQL("SUM(CASE WHEN {} IS NOT NULL THEN 1 ELSE 0 END) AS {}").format(
            sql.Identifier(col),
            sql.Identifier(col),
        )
        for col in columns
    )
    with pg_conn.cursor() as cur:
        cur.execute(
            sql.SQL("SELECT {} FROM {}{}").format(
                select_sql,
                sql.Identifier(table),
                where_sql,
            ),
            params,
        )
        row = cur.fetchone()
    if not row:
        return {str(col): 0 for col in columns}
    return {str(col): int(row[idx] or 0) for idx, col in enumerate(columns)}


def _sqlite_recent_dates(
    conn: sqlite3.Connection,
    *,
    table: str,
    date_col: str,
    cutoff: str | None,
    limit: int = 3,
) -> list[str]:
    params: list[Any] = []
    where = ""
    if cutoff:
        where = f" WHERE {date_col} >= ?"
        params.append(cutoff)
    rows = conn.execute(
        f"""
        SELECT DISTINCT {date_col}
        FROM {table}
        {where}
        ORDER BY {date_col} DESC
        LIMIT ?
        """,
        [*params, int(limit)],
    ).fetchall()
    return [str(row[0]) for row in rows if row and row[0] is not None]


def _sqlite_factor_return_values(
    conn: sqlite3.Connection,
    *,
    table: str,
    dates: list[str],
) -> dict[tuple[str, str], tuple[float, ...]]:
    if not dates:
        return {}
    placeholders = ",".join("?" for _ in dates)
    rows = conn.execute(
        f"""
        SELECT
            date,
            factor_name,
            factor_return,
            COALESCE(robust_se, 0.0),
            COALESCE(t_stat, 0.0),
            r_squared,
            residual_vol,
            COALESCE(cross_section_n, 0),
            COALESCE(eligible_n, 0),
            COALESCE(coverage, 0.0)
        FROM {table}
        WHERE date IN ({placeholders})
        ORDER BY date, factor_name
        """,
        tuple(dates),
    ).fetchall()
    return {
        (str(row[0]), str(row[1])): (
            float(row[2] or 0.0),
            float(row[3] or 0.0),
            float(row[4] or 0.0),
            float(row[5] or 0.0),
            float(row[6] or 0.0),
            float(row[7] or 0),
            float(row[8] or 0),
            float(row[9] or 0.0),
        )
        for row in rows
    }


def _pg_factor_return_values(
    pg_conn,
    *,
    table: str,
    dates: list[str],
) -> dict[tuple[str, str], tuple[float, ...]]:
    if not dates:
        return {}
    with pg_conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                SELECT
                    date::text,
                    factor_name,
                    factor_return,
                    COALESCE(robust_se, 0.0),
                    COALESCE(t_stat, 0.0),
                    r_squared,
                    residual_vol,
                    COALESCE(cross_section_n, 0),
                    COALESCE(eligible_n, 0),
                    COALESCE(coverage, 0.0)
                FROM {}
                WHERE date = ANY(%s)
                ORDER BY date, factor_name
                """
            ).format(sql.Identifier(table)),
            (dates,),
        )
        rows = cur.fetchall()
    return {
        (str(row[0]), str(row[1])): (
            float(row[2] or 0.0),
            float(row[3] or 0.0),
            float(row[4] or 0.0),
            float(row[5] or 0.0),
            float(row[6] or 0.0),
            float(row[7] or 0),
            float(row[8] or 0),
            float(row[9] or 0.0),
        )
        for row in rows
    }


def _sqlite_group_count_by_date(
    conn: sqlite3.Connection,
    *,
    table: str,
    date_col: str,
    dates: list[str],
) -> dict[str, int]:
    if not dates:
        return {}
    placeholders = ",".join("?" for _ in dates)
    rows = conn.execute(
        f"""
        SELECT {date_col}, COUNT(*)
        FROM {table}
        WHERE {date_col} IN ({placeholders})
        GROUP BY {date_col}
        """,
        tuple(dates),
    ).fetchall()
    return {str(row[0]): int(row[1] or 0) for row in rows}


def _pg_group_count_by_date(
    pg_conn,
    *,
    table: str,
    date_col: str,
    dates: list[str],
) -> dict[str, int]:
    if not dates:
        return {}
    with pg_conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                SELECT {}::text, COUNT(*)
                FROM {}
                WHERE {} = ANY(%s)
                GROUP BY {}
                """
            ).format(
                sql.Identifier(date_col),
                sql.Identifier(table),
                sql.Identifier(date_col),
                sql.Identifier(date_col),
            ),
            (dates,),
        )
        rows = cur.fetchall()
    return {str(row[0]): int(row[1] or 0) for row in rows}


def _value_maps_match(
    source: dict[tuple[str, str], tuple[float, ...]],
    target: dict[tuple[str, str], tuple[float, ...]],
    *,
    tolerance: float = 1e-9,
) -> tuple[bool, list[str]]:
    issues: list[str] = []
    for key in sorted(set(source.keys()) | set(target.keys())):
        if key not in source:
            issues.append(f"unexpected_target_row:{key[0]}:{key[1]}")
            continue
        if key not in target:
            issues.append(f"missing_target_row:{key[0]}:{key[1]}")
            continue
        lhs = source[key]
        rhs = target[key]
        if len(lhs) != len(rhs):
            issues.append(f"shape_mismatch:{key[0]}:{key[1]}")
            continue
        for idx, (lv, rv) in enumerate(zip(lhs, rhs, strict=True)):
            if abs(float(lv) - float(rv)) > tolerance:
                issues.append(f"value_mismatch:{key[0]}:{key[1]}:col{idx}")
                break
    return (not issues), issues


def run_bounded_parity_audit(
    *,
    sqlite_path: Path,
    cache_path: Path | None = None,
    dsn: str | None = None,
    source_years: int = 10,
    analytics_years: int = 5,
) -> dict[str, Any]:
    source_cutoff = _cutoff_iso(years=source_years)
    analytics_cutoff = _cutoff_iso(years=analytics_years)

    data_specs = [
        ("security_master", "security_master", None, None, None),
        ("security_prices_eod", "security_prices_eod", "date", source_cutoff, "ric"),
        (
            "security_fundamentals_pit",
            "security_fundamentals_pit",
            "as_of_date",
            source_cutoff,
            "ric",
        ),
        (
            "security_classification_pit",
            "security_classification_pit",
            "as_of_date",
            source_cutoff,
            "ric",
        ),
        (
            "barra_raw_cross_section_history",
            "barra_raw_cross_section_history",
            "as_of_date",
            analytics_cutoff,
            "ric",
        ),
    ]

    sqlite_db = Path(sqlite_path).expanduser().resolve()
    if not sqlite_db.exists():
        raise FileNotFoundError(f"sqlite db not found: {sqlite_db}")

    out: dict[str, Any] = {
        "status": "ok",
        "source_cutoff": source_cutoff,
        "analytics_cutoff": analytics_cutoff,
        "sqlite_path": str(sqlite_db),
        "cache_path": str(Path(cache_path).expanduser().resolve()) if cache_path else None,
        "tables": {},
        "issues": [],
    }

    sqlite_conn = sqlite3.connect(str(sqlite_db))
    cache_conn = None
    cache_db = None
    if cache_path is not None:
        cache_db = Path(cache_path).expanduser().resolve()
        if not cache_db.exists():
            raise FileNotFoundError(f"cache db not found: {cache_db}")
        cache_conn = sqlite3.connect(str(cache_db))
    pg_conn = connect(dsn=resolve_dsn(dsn), autocommit=False)
    try:
        for label, source_table, date_col, cutoff, distinct_col in data_specs:
            if date_col is None:
                srow = sqlite_conn.execute(f"SELECT COUNT(*) FROM {source_table}").fetchone()
                source = {"row_count": int(srow[0] or 0)}
                with pg_conn.cursor() as cur:
                    cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(label)))
                    target = {"row_count": int(cur.fetchone()[0] or 0)}
            else:
                source = _sqlite_count_window(
                    sqlite_conn,
                    table=source_table,
                    date_col=date_col,
                    cutoff=cutoff,
                    distinct_col=distinct_col,
                )
                target = _pg_count_window(
                    pg_conn,
                    table=label,
                    date_col=date_col,
                    cutoff=cutoff,
                    distinct_col=distinct_col,
                )

            mismatch = source != target
            if mismatch:
                out["issues"].append(f"mismatch:{label}")
            out["tables"][label] = {
                "source": source,
                "target": target,
                "cutoff": cutoff,
                "status": "ok" if not mismatch else "mismatch",
            }

        if cache_conn is not None:
            source_table = "daily_factor_returns"
            target_table = "model_factor_returns_daily"
            source_exists = _sqlite_table_exists(cache_conn, source_table)
            target_exists = _pg_table_exists(pg_conn, target_table)
            if source_exists and target_exists:
                source = _sqlite_count_window(
                    cache_conn,
                    table=source_table,
                    date_col="date",
                    cutoff=analytics_cutoff,
                    distinct_col=None,
                )
                target = _pg_count_window(
                    pg_conn,
                    table=target_table,
                    date_col="date",
                    cutoff=analytics_cutoff,
                    distinct_col=None,
                )
                mismatch = source != target
                if mismatch:
                    out["issues"].append(f"mismatch:{target_table}")
                out["tables"][target_table] = {
                    "source_table": source_table,
                    "source": source,
                    "target": target,
                    "cutoff": analytics_cutoff,
                    "status": "ok" if not mismatch else "mismatch",
                }
                source_cols = set(_sqlite_columns(cache_conn, source_table))
                target_cols = set(_pg_columns(pg_conn, target_table))
                required_cols = {
                    "date",
                    "factor_name",
                    "factor_return",
                    "robust_se",
                    "t_stat",
                    "r_squared",
                    "residual_vol",
                    "cross_section_n",
                    "eligible_n",
                    "coverage",
                }
                source_missing_required = sorted(required_cols - source_cols)
                target_missing_required = sorted(required_cols - target_cols)
                if source_missing_required:
                    out["issues"].append(f"missing_source_columns:{target_table}")
                if target_missing_required:
                    out["issues"].append(f"missing_target_columns:{target_table}")

                audit_cols = sorted(required_cols - {"date", "factor_name"})
                source_non_null = _sqlite_non_null_counts(
                    cache_conn,
                    table=source_table,
                    columns=audit_cols,
                    where_sql="WHERE date >= ?",
                    params=(analytics_cutoff,),
                )
                target_non_null = _pg_non_null_counts(
                    pg_conn,
                    table=target_table,
                    columns=audit_cols,
                    date_col="date",
                    cutoff=analytics_cutoff,
                )
                if source_non_null != target_non_null:
                    out["issues"].append(f"nonnull_mismatch:{target_table}")

                sample_dates = _sqlite_recent_dates(
                    cache_conn,
                    table=source_table,
                    date_col="date",
                    cutoff=analytics_cutoff,
                    limit=3,
                )
                source_factor_counts = _sqlite_group_count_by_date(
                    cache_conn,
                    table=source_table,
                    date_col="date",
                    dates=sample_dates,
                )
                target_factor_counts = _pg_group_count_by_date(
                    pg_conn,
                    table=target_table,
                    date_col="date",
                    dates=sample_dates,
                )
                if source_factor_counts != target_factor_counts:
                    out["issues"].append(f"factor_count_mismatch:{target_table}")

                source_values = _sqlite_factor_return_values(
                    cache_conn,
                    table=source_table,
                    dates=sample_dates,
                )
                target_values = _pg_factor_return_values(
                    pg_conn,
                    table=target_table,
                    dates=sample_dates,
                )
                values_ok, value_issues = _value_maps_match(source_values, target_values)
                if not values_ok:
                    out["issues"].append(f"value_mismatch:{target_table}")

                out["tables"][target_table].update(
                    {
                        "source_missing_required_columns": source_missing_required,
                        "target_missing_required_columns": target_missing_required,
                        "source_non_null_counts": source_non_null,
                        "target_non_null_counts": target_non_null,
                        "sample_dates": sample_dates,
                        "source_factor_counts_by_date": source_factor_counts,
                        "target_factor_counts_by_date": target_factor_counts,
                        "value_check_status": "ok" if values_ok else "mismatch",
                        "value_check_issues": value_issues[:20],
                    }
                )
            else:
                status = "skipped"
                reason = None
                if source_exists and not target_exists:
                    status = "mismatch"
                    reason = f"missing_target_table:{target_table}"
                    out["issues"].append(f"mismatch:{target_table}")
                elif not source_exists:
                    reason = f"missing_source_table:{source_table}"
                out["tables"][target_table] = {
                    "status": status,
                    "reason": reason,
                    "source_exists": bool(source_exists),
                    "target_exists": bool(target_exists),
                }

        out["status"] = "ok" if not out["issues"] else "mismatch"
        return out
    finally:
        sqlite_conn.close()
        if cache_conn is not None:
            cache_conn.close()
        pg_conn.close()


def run_neon_mirror_cycle(
    *,
    sqlite_path: Path,
    cache_path: Path | None = None,
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
        "factor_returns_sync": None,
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

    if cache_path is not None:
        out["factor_returns_sync"] = sync_factor_returns_to_neon(
            cache_path=Path(cache_path),
            dsn=dsn,
            mode=str(mode),
            batch_size=int(batch_size),
            analytics_years=int(analytics_years),
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
            cache_path=(Path(cache_path) if cache_path is not None else None),
            dsn=dsn,
            source_years=int(source_years),
            analytics_years=int(analytics_years),
        )
        if str(out["parity"].get("status")) != "ok":
            out["status"] = "mismatch"

    return out
