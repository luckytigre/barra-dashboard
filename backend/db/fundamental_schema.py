"""Canonical schema utilities for fundamental_snapshots."""

from __future__ import annotations

import sqlite3


FUNDAMENTAL_SNAPSHOT_COLUMNS: list[tuple[str, str]] = [
    ("ticker", "TEXT"),
    ("fetch_date", "TEXT"),
    ("market_cap", "REAL"),
    ("shares_outstanding", "REAL"),
    ("dividend_yield", "REAL"),
    ("common_name", "TEXT"),
    ("book_value", "REAL"),
    ("forward_eps", "REAL"),
    ("trailing_eps", "REAL"),
    ("total_debt", "REAL"),
    ("cash_and_equivalents", "REAL"),
    ("long_term_debt", "REAL"),
    ("free_cash_flow", "REAL"),
    ("gross_profit", "REAL"),
    ("net_income", "REAL"),
    ("operating_cashflow", "REAL"),
    ("capital_expenditures", "REAL"),
    ("shares_basic", "REAL"),
    ("shares_diluted", "REAL"),
    ("free_float_shares", "REAL"),
    ("free_float_percent", "REAL"),
    ("revenue", "REAL"),
    ("ebitda", "REAL"),
    ("ebit", "REAL"),
    ("total_assets", "REAL"),
    ("total_liabilities", "REAL"),
    ("return_on_equity", "REAL"),
    ("operating_margins", "REAL"),
    ("fundamental_period_end_date", "TEXT"),
    ("report_currency", "TEXT"),
    ("fiscal_year", "INTEGER"),
    ("period_type", "TEXT"),
    ("source", "TEXT"),
    ("job_run_id", "TEXT"),
    ("updated_at", "TEXT"),
]


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type='table' AND name=?
        LIMIT 1
        """,
        (table,),
    ).fetchone()
    return row is not None


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    if not _table_exists(conn, table):
        return set()
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {str(r[1]) for r in rows}


def _create_canonical_table(conn: sqlite3.Connection, table: str) -> None:
    ddl_cols = ",\n            ".join(f"{name} {dtype}" for name, dtype in FUNDAMENTAL_SNAPSHOT_COLUMNS)
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            {ddl_cols}
        )
        """
    )


def _ensure_indexes(conn: sqlite3.Connection, table: str) -> None:
    # Keep stable index names used elsewhere in the project.
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_fund_ticker ON {table}(ticker)")
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_fund_date ON {table}(fetch_date)")
    conn.execute(
        f"CREATE UNIQUE INDEX IF NOT EXISTS idx_fund_ticker_fetch_date_unique "
        f"ON {table}(ticker, fetch_date)"
    )


def _normalize_and_dedupe(conn: sqlite3.Connection, table: str) -> None:
    conn.execute(
        f"""
        DELETE FROM {table}
        WHERE ticker IS NULL
           OR TRIM(ticker) = ''
           OR fetch_date IS NULL
           OR TRIM(fetch_date) = ''
        """
    )
    conn.execute(f"UPDATE {table} SET ticker = UPPER(TRIM(ticker))")
    conn.execute(f"UPDATE {table} SET fetch_date = TRIM(fetch_date)")
    conn.execute(
        f"""
        DELETE FROM {table}
        WHERE rowid IN (
            SELECT rowid
            FROM (
                SELECT
                    rowid,
                    ROW_NUMBER() OVER (
                        PARTITION BY ticker, fetch_date
                        ORDER BY
                            CASE WHEN updated_at IS NULL OR TRIM(updated_at) = '' THEN 0 ELSE 1 END DESC,
                            updated_at DESC,
                            rowid DESC
                    ) AS rn
                FROM {table}
            )
            WHERE rn > 1
        )
        """
    )


def ensure_fundamental_snapshots_schema(
    conn: sqlite3.Connection,
    *,
    prune_extra_columns: bool = False,
) -> dict[str, object]:
    """Ensure canonical column set for fundamental_snapshots.

    When prune_extra_columns=True, rebuilds the table to drop any legacy columns.
    """
    table = "fundamental_snapshots"
    canonical = [c for c, _ in FUNDAMENTAL_SNAPSHOT_COLUMNS]
    canonical_set = set(canonical)

    if not _table_exists(conn, table):
        _create_canonical_table(conn, table)
        _normalize_and_dedupe(conn, table)
        _ensure_indexes(conn, table)
        return {"status": "created", "pruned": False, "extra_columns": []}

    existing = _table_columns(conn, table)
    missing = [c for c, _ in FUNDAMENTAL_SNAPSHOT_COLUMNS if c not in existing]
    for col, ddl in FUNDAMENTAL_SNAPSHOT_COLUMNS:
        if col in missing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {ddl}")

    existing = _table_columns(conn, table)
    extra = sorted(c for c in existing if c not in canonical_set)
    if prune_extra_columns and extra:
        tmp = f"{table}__canonical_tmp"
        conn.execute(f"DROP TABLE IF EXISTS {tmp}")
        _create_canonical_table(conn, tmp)
        copy_cols = [c for c in canonical if c in existing]
        conn.execute(
            f"""
            INSERT INTO {tmp} ({", ".join(copy_cols)})
            SELECT {", ".join(copy_cols)}
            FROM {table}
            """
        )
        conn.execute(f"DROP TABLE {table}")
        conn.execute(f"ALTER TABLE {tmp} RENAME TO {table}")
        _normalize_and_dedupe(conn, table)
        _ensure_indexes(conn, table)
        return {"status": "rebuilt", "pruned": True, "extra_columns": extra}

    _normalize_and_dedupe(conn, table)
    _ensure_indexes(conn, table)
    return {"status": "ok", "pruned": False, "extra_columns": extra}
