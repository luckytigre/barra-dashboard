"""Schema helpers to normalize legacy industry naming to TRBC naming."""

from __future__ import annotations

import sqlite3
from typing import Iterable


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


def _index_exists(conn: sqlite3.Connection, index_name: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type='index' AND name=?
        LIMIT 1
        """,
        (index_name,),
    ).fetchone()
    return row is not None


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
    cols = _table_columns(conn, table)
    if column in cols:
        return
    conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")


def _rename_column_if_exists(
    conn: sqlite3.Connection,
    table: str,
    old_column: str,
    new_column: str,
) -> None:
    cols = _table_columns(conn, table)
    if old_column not in cols or new_column in cols:
        return
    try:
        conn.execute(f"ALTER TABLE {table} RENAME COLUMN {old_column} TO {new_column}")
    except sqlite3.OperationalError as exc:
        msg = str(exc).lower()
        if "no such column" in msg or "duplicate column name" in msg:
            return
        raise


def pick_trbc_industry_column(columns: Iterable[str]) -> str | None:
    cols = set(columns)
    for col in ("trbc_industry_group", "gics_industry_group", "industry_group"):
        if col in cols:
            return col
    return None


def pick_trbc_economic_sector_short_column(columns: Iterable[str]) -> str | None:
    cols = set(columns)
    for col in ("trbc_economic_sector_short", "trbc_sector", "trbc_economic_sector", "sector"):
        if col in cols:
            return col
    return None


def pick_trbc_business_sector_column(columns: Iterable[str]) -> str | None:
    cols = set(columns)
    for col in ("trbc_business_sector", "business_sector"):
        if col in cols:
            return col
    return None


def pick_trbc_industry_name_column(columns: Iterable[str]) -> str | None:
    cols = set(columns)
    for col in ("trbc_industry", "industry_name"):
        if col in cols:
            return col
    return None


def pick_trbc_activity_column(columns: Iterable[str]) -> str | None:
    cols = set(columns)
    for col in ("trbc_activity", "activity_name"):
        if col in cols:
            return col
    return None


def ensure_trbc_naming(conn: sqlite3.Connection) -> None:
    """Apply idempotent table/column/index renames from legacy names."""
    # Rename legacy history table.
    if _table_exists(conn, "gics_industry_history") and not _table_exists(conn, "trbc_industry_history"):
        conn.execute("ALTER TABLE gics_industry_history RENAME TO trbc_industry_history")

    # Rename history column.
    _rename_column_if_exists(
        conn,
        "trbc_industry_history",
        "gics_industry_group",
        "trbc_industry_group",
    )

    # Normalize fundamental snapshots naming.
    _rename_column_if_exists(
        conn,
        "fundamental_snapshots",
        "sector",
        "trbc_economic_sector_short",
    )
    _rename_column_if_exists(
        conn,
        "fundamental_snapshots",
        "trbc_sector",
        "trbc_economic_sector_short",
    )
    _rename_column_if_exists(
        conn,
        "fundamental_snapshots",
        "industry",
        "trbc_industry_group",
    )

    # Normalize historical index names.
    if _table_exists(conn, "trbc_industry_history"):
        _ensure_column(conn, "trbc_industry_history", "trbc_economic_sector", "TEXT")
        _ensure_column(conn, "trbc_industry_history", "trbc_business_sector", "TEXT")
        _ensure_column(conn, "trbc_industry_history", "trbc_industry_group", "TEXT")
        _ensure_column(conn, "trbc_industry_history", "trbc_industry", "TEXT")
        _ensure_column(conn, "trbc_industry_history", "trbc_activity", "TEXT")
        if _index_exists(conn, "idx_gics_industry_history_date"):
            conn.execute("DROP INDEX idx_gics_industry_history_date")
        if _index_exists(conn, "idx_gics_industry_history_ticker"):
            conn.execute("DROP INDEX idx_gics_industry_history_ticker")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_trbc_industry_history_date "
            "ON trbc_industry_history(as_of_date)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_trbc_industry_history_ticker "
            "ON trbc_industry_history(ticker)"
        )

    # fundamental_snapshots is intentionally TRBC-free; TRBC source of truth
    # lives in trbc_industry_history and is PIT-joined downstream.
