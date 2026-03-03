"""Schema hardening utilities for prices_daily."""

from __future__ import annotations

import sqlite3


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


def ensure_prices_daily_schema(conn: sqlite3.Connection) -> dict[str, object]:
    table = "prices_daily"
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            ticker TEXT,
            date TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            adj_close REAL,
            volume REAL,
            currency TEXT,
            exchange TEXT,
            source TEXT,
            updated_at TEXT
        )
        """
    )

    conn.execute(
        f"""
        DELETE FROM {table}
        WHERE ticker IS NULL
           OR TRIM(ticker) = ''
           OR date IS NULL
           OR TRIM(date) = ''
        """
    )
    conn.execute(f"UPDATE {table} SET ticker = UPPER(TRIM(ticker))")
    conn.execute(f"UPDATE {table} SET date = TRIM(date)")
    conn.execute(
        f"""
        DELETE FROM {table}
        WHERE rowid IN (
            SELECT rowid
            FROM (
                SELECT
                    rowid,
                    ROW_NUMBER() OVER (
                        PARTITION BY ticker, date
                        ORDER BY
                            CASE WHEN LOWER(COALESCE(source, '')) = 'lseg_toolkit' THEN 1 ELSE 0 END DESC,
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

    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_prices_ticker ON {table}(ticker)")
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_prices_date ON {table}(date)")
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_prices_ticker_date ON {table}(ticker, date)")
    conn.execute(
        f"CREATE UNIQUE INDEX IF NOT EXISTS idx_prices_ticker_date_unique "
        f"ON {table}(ticker, date)"
    )
    return {"status": "ok"}

