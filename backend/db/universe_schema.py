"""Canonical schema utilities for universe-management tables."""

from __future__ import annotations

import sqlite3


UNIVERSE_SUMMARY_TABLE = "universe_eligibility_summary"
UNIVERSE_SNAPSHOT_TABLE = "universe_constituent_snapshots"


def ensure_universe_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {UNIVERSE_SUMMARY_TABLE} (
            permid TEXT PRIMARY KEY,
            current_ric TEXT,
            ticker TEXT,
            common_name TEXT,
            exchange_name TEXT,
            instrument_is_active INTEGER NOT NULL DEFAULT 0,
            last_quote_date TEXT,
            delisting_reason TEXT,
            eligibility_state TEXT NOT NULL,
            start_date TEXT NOT NULL,
            end_date TEXT NOT NULL,
            in_current_snapshot INTEGER NOT NULL DEFAULT 0,
            in_historical_snapshot INTEGER NOT NULL DEFAULT 0,
            current_snapshot_date TEXT,
            historical_snapshot_date TEXT,
            is_trading_day_active INTEGER NOT NULL DEFAULT 0,
            is_eligible INTEGER NOT NULL DEFAULT 0,
            source TEXT,
            job_run_id TEXT,
            updated_at TEXT
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {UNIVERSE_SNAPSHOT_TABLE} (
            snapshot_label TEXT NOT NULL,
            snapshot_date TEXT NOT NULL,
            input_identifier TEXT,
            retrieval_method TEXT,
            input_ric TEXT NOT NULL,
            resolved_ric TEXT,
            permid TEXT,
            ticker TEXT,
            common_name TEXT,
            exchange_name TEXT,
            instrument_is_active INTEGER,
            source TEXT,
            job_run_id TEXT,
            updated_at TEXT,
            PRIMARY KEY (snapshot_label, snapshot_date, input_ric)
        )
        """
    )

    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{UNIVERSE_SUMMARY_TABLE}_ticker "
        f"ON {UNIVERSE_SUMMARY_TABLE}(ticker)"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{UNIVERSE_SUMMARY_TABLE}_eligibility "
        f"ON {UNIVERSE_SUMMARY_TABLE}(eligibility_state, is_eligible)"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{UNIVERSE_SUMMARY_TABLE}_active_dates "
        f"ON {UNIVERSE_SUMMARY_TABLE}(start_date, end_date)"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{UNIVERSE_SNAPSHOT_TABLE}_permid "
        f"ON {UNIVERSE_SNAPSHOT_TABLE}(permid)"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{UNIVERSE_SNAPSHOT_TABLE}_snapshot "
        f"ON {UNIVERSE_SNAPSHOT_TABLE}(snapshot_date, snapshot_label)"
    )


def clear_universe_tables(conn: sqlite3.Connection) -> None:
    ensure_universe_tables(conn)
    conn.execute(f"DELETE FROM {UNIVERSE_SUMMARY_TABLE}")
    conn.execute(f"DELETE FROM {UNIVERSE_SNAPSHOT_TABLE}")
