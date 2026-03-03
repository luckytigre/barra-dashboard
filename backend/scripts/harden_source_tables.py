"""One-shot hardening pass for source tables in data.db."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from db.fundamental_schema import ensure_fundamental_snapshots_schema
from db.prices_schema import ensure_prices_daily_schema
from db.trbc_schema import ensure_trbc_naming


def _dup_count(conn: sqlite3.Connection, table: str, date_col: str) -> int:
    row = conn.execute(
        f"""
        SELECT COUNT(*)
        FROM (
            SELECT ticker, {date_col}, COUNT(*) AS c
            FROM {table}
            GROUP BY ticker, {date_col}
            HAVING COUNT(*) > 1
        )
        """
    ).fetchone()
    return int(row[0] or 0) if row else 0


def harden(db_path: Path) -> dict[str, object]:
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    try:
        pre_f = _dup_count(conn, "fundamental_snapshots", "fetch_date")
        pre_p = _dup_count(conn, "prices_daily", "date")
        ensure_trbc_naming(conn)
        f_res = ensure_fundamental_snapshots_schema(conn, prune_extra_columns=True)
        p_res = ensure_prices_daily_schema(conn)
        conn.commit()
        post_f = _dup_count(conn, "fundamental_snapshots", "fetch_date")
        post_p = _dup_count(conn, "prices_daily", "date")
    finally:
        conn.close()
    return {
        "db_path": str(db_path),
        "fundamental_pre_dup_groups": pre_f,
        "fundamental_post_dup_groups": post_f,
        "prices_pre_dup_groups": pre_p,
        "prices_post_dup_groups": post_p,
        "fundamental_schema": f_res,
        "prices_schema": p_res,
    }


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Harden source tables and enforce unique stock/date keys.")
    p.add_argument("--db-path", default="backend/data.db", help="Path to data SQLite DB")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    print(harden(Path(args.db_path).expanduser()))
