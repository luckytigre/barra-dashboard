"""Reset universe + source-of-truth tables and run deterministic rebuild steps."""

from __future__ import annotations

import argparse
import sqlite3
from datetime import date
from pathlib import Path

from build_universe_eligibility_lseg import build_universe_eligibility
from download_data_lseg import download_from_lseg
from harden_source_tables import harden


RESET_TABLES = [
    "fundamental_snapshots",
    "prices_daily",
    "trbc_industry_history",
    "ticker_ric_map",
    "barra_raw_cross_section_history",
    "universe_cross_section_snapshot",
]


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), timeout=120)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=120000")
    return conn


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


def _truncate_source_tables(db_path: Path) -> dict[str, int]:
    conn = _connect(db_path)
    stats: dict[str, int] = {}
    try:
        for table in RESET_TABLES:
            if not _table_exists(conn, table):
                continue
            before = int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] or 0)
            conn.execute(f"DELETE FROM {table}")
            stats[table] = before
        conn.commit()
    finally:
        conn.close()
    return stats


def run(
    *,
    db_path: Path,
    current_chain_ric: str,
    historical_index_ric: str,
    historical_date: str,
    current_date: str,
    russell_xlsx: Path | None,
    supplemental_historical_xlsx: list[Path] | None,
    reset_source_tables: bool,
    run_harden: bool,
    ingest_current_snapshot: bool,
    shard_count: int,
    shard_index: int,
) -> dict[str, object]:
    universe = build_universe_eligibility(
        db_path=db_path,
        current_chain_ric=current_chain_ric,
        historical_index_ric=historical_index_ric,
        historical_date=historical_date,
        current_date=current_date,
        russell_xlsx=russell_xlsx,
        supplemental_historical_xlsx=supplemental_historical_xlsx,
        reset=True,
        output_csv=None,
    )

    truncated = _truncate_source_tables(db_path) if reset_source_tables else {}
    hardened = harden(db_path) if run_harden else {"skipped": True}

    ingest = None
    if ingest_current_snapshot:
        ingest = download_from_lseg(
            db_path=db_path,
            index=None,
            tickers_csv=None,
            as_of_date=current_date,
            shard_count=shard_count,
            shard_index=shard_index,
            skip_common_name_backfill=bool(shard_count > 1),
        )

    out = {
        "status": "ok",
        "universe": universe,
        "source_tables_truncated": truncated,
        "harden": hardened,
        "ingest_current_snapshot": ingest,
    }
    print(out)
    return out


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Reset universe/source tables and rebuild deterministic source-of-truth flow.")
    p.add_argument("--db-path", default="backend/data.db", help="Path to SQLite data DB")
    p.add_argument("--current-chain-ric", default="0#.SPX,0#.MID,0#.RTY,0#.NDX", help="Current constituent chain/index RICs")
    p.add_argument("--historical-index-ric", default=".SPX,.MID,.RTY,.NDX", help="Historical index RICs")
    p.add_argument("--historical-date", default="2019-03-02", help="Historical snapshot date")
    p.add_argument("--current-date", default=None, help="Current snapshot date (YYYY-MM-DD). Default=today")
    p.add_argument("--russell-xlsx", default=None, help="Optional XLSX path to use as Russell 2000 constituent source")
    p.add_argument(
        "--supplemental-historical-xlsx",
        default=None,
        help="Optional comma-separated XLSX paths with additional historical constituents",
    )
    p.add_argument("--skip-reset-source", action="store_true", help="Do not truncate source tables")
    p.add_argument("--skip-harden", action="store_true", help="Skip source-table hardening step")
    p.add_argument("--skip-ingest-current", action="store_true", help="Do not run LSEG current snapshot ingest")
    p.add_argument("--shard-count", type=int, default=1, help="Shard count for current ingest")
    p.add_argument("--shard-index", type=int, default=0, help="Shard index for current ingest")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    run(
        db_path=Path(args.db_path).expanduser(),
        current_chain_ric=str(args.current_chain_ric),
        historical_index_ric=str(args.historical_index_ric),
        historical_date=str(args.historical_date),
        current_date=str(args.current_date) if args.current_date else str(date.today()),
        russell_xlsx=Path(args.russell_xlsx).expanduser() if args.russell_xlsx else None,
        supplemental_historical_xlsx=[
            Path(x.strip()).expanduser()
            for x in str(args.supplemental_historical_xlsx).replace(";", ",").split(",")
            if x.strip()
        ]
        if args.supplemental_historical_xlsx
        else None,
        reset_source_tables=not bool(args.skip_reset_source),
        run_harden=not bool(args.skip_harden),
        ingest_current_snapshot=not bool(args.skip_ingest_current),
        shard_count=int(args.shard_count),
        shard_index=int(args.shard_index),
    )
