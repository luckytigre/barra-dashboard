"""Update local SQLite market/fundamental data using jl-lseg-toolkit.

This script is intentionally schema-compatible with the existing dashboard DB:
  - updates `fundamental_snapshots` (market_cap/TRBC sector + basic fields)
  - updates `prices_daily` (latest close snapshot)

It does NOT overwrite `barra_exposures`. Factor exposures are handled separately.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "vendor"))

from lseg_ric_resolver import ensure_ric_map_table, load_ric_map, resolve_ric_map
from db.trbc_schema import ensure_trbc_naming
from portfolio.mock_portfolio import get_tickers

DEFAULT_DB = Path(__file__).resolve().parent.parent / "data.db"
LSEG_BATCH_SIZE = 500
SQLITE_TIMEOUT_SECONDS = 120
SQLITE_BUSY_TIMEOUT_MS = 120000
SQLITE_MAX_RETRIES = 6
SQLITE_RETRY_SLEEP_SECONDS = 2.0


def _to_local_ticker(ric: str) -> str:
    base = str(ric or "").strip().upper()
    if not base:
        return base
    return base.split(".", 1)[0]


def _pick_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    cols = {c.lower(): c for c in df.columns}
    for c in candidates:
        got = cols.get(c.lower())
        if got:
            return got
    return None


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
    cols = {str(c).lower() for c in _existing_cols(conn, table)}
    if column.lower() in cols:
        return
    conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")


def _ensure_tables(conn: sqlite3.Connection) -> None:
    ensure_trbc_naming(conn)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fundamental_snapshots (
            ticker TEXT,
            fetch_date TEXT,
            market_cap TEXT,
            shares_outstanding TEXT,
            dividend_yield TEXT,
            trbc_sector TEXT,
            trbc_industry_group TEXT,
            source TEXT,
            job_run_id TEXT,
            updated_at TEXT
        )
        """
    )
    _ensure_column(conn, "fundamental_snapshots", "common_name", "TEXT")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS prices_daily (
            ticker TEXT,
            date TEXT,
            open TEXT,
            high TEXT,
            low TEXT,
            close TEXT,
            adj_close TEXT,
            volume TEXT,
            currency TEXT,
            exchange TEXT,
            source TEXT,
            updated_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS trbc_industry_history (
            ticker TEXT NOT NULL,
            as_of_date TEXT NOT NULL,
            trbc_industry_group TEXT,
            trbc_economic_sector TEXT,
            source TEXT,
            job_run_id TEXT,
            updated_at TEXT,
            PRIMARY KEY (ticker, as_of_date)
        )
        """
    )


def _existing_cols(conn: sqlite3.Connection, table: str) -> list[str]:
    cur = conn.execute(f"PRAGMA table_info({table})")
    return [str(r[1]) for r in cur.fetchall()]


def _insert_rows(
    conn: sqlite3.Connection,
    table: str,
    rows: list[dict[str, Any]],
    *,
    replace: bool = False,
) -> int:
    if not rows:
        return 0
    cols = _existing_cols(conn, table)
    use_cols = [c for c in cols if c in rows[0]]
    if not use_cols:
        return 0
    placeholders = ",".join("?" for _ in use_cols)
    insert_kw = "INSERT OR REPLACE" if replace else "INSERT"
    sql = f'{insert_kw} INTO {table} ({",".join(use_cols)}) VALUES ({placeholders})'
    payload = [tuple(r.get(c) for c in use_cols) for r in rows]
    for attempt in range(SQLITE_MAX_RETRIES):
        try:
            conn.executemany(sql, payload)
            break
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc).lower() or attempt + 1 >= SQLITE_MAX_RETRIES:
                raise
            time.sleep(SQLITE_RETRY_SLEEP_SECONDS * (attempt + 1))
    return len(rows)


def _connect_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), timeout=SQLITE_TIMEOUT_SECONDS)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS}")
    return conn


def _backfill_common_names(conn: sqlite3.Connection) -> int:
    cols = {str(c).lower() for c in _existing_cols(conn, "fundamental_snapshots")}
    if "common_name" not in cols:
        return 0
    before = conn.total_changes
    conn.execute(
        """
        WITH latest_name AS (
            SELECT ticker, common_name
            FROM (
                SELECT
                    ticker,
                    common_name,
                    ROW_NUMBER() OVER (
                        PARTITION BY ticker
                        ORDER BY fetch_date DESC, updated_at DESC
                    ) AS rn
                FROM fundamental_snapshots
                WHERE common_name IS NOT NULL
                  AND TRIM(common_name) <> ''
            )
            WHERE rn = 1
        )
        UPDATE fundamental_snapshots
        SET common_name = (
            SELECT l.common_name
            FROM latest_name l
            WHERE l.ticker = fundamental_snapshots.ticker
        )
        WHERE (common_name IS NULL OR TRIM(common_name) = '')
          AND ticker IN (SELECT ticker FROM latest_name)
        """
    )
    return int(conn.total_changes - before)


def _resolve_universe(
    *,
    db_path: Path,
    index: str | None,
    tickers_csv: str | None,
    _ric_suffix: str,
) -> list[str]:
    if tickers_csv:
        return [t.strip().upper() for t in tickers_csv.split(",") if t.strip()]
    if index:
        LsegClient = _load_lseg_client()

        with LsegClient() as client:
            rics = client.get_index_constituents(index=index)
        # Convert any returned RICs into local plain tickers for shared resolver path.
        return [_to_local_ticker(str(r)) for r in rics if str(r).strip()]
    # Default: use the local barra universe if available, else fallback to mock portfolio.
    try:
        conn = sqlite3.connect(str(db_path))
        try:
            rows = conn.execute("SELECT DISTINCT ticker FROM barra_exposures ORDER BY ticker").fetchall()
        finally:
            conn.close()
        if rows:
            tickers = [str(r[0]).strip().upper() for r in rows if r and str(r[0]).strip()]
            if tickers:
                return tickers
    except Exception:
        pass
    return [t.strip().upper() for t in get_tickers() if str(t).strip()]


def _load_lseg_client():
    try:
        from lseg_toolkit import LsegClient
    except Exception as exc:
        raise RuntimeError(
            "Unable to import lseg_toolkit/LSEG runtime. "
            "Ensure vendored toolkit is present and `lseg-data` is installed."
        ) from exc
    return LsegClient


def download_from_lseg(
    *,
    db_path: Path = DEFAULT_DB,
    index: str | None = None,
    tickers_csv: str | None = None,
    ric_suffix: str = ".O",
    as_of_date: str | None = None,
    shard_count: int = 1,
    shard_index: int = 0,
    skip_common_name_backfill: bool = False,
) -> dict[str, Any]:
    LsegClient = _load_lseg_client()

    as_of = str(as_of_date or datetime.now(timezone.utc).date().isoformat())
    updated_at = datetime.now(timezone.utc).isoformat()
    universe = _resolve_universe(
        db_path=db_path,
        index=index,
        tickers_csv=tickers_csv,
        _ric_suffix=ric_suffix,
    )
    shard_count = max(1, int(shard_count))
    shard_index = int(shard_index)
    if shard_index < 0 or shard_index >= shard_count:
        raise ValueError(f"shard_index must be in [0, {shard_count - 1}]")
    if shard_count > 1:
        universe = [
            t
            for t in universe
            if int(hashlib.md5(t.encode("utf-8")).hexdigest(), 16) % shard_count == shard_index
        ]
    if not universe:
        return {
            "status": "no-universe",
            "as_of": as_of,
            "shard_index": int(shard_index),
            "shard_count": int(shard_count),
        }

    conn = _connect_db(db_path)
    _ensure_tables(conn)
    ensure_ric_map_table(conn)

    print(f"Resolving LSEG RICs for {len(universe)} tickers...")
    ticker_to_ric: dict[str, str] = load_ric_map(conn)
    company_parts: list[pd.DataFrame] = []
    with LsegClient() as client:
        ticker_to_ric = resolve_ric_map(
            client=client,
            conn=conn,
            tickers=universe,
            as_of_date=as_of,
            source="lseg_daily",
            suffixes=[ric_suffix, ".N", ".O", ".A", ".K", ".P", ".PK", ""],
            batch_size=LSEG_BATCH_SIZE,
        )
        ric_universe = [ticker_to_ric[t] for t in universe if t in ticker_to_ric]
        ric_to_ticker = {v.upper(): k.upper() for k, v in ticker_to_ric.items() if v}

        print(f"Fetching LSEG data for {len(ric_universe)} instruments...")
        for i in range(0, len(ric_universe), LSEG_BATCH_SIZE):
            batch = ric_universe[i : i + LSEG_BATCH_SIZE]
            part = client.get_company_data(
                batch,
                fields=[
                    "TR.CommonName",
                    "TR.TRBCEconomicSector",
                    "TR.TRBCIndustryGroup",
                    "TR.PriceClose",
                    "TR.CompanyMarketCap",
                    "TR.SharesOutstanding",
                    "TR.DividendYield",
                ],
                as_of_date=as_of,
            )
            if part is not None and not part.empty:
                company_parts.append(part)
            done = min(i + LSEG_BATCH_SIZE, len(ric_universe))
            print(f"  company_data: {done:,}/{len(ric_universe):,}")

    company = pd.concat(company_parts, ignore_index=True) if company_parts else pd.DataFrame()

    if company is None or company.empty:
        conn.close()
        return {"status": "no-data", "as_of": as_of, "universe": len(universe)}

    instrument_col = _pick_col(company, ["Instrument"])
    price_col = _pick_col(company, ["Price Close"])
    mcap_col = _pick_col(company, ["Company Market Cap"])
    sector_col = _pick_col(company, ["TRBC Economic Sector Name", "TRBC Economic Sector"])
    industry_col = _pick_col(company, ["TRBC Industry Group Name", "TRBC Industry Group"])
    common_name_col = _pick_col(company, ["Company Common Name", "Common Name", "TR.CommonName"])
    shares_col = _pick_col(company, ["Shares Outstanding", "Shares Outstanding - Common Stock"])
    divy_col = _pick_col(company, ["Dividend Yield"])
    if not instrument_col:
        raise RuntimeError("LSEG response missing Instrument column")

    job_run_id = f"lseg_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    fundamentals_rows: list[dict[str, Any]] = []
    prices_rows: list[dict[str, Any]] = []
    trbc_history_rows: list[dict[str, Any]] = []

    for _, row in company.iterrows():
        ric = str(row.get(instrument_col) or "").strip().upper()
        ticker = ric_to_ticker.get(ric) or _to_local_ticker(ric)
        if not ticker:
            continue
        close = row.get(price_col) if price_col else None
        market_cap = row.get(mcap_col) if mcap_col else None
        trbc_sector = row.get(sector_col) if sector_col else None
        trbc_industry = row.get(industry_col) if industry_col else None
        common_name = row.get(common_name_col) if common_name_col else None
        shares_outstanding = row.get(shares_col) if shares_col else None
        dividend_yield = row.get(divy_col) if divy_col else None

        trbc_industry_group = None if pd.isna(trbc_industry) else str(trbc_industry).strip()
        if trbc_industry_group in {"", "None", "nan"}:
            trbc_industry_group = None

        fundamentals_rows.append(
            {
                "ticker": ticker,
                "fetch_date": as_of,
                "market_cap": None if pd.isna(market_cap) else str(market_cap),
                "shares_outstanding": None if pd.isna(shares_outstanding) else str(shares_outstanding),
                "dividend_yield": None if pd.isna(dividend_yield) else str(dividend_yield),
                "common_name": None if pd.isna(common_name) else str(common_name).strip(),
                "trbc_sector": None if pd.isna(trbc_sector) else str(trbc_sector),
                "trbc_industry_group": None if pd.isna(trbc_industry) else str(trbc_industry),
                "source": "lseg_toolkit",
                "job_run_id": job_run_id,
                "updated_at": updated_at,
            }
        )
        trbc_history_rows.append(
            {
                "ticker": ticker,
                "as_of_date": as_of,
                "trbc_industry_group": trbc_industry_group,
                "trbc_economic_sector": None if pd.isna(trbc_sector) else str(trbc_sector),
                "source": "lseg_toolkit",
                "job_run_id": job_run_id,
                "updated_at": updated_at,
            }
        )

        prices_rows.append(
            {
                "ticker": ticker,
                "date": as_of,
                "open": None if pd.isna(close) else str(close),
                "high": None if pd.isna(close) else str(close),
                "low": None if pd.isna(close) else str(close),
                "close": None if pd.isna(close) else str(close),
                "adj_close": None if pd.isna(close) else str(close),
                "volume": None,
                "currency": None,
                "exchange": None,
                "source": "lseg_toolkit",
                "updated_at": updated_at,
            }
        )

    try:
        n_f = _insert_rows(conn, "fundamental_snapshots", fundamentals_rows)
        n_p = _insert_rows(conn, "prices_daily", prices_rows)
        n_g = _insert_rows(conn, "trbc_industry_history", trbc_history_rows, replace=True)
        n_name_backfill = 0
        if not skip_common_name_backfill:
            n_name_backfill = _backfill_common_names(conn)
        n_ric = conn.execute("SELECT COUNT(*) FROM ticker_ric_map").fetchone()[0]
        conn.execute("CREATE INDEX IF NOT EXISTS idx_fund_ticker ON fundamental_snapshots(ticker)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_fund_date ON fundamental_snapshots(fetch_date)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_prices_ticker ON prices_daily(ticker)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_prices_date ON prices_daily(date)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_prices_ticker_date ON prices_daily(ticker, date)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_trbc_hist_date ON trbc_industry_history(as_of_date)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_trbc_hist_ticker ON trbc_industry_history(ticker)")
        conn.commit()
    finally:
        conn.close()

    out = {
        "status": "ok",
        "as_of": as_of,
        "universe": len(universe),
        "fundamental_rows_inserted": n_f,
        "price_rows_inserted": n_p,
        "trbc_rows_inserted": n_g,
        "common_name_rows_backfilled": int(n_name_backfill),
        "ticker_ric_map_size": int(n_ric or 0),
        "db_path": str(db_path),
        "shard_index": int(shard_index),
        "shard_count": int(shard_count),
        "skip_common_name_backfill": bool(skip_common_name_backfill),
    }
    print(out)
    return out


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Update local SQLite data using jl-lseg-toolkit.")
    p.add_argument("--db-path", default=str(DEFAULT_DB), help="Path to target SQLite DB")
    p.add_argument("--index", default=None, help="Index code (e.g. SPX, NDX). If set, uses index constituents")
    p.add_argument("--tickers", default=None, help="Comma-separated plain tickers to fetch")
    p.add_argument("--ric-suffix", default=".O", help="Suffix when converting plain tickers to RICs")
    p.add_argument("--as-of-date", default=None, help="Override as-of date (YYYY-MM-DD)")
    p.add_argument("--shard-count", type=int, default=1, help="Total number of ticker shards")
    p.add_argument("--shard-index", type=int, default=0, help="Zero-based shard index to process")
    p.add_argument("--skip-common-name-backfill", action="store_true", help="Skip common_name carry-forward pass")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    download_from_lseg(
        db_path=Path(args.db_path),
        index=args.index,
        tickers_csv=args.tickers,
        ric_suffix=args.ric_suffix,
        as_of_date=args.as_of_date,
        shard_count=args.shard_count,
        shard_index=args.shard_index,
        skip_common_name_backfill=bool(args.skip_common_name_backfill),
    )
