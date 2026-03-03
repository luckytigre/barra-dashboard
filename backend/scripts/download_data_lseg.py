"""Update local SQLite market/fundamental data using jl-lseg-toolkit."""

from __future__ import annotations

import argparse
import hashlib
import os
import re
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "vendor"))

from db.fundamental_schema import ensure_fundamental_snapshots_schema
from db.prices_schema import ensure_prices_daily_schema
from db.trbc_schema import ensure_trbc_naming
from lseg_ric_resolver import ensure_ric_map_table, load_ric_map, resolve_ric_map
from trading_calendar import previous_or_same_xnys_session

_DB_RAW = Path(os.getenv("DATA_DB_PATH", "data.db")).expanduser()
DEFAULT_DB = _DB_RAW if _DB_RAW.is_absolute() else (Path(__file__).resolve().parent.parent / _DB_RAW)
LSEG_BATCH_SIZE = 500
SQLITE_TIMEOUT_SECONDS = 120
SQLITE_BUSY_TIMEOUT_MS = 120000
SQLITE_MAX_RETRIES = 6
SQLITE_RETRY_SLEEP_SECONDS = 2.0

LSEG_FIELDS = [
    "TR.CommonName",
    "TR.TRBCEconomicSector",
    "TR.TRBCBusinessSector",
    "TR.TRBCIndustryGroup",
    "TR.TRBCIndustry",
    "TR.TRBCActivity",
    "TR.HQCountryCode",
    "TR.PriceClose",
    "TR.CompanyMarketCap",
    "TR.SharesOutstanding",
    "TR.DividendYield",
    "TR.BookValuePerShare",
    "TR.EPSMean",
    "TR.EPSActValue",
    "TR.TotalDebt",
    "TR.CashAndEquivalents",
    "TR.LongTermDebt",
    "TR.FreeCashFlow",
    "TR.GrossProfit",
    "TR.NetIncome",
    "TR.CashFromOperatingActivities",
    "TR.CapitalExpenditures",
    "TR.BasicWeightedAverageShares",
    "TR.DilutedWeightedAverageShares",
    "TR.FreeFloat",
    "TR.FreeFloatPct",
    "TR.Revenue",
    "TR.Revenue.fperiod",
    "TR.Revenue.currency",
    "TR.Revenue.periodenddate",
    "TR.EBITDA",
    "TR.EBIT",
    "TR.TotalAssets",
    "TR.TotalLiabilities",
    "TR.ROEPercent",
    "TR.OperatingMarginPercent",
]


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


def _existing_cols(conn: sqlite3.Connection, table: str) -> list[str]:
    cur = conn.execute(f"PRAGMA table_info({table})")
    return [str(r[1]) for r in cur.fetchall()]


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
    cols = {str(c).lower() for c in _existing_cols(conn, table)}
    if column.lower() in cols:
        return
    try:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")
    except sqlite3.OperationalError as exc:
        if "duplicate column name" not in str(exc).lower():
            raise


def _ensure_tables(conn: sqlite3.Connection) -> None:
    ensure_trbc_naming(conn)
    ensure_fundamental_snapshots_schema(conn, prune_extra_columns=True)
    ensure_prices_daily_schema(conn)

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
    for col in ["trbc_business_sector", "trbc_industry", "trbc_activity", "hq_country_code"]:
        _ensure_column(conn, "trbc_industry_history", col, "TEXT")


def _connect_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), timeout=SQLITE_TIMEOUT_SECONDS)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS}")
    return conn


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


def _backfill_common_names(conn: sqlite3.Connection) -> int:
    before = conn.total_changes
    conn.execute(
        """
        WITH stable_name AS (
            SELECT s.ticker, MIN(s.common_name) AS common_name
            FROM (
                SELECT
                    UPPER(f.ticker) AS ticker,
                    TRIM(f.common_name) AS common_name
                FROM fundamental_snapshots f
                JOIN ticker_ric_map m
                  ON UPPER(f.ticker) = UPPER(m.ticker)
                WHERE f.common_name IS NOT NULL
                  AND TRIM(f.common_name) <> ''
                  AND COALESCE(m.classification_ok, 0) = 1
            ) s
            GROUP BY s.ticker
            HAVING COUNT(DISTINCT s.common_name) = 1
        )
        UPDATE fundamental_snapshots
        SET common_name = (
            SELECT n.common_name
            FROM stable_name n
            WHERE n.ticker = UPPER(fundamental_snapshots.ticker)
        )
        WHERE (common_name IS NULL OR TRIM(common_name) = '')
          AND UPPER(ticker) IN (SELECT ticker FROM stable_name)
        """
    )
    return int(conn.total_changes - before)


def _resolve_universe(
    *,
    db_path: Path,
    index: str | None,
    tickers_csv: str | None,
    _ric_suffix: str,
    as_of_date: str | None,
) -> list[str]:
    if tickers_csv:
        return [t.strip().upper() for t in tickers_csv.split(",") if t.strip()]
    if index:
        LsegClient = _load_lseg_client()
        with LsegClient() as client:
            rics = client.get_index_constituents(index=index)
        return [_to_local_ticker(str(r)) for r in rics if str(r).strip()]
    conn = sqlite3.connect(str(db_path))
    try:
        tables = {
            str(r[0]) for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
        if "universe_eligibility_summary" in tables:
            if as_of_date:
                rows = conn.execute(
                    """
                    SELECT DISTINCT UPPER(ticker) AS ticker
                    FROM universe_eligibility_summary
                    WHERE ticker IS NOT NULL
                      AND TRIM(ticker) <> ''
                      AND COALESCE(start_date, '') <> ''
                      AND COALESCE(end_date, '') <> ''
                      AND start_date <= ?
                      AND end_date >= ?
                    ORDER BY ticker
                    """,
                    (str(as_of_date), str(as_of_date)),
                ).fetchall()
                tickers = [str(r[0]).strip().upper() for r in rows if r and str(r[0]).strip()]
                if tickers:
                    return tickers

            rows = conn.execute(
                """
                SELECT DISTINCT UPPER(ticker) AS ticker
                FROM universe_eligibility_summary
                WHERE ticker IS NOT NULL
                  AND TRIM(ticker) <> ''
                ORDER BY ticker
                """
            ).fetchall()
            tickers = [str(r[0]).strip().upper() for r in rows if r and str(r[0]).strip()]
            if tickers:
                return tickers
        parts: list[str] = []
        if "ticker_ric_map" in tables:
            parts.append(
                "SELECT DISTINCT UPPER(ticker) AS ticker FROM ticker_ric_map WHERE COALESCE(classification_ok, 0) = 1"
            )
        if "fundamental_snapshots" in tables:
            parts.append("SELECT DISTINCT UPPER(ticker) AS ticker FROM fundamental_snapshots")
        if "prices_daily" in tables:
            parts.append("SELECT DISTINCT UPPER(ticker) AS ticker FROM prices_daily")
        if "trbc_industry_history" in tables:
            parts.append("SELECT DISTINCT UPPER(ticker) AS ticker FROM trbc_industry_history")
        if "barra_raw_cross_section_history" in tables:
            parts.append("SELECT DISTINCT UPPER(ticker) AS ticker FROM barra_raw_cross_section_history")
        if not parts:
            return []
        rows = conn.execute(f"{' UNION '.join(parts)} ORDER BY ticker").fetchall()
    finally:
        conn.close()
    return [str(r[0]).strip().upper() for r in rows if r and str(r[0]).strip()]


def _load_lseg_client():
    try:
        from lseg_toolkit import LsegClient
    except Exception as exc:
        raise RuntimeError(
            "Unable to import lseg_toolkit/LSEG runtime. "
            "Ensure vendored toolkit is present and `lseg-data` is installed."
        ) from exc
    return LsegClient


def _iso_date(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return None
        return str(value.date())
    s = str(value).strip()
    if not s or s.lower() in {"nan", "nat", "none"}:
        return None
    return s


def _float_or_none(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    try:
        return float(value)
    except Exception:
        return None


def _parse_fiscal_period(value: Any) -> tuple[int | None, str | None, str | None]:
    if value is None or pd.isna(value):
        return None, None, None
    s = str(value).strip().upper()
    if not s:
        return None, None, None

    m_fy = re.match(r"^FY\D*(\d{4})$", s)
    if m_fy:
        return int(m_fy.group(1)), None, "FY"

    m_fq = re.match(r"^F?Q([1-4])\D*(\d{4})$", s)
    if m_fq:
        return int(m_fq.group(2)), f"Q{m_fq.group(1)}", "FQ"

    m_year = re.search(r"(\d{4})", s)
    if m_year:
        return int(m_year.group(1)), None, None
    return None, None, None


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

    raw_as_of = str(as_of_date or datetime.now(timezone.utc).date().isoformat())
    as_of = previous_or_same_xnys_session(raw_as_of)
    updated_at = datetime.now(timezone.utc).isoformat()
    universe = _resolve_universe(
        db_path=db_path,
        index=index,
        tickers_csv=tickers_csv,
        _ric_suffix=ric_suffix,
        as_of_date=as_of,
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
    _ = load_ric_map(conn)
    company_parts: list[pd.DataFrame] = []

    with LsegClient() as client:
        ticker_to_ric = resolve_ric_map(
            client=client,
            conn=conn,
            tickers=universe,
            as_of_date=as_of,
            source="lseg_daily",
            suffixes=[ric_suffix, ".N", ".O", ".A", ".K", ".P", ".PK"],
            batch_size=LSEG_BATCH_SIZE,
        )
        ric_universe = [ticker_to_ric[t] for t in universe if t in ticker_to_ric]
        ric_to_ticker = {v.upper(): k.upper() for k, v in ticker_to_ric.items() if v}

        def _fetch_company_data_robust(batch: list[str]) -> tuple[pd.DataFrame, int]:
            """Fetch company data while isolating bad instruments in failing batches."""
            if not batch:
                return pd.DataFrame(), 0
            try:
                part = client.get_company_data(batch, fields=LSEG_FIELDS, as_of_date=as_of)
                return (part if part is not None else pd.DataFrame(), 0)
            except Exception as exc:
                if len(batch) <= 1:
                    bad = batch[0] if batch else "UNKNOWN"
                    print(f"  skipped bad instrument: {bad} ({exc})")
                    return pd.DataFrame(), 1
                mid = len(batch) // 2
                left_df, left_bad = _fetch_company_data_robust(batch[:mid])
                right_df, right_bad = _fetch_company_data_robust(batch[mid:])
                frames = [df for df in [left_df, right_df] if df is not None and not df.empty]
                merged = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
                return merged, left_bad + right_bad

        print(f"Fetching LSEG data for {len(ric_universe)} instruments...")
        bad_instruments = 0
        for i in range(0, len(ric_universe), LSEG_BATCH_SIZE):
            batch = ric_universe[i : i + LSEG_BATCH_SIZE]
            part, bad_n = _fetch_company_data_robust(batch)
            bad_instruments += int(bad_n)
            if part is not None and not part.empty:
                company_parts.append(part)
            done = min(i + LSEG_BATCH_SIZE, len(ric_universe))
            print(f"  company_data: {done:,}/{len(ric_universe):,}")

    company = pd.concat(company_parts, ignore_index=True) if company_parts else pd.DataFrame()
    if company is None or company.empty:
        conn.close()
        return {"status": "no-data", "as_of": as_of, "universe": len(universe)}

    instrument_col = _pick_col(company, ["Instrument"])
    if not instrument_col:
        raise RuntimeError("LSEG response missing Instrument column")

    col = {
        "price": _pick_col(company, ["Price Close"]),
        "market_cap": _pick_col(company, ["Company Market Cap"]),
        "shares_outstanding": _pick_col(company, ["Outstanding Shares", "Shares Outstanding", "Shares Outstanding - Common Stock"]),
        "dividend_yield": _pick_col(company, ["Dividend yield", "Dividend Yield"]),
        "common_name": _pick_col(company, ["Company Common Name", "Common Name", "TR.CommonName"]),
        "trbc_economic_sector": _pick_col(company, ["TRBC Economic Sector Name", "TRBC Economic Sector"]),
        "trbc_business_sector": _pick_col(company, ["TRBC Business Sector Name", "TRBC Business Sector"]),
        "trbc_industry_group": _pick_col(company, ["TRBC Industry Group Name", "TRBC Industry Group"]),
        "trbc_industry": _pick_col(company, ["TRBC Industry Name", "TRBC Industry"]),
        "trbc_activity": _pick_col(company, ["TRBC Activity Name", "TRBC Activity"]),
        "hq_country_code": _pick_col(
            company,
            [
                "Country ISO Code of Headquarters",
                "Headquarters Country Code",
                "HQ Country Code",
                "Country Code",
            ],
        ),
        "book_value": _pick_col(company, ["Book Value Per Share"]),
        "forward_eps": _pick_col(company, ["Earnings Per Share - Mean"]),
        "trailing_eps": _pick_col(company, ["Earnings Per Share - Actual"]),
        "total_debt": _pick_col(company, ["Total Debt"]),
        "cash_and_equivalents": _pick_col(company, ["Cash and Equivalents"]),
        "long_term_debt": _pick_col(company, ["Long Term Debt"]),
        "free_cash_flow": _pick_col(company, ["Free Cash Flow"]),
        "gross_profit": _pick_col(company, ["Gross Profit"]),
        "net_income": _pick_col(company, ["Net Income Incl Extra Before Distributions", "Net Income"]),
        "operating_cashflow": _pick_col(company, ["Cash from Operating Activities"]),
        "capital_expenditures": _pick_col(company, ["Capital Expenditures, Cumulative"]),
        "shares_basic": _pick_col(company, ["Basic Weighted Average Shares"]),
        "shares_diluted": _pick_col(company, ["Diluted Weighted Average Shares"]),
        "free_float_shares": _pick_col(company, ["Free Float"]),
        "free_float_percent": _pick_col(company, ["Free Float (Percent)", "Float Percent"]),
        "revenue": _pick_col(company, ["Revenue"]),
        "financial_period_abs": _pick_col(company, ["Financial Period Absolute"]),
        "report_currency": _pick_col(company, ["Currency"]),
        "ebitda": _pick_col(company, ["EBITDA"]),
        "ebit": _pick_col(company, ["EBIT"]),
        "total_assets": _pick_col(company, ["Total Assets"]),
        "total_liabilities": _pick_col(company, ["Total Liabilities"]),
        "return_on_equity": _pick_col(company, ["Pretax ROE Total Equity %", "ROE"]),
        "operating_margins": _pick_col(company, ["Operating Margin, Percent", "Operating Margin %"]),
        "fundamental_period_end_date": _pick_col(company, ["Period End Date"]),
    }

    job_run_id = f"lseg_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    fundamentals_rows: list[dict[str, Any]] = []
    prices_rows: list[dict[str, Any]] = []
    trbc_history_rows: list[dict[str, Any]] = []

    for _, row in company.iterrows():
        ric = str(row.get(instrument_col) or "").strip().upper()
        ticker = ric_to_ticker.get(ric) or _to_local_ticker(ric)
        if not ticker:
            continue

        trbc_economic_sector_short = row.get(col["trbc_economic_sector"]) if col["trbc_economic_sector"] else None
        trbc_business = row.get(col["trbc_business_sector"]) if col["trbc_business_sector"] else None
        trbc_industry_group = row.get(col["trbc_industry_group"]) if col["trbc_industry_group"] else None
        trbc_industry = row.get(col["trbc_industry"]) if col["trbc_industry"] else None
        trbc_activity = row.get(col["trbc_activity"]) if col["trbc_activity"] else None
        hq_country_code = row.get(col["hq_country_code"]) if col["hq_country_code"] else None
        fiscal_year, _, period_type = _parse_fiscal_period(
            row.get(col["financial_period_abs"]) if col["financial_period_abs"] else None
        )

        def _txt(v: Any) -> str | None:
            if v is None or pd.isna(v):
                return None
            s = str(v).strip()
            if not s or s.lower() in {"nan", "none"}:
                return None
            return s

        fundamentals_rows.append(
            {
                "ticker": ticker,
                "fetch_date": as_of,
                "market_cap": _float_or_none(row.get(col["market_cap"]) if col["market_cap"] else None),
                "shares_outstanding": _float_or_none(row.get(col["shares_outstanding"]) if col["shares_outstanding"] else None),
                "dividend_yield": _float_or_none(row.get(col["dividend_yield"]) if col["dividend_yield"] else None),
                "common_name": _txt(row.get(col["common_name"]) if col["common_name"] else None),
                "book_value": _float_or_none(row.get(col["book_value"]) if col["book_value"] else None),
                "forward_eps": _float_or_none(row.get(col["forward_eps"]) if col["forward_eps"] else None),
                "trailing_eps": _float_or_none(row.get(col["trailing_eps"]) if col["trailing_eps"] else None),
                "total_debt": _float_or_none(row.get(col["total_debt"]) if col["total_debt"] else None),
                "cash_and_equivalents": _float_or_none(row.get(col["cash_and_equivalents"]) if col["cash_and_equivalents"] else None),
                "long_term_debt": _float_or_none(row.get(col["long_term_debt"]) if col["long_term_debt"] else None),
                "free_cash_flow": _float_or_none(row.get(col["free_cash_flow"]) if col["free_cash_flow"] else None),
                "gross_profit": _float_or_none(row.get(col["gross_profit"]) if col["gross_profit"] else None),
                "net_income": _float_or_none(row.get(col["net_income"]) if col["net_income"] else None),
                "operating_cashflow": _float_or_none(
                    row.get(col["operating_cashflow"]) if col["operating_cashflow"] else None
                ),
                "capital_expenditures": _float_or_none(
                    row.get(col["capital_expenditures"]) if col["capital_expenditures"] else None
                ),
                "shares_basic": _float_or_none(row.get(col["shares_basic"]) if col["shares_basic"] else None),
                "shares_diluted": _float_or_none(row.get(col["shares_diluted"]) if col["shares_diluted"] else None),
                "free_float_shares": _float_or_none(
                    row.get(col["free_float_shares"]) if col["free_float_shares"] else None
                ),
                "free_float_percent": _float_or_none(
                    row.get(col["free_float_percent"]) if col["free_float_percent"] else None
                ),
                "revenue": _float_or_none(row.get(col["revenue"]) if col["revenue"] else None),
                "ebitda": _float_or_none(row.get(col["ebitda"]) if col["ebitda"] else None),
                "ebit": _float_or_none(row.get(col["ebit"]) if col["ebit"] else None),
                "total_assets": _float_or_none(row.get(col["total_assets"]) if col["total_assets"] else None),
                "total_liabilities": _float_or_none(row.get(col["total_liabilities"]) if col["total_liabilities"] else None),
                "return_on_equity": _float_or_none(row.get(col["return_on_equity"]) if col["return_on_equity"] else None),
                "operating_margins": _float_or_none(row.get(col["operating_margins"]) if col["operating_margins"] else None),
                "fundamental_period_end_date": _iso_date(
                    row.get(col["fundamental_period_end_date"]) if col["fundamental_period_end_date"] else None
                ),
                "report_currency": _txt(row.get(col["report_currency"]) if col["report_currency"] else None),
                "fiscal_year": fiscal_year,
                "period_type": period_type,
                "source": "lseg_toolkit",
                "job_run_id": job_run_id,
                "updated_at": updated_at,
            }
        )

        trbc_history_rows.append(
            {
                "ticker": ticker,
                "as_of_date": as_of,
                "trbc_economic_sector": _txt(trbc_economic_sector_short),
                "trbc_business_sector": _txt(trbc_business),
                "trbc_industry_group": _txt(trbc_industry_group),
                "trbc_industry": _txt(trbc_industry),
                "trbc_activity": _txt(trbc_activity),
                "hq_country_code": (_txt(hq_country_code) or "").upper() or None,
                "source": "lseg_toolkit",
                "job_run_id": job_run_id,
                "updated_at": updated_at,
            }
        )

        close = _float_or_none(row.get(col["price"]) if col["price"] else None)
        prices_rows.append(
            {
                "ticker": ticker,
                "date": as_of,
                "open": close,
                "high": close,
                "low": close,
                "close": close,
                "adj_close": close,
                "volume": None,
                "currency": None,
                "exchange": None,
                "source": "lseg_toolkit",
                "updated_at": updated_at,
            }
        )

    do_common_name_backfill = bool(skip_common_name_backfill) is False and shard_count <= 1
    if shard_count > 1 and not skip_common_name_backfill:
        print("Sharded ingest detected; skipping common_name backfill in-shard to avoid SQLite lock contention.")

    try:
        deleted_f = 0
        deleted_p = 0
        n_f = _insert_rows(conn, "fundamental_snapshots", fundamentals_rows, replace=True)
        conn.commit()
        n_p = _insert_rows(conn, "prices_daily", prices_rows, replace=True)
        conn.commit()
        n_g = _insert_rows(conn, "trbc_industry_history", trbc_history_rows, replace=True)
        conn.commit()
        n_name_backfill = 0
        if do_common_name_backfill:
            n_name_backfill = _backfill_common_names(conn)
            conn.commit()
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
        "fundamental_rows_deleted": int(deleted_f),
        "price_rows_inserted": n_p,
        "price_rows_deleted": int(deleted_p),
        "trbc_rows_inserted": n_g,
        "common_name_rows_backfilled": int(n_name_backfill),
        "ticker_ric_map_size": int(n_ric or 0),
        "db_path": str(db_path),
        "shard_index": int(shard_index),
        "shard_count": int(shard_count),
        "skip_common_name_backfill": bool(skip_common_name_backfill),
        "bad_instruments_skipped": int(bad_instruments if 'bad_instruments' in locals() else 0),
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
