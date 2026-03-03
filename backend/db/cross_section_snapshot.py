"""Canonical cross-section snapshot builder keyed by (ticker, as_of_date)."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from db.trbc_schema import ensure_trbc_naming

TABLE = "universe_cross_section_snapshot"


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


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
    cols = _table_columns(conn, table)
    if column in cols:
        return
    conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")


def ensure_cross_section_snapshot_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            ticker TEXT NOT NULL,
            as_of_date TEXT NOT NULL,
            fundamental_fetch_date TEXT,
            fundamental_period_end_date TEXT,
            market_cap REAL,
            shares_outstanding REAL,
            dividend_yield REAL,
            common_name TEXT,
            book_value REAL,
            forward_eps REAL,
            trailing_eps REAL,
            total_debt REAL,
            cash_and_equivalents REAL,
            long_term_debt REAL,
            free_cash_flow REAL,
            gross_profit REAL,
            net_income REAL,
            operating_cashflow REAL,
            capital_expenditures REAL,
            shares_basic REAL,
            shares_diluted REAL,
            free_float_shares REAL,
            free_float_percent REAL,
            revenue REAL,
            ebitda REAL,
            ebit REAL,
            total_assets REAL,
            total_liabilities REAL,
            return_on_equity REAL,
            operating_margins REAL,
            report_currency TEXT,
            fiscal_year INTEGER,
            period_type TEXT,
            trbc_economic_sector_short TEXT,
            trbc_economic_sector TEXT,
            trbc_business_sector TEXT,
            trbc_industry_group TEXT,
            trbc_industry TEXT,
            trbc_activity TEXT,
            trbc_effective_date TEXT,
            price_date TEXT,
            price_close REAL,
            price_currency TEXT,
            price_exchange TEXT,
            fundamental_source TEXT,
            trbc_source TEXT,
            price_source TEXT,
            fundamental_job_run_id TEXT,
            trbc_job_run_id TEXT,
            snapshot_job_run_id TEXT,
            updated_at TEXT,
            PRIMARY KEY (ticker, as_of_date)
        )
        """
    )
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_asof ON {TABLE}(as_of_date)")
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_ticker ON {TABLE}(ticker)")
    for col, ddl in [
        ("cash_and_equivalents", "REAL"),
        ("long_term_debt", "REAL"),
        ("gross_profit", "REAL"),
        ("net_income", "REAL"),
        ("operating_cashflow", "REAL"),
        ("capital_expenditures", "REAL"),
        ("shares_basic", "REAL"),
        ("shares_diluted", "REAL"),
        ("free_float_shares", "REAL"),
        ("free_float_percent", "REAL"),
        ("report_currency", "TEXT"),
        ("fiscal_year", "INTEGER"),
        ("period_type", "TEXT"),
        ("trbc_economic_sector_short", "TEXT"),
    ]:
        _ensure_column(conn, TABLE, col, ddl)


def _load_base_cross_sections(
    conn: sqlite3.Connection,
    *,
    start_date: str | None,
    end_date: str | None,
    tickers: list[str] | None,
) -> pd.DataFrame:
    source_table = "barra_raw_cross_section_history"
    source_cols = _table_columns(conn, source_table)
    if "ticker" not in source_cols or "as_of_date" not in source_cols:
        return pd.DataFrame(columns=["ticker", "as_of_date"])

    clauses: list[str] = []
    params: list[Any] = []
    if start_date:
        clauses.append("as_of_date >= ?")
        params.append(str(start_date))
    if end_date:
        clauses.append("as_of_date <= ?")
        params.append(str(end_date))
    if tickers:
        clean = [str(t).upper().strip() for t in tickers if str(t).strip()]
        if clean:
            placeholders = ",".join("?" for _ in clean)
            clauses.append(f"ticker IN ({placeholders})")
            params.extend(clean)

    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    df = pd.read_sql_query(
        f"""
        SELECT DISTINCT ticker, as_of_date
        FROM {source_table}
        {where_sql}
        ORDER BY ticker, as_of_date
        """,
        conn,
        params=params,
    )
    if df.empty:
        return df
    df["ticker"] = df["ticker"].astype(str).str.upper()
    df["as_of_date"] = df["as_of_date"].astype(str)
    return df


def _merge_asof_by_ticker(
    base: pd.DataFrame,
    events: pd.DataFrame,
    *,
    left_date_col: str,
    right_date_col: str,
) -> pd.DataFrame:
    if events.empty:
        return base
    merged_parts: list[pd.DataFrame] = []
    events_by_ticker = {str(t): grp.copy() for t, grp in events.groupby("ticker", sort=False)}
    for ticker, left_grp in base.groupby("ticker", sort=False):
        left_sorted = left_grp.sort_values(left_date_col).reset_index(drop=True)
        right_grp = events_by_ticker.get(str(ticker))
        if right_grp is None or right_grp.empty:
            merged_parts.append(left_sorted)
            continue
        right_sorted = right_grp.sort_values(right_date_col).reset_index(drop=True)
        right_sorted = right_sorted.drop(columns=["ticker"], errors="ignore")
        out = pd.merge_asof(
            left_sorted,
            right_sorted,
            left_on=left_date_col,
            right_on=right_date_col,
            direction="backward",
            allow_exact_matches=True,
        )
        merged_parts.append(out)
    if not merged_parts:
        return base
    return pd.concat(merged_parts, ignore_index=True)


def _sanitize_num(df: pd.DataFrame, cols: list[str]) -> None:
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")


def _coalesce_columns(df: pd.DataFrame, target: str, candidates: list[str]) -> None:
    vals = None
    for col in candidates:
        if col not in df.columns:
            continue
        cur = df[col]
        vals = cur if vals is None else vals.combine_first(cur)
    if vals is None:
        if target not in df.columns:
            df[target] = None
        return
    df[target] = vals


def rebuild_cross_section_snapshot(
    data_db: Path,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    tickers: list[str] | None = None,
) -> dict[str, Any]:
    conn = sqlite3.connect(str(data_db))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    try:
        ensure_trbc_naming(conn)
        ensure_cross_section_snapshot_table(conn)

        base = _load_base_cross_sections(
            conn,
            start_date=start_date,
            end_date=end_date,
            tickers=tickers,
        )
        if base.empty:
            return {"status": "no-op", "rows_upserted": 0, "table": TABLE}

        base["as_of_date_dt"] = pd.to_datetime(base["as_of_date"], errors="coerce")
        base = base.dropna(subset=["as_of_date_dt"])
        if base.empty:
            return {"status": "no-op", "rows_upserted": 0, "table": TABLE}

        max_asof = str(base["as_of_date"].max())

        # Fundamental events
        fcols = _table_columns(conn, "fundamental_snapshots")
        fundamental_cols = [
            "market_cap",
            "shares_outstanding",
            "dividend_yield",
            "common_name",
            "book_value",
            "forward_eps",
            "trailing_eps",
            "total_debt",
            "cash_and_equivalents",
            "long_term_debt",
            "free_cash_flow",
            "gross_profit",
            "net_income",
            "operating_cashflow",
            "capital_expenditures",
            "shares_basic",
            "shares_diluted",
            "free_float_shares",
            "free_float_percent",
            "revenue",
            "ebitda",
            "ebit",
            "total_assets",
            "total_liabilities",
            "return_on_equity",
            "operating_margins",
            "fundamental_period_end_date",
            "report_currency",
            "fiscal_year",
            "period_type",
        ]
        keep_fcols = [c for c in fundamental_cols if c in fcols]
        fsel = ", ".join(["ticker", "fetch_date", *keep_fcols, "source", "job_run_id", "updated_at"])
        fundamentals = pd.read_sql_query(
            f"""
            SELECT {fsel}
            FROM fundamental_snapshots
            WHERE fetch_date <= ?
            ORDER BY ticker, fetch_date, updated_at
            """,
            conn,
            params=(max_asof,),
        )
        if not fundamentals.empty:
            fundamentals["ticker"] = fundamentals["ticker"].astype(str).str.upper()
            fundamentals["fetch_date_dt"] = pd.to_datetime(fundamentals["fetch_date"], errors="coerce")
            fundamentals = fundamentals.dropna(subset=["fetch_date_dt"])
            fundamentals = (
                fundamentals.sort_values(["ticker", "fetch_date", "updated_at"])
                .drop_duplicates(subset=["ticker", "fetch_date"], keep="last")
            )
            fundamentals = fundamentals.rename(
                columns={
                    "source": "fundamental_source",
                    "job_run_id": "fundamental_job_run_id",
                }
            )

        # TRBC point-in-time events
        hcols = _table_columns(conn, "trbc_industry_history")
        trbc_cols = [
            c for c in [
                "trbc_economic_sector",
                "trbc_business_sector",
                "trbc_industry_group",
                "trbc_industry",
                "trbc_activity",
            ]
            if c in hcols
        ]
        hsel = ", ".join(["ticker", "as_of_date", *trbc_cols, "source", "job_run_id", "updated_at"])
        trbc_hist = pd.read_sql_query(
            f"""
            SELECT {hsel}
            FROM trbc_industry_history
            WHERE as_of_date <= ?
            ORDER BY ticker, as_of_date, updated_at
            """,
            conn,
            params=(max_asof,),
        ) if _table_exists(conn, "trbc_industry_history") else pd.DataFrame()
        if not trbc_hist.empty:
            trbc_hist["ticker"] = trbc_hist["ticker"].astype(str).str.upper()
            trbc_hist["trbc_effective_date"] = trbc_hist["as_of_date"].astype(str)
            trbc_hist["trbc_effective_date_dt"] = pd.to_datetime(trbc_hist["trbc_effective_date"], errors="coerce")
            trbc_hist = trbc_hist.dropna(subset=["trbc_effective_date_dt"])
            trbc_hist = (
                trbc_hist.sort_values(["ticker", "trbc_effective_date", "updated_at"])
                .drop_duplicates(subset=["ticker", "trbc_effective_date"], keep="last")
            )
            trbc_hist = trbc_hist.rename(
                columns={
                    "source": "trbc_source",
                    "job_run_id": "trbc_job_run_id",
                }
            )

        # Price events
        prices = pd.read_sql_query(
            """
            SELECT ticker, date, close, currency, exchange, source, updated_at
            FROM prices_daily
            WHERE date <= ?
            ORDER BY ticker, date
            """,
            conn,
            params=(max_asof,),
        )
        if not prices.empty:
            prices["ticker"] = prices["ticker"].astype(str).str.upper()
            prices["price_date"] = prices["date"].astype(str)
            prices["price_date_dt"] = pd.to_datetime(prices["price_date"], errors="coerce")
            prices = prices.dropna(subset=["price_date_dt"])
            prices = (
                prices.sort_values(["ticker", "price_date", "updated_at"])
                .drop_duplicates(subset=["ticker", "price_date"], keep="last")
            )
            prices = prices.rename(
                columns={
                    "close": "price_close",
                    "currency": "price_currency",
                    "exchange": "price_exchange",
                    "source": "price_source",
                }
            )

        out = base.copy()
        if not fundamentals.empty:
            fmerge_cols = [
                "ticker",
                "fetch_date",
                "fetch_date_dt",
                *[c for c in keep_fcols if c in fundamentals.columns],
                "fundamental_source",
                "fundamental_job_run_id",
            ]
            out = _merge_asof_by_ticker(
                out,
                fundamentals[fmerge_cols],
                left_date_col="as_of_date_dt",
                right_date_col="fetch_date_dt",
            )
            out = out.rename(columns={"fetch_date": "fundamental_fetch_date"})

        if not trbc_hist.empty:
            hmerge_cols = [
                "ticker",
                "trbc_effective_date",
                "trbc_effective_date_dt",
                *[c for c in trbc_cols if c in trbc_hist.columns],
                "trbc_source",
                "trbc_job_run_id",
            ]
            out = _merge_asof_by_ticker(
                out,
                trbc_hist[hmerge_cols],
                left_date_col="as_of_date_dt",
                right_date_col="trbc_effective_date_dt",
            )

        if not prices.empty:
            pmerge_cols = [
                "ticker",
                "price_date",
                "price_date_dt",
                "price_close",
                "price_currency",
                "price_exchange",
                "price_source",
            ]
            out = _merge_asof_by_ticker(
                out,
                prices[pmerge_cols],
                left_date_col="as_of_date_dt",
                right_date_col="price_date_dt",
            )

        # Merge collisions between fundamental and TRBC-history fields.
        for col in [
            "trbc_economic_sector",
            "trbc_business_sector",
            "trbc_industry_group",
            "trbc_industry",
            "trbc_activity",
        ]:
            _coalesce_columns(out, col, [f"{col}_y", col, f"{col}_x"])

        # Fill compatibility sector alias
        if "trbc_economic_sector_short" not in out.columns:
            out["trbc_economic_sector_short"] = ""
        if "trbc_sector" in out.columns:
            out["trbc_economic_sector_short"] = out["trbc_economic_sector_short"].fillna(out["trbc_sector"])
        if "trbc_economic_sector" in out.columns:
            out["trbc_economic_sector_short"] = out["trbc_economic_sector_short"].fillna(out["trbc_economic_sector"])

        _sanitize_num(
            out,
            [
                "market_cap",
                "shares_outstanding",
                "dividend_yield",
                "book_value",
                "forward_eps",
                "trailing_eps",
                "total_debt",
                "cash_and_equivalents",
                "long_term_debt",
                "free_cash_flow",
                "gross_profit",
                "net_income",
                "operating_cashflow",
                "capital_expenditures",
                "shares_basic",
                "shares_diluted",
                "free_float_shares",
                "free_float_percent",
                "revenue",
                "ebitda",
                "ebit",
                "total_assets",
                "total_liabilities",
                "return_on_equity",
                "operating_margins",
                "fiscal_year",
                "price_close",
            ],
        )

        now_iso = datetime.now(timezone.utc).isoformat()
        job_run_id = f"cross_snapshot_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
        out["updated_at"] = now_iso
        out["snapshot_job_run_id"] = job_run_id

        for dcol in [
            "as_of_date",
            "fundamental_fetch_date",
            "fundamental_period_end_date",
            "trbc_effective_date",
            "price_date",
        ]:
            if dcol in out.columns:
                out[dcol] = out[dcol].astype("object")

        target_cols = [
            "ticker",
            "as_of_date",
            "fundamental_fetch_date",
            "fundamental_period_end_date",
            "market_cap",
            "shares_outstanding",
            "dividend_yield",
            "common_name",
            "book_value",
            "forward_eps",
            "trailing_eps",
            "total_debt",
            "cash_and_equivalents",
            "long_term_debt",
            "free_cash_flow",
            "gross_profit",
            "net_income",
            "operating_cashflow",
            "capital_expenditures",
            "shares_basic",
            "shares_diluted",
            "free_float_shares",
            "free_float_percent",
            "revenue",
            "ebitda",
            "ebit",
            "total_assets",
            "total_liabilities",
            "return_on_equity",
            "operating_margins",
            "report_currency",
            "fiscal_year",
            "period_type",
            "trbc_economic_sector_short",
            "trbc_economic_sector",
            "trbc_business_sector",
            "trbc_industry_group",
            "trbc_industry",
            "trbc_activity",
            "trbc_effective_date",
            "price_date",
            "price_close",
            "price_currency",
            "price_exchange",
            "fundamental_source",
            "trbc_source",
            "price_source",
            "fundamental_job_run_id",
            "trbc_job_run_id",
            "snapshot_job_run_id",
            "updated_at",
        ]
        for col in target_cols:
            if col not in out.columns:
                out[col] = None

        payload = out[target_cols].where(pd.notna(out[target_cols]), None)
        conn.executemany(
            f"""
            INSERT OR REPLACE INTO {TABLE}
            ({", ".join(target_cols)})
            VALUES ({", ".join(['?'] * len(target_cols))})
            """,
            payload.itertuples(index=False, name=None),
        )
        conn.commit()
        return {
            "status": "ok",
            "table": TABLE,
            "rows_upserted": int(len(payload)),
            "job_run_id": job_run_id,
            "max_asof": max_asof,
        }
    finally:
        conn.close()
