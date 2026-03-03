"""Data queries for Barra dashboard.

Reads from local data.db maintained by scripts/download_data_lseg.py
(LSEG gatherer by default; legacy Postgres snapshot optional).
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from db.trbc_schema import ensure_trbc_naming
from trading_calendar import previous_or_same_xnys_session

import config

DATA_DB = Path(config.DATA_DB_PATH)


def _coalesce_series(df: pd.DataFrame, *cols: str, default: str = "") -> pd.Series:
    out: pd.Series | None = None
    for col in cols:
        if col in df.columns:
            cur = (
                df[col]
                .astype("object")
                .replace({"": pd.NA, "nan": pd.NA, "None": pd.NA, "Unmapped": pd.NA, "unmapped": pd.NA})
            )
            out = cur if out is None else out.combine_first(cur)
    if out is None:
        out = pd.Series([default] * len(df), index=df.index, dtype="object")
    return out.fillna(default).astype(str)


def _fetch_rows(sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
    conn = sqlite3.connect(str(DATA_DB))
    conn.row_factory = sqlite3.Row
    try:
        ensure_trbc_naming(conn)
        cur = conn.execute(sql, params or [])
        return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def _table_exists(table: str) -> bool:
    rows = _fetch_rows(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type='table' AND name=?
        LIMIT 1
        """,
        [table],
    )
    return bool(rows)


def _resolve_latest_barra_tuple() -> dict[str, str] | None:
    table = _exposure_source_table_required()
    rows = _fetch_rows(
        f"""
        SELECT as_of_date, barra_model_version, descriptor_schema_version, assumption_set_version
        FROM {table}
        ORDER BY as_of_date DESC, updated_at DESC
        LIMIT 1
        """,
    )
    if not rows:
        return None
    row = rows[0]
    return {
        "as_of_date": str(row.get("as_of_date") or ""),
        "barra_model_version": str(row.get("barra_model_version") or ""),
        "descriptor_schema_version": str(row.get("descriptor_schema_version") or ""),
        "assumption_set_version": str(row.get("assumption_set_version") or ""),
    }


def _exposure_source_table_required() -> str:
    table = "barra_raw_cross_section_history"
    if not _table_exists(table):
        raise RuntimeError(
            "Required exposure table missing: barra_raw_cross_section_history. "
            "Build it via backend/scripts/build_barra_raw_cross_section_history.py."
        )
    return table


def load_raw_cross_section_latest(tickers: list[str] | None = None) -> pd.DataFrame:
    table = _exposure_source_table_required()
    params: list[Any] = []
    ticker_clause = ""
    if tickers:
        clean = [t.upper() for t in tickers if t.strip()]
        if clean:
            placeholders = ",".join("?" for _ in clean)
            ticker_clause = f" WHERE ticker IN ({placeholders})"
            params.extend(clean)

    rows = _fetch_rows(
        f"""
        WITH ranked AS (
            SELECT
                e.*,
                ROW_NUMBER() OVER (
                    PARTITION BY e.ticker
                    ORDER BY e.as_of_date DESC, e.updated_at DESC
                ) AS rn
            FROM {table} e
            {ticker_clause}
        )
        SELECT *
        FROM ranked
        WHERE rn = 1
        ORDER BY ticker ASC
        """,
        params,
    )
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def load_fundamental_snapshots(
    tickers: list[str] | None = None,
    as_of_date: str | None = None,
) -> pd.DataFrame:
    clean = [t.upper() for t in (tickers or []) if t.strip()]
    as_of = previous_or_same_xnys_session(
        str(as_of_date or datetime.now(timezone.utc).date().isoformat())
    )

    # Canonical point-in-time source keyed by (ticker, as_of_date).
    if _table_exists("universe_cross_section_snapshot"):
        ticker_filter = ""
        params: list[Any] = [as_of]
        if clean:
            placeholders = ",".join("?" for _ in clean)
            ticker_filter = f" AND ticker IN ({placeholders})"
            params.extend(clean)
        rows = _fetch_rows(
            f"""
            WITH latest AS (
                SELECT ticker, MAX(as_of_date) AS as_of_date
                FROM universe_cross_section_snapshot
                WHERE as_of_date <= ?
                  {ticker_filter}
                GROUP BY ticker
            )
            SELECT s.*
            FROM universe_cross_section_snapshot s
            JOIN latest l
              ON s.ticker = l.ticker
             AND s.as_of_date = l.as_of_date
            ORDER BY s.ticker ASC
            """,
            params,
        )
        if rows:
            out = pd.DataFrame(rows)
            if "trbc_economic_sector_short" not in out.columns:
                out["trbc_economic_sector_short"] = ""
            out["trbc_economic_sector_short"] = _coalesce_series(
                out,
                "trbc_economic_sector_short",
                "trbc_sector",
                "trbc_economic_sector",
                default="",
            )
            return out

    ticker_filter = ""
    params: list[Any] = [as_of]
    if clean:
        placeholders = ",".join("?" for _ in clean)
        ticker_filter = f" AND ticker IN ({placeholders})"
        params.extend(clean)

    rows = _fetch_rows(
        f"""
        WITH latest AS (
            SELECT ticker, MAX(fetch_date) AS fetch_date
            FROM fundamental_snapshots
            WHERE fetch_date <= ?
              {ticker_filter}
            GROUP BY ticker
        )
        SELECT f.*
        FROM fundamental_snapshots f
        JOIN latest l
          ON f.ticker = l.ticker
         AND f.fetch_date = l.fetch_date
        ORDER BY f.ticker ASC
        """,
        params,
    )
    if not rows:
        return pd.DataFrame()
    fundamentals_df = pd.DataFrame(rows)

    # Overlay the latest available TRBC classifications up to the same as_of_date.
    if not _table_exists("trbc_industry_history"):
        return fundamentals_df

    hist_filter = ""
    hist_params: list[Any] = [as_of]
    if clean:
        placeholders = ",".join("?" for _ in clean)
        hist_filter = f" AND ticker IN ({placeholders})"
        hist_params.extend(clean)

    hist_rows = _fetch_rows(
        f"""
        WITH latest AS (
            SELECT ticker, MAX(as_of_date) AS as_of_date
            FROM trbc_industry_history
            WHERE as_of_date <= ?
              {hist_filter}
            GROUP BY ticker
        )
        SELECT h.ticker, h.trbc_industry_group, h.trbc_economic_sector
        FROM trbc_industry_history h
        JOIN latest l
          ON h.ticker = l.ticker
         AND h.as_of_date = l.as_of_date
        """,
        hist_params,
    )
    if not hist_rows:
        return fundamentals_df

    hist_df = pd.DataFrame(hist_rows).drop_duplicates(subset=["ticker"], keep="last")
    merged = fundamentals_df.merge(hist_df, on="ticker", how="left")

    if "trbc_economic_sector_short" not in merged.columns:
        merged["trbc_economic_sector_short"] = ""
    merged["trbc_economic_sector_short"] = _coalesce_series(
        merged,
        "trbc_economic_sector_short",
        "trbc_sector",
        "trbc_economic_sector",
        default="",
    )

    if "trbc_industry_group" not in merged.columns:
        merged["trbc_industry_group"] = ""
    merged["trbc_industry_group"] = _coalesce_series(
        merged,
        "trbc_industry_group_y",
        "trbc_industry_group_x",
        "trbc_industry_group",
        default="",
    )

    keep_cols = [c for c in merged.columns if c not in {"trbc_economic_sector", "trbc_industry_group_x", "trbc_industry_group_y"}]
    return merged[keep_cols]


def load_latest_prices(tickers: list[str] | None = None) -> pd.DataFrame:
    clean = [t.upper() for t in (tickers or []) if t.strip()]
    ticker_filter = ""
    params: list[Any] = []
    if clean:
        placeholders = ",".join("?" for _ in clean)
        ticker_filter = f" WHERE ticker IN ({placeholders})"
        params = clean
    rows = _fetch_rows(
        f"""
        WITH latest AS (
            SELECT ticker, MAX(date) AS date
            FROM prices_daily
            {ticker_filter}
            GROUP BY ticker
        )
        SELECT p.ticker, p.date, CAST(p.close AS REAL) as close
        FROM prices_daily p
        JOIN latest l
          ON p.ticker = l.ticker
         AND p.date = l.date
        ORDER BY p.ticker ASC
        """,
        params,
    )
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def load_source_dates() -> dict[str, str | None]:
    def _max_val(sql: str) -> str | None:
        rows = _fetch_rows(sql)
        if not rows:
            return None
        val = rows[0].get("latest")
        return str(val) if val else None

    fundamentals_asof = None
    if _table_exists("universe_cross_section_snapshot"):
        fundamentals_asof = _max_val("SELECT MAX(as_of_date) AS latest FROM universe_cross_section_snapshot")
    if not fundamentals_asof:
        fundamentals_asof = _max_val("SELECT MAX(fetch_date) AS latest FROM fundamental_snapshots")

    return {
        "fundamentals_asof": fundamentals_asof,
        "exposures_asof": _max_val(
            f"SELECT MAX(as_of_date) AS latest FROM {_exposure_source_table_required()}"
        ),
    }
