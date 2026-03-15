from __future__ import annotations

import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd

from backend.analytics.services.universe_loadings import (
    build_universe_ticker_loadings,
    load_latest_factor_coverage,
)


def test_load_latest_factor_coverage_reads_latest_day(tmp_path: Path) -> None:
    cache_db = tmp_path / "cache.db"
    conn = sqlite3.connect(str(cache_db))
    conn.execute(
        """
        CREATE TABLE daily_factor_returns (
            date TEXT NOT NULL,
            factor_name TEXT NOT NULL,
            cross_section_n INTEGER,
            eligible_n INTEGER,
            coverage REAL
        )
        """
    )
    conn.executemany(
        "INSERT INTO daily_factor_returns (date, factor_name, cross_section_n, eligible_n, coverage) VALUES (?, ?, ?, ?, ?)",
        [
            ("2026-03-01", "Beta", 100, 95, 0.95),
            ("2026-03-02", "Beta", 101, 96, 0.96),
            ("2026-03-02", "Book-to-Price", 101, 94, 0.93),
        ],
    )
    conn.commit()
    conn.close()

    latest, cov = load_latest_factor_coverage(cache_db)
    assert latest == "2026-03-02"
    assert cov["Beta"] == {"cross_section_n": 101, "eligible_n": 96, "coverage_pct": 0.96}
    assert cov["Book-to-Price"] == {"cross_section_n": 101, "eligible_n": 94, "coverage_pct": 0.93}


def test_build_universe_ticker_loadings_empty_inputs(tmp_path: Path) -> None:
    out = build_universe_ticker_loadings(
        exposures_df=pd.DataFrame(),
        fundamentals_df=pd.DataFrame(),
        prices_df=pd.DataFrame(),
        cov=pd.DataFrame(),
        data_db=tmp_path / "data.db",
    )
    assert out["ticker_count"] == 0
    assert out["eligible_ticker_count"] == 0
    assert out["factor_count"] == 0
    assert out["factors"] == []
    assert out["by_ticker"] == {}


def test_build_universe_ticker_loadings_prefers_well_covered_snapshot(monkeypatch, tmp_path: Path) -> None:
    style_cols = {
        "size_score": 0.1,
        "beta_score": 0.2,
        "growth_score": 0.3,
        "investment_score": 0.4,
        "resid_vol_score": 0.5,
    }
    exposure_rows: list[dict[str, object]] = []
    fundamentals_rows: list[dict[str, object]] = []
    prices_rows: list[dict[str, object]] = []
    eligibility_rows: list[dict[str, object]] = []

    tickers = [f"T{i:03d}" for i in range(100)] + ["LAZ"]
    for idx, ticker in enumerate(tickers):
        ric = f"{ticker}.N"
        exposure_rows.append(
            {
                "ric": ric,
                "ticker": ticker,
                "as_of_date": "2026-03-03",
                **style_cols,
            }
        )
        fundamentals_rows.append(
            {
                "ric": ric,
                "ticker": ticker,
                "market_cap": 1000.0 + idx,
                "trbc_business_sector": "Technology Equipment",
                "trbc_industry_group": "Semiconductors & Semiconductor Equipment",
                "trbc_economic_sector_short": "Technology",
                "company_name": ticker,
            }
        )
        prices_rows.append(
            {
                "ric": ric,
                "ticker": ticker,
                "close": 100.0 + idx,
            }
        )
        eligibility_rows.append(
            {
                "ric": ric,
                "is_structural_eligible": True,
                "exclusion_reason": "",
                "market_cap": 1000.0 + idx,
                "trbc_business_sector": "Technology Equipment",
                "trbc_industry_group": "Semiconductors & Semiconductor Equipment",
                "trbc_economic_sector_short": "Technology",
                "hq_country_code": "US",
            }
        )

    for ticker in ["LAZ", "APP"]:
        exposure_rows.append(
            {
                "ric": f"{ticker}.N",
                "ticker": ticker,
                "as_of_date": "2026-03-04",
                "size_score": None,
                "beta_score": None,
                "growth_score": None,
                "investment_score": None,
                "resid_vol_score": None,
            }
        )

    eligibility_df = pd.DataFrame(eligibility_rows).set_index("ric")
    monkeypatch.setattr(
        "backend.analytics.services.universe_loadings.build_eligibility_context",
        lambda data_db, dates: object(),
    )
    monkeypatch.setattr(
        "backend.analytics.services.universe_loadings.structural_eligibility_for_date",
        lambda ctx, as_of_date: (None, eligibility_df),
    )

    cov = pd.DataFrame(
        np.eye(5, dtype=float),
        index=["Size", "Beta", "Growth", "Investment", "Residual Volatility"],
        columns=["Size", "Beta", "Growth", "Investment", "Residual Volatility"],
    )

    out = build_universe_ticker_loadings(
        exposures_df=pd.DataFrame(exposure_rows),
        fundamentals_df=pd.DataFrame(fundamentals_rows),
        prices_df=pd.DataFrame(prices_rows),
        cov=cov,
        data_db=tmp_path / "data.db",
    )

    laz = out["by_ticker"]["LAZ"]
    assert laz["as_of_date"] == "2026-03-03"
    assert laz["eligible_for_model"] is True
    assert laz["exposures"]["Country: US"] == 1.0
    assert laz["exposures"]["Technology Equipment"] == 1.0
    assert "Growth" in laz["exposures"]


def test_build_universe_ticker_loadings_downgrades_structural_names_without_factor_vectors(
    monkeypatch,
    tmp_path: Path,
) -> None:
    exposures_df = pd.DataFrame(
        [
            {
                "ric": "LAZ.N",
                "ticker": "LAZ",
                "as_of_date": "2026-03-03",
                "size_score": None,
                "beta_score": None,
                "growth_score": None,
                "investment_score": None,
                "resid_vol_score": None,
            }
        ]
    )
    fundamentals_df = pd.DataFrame(
        [
            {
                "ric": "LAZ.N",
                "ticker": "LAZ",
                "market_cap": 1000.0,
                "trbc_business_sector": "Banking & Investment Services",
                "trbc_industry_group": "Investment Banking & Investment Services",
                "trbc_economic_sector_short": "Financials",
                "company_name": "Lazard",
            }
        ]
    )
    prices_df = pd.DataFrame(
        [{"ric": "LAZ.N", "ticker": "LAZ", "close": 48.39}]
    )
    eligibility_df = pd.DataFrame(
        [
            {
                "ric": "LAZ.N",
                "is_structural_eligible": True,
                "exclusion_reason": "",
                "market_cap": 1000.0,
                "trbc_business_sector": "Banking & Investment Services",
                "trbc_industry_group": "Investment Banking & Investment Services",
                "trbc_economic_sector_short": "Financials",
                "hq_country_code": "US",
            }
        ]
    ).set_index("ric")
    monkeypatch.setattr(
        "backend.analytics.services.universe_loadings.build_eligibility_context",
        lambda data_db, dates: object(),
    )
    monkeypatch.setattr(
        "backend.analytics.services.universe_loadings.structural_eligibility_for_date",
        lambda ctx, as_of_date: (None, eligibility_df),
    )

    cov = pd.DataFrame(
        np.eye(5, dtype=float),
        index=["Size", "Beta", "Growth", "Investment", "Residual Volatility"],
        columns=["Size", "Beta", "Growth", "Investment", "Residual Volatility"],
    )

    out = build_universe_ticker_loadings(
        exposures_df=exposures_df,
        fundamentals_df=fundamentals_df,
        prices_df=prices_df,
        cov=cov,
        data_db=tmp_path / "data.db",
    )

    laz = out["by_ticker"]["LAZ"]
    assert laz["eligible_for_model"] is False
    assert laz["exposures"] == {}
    assert laz["eligibility_reason"] == "missing_factor_exposures"
