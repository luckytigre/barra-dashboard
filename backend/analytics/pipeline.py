"""Analytics pipeline: fetch → compute → cache."""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

import config
from analytics.health import compute_health_diagnostics
from barra.covariance import build_factor_covariance_from_cache
from barra.daily_factor_returns import compute_daily_factor_returns
from barra.descriptors import FULL_STYLE_ORTH_RULES, canonicalize_style_scores
from barra.eligibility import build_eligibility_context, structural_eligibility_for_date
from barra.risk_attribution import (
    STYLE_COLUMN_TO_LABEL,
    portfolio_factor_exposure,
    risk_decomposition,
)
from barra.specific_risk import build_specific_risk_from_cache
from analytics.trbc_sector import abbreviate_trbc_sector
from db import postgres, sqlite
from portfolio.mock_portfolio import get_position_meta, get_shares, get_tickers

logger = logging.getLogger(__name__)

DATA_DB = Path(__file__).resolve().parent.parent / "data.db"
CACHE_DB = Path(config.SQLITE_PATH)


def _finite_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return float(default)
    return out if np.isfinite(out) else float(default)


def _build_universe_ticker_loadings(
    exposures_df: pd.DataFrame,
    fundamentals_df: pd.DataFrame,
    prices_df: pd.DataFrame,
    cov: pd.DataFrame,
    specific_risk_by_ticker: dict[str, dict[str, float | int | str]] | None = None,
) -> dict[str, Any]:
    """Build full-universe cached loadings/risk context keyed by ticker."""
    exposures_df = exposures_df.copy() if exposures_df is not None else pd.DataFrame()
    fundamentals_df = fundamentals_df.copy() if fundamentals_df is not None else pd.DataFrame()
    prices_df = prices_df.copy() if prices_df is not None else pd.DataFrame()

    if not exposures_df.empty:
        exposures_df["ticker"] = exposures_df["ticker"].astype(str).str.upper()
    if not fundamentals_df.empty:
        fundamentals_df["ticker"] = fundamentals_df["ticker"].astype(str).str.upper()
    if not prices_df.empty:
        prices_df["ticker"] = prices_df["ticker"].astype(str).str.upper()

    # Latest price map for whole universe
    price_map: dict[str, float] = {}
    if not prices_df.empty:
        for _, row in prices_df.iterrows():
            ticker = str(row.get("ticker", "")).upper()
            if ticker:
                price_map[ticker] = _finite_float(row.get("close"), 0.0)

    # Fundamentals maps for whole universe
    mcap_map: dict[str, float] = {}
    trbc_sector_map: dict[str, str] = {}
    trbc_industry_map: dict[str, str] = {}
    name_map: dict[str, str] = {}
    if not fundamentals_df.empty:
        name_col = None
        for col in ("company_name", "common_name", "name", "Company Common Name"):
            if col in fundamentals_df.columns:
                name_col = col
                break

        for _, row in fundamentals_df.iterrows():
            ticker = str(row.get("ticker", "")).upper()
            if not ticker:
                continue
            mcap = _finite_float(row.get("market_cap"), np.nan)
            if np.isfinite(mcap) and mcap > 0:
                mcap_map[ticker] = mcap
            trbc_sector = str(row.get("trbc_sector") or "").strip()
            if trbc_sector:
                trbc_sector_map[ticker] = trbc_sector
            trbc_industry = str(row.get("trbc_industry_group") or "").strip()
            if trbc_industry:
                trbc_industry_map[ticker] = trbc_industry
            if name_col:
                raw_name = row.get(name_col)
                if raw_name is not None:
                    s = str(raw_name).strip()
                    if s and s.lower() != "nan":
                        name_map[ticker] = s

    latest_asof = ""
    if "as_of_date" in exposures_df.columns and not exposures_df.empty:
        latest_asof = str(exposures_df["as_of_date"].astype(str).max())

    eligibility_df = pd.DataFrame()
    if latest_asof:
        elig_ctx = build_eligibility_context(DATA_DB, dates=[latest_asof])
        _, eligibility_df = structural_eligibility_for_date(elig_ctx, latest_asof)
    if eligibility_df.empty and not exposures_df.empty:
        # If eligibility context has no dates yet, treat everyone as ineligible.
        fallback_idx = pd.Index(exposures_df["ticker"].astype(str).str.upper().unique(), name="ticker")
        eligibility_df = pd.DataFrame(
            {
                "is_structural_eligible": False,
                "exclusion_reason": "eligibility_unavailable",
                "market_cap": np.nan,
                "trbc_sector": "",
                "trbc_industry_group": "",
            },
            index=fallback_idx,
        )

    eligible_mask = eligibility_df.get("is_structural_eligible", pd.Series(dtype=bool)).astype(bool)
    eligible_tickers = set(eligibility_df.index[eligible_mask].astype(str))
    ineligible_reason = {
        str(t): str(eligibility_df.loc[t, "exclusion_reason"] or "")
        for t in eligibility_df.index
    }

    # Canonicalize style scores on the structurally eligible cross-section only.
    canonical_style_map: dict[str, dict[str, float]] = {}
    style_cols_present = [c for c in STYLE_COLUMN_TO_LABEL if c in exposures_df.columns] if not exposures_df.empty else []
    if not exposures_df.empty and style_cols_present and eligible_tickers:
        style_names = [STYLE_COLUMN_TO_LABEL[c] for c in style_cols_present]
        style_scores = exposures_df[["ticker", *style_cols_present]].copy()
        style_scores["ticker"] = style_scores["ticker"].astype(str).str.upper()
        style_scores = style_scores[style_scores["ticker"].isin(eligible_tickers)]
        style_scores = style_scores.drop_duplicates(subset=["ticker"], keep="last").set_index("ticker")
        style_scores.columns = style_names
        if not style_scores.empty:
            caps_from_elig = pd.to_numeric(
                eligibility_df.reindex(style_scores.index)["market_cap"],
                errors="coerce",
            )
            industries_from_elig = (
                eligibility_df.reindex(style_scores.index)["trbc_industry_group"]
                .fillna("")
                .astype(str)
            )
            valid = (
                style_scores.notna().all(axis=1).to_numpy(dtype=bool)
                & np.isfinite(style_scores.to_numpy(dtype=float)).all(axis=1)
                & np.isfinite(caps_from_elig.to_numpy(dtype=float))
                & (caps_from_elig.to_numpy(dtype=float) > 0.0)
                & (industries_from_elig.str.len().to_numpy(dtype=float) > 0)
            )
            if int(valid.sum()) > 0:
                valid_idx = style_scores.index[valid]
                style_scores = style_scores.loc[valid_idx]
                caps_from_elig = caps_from_elig.loc[valid_idx]
                industries_from_elig = industries_from_elig.loc[valid_idx]
                industry_dummies = pd.get_dummies(industries_from_elig, dtype=float)
                canonical_scores = canonicalize_style_scores(
                    style_scores=style_scores,
                    market_caps=caps_from_elig,
                    orth_rules=FULL_STYLE_ORTH_RULES,
                    industry_exposures=industry_dummies,
                )
                for ticker, row in canonical_scores.iterrows():
                    canonical_style_map[str(ticker).upper()] = {
                        factor: _finite_float(row.get(factor), 0.0)
                        for factor in canonical_scores.columns
                    }

    # Factor vol map from full-universe covariance
    factor_vol_map: dict[str, float] = {}
    if cov is not None and not cov.empty:
        for factor in cov.columns:
            factor_vol_map[str(factor)] = float(np.sqrt(max(0.0, _finite_float(cov.loc[factor, factor], 0.0))))

    all_tickers = sorted(
        {
            *exposures_df.get("ticker", pd.Series(dtype=str)).astype(str).str.upper().tolist(),
            *fundamentals_df.get("ticker", pd.Series(dtype=str)).astype(str).str.upper().tolist(),
            *prices_df.get("ticker", pd.Series(dtype=str)).astype(str).str.upper().tolist(),
        }
    )
    universe_by_ticker: dict[str, dict[str, Any]] = {}
    for ticker in all_tickers:
        if not ticker:
            continue

        eligible = bool(ticker in eligible_tickers)
        trbc_sector = str(
            (eligibility_df.loc[ticker, "trbc_sector"] if ticker in eligibility_df.index else "")
            or trbc_sector_map.get(ticker, "")
        )
        trbc_industry_group = str(
            (eligibility_df.loc[ticker, "trbc_industry_group"] if ticker in eligibility_df.index else "")
            or trbc_industry_map.get(ticker, "")
        )
        market_cap = _finite_float(
            eligibility_df.loc[ticker, "market_cap"] if ticker in eligibility_df.index else mcap_map.get(ticker),
            np.nan,
        )

        exposures: dict[str, float] = {}
        if eligible and ticker in canonical_style_map:
            exposures.update(canonical_style_map[ticker])
            if trbc_industry_group:
                exposures[trbc_industry_group] = 1.0

        sensitivities = {
            factor: round(_finite_float(exposures.get(factor), 0.0) * _finite_float(vol, 0.0), 6)
            for factor, vol in factor_vol_map.items()
        }
        risk_loading = round(float(sum(abs(v) for v in sensitivities.values())), 6) if eligible else None
        spec = (specific_risk_by_ticker or {}).get(ticker, {})
        spec_var = _finite_float(spec.get("specific_var"), np.nan) if eligible else np.nan
        spec_vol = _finite_float(spec.get("specific_vol"), np.nan) if eligible else np.nan

        universe_by_ticker[ticker] = {
            "ticker": ticker,
            "name": name_map.get(ticker, ticker),
            "trbc_sector": trbc_sector,
            "trbc_sector_abbr": abbreviate_trbc_sector(trbc_sector),
            "trbc_industry_group": trbc_industry_group,
            "market_cap": round(float(market_cap), 2) if np.isfinite(market_cap) else None,
            "price": round(_finite_float(price_map.get(ticker), 0.0), 4),
            "exposures": exposures,
            "sensitivities": sensitivities,
            "risk_loading": risk_loading,
            "specific_var": round(spec_var, 8) if np.isfinite(spec_var) else None,
            "specific_vol": round(spec_vol, 6) if np.isfinite(spec_vol) else None,
            "eligible_for_model": eligible,
            "eligibility_reason": "" if eligible else ineligible_reason.get(ticker, "ineligible"),
            "model_warning": "" if eligible else "Ticker is ineligible for strict equity model; analytics shown as N/A.",
            "as_of_date": latest_asof,
        }

    # Lightweight search index for instant lookup
    search_index = [
        {
            "ticker": t,
            "name": d.get("name", t),
            "trbc_sector": d.get("trbc_sector", ""),
            "trbc_sector_abbr": d.get("trbc_sector_abbr", ""),
            "risk_loading": d.get("risk_loading", 0.0),
            "specific_vol": d.get("specific_vol", None),
            "eligible_for_model": bool(d.get("eligible_for_model", False)),
            "eligibility_reason": str(d.get("eligibility_reason") or ""),
        }
        for t, d in universe_by_ticker.items()
    ]
    search_index.sort(key=lambda x: str(x["ticker"]))

    eligible_count = int(sum(1 for d in universe_by_ticker.values() if bool(d.get("eligible_for_model", False))))
    return {
        "ticker_count": len(universe_by_ticker),
        "eligible_ticker_count": eligible_count,
        "factor_count": len(factor_vol_map),
        "factors": sorted(factor_vol_map.keys()),
        "factor_vols": {k: round(v, 6) for k, v in factor_vol_map.items()},
        "index": search_index,
        "by_ticker": universe_by_ticker,
    }


def _build_positions_from_universe(universe_by_ticker: dict[str, dict[str, Any]]) -> tuple[list[dict[str, Any]], float]:
    """Project held positions from full-universe cached analytics."""
    shares_map = get_shares()
    tickers = get_tickers()

    positions: list[dict[str, Any]] = []
    total_value = 0.0
    for ticker in tickers:
        t = ticker.upper()
        base = universe_by_ticker.get(t, {})
        shares = _finite_float(shares_map.get(t, 0.0), 0.0)
        price = _finite_float(base.get("price"), 0.0)
        mv = shares * price
        total_value += mv
        meta = get_position_meta(t)
        positions.append({
            "ticker": t,
            "name": str(base.get("name") or t),
            "long_short": "LONG" if shares >= 0 else "SHORT",
            "shares": shares,
            "price": round(price, 2),
            "market_value": round(mv, 2),
            "weight": 0.0,
            "trbc_sector": str(base.get("trbc_sector") or ""),
            "trbc_sector_abbr": str(base.get("trbc_sector_abbr") or ""),
            "account": meta["account"],
            "sleeve": meta["sleeve"],
            "source": meta["source"],
            "trbc_industry_group": str(base.get("trbc_industry_group") or ""),
            "exposures": dict(base.get("exposures") or {}),
            "specific_var": (
                _finite_float(base.get("specific_var"), 0.0)
                if np.isfinite(_finite_float(base.get("specific_var"), np.nan))
                else None
            ),
            "specific_vol": (
                _finite_float(base.get("specific_vol"), 0.0)
                if np.isfinite(_finite_float(base.get("specific_vol"), np.nan))
                else None
            ),
            "risk_contrib_pct": 0.0,
            "eligible_for_model": bool(base.get("eligible_for_model", False)),
            "eligibility_reason": str(base.get("eligibility_reason") or ""),
        })

    for pos in positions:
        mv = _finite_float(pos.get("market_value"), 0.0)
        pos["weight"] = round(mv / total_value, 6) if total_value != 0 else 0.0

    return positions, round(total_value, 2)


def _load_latest_factor_coverage(cache_db: Path) -> tuple[str | None, dict[str, dict[str, float | int]]]:
    """Load latest per-factor cross-section coverage stats from cache DB."""
    conn = sqlite3.connect(str(cache_db))
    try:
        latest_row = conn.execute("SELECT MAX(date) FROM daily_factor_returns").fetchone()
        latest = str(latest_row[0]) if latest_row and latest_row[0] else None
        if latest is None:
            return None, {}
        rows = conn.execute(
            """
            SELECT factor_name, cross_section_n, eligible_n, coverage
            FROM daily_factor_returns
            WHERE date = ?
            """,
            (latest,),
        ).fetchall()
    except sqlite3.OperationalError:
        # Backward-compat if cache schema doesn't have coverage columns yet.
        return None, {}
    finally:
        conn.close()

    out: dict[str, dict[str, float | int]] = {}
    for factor_name, cross_n, eligible_n, coverage in rows:
        out[str(factor_name)] = {
            "cross_section_n": int(cross_n or 0),
            "eligible_n": int(eligible_n or 0),
            "coverage_pct": float(coverage or 0.0),
        }
    return latest, out


def _load_latest_eligibility_summary(cache_db: Path) -> dict[str, Any]:
    conn = sqlite3.connect(str(cache_db))
    try:
        row = conn.execute(
            """
            SELECT date, exp_date, exposure_n, structural_eligible_n, regression_member_n,
                   structural_coverage, regression_coverage, drop_pct_from_prev, alert_level
            FROM daily_universe_eligibility_summary
            ORDER BY date DESC
            LIMIT 1
            """
        ).fetchone()
    except sqlite3.OperationalError:
        row = None
    finally:
        conn.close()
    if not row:
        return {"status": "no-data"}
    return {
        "status": "ok",
        "date": str(row[0]),
        "exp_date": str(row[1]) if row[1] is not None else None,
        "exposure_n": int(row[2] or 0),
        "structural_eligible_n": int(row[3] or 0),
        "regression_member_n": int(row[4] or 0),
        "structural_coverage": float(row[5] or 0.0),
        "regression_coverage": float(row[6] or 0.0),
        "drop_pct_from_prev": float(row[7] or 0.0),
        "alert_level": str(row[8] or ""),
    }


def _compute_exposures_modes(
    positions: list[dict[str, Any]],
    cov,
    factor_details: list[dict],
    factor_coverage: dict[str, dict[str, float | int]] | None = None,
    coverage_date: str | None = None,
) -> dict[str, Any]:
    """Compute the 3-mode exposure data for all factors."""
    # Gather all factor names
    all_factors: set[str] = set()
    for pos in positions:
        all_factors.update(pos.get("exposures", {}).keys())

    factor_detail_map = {d["factor"]: d for d in factor_details}
    coverage_map = factor_coverage or {}
    exposure_map = {factor: portfolio_factor_exposure(positions, factor) for factor in all_factors}

    # Covariance-aware marginal scalar per factor: (F @ h)_f
    cov_adj_map: dict[str, float] = {}
    if cov is not None and not cov.empty:
        cov_factors = [str(c) for c in cov.columns if str(c).lower() != "market"]
        if cov_factors:
            h_vec = np.array([_finite_float(exposure_map.get(f), 0.0) for f in cov_factors], dtype=float)
            f_mat = cov.reindex(index=cov_factors, columns=cov_factors).to_numpy(dtype=float)
            fh_vec = f_mat @ h_vec
            cov_adj_map = {
                cov_factors[i]: _finite_float(fh_vec[i], 0.0)
                for i in range(len(cov_factors))
            }

    result: dict[str, list[dict]] = {"raw": [], "sensitivity": [], "risk_contribution": []}

    for factor in sorted(all_factors):
        # Raw portfolio-weighted exposure
        raw_exp = _finite_float(exposure_map.get(factor), 0.0)

        # Factor vol from cov matrix
        detail = factor_detail_map.get(factor)
        factor_vol = _finite_float(detail.get("factor_vol"), 0.0) if detail else 0.0
        # 1-sigma scaled factor exposure.
        sensitivity = _finite_float(detail.get("sensitivity"), raw_exp * factor_vol) if detail else raw_exp * factor_vol
        # Contribution to total portfolio variance (%) from risk decomposition.
        risk_pct = _finite_float(detail.get("pct_of_total"), 0.0) if detail else 0.0
        # Factor-level marginal variance contribution (includes cross-factor correlations).
        marginal_var = _finite_float(detail.get("marginal_var_contrib"), 0.0) if detail else 0.0
        cov_adj = _finite_float(cov_adj_map.get(factor), 0.0)

        # Position-level drilldown
        drilldown_raw = []
        drilldown_sens = []
        drilldown_risk = []
        for pos in positions:
            pos_exp = _finite_float(pos.get("exposures", {}).get(factor, 0.0), 0.0)
            if abs(pos_exp) > 1e-8:
                weight = _finite_float(pos.get("weight", 0.0), 0.0)
                raw_contrib = weight * pos_exp
                pos_sens = pos_exp * factor_vol
                sens_contrib = weight * pos_sens
                # Variance-units position contribution for this factor:
                # (w_i * x_{i,f}) * (F @ h)_f
                risk_var_contrib = weight * pos_exp * cov_adj
                # Scale to displayed risk % so row sums align with bar value.
                if abs(marginal_var) > 1e-12:
                    risk_pct_contrib = (risk_var_contrib / marginal_var) * risk_pct
                else:
                    risk_pct_contrib = 0.0
                drilldown_raw.append({
                    "ticker": pos["ticker"],
                    "weight": weight,
                    "exposure": round(pos_exp, 4),
                    "contribution": round(raw_contrib, 6),
                })
                drilldown_sens.append({
                    "ticker": pos["ticker"],
                    "weight": weight,
                    "exposure": round(pos_exp, 4),
                    "sensitivity": round(pos_sens, 6),
                    "contribution": round(sens_contrib, 6),
                })
                drilldown_risk.append({
                    "ticker": pos["ticker"],
                    "weight": weight,
                    "exposure": round(pos_exp, 4),
                    # Covariance-adjusted loading scalar (not yet weight-scaled).
                    "sensitivity": round(pos_exp * cov_adj, 8),
                    # Percent contribution to total portfolio variance for this factor.
                    "contribution": round(risk_pct_contrib, 8),
                })

        fv_rounded = round(factor_vol, 6)
        cov_stats = coverage_map.get(factor, {})
        cross_section_n = int(cov_stats.get("cross_section_n", 0) or 0)
        eligible_n = int(cov_stats.get("eligible_n", 0) or 0)
        coverage_pct = float(cov_stats.get("coverage_pct", 0.0) or 0.0)
        result["raw"].append({
            "factor": factor,
            "value": round(raw_exp, 6),
            "factor_vol": fv_rounded,
            "cross_section_n": cross_section_n,
            "eligible_n": eligible_n,
            "coverage_pct": round(coverage_pct, 6),
            "coverage_date": coverage_date,
            "drilldown": drilldown_raw,
        })
        result["sensitivity"].append({
            "factor": factor,
            "value": round(sensitivity, 6),
            "factor_vol": fv_rounded,
            "cross_section_n": cross_section_n,
            "eligible_n": eligible_n,
            "coverage_pct": round(coverage_pct, 6),
            "coverage_date": coverage_date,
            "drilldown": drilldown_sens,
        })
        result["risk_contribution"].append({
            "factor": factor,
            "value": round(risk_pct, 4),
            "factor_vol": fv_rounded,
            "cross_section_n": cross_section_n,
            "eligible_n": eligible_n,
            "coverage_pct": round(coverage_pct, 6),
            "coverage_date": coverage_date,
            "drilldown": drilldown_risk,
        })

    return result


def _compute_position_risk_mix(
    positions: list[dict[str, Any]],
    cov,
    specific_risk_by_ticker: dict[str, dict[str, float | int | str]] | None = None,
) -> dict[str, dict[str, float]]:
    """Per-position risk split using Barra-style factor + specific variance."""
    if cov is None or cov.empty:
        out: dict[str, dict[str, float]] = {}
        for pos in positions:
            ticker = str(pos.get("ticker", "")).upper()
            if ticker:
                out[ticker] = {"industry": 0.0, "style": 0.0, "idio": 0.0}
        return out

    factors = [str(c) for c in cov.columns if str(c).lower() != "market"]
    if not factors:
        return {}
    idx = {f: i for i, f in enumerate(factors)}
    f_mat = cov.reindex(index=factors, columns=factors).to_numpy(dtype=float)
    style_idx = [idx[f] for f in factors if f in STYLE_COLUMN_TO_LABEL.values()]
    industry_idx = [idx[f] for f in factors if f not in STYLE_COLUMN_TO_LABEL.values()]

    spec_map = specific_risk_by_ticker or {}
    out: dict[str, dict[str, float]] = {}
    for pos in positions:
        ticker = str(pos.get("ticker", "")).upper()
        if not ticker:
            continue
        exps = pos.get("exposures", {}) or {}
        x = np.array([_finite_float(exps.get(f), 0.0) for f in factors], dtype=float)

        # Decompose systematic variance into industry/style blocks and allocate cross term.
        var_ind = 0.0
        var_style = 0.0
        var_cross = 0.0
        if industry_idx:
            xi = x[industry_idx]
            fii = f_mat[np.ix_(industry_idx, industry_idx)]
            var_ind = _finite_float(float(xi.T @ fii @ xi), 0.0)
        if style_idx:
            xs = x[style_idx]
            fss = f_mat[np.ix_(style_idx, style_idx)]
            var_style = _finite_float(float(xs.T @ fss @ xs), 0.0)
        if industry_idx and style_idx:
            xi = x[industry_idx]
            xs = x[style_idx]
            fis = f_mat[np.ix_(industry_idx, style_idx)]
            var_cross = _finite_float(float(2.0 * xi.T @ fis @ xs), 0.0)

        base_ind = max(0.0, var_ind)
        base_style = max(0.0, var_style)
        denom = base_ind + base_style
        if abs(var_cross) <= 1e-12:
            cross_ind = 0.0
            cross_style = 0.0
        elif denom <= 1e-12:
            cross_ind = 0.5 * var_cross
            cross_style = 0.5 * var_cross
        else:
            w_ind = base_ind / denom
            w_style = base_style / denom
            cross_ind = w_ind * var_cross
            cross_style = w_style * var_cross

        sys_ind = max(0.0, base_ind + cross_ind)
        sys_style = max(0.0, base_style + cross_style)
        spec_var = _finite_float(spec_map.get(ticker, {}).get("specific_var"), 0.0)
        if spec_var < 0:
            spec_var = 0.0

        total = sys_ind + sys_style + spec_var
        if total <= 0:
            out[ticker] = {"industry": 0.0, "style": 0.0, "idio": 0.0}
            continue

        ind_pct = 100.0 * sys_ind / total
        style_pct = 100.0 * sys_style / total
        idio_pct = 100.0 * spec_var / total

        # Normalize after clipping to keep sums at exactly 100.
        ind_pct = max(0.0, ind_pct)
        style_pct = max(0.0, style_pct)
        idio_pct = max(0.0, idio_pct)
        s = ind_pct + style_pct + idio_pct
        if s > 0:
            k = 100.0 / s
            ind_pct *= k
            style_pct *= k
            idio_pct *= k

        out[ticker] = {
            "industry": round(ind_pct, 2),
            "style": round(style_pct, 2),
            "idio": round(idio_pct, 2),
        }
    return out


def run_refresh() -> dict[str, Any]:
    """Full pipeline: compute daily factor returns → covariance → risk → cache."""
    logger.info("Starting refresh pipeline...")

    # 1. Ensure daily factor returns are computed (incremental)
    logger.info("Computing daily factor returns (incremental)...")
    compute_daily_factor_returns(DATA_DB, CACHE_DB)

    # 2. Fetch full-universe data from local data.db
    logger.info("Fetching data from local database...")
    source_dates = postgres.load_source_dates()
    fundamentals_asof = source_dates.get("exposures_asof")
    prices_universe_df = postgres.load_latest_prices()
    fundamentals_universe_df = postgres.load_fundamental_snapshots(
        as_of_date=str(fundamentals_asof) if fundamentals_asof else None,
    )
    exposures_universe_df = postgres.load_barra_exposures()

    # 4. Build factor covariance from daily factor returns (504-day lookback)
    logger.info("Computing factor covariance from daily returns...")
    cov, latest_r2 = build_factor_covariance_from_cache(
        CACHE_DB, lookback_days=config.LOOKBACK_DAYS
    )
    logger.info("Computing stock-level specific risk from residual history...")
    specific_risk_by_ticker = build_specific_risk_from_cache(
        CACHE_DB,
        lookback_days=config.LOOKBACK_DAYS,
    )

    # 5. Build/cached full-universe loadings first (portfolio is a final projection only).
    logger.info("Building full-universe ticker loadings...")
    universe_loadings = _build_universe_ticker_loadings(
        exposures_universe_df,
        fundamentals_universe_df,
        prices_universe_df,
        cov,
        specific_risk_by_ticker=specific_risk_by_ticker,
    )

    # 6. Project held positions from full-universe cache
    logger.info("Projecting held positions from full-universe cache...")
    positions, total_value = _build_positions_from_universe(universe_loadings["by_ticker"])

    # 7. Risk decomposition
    logger.info("Computing risk decomposition...")
    risk_shares, component_shares, factor_details = risk_decomposition(
        cov=cov,
        positions=positions,
        specific_risk_by_ticker=specific_risk_by_ticker,
    )
    position_risk_mix = _compute_position_risk_mix(
        positions=positions,
        cov=cov,
        specific_risk_by_ticker=specific_risk_by_ticker,
    )

    # 8. Compute per-position risk contributions
    for pos in positions:
        exps = pos.get("exposures", {})
        risk_score = sum(
            abs(float(exps.get(d["factor"], 0.0)) * d["sensitivity"])
            for d in factor_details
        )
        pos["risk_contrib_pct"] = round(risk_score * pos["weight"] * 100, 2)
        pos["risk_mix"] = dict(position_risk_mix.get(str(pos.get("ticker", "")).upper(), {
            "industry": 0.0,
            "style": 0.0,
            "idio": 0.0,
        }))

    # 9. Compute condition number from cov matrix
    condition_number = 0.0
    if not cov.empty:
        try:
            cn = float(np.linalg.cond(cov.to_numpy()))
            condition_number = cn if np.isfinite(cn) else 9999.99
        except Exception:
            pass

    # 10. Compute exposure modes
    logger.info("Computing exposure modes...")
    coverage_date, factor_coverage = _load_latest_factor_coverage(CACHE_DB)
    exposure_modes = _compute_exposures_modes(
        positions,
        cov,
        factor_details,
        factor_coverage=factor_coverage,
        coverage_date=coverage_date,
    )

    # 11. Build covariance matrix for frontend (correlation) — style factors only
    STYLE_FACTOR_NAMES = {
        "Size", "Nonlinear Size", "Liquidity", "Beta",
        "Book-to-Price", "Earnings Yield", "Value", "Leverage",
        "Growth", "Profitability", "Investment", "Dividend Yield",
        "Momentum", "Short-Term Reversal", "Residual Volatility",
    }
    cov_matrix: dict[str, Any] = {}
    if not cov.empty:
        all_factors = list(cov.columns)
        style_idx = [i for i, f in enumerate(all_factors) if f in STYLE_FACTOR_NAMES]
        if style_idx:
            style_factors = [all_factors[i] for i in style_idx]
            sub_cov = cov.to_numpy()[np.ix_(style_idx, style_idx)]
            stds = np.sqrt(np.diag(sub_cov))
            stds[stds == 0] = 1.0
            corr = sub_cov / np.outer(stds, stds)
            corr = np.clip(corr, -1.0, 1.0)
            cov_matrix = {
                "factors": style_factors,
                "correlation": [[round(float(v), 4) for v in row] for row in corr],
            }

    # 12. Sanitize non-finite floats (NaN/Inf break JSON serialization)
    def _safe(v):
        if isinstance(v, float) and not np.isfinite(v):
            return 0.0
        return v

    for d in factor_details:
        for k, v in d.items():
            d[k] = _safe(v)
    for k in risk_shares:
        risk_shares[k] = _safe(risk_shares[k])
    for k in component_shares:
        component_shares[k] = _safe(component_shares[k])

    # 13. Cache everything
    logger.info("Caching results...")
    portfolio_data = {
        "positions": positions,
        "total_value": round(total_value, 2),
        "position_count": len(positions),
    }
    sqlite.cache_set("portfolio", portfolio_data)

    risk_data = {
        "risk_shares": risk_shares,
        "component_shares": component_shares,
        "factor_details": factor_details,
        "cov_matrix": cov_matrix,
        "r_squared": round(latest_r2, 4),
        "condition_number": round(condition_number, 2),
    }
    sqlite.cache_set("risk", risk_data)

    sqlite.cache_set("universe_loadings", universe_loadings)
    sqlite.cache_set(
        "universe_factors",
        {
            "factors": universe_loadings.get("factors", []),
            "factor_vols": universe_loadings.get("factor_vols", {}),
            "r_squared": round(latest_r2, 4),
            "condition_number": round(condition_number, 2),
            "ticker_count": universe_loadings.get("ticker_count", 0),
            "eligible_ticker_count": universe_loadings.get("eligible_ticker_count", 0),
        },
    )
    sqlite.cache_set("exposures", exposure_modes)
    sqlite.cache_set("eligibility", _load_latest_eligibility_summary(CACHE_DB))
    sqlite.cache_set("health_diagnostics", compute_health_diagnostics(DATA_DB, CACHE_DB))

    logger.info("Refresh complete.")
    return {"status": "ok", "positions": len(positions), "total_value": round(total_value, 2)}
