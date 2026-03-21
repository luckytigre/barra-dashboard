"""Helpers for cPAR explanatory display loadings."""

from __future__ import annotations

import math
from typing import Any

from backend.cpar.factor_registry import MARKET_FACTOR_ID, build_cpar1_factor_registry

_EPSILON = 1e-12


def _finite_float(value: Any) -> float | None:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(numeric):
        return None
    return numeric


def ordered_factor_rows(loadings: dict[str, float]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for spec in build_cpar1_factor_registry():
        factor_id = str(spec.factor_id)
        if factor_id not in loadings:
            continue
        rows.append(
            {
                "factor_id": factor_id,
                "label": spec.label,
                "group": spec.group,
                "display_order": int(spec.display_order),
                "beta": float(loadings[factor_id]),
            }
        )
    return rows


def display_loadings_from_fit(fit: dict[str, Any] | None) -> dict[str, float]:
    clean_fit = dict(fit or {})
    # Persisted `raw_loadings` already carry the de-standardized post-ridge
    # coefficients for non-market factors. Only the SPY leg differs between
    # explanatory display (`market_step_beta`) and hedge trade-space
    # (`spy_trade_beta_raw` / raw_loadings["SPY"]).
    raw_loadings = {
        str(factor_id): numeric
        for factor_id, raw_value in dict(clean_fit.get("raw_loadings") or {}).items()
        if (numeric := _finite_float(raw_value)) is not None
    }
    market_beta = _finite_float(clean_fit.get("market_step_beta"))
    if market_beta is None:
        market_beta = raw_loadings.get(MARKET_FACTOR_ID, 0.0)

    display: dict[str, float] = {}
    if abs(float(market_beta)) > _EPSILON or MARKET_FACTOR_ID in raw_loadings:
        display[MARKET_FACTOR_ID] = float(market_beta)

    for spec in build_cpar1_factor_registry():
        factor_id = str(spec.factor_id)
        if factor_id == MARKET_FACTOR_ID:
            continue
        beta = raw_loadings.get(factor_id)
        if beta is None:
            continue
        display[factor_id] = float(beta)
    return display


def scaled_display_contributions(
    *,
    portfolio_weight: float,
    fit: dict[str, Any] | None,
) -> dict[str, float]:
    display_loadings = display_loadings_from_fit(fit)
    contributions = {
        factor_id: float(portfolio_weight) * float(beta)
        for factor_id, beta in display_loadings.items()
        if abs(float(portfolio_weight) * float(beta)) > _EPSILON
    }
    return contributions
