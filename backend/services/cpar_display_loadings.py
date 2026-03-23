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
    raw_loadings = {
        str(factor_id): numeric
        for factor_id, raw_value in dict(clean_fit.get("raw_loadings") or {}).items()
        if (numeric := _finite_float(raw_value)) is not None
    }
    return {
        factor_id: float(beta)
        for factor_id, beta in raw_loadings.items()
        if abs(float(beta)) > _EPSILON or factor_id == MARKET_FACTOR_ID
    }


def hedge_trade_loadings_from_fit(
    fit: dict[str, Any] | None,
    *,
    thresholded: bool,
) -> dict[str, float]:
    clean_fit = dict(fit or {})
    base_loadings = dict(clean_fit.get("thresholded_loadings") if thresholded else clean_fit.get("raw_loadings") or {})
    rendered = {
        str(factor_id): numeric
        for factor_id, raw_value in base_loadings.items()
        if (numeric := _finite_float(raw_value)) is not None
    }
    market_trade_beta = _finite_float(clean_fit.get("spy_trade_beta_raw"))
    if market_trade_beta is not None:
        rendered[MARKET_FACTOR_ID] = float(market_trade_beta)
    return rendered


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
