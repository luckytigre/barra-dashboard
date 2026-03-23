"""Shared contracts for the pure cPAR1 math kernel."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np


@dataclass(frozen=True)
class FactorSpec:
    factor_id: str
    ticker: str
    label: str
    group: str
    display_order: int


@dataclass(frozen=True)
class WeeklyPriceSelection:
    anchor_date: str
    week_start_date: str
    price_date: str | None
    price_value: float | None
    price_field: str | None


@dataclass
class WeeklyReturnSeries:
    package_date: str
    lookback_weeks: int
    half_life_weeks: int
    price_anchors: tuple[str, ...]
    return_anchors: tuple[str, ...]
    price_selections: tuple[WeeklyPriceSelection, ...]
    returns: np.ndarray
    observed_mask: np.ndarray
    weights: np.ndarray
    price_field_used: str
    observed_weeks: int
    longest_gap_weeks: int


@dataclass(frozen=True)
class FitStatusSummary:
    fit_status: str
    warnings: tuple[str, ...]
    observed_weeks: int
    lookback_weeks: int
    longest_gap_weeks: int


@dataclass
class WeightedLeastSquaresResult:
    intercept: float
    coefficients: np.ndarray
    fitted: np.ndarray
    residuals: np.ndarray


@dataclass
class MarketStepResult:
    alpha: float
    beta: float
    fitted: np.ndarray
    residuals: np.ndarray


@dataclass
class OrthogonalizationResult:
    factor_ids: tuple[str, ...]
    factor_groups: dict[str, str]
    intercepts: dict[str, float]
    market_betas: dict[str, float]
    residual_matrix: np.ndarray


@dataclass
class PostMarketRegressionResult:
    alpha: float
    factor_ids: tuple[str, ...]
    factor_groups: dict[str, str]
    orthogonalized_betas: dict[str, float]
    standardized_betas: dict[str, float]
    means: dict[str, float]
    scales: dict[str, float]
    penalties: dict[str, float]
    dropped_factors: tuple[str, ...]
    fitted: np.ndarray
    residuals: np.ndarray


@dataclass
class RawTradeSpaceResult:
    total_intercept: float
    market_step_alpha: float
    market_step_beta: float
    block_alpha: float
    spy_trade_beta: float
    raw_loadings: dict[str, float]


@dataclass(frozen=True)
class HedgeLeg:
    factor_id: str
    factor_group: str
    weight: float


@dataclass(frozen=True)
class HedgeStabilityDiagnostics:
    leg_overlap_ratio: float | None
    gross_hedge_notional_change: float | None
    net_hedge_notional_change: float | None


@dataclass
class HedgePreview:
    mode: str
    status: str
    reason: str | None
    hedge_legs: tuple[HedgeLeg, ...]
    hedge_weights: dict[str, float]
    post_hedge_loadings: dict[str, float]
    pre_hedge_variance_proxy: float
    post_hedge_variance_proxy: float
    gross_hedge_notional: float
    net_hedge_notional: float
    non_market_reduction_ratio: float | None
    stability: HedgeStabilityDiagnostics


def to_serializable_mapping(values: dict[str, Any]) -> dict[str, Any]:
    """Convert numpy scalar values into JSON-friendly Python scalars."""
    out: dict[str, Any] = {}
    for key, value in values.items():
        if isinstance(value, np.generic):
            out[str(key)] = value.item()
        else:
            out[str(key)] = value
    return out
