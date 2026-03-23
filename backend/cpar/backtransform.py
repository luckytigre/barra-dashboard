"""Raw ETF trade-space back-transform helpers for cPAR1."""

from __future__ import annotations

from collections.abc import Mapping

from backend.cpar.contracts import (
    MarketStepResult,
    OneShotRegressionResult,
    OrthogonalizationResult,
    PostMarketRegressionResult,
    RawTradeSpaceResult,
)
from backend.cpar.factor_registry import MARKET_FACTOR_ID

DEFAULT_THRESHOLD = 0.05


def backtransform_trade_space(
    *,
    market_step: MarketStepResult,
    post_market: PostMarketRegressionResult,
    orthogonalization: OrthogonalizationResult,
) -> RawTradeSpaceResult:
    raw_loadings = {
        factor_id: float(post_market.orthogonalized_betas.get(factor_id, 0.0))
        for factor_id in post_market.factor_ids
    }
    market_adjustment = sum(
        raw_loadings[factor_id] * float(orthogonalization.market_betas.get(factor_id, 0.0))
        for factor_id in post_market.factor_ids
    )
    intercept_adjustment = sum(
        raw_loadings[factor_id] * float(orthogonalization.intercepts.get(factor_id, 0.0))
        for factor_id in post_market.factor_ids
    )
    spy_trade_beta = float(market_step.beta - market_adjustment)
    total_intercept = float(market_step.alpha + post_market.alpha - intercept_adjustment)
    all_loadings = {MARKET_FACTOR_ID: spy_trade_beta, **raw_loadings}
    return RawTradeSpaceResult(
        total_intercept=total_intercept,
        market_step_alpha=float(market_step.alpha),
        market_step_beta=float(market_step.beta),
        block_alpha=float(post_market.alpha),
        spy_trade_beta=spy_trade_beta,
        raw_loadings=all_loadings,
    )


def threshold_trade_space_loadings(
    raw_loadings: Mapping[str, float],
    *,
    threshold: float = DEFAULT_THRESHOLD,
    market_factor_id: str = MARKET_FACTOR_ID,
) -> dict[str, float]:
    clean_threshold = float(threshold)
    if clean_threshold < 0.0:
        raise ValueError("threshold must be non-negative")
    thresholded: dict[str, float] = {}
    for factor_id, raw_value in raw_loadings.items():
        value = float(raw_value)
        if str(factor_id).upper() == str(market_factor_id).upper():
            thresholded[str(factor_id)] = value
        elif abs(value) < clean_threshold:
            thresholded[str(factor_id)] = 0.0
        else:
            thresholded[str(factor_id)] = value
    return thresholded


def backtransform_market_trade_beta(
    *,
    market_beta: float,
    residualized_betas: Mapping[str, float],
    orthogonalization: OrthogonalizationResult,
) -> float:
    market_adjustment = sum(
        float(residualized_betas.get(factor_id, 0.0)) * float(orthogonalization.market_betas.get(factor_id, 0.0))
        for factor_id in orthogonalization.factor_ids
    )
    return float(market_beta - market_adjustment)


def backtransform_trade_space_from_one_shot(
    *,
    fit: OneShotRegressionResult,
    orthogonalization: OrthogonalizationResult,
) -> RawTradeSpaceResult:
    raw_loadings = {
        factor_id: float(fit.residualized_betas.get(factor_id, 0.0))
        for factor_id in fit.factor_ids
    }
    market_adjustment = sum(
        raw_loadings[factor_id] * float(orthogonalization.market_betas.get(factor_id, 0.0))
        for factor_id in fit.factor_ids
    )
    intercept_adjustment = sum(
        raw_loadings[factor_id] * float(orthogonalization.intercepts.get(factor_id, 0.0))
        for factor_id in fit.factor_ids
    )
    spy_trade_beta = float(fit.market_beta - market_adjustment)
    total_intercept = float(fit.alpha - intercept_adjustment)
    return RawTradeSpaceResult(
        total_intercept=total_intercept,
        market_step_alpha=float(fit.alpha),
        market_step_beta=float(fit.market_beta),
        block_alpha=0.0,
        spy_trade_beta=spy_trade_beta,
        raw_loadings={MARKET_FACTOR_ID: spy_trade_beta, **raw_loadings},
    )
