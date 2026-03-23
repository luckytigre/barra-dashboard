"""Weighted least-squares and weighted ridge helpers for cPAR1."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

import numpy as np

from backend.cpar.contracts import (
    MarketStepResult,
    PostMarketRegressionResult,
    WeightedLeastSquaresResult,
)
from backend.cpar.factor_registry import factor_group_for_id

_MIN_SCALE = 1e-12


def _as_float_vector(values: Sequence[float] | np.ndarray, *, name: str) -> np.ndarray:
    array = np.asarray(values, dtype=float)
    if array.ndim != 1:
        raise ValueError(f"{name} must be a 1D array")
    if array.size == 0:
        raise ValueError(f"{name} must not be empty")
    if not np.all(np.isfinite(array)):
        raise ValueError(f"{name} must be finite")
    return array


def _as_float_matrix(values: Sequence[Sequence[float]] | np.ndarray, *, name: str) -> np.ndarray:
    array = np.asarray(values, dtype=float)
    if array.ndim != 2:
        raise ValueError(f"{name} must be a 2D array")
    if array.shape[0] == 0:
        raise ValueError(f"{name} must have at least one row")
    if not np.all(np.isfinite(array)):
        raise ValueError(f"{name} must be finite")
    return array


def normalize_weights(weights: Sequence[float] | np.ndarray) -> np.ndarray:
    array = _as_float_vector(weights, name="weights")
    if np.any(array < 0.0):
        raise ValueError("weights must be non-negative")
    total = float(array.sum())
    if total <= 0.0:
        raise ValueError("weights must sum to a positive value")
    return array / total


def weighted_mean(values: Sequence[float] | np.ndarray, weights: Sequence[float] | np.ndarray) -> float:
    y = _as_float_vector(values, name="values")
    w = normalize_weights(weights)
    if y.shape[0] != w.shape[0]:
        raise ValueError("values and weights must have the same length")
    return float(np.dot(w, y))


def weighted_std(values: Sequence[float] | np.ndarray, weights: Sequence[float] | np.ndarray) -> float:
    y = _as_float_vector(values, name="values")
    w = normalize_weights(weights)
    mu = weighted_mean(y, w)
    return float(np.sqrt(np.dot(w, np.square(y - mu))))


def weighted_ols_with_intercept(
    y: Sequence[float] | np.ndarray,
    x: Sequence[float] | np.ndarray | Sequence[Sequence[float]],
    weights: Sequence[float] | np.ndarray,
) -> WeightedLeastSquaresResult:
    y_vec = _as_float_vector(y, name="y")
    w = normalize_weights(weights)
    x_arr = np.asarray(x, dtype=float)
    if x_arr.ndim == 1:
        x_mat = x_arr.reshape(-1, 1)
    else:
        x_mat = _as_float_matrix(x_arr, name="x")
    if x_mat.shape[0] != y_vec.shape[0] or w.shape[0] != y_vec.shape[0]:
        raise ValueError("y, x, and weights must share the same row count")
    design = np.column_stack([np.ones(y_vec.shape[0], dtype=float), x_mat])
    sqrt_w = np.sqrt(w)
    lhs = design * sqrt_w[:, None]
    rhs = y_vec * sqrt_w
    coefficients, *_ = np.linalg.lstsq(lhs, rhs, rcond=None)
    fitted = design @ coefficients
    residuals = y_vec - fitted
    return WeightedLeastSquaresResult(
        intercept=float(coefficients[0]),
        coefficients=np.asarray(coefficients[1:], dtype=float),
        fitted=np.asarray(fitted, dtype=float),
        residuals=np.asarray(residuals, dtype=float),
    )


def fit_market_step(
    y: Sequence[float] | np.ndarray,
    market_returns: Sequence[float] | np.ndarray,
    weights: Sequence[float] | np.ndarray,
) -> MarketStepResult:
    result = weighted_ols_with_intercept(y, market_returns, weights)
    if result.coefficients.shape[0] != 1:
        raise RuntimeError("market-step regression should return exactly one coefficient")
    return MarketStepResult(
        alpha=float(result.intercept),
        beta=float(result.coefficients[0]),
        fitted=result.fitted,
        residuals=result.residuals,
    )


def weighted_standardize_matrix(
    x: Sequence[Sequence[float]] | np.ndarray,
    weights: Sequence[float] | np.ndarray,
    *,
    min_scale: float = _MIN_SCALE,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    x_mat = _as_float_matrix(x, name="x")
    w = normalize_weights(weights)
    if x_mat.shape[0] != w.shape[0]:
        raise ValueError("x and weights must share the same row count")
    means = np.sum(x_mat * w[:, None], axis=0)
    centered = x_mat - means
    scales = np.sqrt(np.sum(np.square(centered) * w[:, None], axis=0))
    active_mask = scales >= float(min_scale)
    standardized = np.zeros_like(x_mat, dtype=float)
    if np.any(active_mask):
        standardized[:, active_mask] = centered[:, active_mask] / scales[active_mask]
    return standardized, means, scales, active_mask


def build_penalties(
    factor_ids: Sequence[str],
    *,
    factor_groups: Mapping[str, str] | None = None,
    sector_lambda: float = 4.0,
    style_lambda: float = 8.0,
) -> np.ndarray:
    penalties: list[float] = []
    for factor_id in factor_ids:
        group = str((factor_groups or {}).get(str(factor_id)) or factor_group_for_id(str(factor_id))).lower()
        if group == "sector":
            penalties.append(float(sector_lambda))
        elif group == "style":
            penalties.append(float(style_lambda))
        else:
            raise ValueError(f"Unsupported post-market factor group for {factor_id}: {group}")
    return np.asarray(penalties, dtype=float)


def fit_post_market_block(
    residual_returns: Sequence[float] | np.ndarray,
    orthogonalized_factor_returns: Mapping[str, Sequence[float] | np.ndarray],
    weights: Sequence[float] | np.ndarray,
    *,
    factor_groups: Mapping[str, str] | None = None,
    sector_lambda: float = 4.0,
    style_lambda: float = 8.0,
    min_scale: float = _MIN_SCALE,
) -> PostMarketRegressionResult:
    y = _as_float_vector(residual_returns, name="residual_returns")
    factor_ids = tuple(str(factor_id) for factor_id in orthogonalized_factor_returns.keys())
    if not factor_ids:
        raise ValueError("orthogonalized_factor_returns must not be empty")
    x_mat = np.column_stack(
        [_as_float_vector(orthogonalized_factor_returns[factor_id], name=factor_id) for factor_id in factor_ids]
    )
    w = normalize_weights(weights)
    if x_mat.shape[0] != y.shape[0] or w.shape[0] != y.shape[0]:
        raise ValueError("residual_returns, factor rows, and weights must share the same row count")
    groups = {
        factor_id: str((factor_groups or {}).get(factor_id) or factor_group_for_id(factor_id)).lower()
        for factor_id in factor_ids
    }
    penalties = build_penalties(
        factor_ids,
        factor_groups=groups,
        sector_lambda=sector_lambda,
        style_lambda=style_lambda,
    )
    standardized, means, scales, active_mask = weighted_standardize_matrix(
        x_mat,
        w,
        min_scale=min_scale,
    )
    active_indices = np.flatnonzero(active_mask)
    if active_indices.size == 0:
        intercept = weighted_mean(y, w)
        fitted = np.full_like(y, intercept, dtype=float)
        residuals = y - fitted
        return PostMarketRegressionResult(
            alpha=float(intercept),
            factor_ids=factor_ids,
            factor_groups=groups,
            orthogonalized_betas={factor_id: 0.0 for factor_id in factor_ids},
            standardized_betas={factor_id: 0.0 for factor_id in factor_ids},
            means={factor_id: float(means[idx]) for idx, factor_id in enumerate(factor_ids)},
            scales={factor_id: float(scales[idx]) for idx, factor_id in enumerate(factor_ids)},
            penalties={factor_id: float(penalties[idx]) for idx, factor_id in enumerate(factor_ids)},
            dropped_factors=factor_ids,
            fitted=fitted,
            residuals=residuals,
        )
    x_active = standardized[:, active_indices]
    penalties_active = penalties[active_indices]
    design = np.column_stack([np.ones(y.shape[0], dtype=float), x_active])
    sqrt_w = np.sqrt(w)
    lhs = design * sqrt_w[:, None]
    rhs = y * sqrt_w
    penalty_matrix = np.zeros((design.shape[1], design.shape[1]), dtype=float)
    penalty_matrix[1:, 1:] = np.diag(penalties_active)
    solution = np.linalg.solve(lhs.T @ lhs + penalty_matrix, lhs.T @ rhs)
    intercept_std = float(solution[0])
    beta_std_active = np.asarray(solution[1:], dtype=float)
    beta_std = np.zeros(len(factor_ids), dtype=float)
    beta_std[active_indices] = beta_std_active
    beta_raw = np.zeros(len(factor_ids), dtype=float)
    beta_raw[active_indices] = beta_std_active / scales[active_indices]
    intercept_raw = intercept_std - float(
        np.sum(beta_std_active * means[active_indices] / scales[active_indices])
    )
    fitted = intercept_raw + x_mat @ beta_raw
    residuals = y - fitted
    return PostMarketRegressionResult(
        alpha=float(intercept_raw),
        factor_ids=factor_ids,
        factor_groups=groups,
        orthogonalized_betas={factor_id: float(beta_raw[idx]) for idx, factor_id in enumerate(factor_ids)},
        standardized_betas={factor_id: float(beta_std[idx]) for idx, factor_id in enumerate(factor_ids)},
        means={factor_id: float(means[idx]) for idx, factor_id in enumerate(factor_ids)},
        scales={factor_id: float(scales[idx]) for idx, factor_id in enumerate(factor_ids)},
        penalties={factor_id: float(penalties[idx]) for idx, factor_id in enumerate(factor_ids)},
        dropped_factors=tuple(factor_ids[idx] for idx in range(len(factor_ids)) if not active_mask[idx]),
        fitted=np.asarray(fitted, dtype=float),
        residuals=np.asarray(residuals, dtype=float),
    )
