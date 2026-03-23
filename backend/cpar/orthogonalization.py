"""Package-level proxy orthogonalization for cPAR1."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

import numpy as np

from backend.cpar.contracts import OrthogonalizationResult
from backend.cpar.factor_registry import MARKET_FACTOR_ID, factor_group_for_id
from backend.cpar.regression import normalize_weights, weighted_ols_with_intercept


def orthogonalize_factor_to_market(
    market_returns: Sequence[float] | np.ndarray,
    factor_returns: Sequence[float] | np.ndarray,
    weights: Sequence[float] | np.ndarray,
) -> tuple[float, float, np.ndarray]:
    result = weighted_ols_with_intercept(factor_returns, market_returns, weights)
    if result.coefficients.shape[0] != 1:
        raise RuntimeError("orthogonalization should return exactly one market coefficient")
    return float(result.intercept), float(result.coefficients[0]), np.asarray(result.residuals, dtype=float)


def orthogonalize_proxy_panel(
    market_returns: Sequence[float] | np.ndarray,
    proxy_returns: Mapping[str, Sequence[float] | np.ndarray],
    weights: Sequence[float] | np.ndarray,
    *,
    factor_groups: Mapping[str, str] | None = None,
) -> OrthogonalizationResult:
    factor_ids = tuple(str(factor_id) for factor_id in proxy_returns.keys())
    if not factor_ids:
        raise ValueError("proxy_returns must not be empty")
    groups: dict[str, str] = {}
    intercepts: dict[str, float] = {}
    market_betas: dict[str, float] = {}
    residual_columns: list[np.ndarray] = []
    normalized_weights = normalize_weights(weights)
    for factor_id in factor_ids:
        group = str((factor_groups or {}).get(factor_id) or factor_group_for_id(factor_id)).lower()
        if factor_id == MARKET_FACTOR_ID or group == "market":
            raise ValueError("market factor must not be included in the orthogonalized proxy panel")
        groups[factor_id] = group
        alpha, beta, residuals = orthogonalize_factor_to_market(
            market_returns,
            proxy_returns[factor_id],
            normalized_weights,
        )
        intercepts[factor_id] = alpha
        market_betas[factor_id] = beta
        residual_columns.append(residuals)
    return OrthogonalizationResult(
        factor_ids=factor_ids,
        factor_groups=groups,
        intercepts=intercepts,
        market_betas=market_betas,
        residual_matrix=np.column_stack(residual_columns),
    )
