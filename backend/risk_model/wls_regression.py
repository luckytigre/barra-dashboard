"""Two-phase cross-sectional WLS factor return estimation."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np


@dataclass(frozen=True)
class WlsTwoPhaseResult:
    factor_returns: dict[str, float]
    robust_se: dict[str, float]
    t_stats: dict[str, float]
    residuals: np.ndarray
    raw_residuals: np.ndarray
    r_squared: float
    condition_number: float
    phase_a_condition_number: float
    phase_b_condition_number: float
    residual_vol: float


def _weighted_lstsq(x: np.ndarray, y: np.ndarray, omega_sqrt: np.ndarray) -> np.ndarray:
    w = np.clip(np.asarray(omega_sqrt, dtype=float), 0.0, None)
    xw = x * w[:, None]
    yw = y * w
    beta, *_ = np.linalg.lstsq(xw, yw, rcond=None)
    return beta


def _safe_weighted_r2(y: np.ndarray, y_hat: np.ndarray, omega: np.ndarray) -> float:
    ww = np.clip(np.asarray(omega, dtype=float), 0.0, None)
    denom = float(np.sum(ww))
    if denom <= 0:
        return 0.0
    ww = ww / denom
    mu = float(np.sum(ww * y))
    sst = float(np.sum(ww * (y - mu) ** 2))
    if sst <= 0:
        return 0.0
    sse = float(np.sum(ww * (y - y_hat) ** 2))
    return 1.0 - (sse / sst)


def _safe_condition_number(x: np.ndarray, omega_sqrt: np.ndarray) -> float:
    if x.size == 0:
        return 0.0
    ww = np.clip(np.asarray(omega_sqrt, dtype=float), 0.0, None).reshape(-1)
    if ww.size != x.shape[0]:
        return 0.0
    if float(np.sum(ww)) <= 0:
        return 0.0
    try:
        cond = float(np.linalg.cond(x * ww[:, None]))
    except np.linalg.LinAlgError:
        return float("inf")
    if not np.isfinite(cond):
        return float("inf")
    return max(0.0, cond)


def _industry_constraint_basis(m: int) -> np.ndarray:
    if m <= 1:
        return np.zeros((m, 0), dtype=float)
    return np.vstack(
        [
            np.eye(m - 1, dtype=float),
            -1.0 * np.ones((1, m - 1), dtype=float),
        ]
    )


def _hc_scale(n: int, p: int) -> float:
    dof = max(1, n - p)
    return float(n / dof)


def _linear_map_hc_cov(
    linear_map: np.ndarray,
    residuals: np.ndarray,
    *,
    scale: float,
) -> np.ndarray:
    if linear_map.size == 0:
        return np.zeros((0, 0), dtype=float)
    u2 = np.square(np.asarray(residuals, dtype=float)).reshape(-1)
    weighted = linear_map * u2[None, :]
    cov = weighted @ linear_map.T
    cov *= float(scale)
    cov = 0.5 * (cov + cov.T)
    return cov


def _safe_diag_sqrt(matrix: np.ndarray) -> np.ndarray:
    if matrix.size == 0:
        return np.zeros(0, dtype=float)
    diag = np.diag(matrix).astype(float, copy=False)
    diag = np.clip(diag, 0.0, None)
    diag = np.where(np.isfinite(diag), diag, 0.0)
    return np.sqrt(diag)


def estimate_factor_returns_two_phase(
    *,
    returns: np.ndarray,
    raw_returns: np.ndarray | None,
    market_caps: np.ndarray,
    country_exposures: np.ndarray | None,
    industry_exposures: np.ndarray | None,
    style_exposures: np.ndarray | None,
    country_names: list[str],
    industry_names: list[str],
    style_names: list[str],
    residualize_styles: bool = True,
) -> WlsTwoPhaseResult:
    """Estimate country/industry then style returns with sequential cap-weighted WLS."""
    if not residualize_styles:
        raise NotImplementedError("residualize_styles=False is not supported for the sequential estimator")

    y = np.asarray(returns, dtype=float).reshape(-1)
    raw_y = np.asarray(raw_returns, dtype=float).reshape(-1) if raw_returns is not None else y
    omega = np.clip(np.asarray(market_caps, dtype=float).reshape(-1), 0.0, None)
    w = np.sqrt(omega)
    n = y.shape[0]

    intercept = np.ones((n, 1), dtype=float)
    country_x = (
        np.asarray(country_exposures, dtype=float)
        if country_exposures is not None and country_exposures.size
        else np.zeros((n, 0), dtype=float)
    )
    industry_x = (
        np.asarray(industry_exposures, dtype=float)
        if industry_exposures is not None and industry_exposures.size
        else np.zeros((n, 0), dtype=float)
    )
    style_x = (
        np.asarray(style_exposures, dtype=float)
        if style_exposures is not None and style_exposures.size
        else np.zeros((n, 0), dtype=float)
    )

    industry_basis = _industry_constraint_basis(industry_x.shape[1])
    industry_reduced_x = industry_x @ industry_basis if industry_basis.size else np.zeros((n, 0), dtype=float)

    phase_a_design = np.hstack([intercept, country_x, industry_reduced_x])
    phase_a_condition = _safe_condition_number(phase_a_design, w)
    phase_a_beta = _weighted_lstsq(phase_a_design, y, w)
    y_a_hat = phase_a_design @ phase_a_beta
    residual_a = y - y_a_hat

    gram_a = phase_a_design.T @ (omega[:, None] * phase_a_design)
    phase_a_linear_map = np.linalg.pinv(gram_a) @ (phase_a_design.T * omega[None, :])

    phase_a_report_map = np.zeros((country_x.shape[1] + industry_x.shape[1], phase_a_design.shape[1]), dtype=float)
    phase_a_names: list[str] = []
    if country_x.shape[1]:
        for idx, name in enumerate(country_names[:country_x.shape[1]]):
            phase_a_names.append(name)
            phase_a_report_map[idx, 1 + idx] = 1.0
    if industry_x.shape[1]:
        row_start = country_x.shape[1]
        col_start = 1 + country_x.shape[1]
        phase_a_report_map[row_start:row_start + industry_x.shape[1], col_start:] = industry_basis
        phase_a_names.extend(industry_names[:industry_x.shape[1]])

    factor_returns: dict[str, float] = {}
    if phase_a_names:
        phase_a_reported = phase_a_report_map @ phase_a_beta
        for idx, name in enumerate(phase_a_names):
            factor_returns[name] = float(phase_a_reported[idx])

    residual_final = residual_a.copy()
    y_hat_final = y_a_hat.copy()
    raw_residual_final = raw_y - y_hat_final
    phase_b_condition = 0.0
    style_linear_map = np.zeros((0, n), dtype=float)

    if style_x.size:
        phase_b_condition = _safe_condition_number(style_x, w)
        gram_b = style_x.T @ (omega[:, None] * style_x)
        lhs = np.linalg.pinv(gram_b) @ (style_x.T * omega[None, :])
        style_linear_map = lhs - (lhs @ phase_a_design) @ phase_a_linear_map
        beta_b = style_linear_map @ y
        y_b_hat = style_x @ beta_b
        residual_final = residual_a - y_b_hat
        y_hat_final = y_a_hat + y_b_hat
        raw_residual_final = raw_y - y_hat_final
        for idx, name in enumerate(style_names[:beta_b.shape[0]]):
            factor_returns[name] = float(beta_b[idx])

    scale = _hc_scale(
        n=n,
        p=phase_a_design.shape[1] + (style_x.shape[1] if style_x.size else 0),
    )
    robust_se: dict[str, float] = {}
    t_stats: dict[str, float] = {}

    if phase_a_names:
        phase_a_cov = _linear_map_hc_cov(
            phase_a_report_map @ phase_a_linear_map,
            residual_final,
            scale=scale,
        )
        phase_a_se = _safe_diag_sqrt(phase_a_cov)
        for idx, name in enumerate(phase_a_names):
            se = float(phase_a_se[idx]) if idx < phase_a_se.shape[0] else 0.0
            beta = float(factor_returns.get(name, 0.0))
            robust_se[name] = se
            t_stats[name] = float(beta / se) if se > 0 else 0.0

    if style_x.size:
        style_cov = _linear_map_hc_cov(style_linear_map, residual_final, scale=scale)
        style_se = _safe_diag_sqrt(style_cov)
        for idx, name in enumerate(style_names[:style_se.shape[0]]):
            se = float(style_se[idx])
            beta = float(factor_returns.get(name, 0.0))
            robust_se[name] = se
            t_stats[name] = float(beta / se) if se > 0 else 0.0

    r2 = _safe_weighted_r2(y, y_hat_final, omega)
    residual_vol = float(np.std(residual_final, ddof=1)) if residual_final.size > 1 else 0.0
    if not np.isfinite(residual_vol):
        residual_vol = 0.0
    condition_number = max(phase_a_condition, phase_b_condition)

    return WlsTwoPhaseResult(
        factor_returns=factor_returns,
        robust_se=robust_se,
        t_stats=t_stats,
        residuals=residual_final,
        raw_residuals=raw_residual_final,
        r_squared=r2,
        condition_number=condition_number,
        phase_a_condition_number=phase_a_condition,
        phase_b_condition_number=phase_b_condition,
        residual_vol=residual_vol,
    )
