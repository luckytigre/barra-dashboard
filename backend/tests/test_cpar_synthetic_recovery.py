import numpy as np

from backend.cpar.backtransform import backtransform_trade_space, backtransform_trade_space_from_one_shot
from backend.cpar.orthogonalization import orthogonalize_proxy_panel
from backend.cpar.regression import fit_market_plus_residualized_block, fit_market_step, fit_post_market_block


def _project_out_market(base: np.ndarray, market: np.ndarray) -> np.ndarray:
    design = np.column_stack([np.ones(base.shape[0], dtype=float), market])
    coefficients, *_ = np.linalg.lstsq(design, base, rcond=None)
    return base - (design @ coefficients)


def test_two_step_cpar_kernel_recovers_raw_trade_space_loadings_without_penalty() -> None:
    market = np.asarray([-0.03, -0.02, -0.01, 0.0, 0.01, 0.02, 0.03], dtype=float)
    weights = np.ones_like(market)

    u_sector = _project_out_market(np.asarray([0.5, -0.2, 0.1, 0.4, -0.1, -0.3, 0.2], dtype=float), market)
    u_style = _project_out_market(np.asarray([-0.3, 0.2, 0.4, -0.5, 0.1, 0.0, 0.3], dtype=float), market)

    sector_alpha = 0.002
    style_alpha = -0.001
    sector_market_beta = 0.60
    style_market_beta = -0.35

    sector_raw = sector_alpha + (sector_market_beta * market) + u_sector
    style_raw = style_alpha + (style_market_beta * market) + u_style

    true_intercept = 0.004
    true_spy_trade_beta = 0.75
    true_sector_beta = 0.40
    true_style_beta = -0.25

    instrument_returns = (
        true_intercept
        + (true_spy_trade_beta * market)
        + (true_sector_beta * sector_raw)
        + (true_style_beta * style_raw)
    )

    market_step = fit_market_step(instrument_returns, market, weights)
    orth = orthogonalize_proxy_panel(
        market,
        {"XLB": sector_raw, "MTUM": style_raw},
        weights,
        factor_groups={"XLB": "sector", "MTUM": "style"},
    )
    post_market = fit_post_market_block(
        market_step.residuals,
        {
            factor_id: orth.residual_matrix[:, idx]
            for idx, factor_id in enumerate(orth.factor_ids)
        },
        weights,
        factor_groups=orth.factor_groups,
        sector_lambda=0.0,
        style_lambda=0.0,
    )
    trade_space = backtransform_trade_space(
        market_step=market_step,
        post_market=post_market,
        orthogonalization=orth,
    )

    assert np.isclose(trade_space.total_intercept, true_intercept, atol=1e-12)
    assert np.isclose(trade_space.spy_trade_beta, true_spy_trade_beta, atol=1e-12)
    assert np.isclose(trade_space.raw_loadings["XLB"], true_sector_beta, atol=1e-12)
    assert np.isclose(trade_space.raw_loadings["MTUM"], true_style_beta, atol=1e-12)


def test_one_shot_cpar_kernel_recovers_residualized_and_trade_space_loadings_without_penalty() -> None:
    market = np.asarray([-0.03, -0.02, -0.01, 0.0, 0.01, 0.02, 0.03], dtype=float)
    weights = np.ones_like(market)

    u_sector = _project_out_market(np.asarray([0.5, -0.2, 0.1, 0.4, -0.1, -0.3, 0.2], dtype=float), market)
    u_style = _project_out_market(np.asarray([-0.3, 0.2, 0.4, -0.5, 0.1, 0.0, 0.3], dtype=float), market)

    sector_alpha = 0.002
    style_alpha = -0.001
    sector_market_beta = 0.60
    style_market_beta = -0.35

    sector_raw = sector_alpha + (sector_market_beta * market) + u_sector
    style_raw = style_alpha + (style_market_beta * market) + u_style

    true_intercept = 0.004
    true_spy_trade_beta = 0.75
    true_sector_beta = 0.40
    true_style_beta = -0.25

    instrument_returns = (
        true_intercept
        + (true_spy_trade_beta * market)
        + (true_sector_beta * sector_raw)
        + (true_style_beta * style_raw)
    )

    orth = orthogonalize_proxy_panel(
        market,
        {"XLB": sector_raw, "MTUM": style_raw},
        weights,
        factor_groups={"XLB": "sector", "MTUM": "style"},
    )
    fit = fit_market_plus_residualized_block(
        instrument_returns,
        market,
        {
            factor_id: orth.residual_matrix[:, idx]
            for idx, factor_id in enumerate(orth.factor_ids)
        },
        weights,
        factor_groups=orth.factor_groups,
        sector_lambda=0.0,
        style_lambda=0.0,
    )
    trade_space = backtransform_trade_space_from_one_shot(
        fit=fit,
        orthogonalization=orth,
    )

    assert np.isclose(fit.alpha, 0.00505, atol=1e-12)
    assert np.isclose(fit.market_beta, 1.0775, atol=1e-12)
    assert np.isclose(fit.residualized_betas["XLB"], true_sector_beta, atol=1e-12)
    assert np.isclose(fit.residualized_betas["MTUM"], true_style_beta, atol=1e-12)
    assert np.isclose(trade_space.spy_trade_beta, true_spy_trade_beta, atol=1e-12)
