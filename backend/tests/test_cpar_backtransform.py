import numpy as np

from backend.cpar.backtransform import backtransform_trade_space, threshold_trade_space_loadings
from backend.cpar.contracts import MarketStepResult, OrthogonalizationResult, PostMarketRegressionResult


def test_backtransform_adjusts_spy_trade_beta_and_total_intercept() -> None:
    market_step = MarketStepResult(
        alpha=0.01,
        beta=1.2,
        fitted=np.zeros(3, dtype=float),
        residuals=np.zeros(3, dtype=float),
    )
    post_market = PostMarketRegressionResult(
        alpha=0.02,
        factor_ids=("XLB", "MTUM"),
        factor_groups={"XLB": "sector", "MTUM": "style"},
        orthogonalized_betas={"XLB": 0.4, "MTUM": -0.2},
        standardized_betas={"XLB": 0.8, "MTUM": -0.2},
        means={"XLB": 0.0, "MTUM": 0.0},
        scales={"XLB": 1.0, "MTUM": 1.0},
        penalties={"XLB": 4.0, "MTUM": 8.0},
        dropped_factors=(),
        fitted=np.zeros(3, dtype=float),
        residuals=np.zeros(3, dtype=float),
    )
    orth = OrthogonalizationResult(
        factor_ids=("XLB", "MTUM"),
        factor_groups={"XLB": "sector", "MTUM": "style"},
        intercepts={"XLB": 0.1, "MTUM": -0.05},
        market_betas={"XLB": 0.5, "MTUM": -0.25},
        residual_matrix=np.zeros((3, 2), dtype=float),
    )

    out = backtransform_trade_space(
        market_step=market_step,
        post_market=post_market,
        orthogonalization=orth,
    )

    assert np.isclose(out.spy_trade_beta, 0.95)
    assert np.isclose(out.total_intercept, -0.02)
    assert out.raw_loadings["SPY"] == out.spy_trade_beta
    assert out.raw_loadings["XLB"] == 0.4
    assert out.raw_loadings["MTUM"] == -0.2


def test_threshold_trade_space_loadings_respects_cpar1_boundary_rule() -> None:
    out = threshold_trade_space_loadings(
        {
            "SPY": 0.04,
            "XLB": 0.049999,
            "XLK": 0.05,
            "MTUM": -0.050001,
        }
    )

    assert out["SPY"] == 0.04
    assert out["XLB"] == 0.0
    assert out["XLK"] == 0.05
    assert out["MTUM"] == -0.050001
