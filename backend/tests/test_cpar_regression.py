import numpy as np

from backend.cpar.regression import fit_market_step, fit_post_market_block


def test_fit_market_step_recovers_known_weighted_solution() -> None:
    market = np.asarray([-0.02, -0.01, 0.0, 0.01, 0.02], dtype=float)
    weights = np.asarray([1.0, 2.0, 3.0, 2.0, 1.0], dtype=float)
    y = 0.01 + (1.5 * market)

    out = fit_market_step(y, market, weights)

    assert np.isclose(out.alpha, 0.01)
    assert np.isclose(out.beta, 1.5)
    assert np.allclose(out.residuals, np.zeros_like(y))


def test_fit_post_market_block_recovers_unpenalized_coefficients_after_standardization() -> None:
    weights = np.asarray([1.0, 2.0, 2.0, 1.0], dtype=float)
    sector = np.asarray([1.0, -1.0, 0.5, -0.5], dtype=float)
    style = np.asarray([0.2, 0.3, -0.1, -0.4], dtype=float)
    y = 0.03 + (0.4 * sector) - (0.2 * style)

    out = fit_post_market_block(
        y,
        {"XLB": sector, "MTUM": style},
        weights,
        factor_groups={"XLB": "sector", "MTUM": "style"},
        sector_lambda=0.0,
        style_lambda=0.0,
    )

    assert np.isclose(out.alpha, 0.03)
    assert np.isclose(out.orthogonalized_betas["XLB"], 0.4)
    assert np.isclose(out.orthogonalized_betas["MTUM"], -0.2)
    assert np.allclose(out.residuals, np.zeros_like(y), atol=1e-12)


def test_fit_post_market_block_drops_zero_variance_factors() -> None:
    weights = np.ones(4, dtype=float)
    y = np.asarray([0.1, -0.1, 0.1, -0.1], dtype=float)
    out = fit_post_market_block(
        y,
        {"XLB": np.asarray([1.0, -1.0, 1.0, -1.0], dtype=float), "XLK": np.ones(4, dtype=float)},
        weights,
        factor_groups={"XLB": "sector", "XLK": "sector"},
        sector_lambda=0.0,
        style_lambda=0.0,
    )

    assert out.orthogonalized_betas["XLB"] == 0.1
    assert out.orthogonalized_betas["XLK"] == 0.0
    assert out.dropped_factors == ("XLK",)
