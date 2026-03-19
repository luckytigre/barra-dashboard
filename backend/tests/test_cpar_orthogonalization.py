import numpy as np

from backend.cpar.orthogonalization import orthogonalize_proxy_panel


def test_orthogonalize_proxy_panel_recovers_market_loadings_and_residuals() -> None:
    market = np.asarray([-2.0, -1.0, 0.0, 1.0, 2.0], dtype=float)
    weights = np.ones_like(market)
    sector_residual = np.asarray([1.0, -2.0, 0.0, 2.0, -1.0], dtype=float)
    style_residual = np.asarray([1.0, 0.0, -2.0, 0.0, 1.0], dtype=float)
    sector_raw = 0.5 + (0.3 * market) + sector_residual
    style_raw = -0.2 + (-0.4 * market) + style_residual

    out = orthogonalize_proxy_panel(
        market,
        {"XLB": sector_raw, "MTUM": style_raw},
        weights,
        factor_groups={"XLB": "sector", "MTUM": "style"},
    )

    assert out.factor_ids == ("XLB", "MTUM")
    assert np.allclose(out.residual_matrix[:, 0], sector_residual)
    assert np.allclose(out.residual_matrix[:, 1], style_residual)
    assert np.isclose(out.intercepts["XLB"], 0.5)
    assert np.isclose(out.market_betas["XLB"], 0.3)
    assert np.isclose(out.intercepts["MTUM"], -0.2)
    assert np.isclose(out.market_betas["MTUM"], -0.4)
