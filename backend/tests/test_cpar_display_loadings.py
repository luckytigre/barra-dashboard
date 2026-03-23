from __future__ import annotations

import pytest

from backend.services import cpar_display_loadings


def test_display_loadings_use_market_step_beta_but_keep_non_market_raw_coefficients() -> None:
    fit = {
        "market_step_beta": 0.91,
        "spy_trade_beta_raw": 1.18,
        "raw_loadings": {
            "SPY": 1.18,
            "XLK": -0.14,
            "QUAL": 0.07,
        },
    }

    display = cpar_display_loadings.display_loadings_from_fit(fit)

    assert display == {
        "SPY": pytest.approx(0.91),
        "XLK": pytest.approx(-0.14),
        "QUAL": pytest.approx(0.07),
    }


def test_ordered_factor_rows_follow_registry_order() -> None:
    rows = cpar_display_loadings.ordered_factor_rows(
        {
            "QUAL": 0.07,
            "SPY": 0.91,
            "XLK": -0.14,
        }
    )

    assert [row["factor_id"] for row in rows] == ["SPY", "XLK", "QUAL"]
