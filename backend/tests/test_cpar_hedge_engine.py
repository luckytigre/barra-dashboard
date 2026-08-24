import math

import pytest

from backend.cpar.hedge_engine import (
    build_factor_neutral_hedge,
    build_factor_neutral_recommendation,
    build_market_neutral_hedge,
)


def _identity_covariance(factor_ids: list[str]) -> dict[tuple[str, str], float]:
    covariance: dict[tuple[str, str], float] = {}
    for left in factor_ids:
        for right in factor_ids:
            covariance[(left, right)] = 1.0 if left == right else 0.0
    return covariance


def test_market_neutral_hedge_skips_small_market_beta() -> None:
    preview = build_market_neutral_hedge(
        {"SPY": 0.09, "XLK": 0.25},
        _identity_covariance(["SPY", "XLK"]),
        fit_status="ok",
    )

    assert preview.status == "hedge_ok"
    assert preview.reason == "below_market_materiality_threshold"
    assert preview.hedge_legs == ()


def test_market_neutral_hedge_sizes_on_total_market_beta_when_supplied() -> None:
    # The trade-space market entry (0.26) is net of the market exposure the
    # factor legs would carry. This package trades SPY alone, so it must size on
    # total beta (1.10) or it systematically under-hedges.
    preview = build_market_neutral_hedge(
        {"SPY": 0.26, "XLK": 0.80},
        _identity_covariance(["SPY", "XLK"]),
        fit_status="ok",
        market_total_beta=1.10,
    )

    assert preview.status == "hedge_ok"
    assert math.isclose(float(preview.hedge_weights["SPY"]), -1.10, rel_tol=0.0, abs_tol=1e-12)
    assert math.isclose(float(preview.post_hedge_loadings["SPY"]), 0.0, rel_tol=0.0, abs_tol=1e-12)


def test_market_neutral_gate_applies_to_total_market_beta() -> None:
    # A small trade-space coefficient must not gate out a hedge for a position
    # whose actual market beta is material.
    preview = build_market_neutral_hedge(
        {"SPY": 0.02, "XLK": 0.80},
        _identity_covariance(["SPY", "XLK"]),
        fit_status="ok",
        market_total_beta=1.05,
    )

    assert preview.reason is None
    assert math.isclose(float(preview.hedge_weights["SPY"]), -1.05, rel_tol=0.0, abs_tol=1e-12)


def test_factor_neutral_hedge_keeps_market_leg_despite_high_correlation() -> None:
    # Regression: correlated-substitute pruning used to delete the market leg,
    # because SPY correlates above 0.90 with most of the registry and its
    # trade-space loading is structurally the smallest. A dropped market leg was
    # invisible to non_market_reduction_ratio, which excludes market.
    covariance = _identity_covariance(["SPY", "QUAL"])
    covariance[("SPY", "QUAL")] = 0.98
    covariance[("QUAL", "SPY")] = 0.98
    preview = build_factor_neutral_hedge(
        {"SPY": 0.15, "QUAL": 0.45},
        covariance,
        fit_status="ok",
    )

    assert "SPY" in preview.hedge_weights
    assert math.isclose(float(preview.hedge_weights["SPY"]), -0.15, rel_tol=0.0, abs_tol=1e-12)
    assert math.isclose(float(preview.post_hedge_loadings["SPY"]), 0.0, rel_tol=0.0, abs_tol=1e-12)


def test_factor_neutral_hedge_never_increases_variance() -> None:
    # Fix-agnostic guard. Under correlated-substitute pruning an opposite-sign
    # pair could yield a "hedge" that raised total variance while reporting full
    # reduction. Hedging can leave exposure unhedged; it must never add risk.
    covariance = {
        ("SPY", "SPY"): 0.022 * 0.022,
        ("VLUE", "VLUE"): 0.030 * 0.030,
        ("SPY", "VLUE"): 0.92 * 0.022 * 0.030,
        ("VLUE", "SPY"): 0.92 * 0.022 * 0.030,
    }
    preview = build_factor_neutral_hedge(
        {"SPY": 0.30, "VLUE": -0.40},
        covariance,
        fit_status="ok",
    )

    assert preview.post_hedge_variance_proxy <= preview.pre_hedge_variance_proxy


def test_factor_neutral_hedge_caps_factor_legs_and_keeps_market_alongside() -> None:
    # The cap bounds non-market legs; the market leg does not compete for a
    # slot, so a full package is max_hedge_legs + 1 instruments.
    covariance = _identity_covariance(["SPY", "XLK", "XLY", "MTUM", "VLUE", "QUAL", "USMV"])
    preview = build_factor_neutral_hedge(
        {
            "SPY": 0.20,
            "XLK": 0.40,
            "XLY": 0.30,
            "MTUM": 0.20,
            "VLUE": -0.10,
            "QUAL": 0.08,
            "USMV": 0.06,
        },
        covariance,
        fit_status="ok",
    )

    assert preview.status == "hedge_ok"
    assert sorted(preview.hedge_weights) == ["MTUM", "QUAL", "SPY", "VLUE", "XLK", "XLY"]
    assert preview.hedge_weights["SPY"] == -0.20
    assert "USMV" not in preview.hedge_weights


def test_factor_neutral_hedge_marks_degraded_when_leg_cap_leaves_too_much_residual() -> None:
    loadings = {
        "SPY": 0.20,
        "XLB": 0.10, "XLC": 0.10, "XLE": 0.10, "XLF": 0.10,
        "XLI": 0.10, "XLK": 0.10, "XLP": 0.10, "XLRE": 0.10,
        "XLU": 0.10, "XLV": 0.10, "XLY": 0.10, "MTUM": 0.10,
    }
    preview = build_factor_neutral_hedge(
        loadings,
        _identity_covariance(list(loadings)),
        fit_status="ok",
    )

    # 12 non-market legs at 0.10, five hedged: 1 - 0.7/1.2.
    assert preview.status == "hedge_degraded"
    assert math.isclose(float(preview.non_market_reduction_ratio or 0.0), 1.0 - 0.7 / 1.2, rel_tol=0.0, abs_tol=1e-12)


def test_factor_neutral_hedge_respects_explicit_leg_cap() -> None:
    preview = build_factor_neutral_hedge(
        {
            "SPY": 0.30,
            "XLK": 0.25,
            "XLF": -0.20,
            "XLV": 0.15,
        },
        _identity_covariance(["SPY", "XLK", "XLF", "XLV"]),
        fit_status="ok",
        max_hedge_legs=2,
    )

    assert sorted(preview.hedge_weights) == ["SPY", "XLF", "XLK"]
    assert "XLV" not in preview.hedge_weights


def test_factor_neutral_recommendation_matches_hedge_leg_selection() -> None:
    loadings = {
        "SPY": 0.30,
        "XLK": 0.25,
        "XLF": -0.20,
        "XLV": 0.15,
    }
    covariance = _identity_covariance(list(loadings))
    recommendation = build_factor_neutral_recommendation(
        loadings, covariance, fit_status="ok", max_hedge_legs=3
    )
    hedge = build_factor_neutral_hedge(
        loadings, covariance, fit_status="ok", max_hedge_legs=3
    )

    assert recommendation.status == "hedge_ok"
    assert sorted(recommendation.hedge_weights) == ["SPY", "XLF", "XLK", "XLV"]
    # The two surfaces must not recommend different instruments for one vector.
    assert sorted(recommendation.hedge_weights) == sorted(hedge.hedge_weights)
    assert math.isclose(float(recommendation.hedge_weights["SPY"]), -0.30, rel_tol=0.0, abs_tol=1e-12)
    assert recommendation.reason is None


def test_hedge_is_unavailable_for_insufficient_history() -> None:
    preview = build_factor_neutral_hedge(
        {"SPY": 0.20, "XLK": 0.40},
        _identity_covariance(["SPY", "XLK"]),
        fit_status="insufficient_history",
    )

    assert preview.status == "hedge_unavailable"
    assert preview.hedge_legs == ()


def test_factor_neutral_hedge_fails_closed_on_incomplete_covariance_surface() -> None:
    with pytest.raises(ValueError, match="Incomplete covariance coverage"):
        build_factor_neutral_hedge(
            {"SPY": 0.20, "XLK": 0.40, "MTUM": -0.10},
            {
                ("SPY", "SPY"): 1.0,
                ("XLK", "XLK"): 1.0,
                ("MTUM", "MTUM"): 1.0,
                ("SPY", "XLK"): 0.2,
                ("XLK", "SPY"): 0.2,
            },
            fit_status="ok",
        )
