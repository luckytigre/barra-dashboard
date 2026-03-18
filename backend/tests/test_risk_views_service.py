from __future__ import annotations

from backend.analytics.services.risk_views import build_positions_from_snapshot, compute_exposures_modes


def test_build_positions_from_snapshot_downgrades_empty_exposure_rows() -> None:
    positions, total_value = build_positions_from_snapshot(
        universe_by_ticker={
            "LAZ": {
                "ticker": "LAZ",
                "name": "Lazard",
                "price": 48.39,
                "model_status": "core_estimated",
                "eligibility_reason": "",
                "exposures": {},
                "specific_var": 0.01,
                "specific_vol": 0.1,
            }
        },
        shares_map={"LAZ": -200.0},
    )

    assert total_value == -9678.0
    assert len(positions) == 1
    assert positions[0]["model_status"] == "ineligible"
    assert positions[0]["model_status_reason"] == "missing_factor_exposures"
    assert positions[0]["eligibility_reason"] == "missing_factor_exposures"
    assert positions[0]["exposures"] == {}


def test_compute_exposures_modes_emits_canonical_factor_coverage_asof_alias() -> None:
    out = compute_exposures_modes(
        positions=[
            {
                "ticker": "AAPL",
                "weight": 1.0,
                "exposures": {"style_beta_score": 1.25},
            }
        ],
        cov=None,
        factor_details=[
            {
                "factor_id": "style_beta_score",
                "exposure": 1.25,
                "factor_vol": 0.2,
                "sensitivity": 0.25,
                "marginal_var_contrib": 0.0,
                "pct_of_total": 5.0,
            }
        ],
        factor_coverage={
            "style_beta_score": {
                "cross_section_n": 3000,
                "eligible_n": 2800,
                "coverage_pct": 0.9333,
            }
        },
        factor_coverage_asof="2026-03-13",
    )

    raw_row = out["raw"][0]
    assert raw_row["factor_coverage_asof"] == "2026-03-13"
    assert raw_row["coverage_date"] == "2026-03-13"


def test_compute_exposures_modes_preserves_projection_metadata_in_drilldown() -> None:
    out = compute_exposures_modes(
        positions=[
            {
                "ticker": "ASML",
                "weight": 1.0,
                "model_status": "projected_only",
                "exposure_origin": "projected_fundamental",
                "exposures": {"style_beta_score": 0.8},
            }
        ],
        cov=None,
        factor_details=[
            {
                "factor_id": "style_beta_score",
                "exposure": 0.8,
                "factor_vol": 0.2,
                "sensitivity": 0.16,
                "marginal_var_contrib": 0.0,
                "pct_of_total": 2.0,
            }
        ],
    )

    raw_item = out["raw"][0]["drilldown"][0]
    assert raw_item["ticker"] == "ASML"
    assert raw_item["model_status"] == "projected_only"
    assert raw_item["exposure_origin"] == "projected_fundamental"


def test_build_positions_from_snapshot_preserves_exposure_origin() -> None:
    positions, _ = build_positions_from_snapshot(
        universe_by_ticker={
            "SPY": {
                "ticker": "SPY",
                "name": "SPDR S&P 500 ETF Trust",
                "price": 510.0,
                "model_status": "projected_only",
                "model_status_reason": "projected_returns_regression",
                "exposure_origin": "projected_returns",
                "exposures": {"market": 1.0},
            }
        },
        shares_map={"SPY": 10.0},
    )

    assert positions[0]["model_status"] == "projected_only"
    assert positions[0]["exposure_origin"] == "projected_returns"
