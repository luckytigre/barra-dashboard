from __future__ import annotations

import pandas as pd

from backend.services import portfolio_whatif


def test_preview_portfolio_whatif_projects_current_and_hypothetical_without_writes(monkeypatch) -> None:
    universe_loadings = {
        "by_ticker": {
            "AAA": {
                "ticker": "AAA",
                "name": "AAA",
                "price": 10.0,
                "exposures": {"Beta": 1.0, "Value": 0.5},
                "specific_var": 0.01,
                "specific_vol": 0.1,
                "eligible_for_model": True,
                "eligibility_reason": "",
                "trbc_economic_sector_short": "Technology",
                "trbc_economic_sector_short_abbr": "Tech",
                "trbc_industry_group": "Software",
            },
            "BBB": {
                "ticker": "BBB",
                "name": "BBB",
                "price": 20.0,
                "exposures": {"Beta": 0.8, "Value": -0.2},
                "specific_var": 0.02,
                "specific_vol": 0.1414,
                "eligible_for_model": True,
                "eligibility_reason": "",
                "trbc_economic_sector_short": "Financials",
                "trbc_economic_sector_short_abbr": "Fins",
                "trbc_industry_group": "Banks",
            },
        },
        "source_dates": {
            "prices_asof": "2026-03-03",
            "fundamentals_asof": "2026-03-03",
            "classification_asof": "2026-03-03",
            "exposures_asof": "2026-03-03",
        },
    }
    cov = pd.DataFrame(
        [[0.04, 0.01], [0.01, 0.09]],
        index=["Beta", "Value"],
        columns=["Beta", "Value"],
    )
    live_rows = [
        {"account_id": "acct_a", "ticker": "AAA", "ric": "AAA.N", "quantity": 10.0, "source": "ui_edit"},
        {"account_id": "acct_b", "ticker": "BBB", "ric": "BBB.N", "quantity": 5.0, "source": "ui_edit"},
    ]

    monkeypatch.setattr(portfolio_whatif, "_load_universe_loadings", lambda: universe_loadings)
    monkeypatch.setattr(portfolio_whatif, "_load_covariance_frame", lambda: cov)
    monkeypatch.setattr(
        portfolio_whatif,
        "_load_specific_risk_by_ticker",
        lambda: {
            "AAA": {"specific_var": 0.01},
            "BBB": {"specific_var": 0.02},
        },
    )
    monkeypatch.setattr(portfolio_whatif.holdings_service, "load_holdings_positions", lambda account_id=None: live_rows)
    monkeypatch.setattr(
        portfolio_whatif,
        "load_latest_factor_coverage",
        lambda cache_db: ("2026-03-03", {}),
    )
    monkeypatch.setattr(
        portfolio_whatif.holdings_service,
        "run_holdings_import",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("preview must not write holdings")),
    )

    out = portfolio_whatif.preview_portfolio_whatif(
        scenario_rows=[
            {"account_id": "acct_a", "ticker": "AAA", "quantity": 20.0},
            {"account_id": "acct_b", "ticker": "BBB", "quantity": 0.0},
        ]
    )

    assert out["_preview_only"] is True
    assert out["current"]["position_count"] == 2
    assert out["hypothetical"]["position_count"] == 1
    assert out["current"]["total_value"] == 200.0
    assert out["hypothetical"]["total_value"] == 200.0
    assert len(out["holding_deltas"]) == 2
    assert out["holding_deltas"][0]["account_id"] == "acct_a"
    assert "raw" in out["hypothetical"]["exposure_modes"]
    assert "risk_contribution" in out["diff"]["factor_deltas"]


def test_preview_portfolio_whatif_requires_account_id() -> None:
    try:
        portfolio_whatif.preview_portfolio_whatif(
            scenario_rows=[{"ticker": "AAA", "quantity": 10.0}],
        )
    except ValueError as exc:
        assert str(exc) == "Each what-if row requires account_id."
        return
    raise AssertionError("preview_portfolio_whatif should reject missing account_id")
