from __future__ import annotations

import pandas as pd
import pytest


def test_build_positions_from_universe_loads_holdings_snapshot_once(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"count": 0}

    def _load_once():
        calls["count"] += 1
        return (
            {"AAPL": 10.0, "MSFT": -5.0},
            {
                "AAPL": {"account": "MAIN", "sleeve": "NEON HOLDINGS", "source": "NEON"},
                "MSFT": {"account": "MAIN", "sleeve": "NEON HOLDINGS", "source": "NEON"},
            },
        )

    from backend.analytics.services import risk_views

    monkeypatch.setattr(risk_views, "load_positions_snapshot", _load_once)

    positions, total_value = risk_views.build_positions_from_universe(
        {
            "AAPL": {"price": 100.0, "name": "Apple"},
            "MSFT": {"price": 50.0, "name": "Microsoft"},
        }
    )

    assert calls["count"] == 1
    assert len(positions) == 2
    assert total_value == 750.0


def test_build_positions_from_universe_uses_signed_gross_weights_for_long_short_books(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _load_snapshot():
        return (
            {"VRT": -3.0, "ORCL": 2.0, "WMT": -1.0},
            {
                "VRT": {"account": "MAIN", "sleeve": "NEON HOLDINGS", "source": "NEON"},
                "ORCL": {"account": "MAIN", "sleeve": "NEON HOLDINGS", "source": "NEON"},
                "WMT": {"account": "MAIN", "sleeve": "NEON HOLDINGS", "source": "NEON"},
            },
        )

    from backend.analytics.services import risk_views

    monkeypatch.setattr(risk_views, "load_positions_snapshot", _load_snapshot)

    positions, total_value = risk_views.build_positions_from_universe(
        {
            "VRT": {"price": 100.0, "name": "Vertiv"},
            "ORCL": {"price": 50.0, "name": "Oracle"},
            "WMT": {"price": 25.0, "name": "Walmart"},
        }
    )

    assert total_value == -225.0
    by_ticker = {row["ticker"]: row for row in positions}
    assert by_ticker["VRT"]["market_value"] == -300.0
    assert by_ticker["ORCL"]["market_value"] == 100.0
    assert by_ticker["WMT"]["market_value"] == -25.0
    assert by_ticker["VRT"]["weight"] == pytest.approx(-300.0 / 425.0, abs=1e-6)
    assert by_ticker["ORCL"]["weight"] == pytest.approx(100.0 / 425.0, abs=1e-6)
    assert by_ticker["WMT"]["weight"] == pytest.approx(-25.0 / 425.0, abs=1e-6)


def test_compute_position_total_risk_contributions_sums_to_total_variance_pct() -> None:
    from backend.analytics.services import risk_views

    positions = [
        {
            "ticker": "LONG1",
            "weight": 0.6,
            "exposures": {"Beta": 1.0, "Book-to-Price": 0.5},
        },
        {
            "ticker": "SHORT1",
            "weight": -0.4,
            "exposures": {"Beta": 0.8, "Book-to-Price": -0.2},
        },
    ]
    cov = pd.DataFrame(
        [[0.04, 0.01], [0.01, 0.09]],
        index=["Beta", "Book-to-Price"],
        columns=["Beta", "Book-to-Price"],
    )
    specific = {
        "LONG1": {"specific_var": 0.02},
        "SHORT1": {"specific_var": 0.03},
    }

    contrib = risk_views.compute_position_total_risk_contributions(
        positions,
        cov,
        specific_risk_by_ticker=specific,
    )

    assert set(contrib) == {"LONG1", "SHORT1"}
    assert sum(contrib.values()) == pytest.approx(100.0, abs=0.05)
