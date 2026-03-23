from __future__ import annotations

from pathlib import Path

import pytest

from backend.data import cpar_source_reads
from backend.services import cpar_factor_history_service, cpar_meta_service


def test_load_cpar_factor_history_payload_returns_market_cumulative_points(monkeypatch) -> None:
    monkeypatch.setattr(
        cpar_factor_history_service.cpar_meta_service,
        "require_active_package",
        lambda *, data_db=None: {"package_run_id": "pkg-1"},
    )
    monkeypatch.setattr(
        cpar_factor_history_service.cpar_source_reads,
        "resolve_factor_proxy_rows",
        lambda tickers, *, data_db=None: [{"ric": "SPY.P", "ticker": "SPY"}],
    )
    monkeypatch.setattr(
        cpar_factor_history_service,
        "load_price_history_rows",
        lambda data_db, *, ric, years: (
            "2026-03-14",
            [("2026-03-12", 100.0), ("2026-03-13", 102.0), ("2026-03-14", 101.0)],
        ),
    )

    payload = cpar_factor_history_service.load_cpar_factor_history_payload(factor_id="SPY", years=5)

    assert payload["factor_id"] == "SPY"
    assert payload["factor_name"] == "Market"
    assert payload["history_mode"] == "market_adjusted"
    assert payload["points"] == [
        {"date": "2026-03-13", "factor_return": 0.02, "cum_return": 0.02},
        {"date": "2026-03-14", "factor_return": -0.00980392, "cum_return": 0.01019608},
    ]


def test_load_cpar_factor_history_payload_returns_daily_market_residual_points(monkeypatch) -> None:
    monkeypatch.setattr(
        cpar_factor_history_service.cpar_meta_service,
        "require_active_package",
        lambda *, data_db=None: {"package_run_id": "pkg-1"},
    )
    monkeypatch.setattr(
        cpar_factor_history_service.cpar_source_reads,
        "resolve_factor_proxy_rows",
        lambda tickers, *, data_db=None: [
            {"ric": "XLK.P", "ticker": "XLK"},
            {"ric": "SPY.P", "ticker": "SPY"},
        ],
    )

    def fake_history(_data_db, *, ric, years):
        if ric == "XLK.P":
            return "2026-03-14", [
                ("2026-03-11", 100.0),
                ("2026-03-12", 102.0),
                ("2026-03-13", 101.0),
                ("2026-03-14", 103.0),
            ]
        return "2026-03-14", [
            ("2026-03-11", 100.0),
            ("2026-03-12", 101.0),
            ("2026-03-13", 100.0),
            ("2026-03-14", 101.0),
        ]

    monkeypatch.setattr(cpar_factor_history_service, "load_price_history_rows", fake_history)

    payload = cpar_factor_history_service.load_cpar_factor_history_payload(factor_id="XLK", years=5)

    assert payload["factor_id"] == "XLK"
    assert payload["history_mode"] == "market_adjusted"
    assert len(payload["points"]) == 3
    assert [row["date"] for row in payload["points"]] == ["2026-03-12", "2026-03-13", "2026-03-14"]
    assert payload["points"] == [
        {"date": "2026-03-12", "factor_return": 0.00507365, "cum_return": 0.00507365},
        {"date": "2026-03-13", "factor_return": 0.00497464, "cum_return": 0.01004829},
        {"date": "2026-03-14", "factor_return": 0.00487563, "cum_return": 0.01492392},
    ]


def test_load_cpar_factor_history_payload_supports_zero_mean_residual_mode(monkeypatch) -> None:
    monkeypatch.setattr(
        cpar_factor_history_service.cpar_meta_service,
        "require_active_package",
        lambda *, data_db=None: {"package_run_id": "pkg-1"},
    )
    monkeypatch.setattr(
        cpar_factor_history_service.cpar_source_reads,
        "resolve_factor_proxy_rows",
        lambda tickers, *, data_db=None: [
            {"ric": "XLK.P", "ticker": "XLK"},
            {"ric": "SPY.P", "ticker": "SPY"},
        ],
    )

    def fake_history(_data_db, *, ric, years):
        if ric == "XLK.P":
            return "2026-03-14", [
                ("2026-03-11", 100.0),
                ("2026-03-12", 102.0),
                ("2026-03-13", 101.0),
                ("2026-03-14", 103.0),
            ]
        return "2026-03-14", [
            ("2026-03-11", 100.0),
            ("2026-03-12", 101.0),
            ("2026-03-13", 100.0),
            ("2026-03-14", 101.0),
        ]

    monkeypatch.setattr(cpar_factor_history_service, "load_price_history_rows", fake_history)

    payload = cpar_factor_history_service.load_cpar_factor_history_payload(
        factor_id="XLK",
        years=5,
        mode="residual",
    )

    assert payload["history_mode"] == "residual"
    assert payload["points"] == [
        {"date": "2026-03-12", "factor_return": 0.00009901, "cum_return": 0.00009901},
        {"date": "2026-03-13", "factor_return": -0.0, "cum_return": 0.00009901},
        {"date": "2026-03-14", "factor_return": -0.00009901, "cum_return": -0.0},
    ]


def test_load_cpar_factor_history_payload_rejects_unknown_factor() -> None:
    with pytest.raises(cpar_factor_history_service.CparFactorNotFound, match="Unknown cPAR factor_id"):
        cpar_factor_history_service.load_cpar_factor_history_payload(factor_id="BAD", years=5)


def test_load_cpar_factor_history_payload_maps_missing_history_to_not_ready(monkeypatch) -> None:
    monkeypatch.setattr(
        cpar_factor_history_service.cpar_meta_service,
        "require_active_package",
        lambda *, data_db=None: {"package_run_id": "pkg-1"},
    )
    monkeypatch.setattr(
        cpar_factor_history_service.cpar_source_reads,
        "resolve_factor_proxy_rows",
        lambda tickers, *, data_db=None: [],
    )

    with pytest.raises(cpar_meta_service.CparReadNotReady, match="Historical cPAR factor returns are not available yet"):
        cpar_factor_history_service.load_cpar_factor_history_payload(factor_id="SPY", years=5)


def test_load_cpar_factor_history_payload_maps_authority_failures_to_unavailable(monkeypatch) -> None:
    monkeypatch.setattr(
        cpar_factor_history_service.cpar_meta_service,
        "require_active_package",
        lambda *, data_db=None: {"package_run_id": "pkg-1"},
    )
    monkeypatch.setattr(
        cpar_factor_history_service.cpar_source_reads,
        "resolve_factor_proxy_rows",
        lambda tickers, *, data_db=None: (_ for _ in ()).throw(
            cpar_source_reads.CparSourceReadError("source read failed")
        ),
    )

    with pytest.raises(cpar_meta_service.CparReadUnavailable, match="source read failed"):
        cpar_factor_history_service.load_cpar_factor_history_payload(factor_id="SPY", years=5)
