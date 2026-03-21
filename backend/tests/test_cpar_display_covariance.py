from __future__ import annotations

import pytest

from backend.data import cpar_outputs
from backend.services import cpar_display_covariance


def _proxy_return_rows() -> list[dict[str, object]]:
    return [
        {
            "package_run_id": "run_curr",
            "package_date": "2026-03-14",
            "week_end": "2026-02-28",
            "factor_id": "SPY",
            "factor_group": "market",
            "proxy_ric": "SPY.P",
            "proxy_ticker": "SPY",
            "return_value": -1.0,
            "weight_value": 1.0,
            "updated_at": "2026-03-15T00:03:00Z",
        },
        {
            "package_run_id": "run_curr",
            "package_date": "2026-03-14",
            "week_end": "2026-03-07",
            "factor_id": "SPY",
            "factor_group": "market",
            "proxy_ric": "SPY.P",
            "proxy_ticker": "SPY",
            "return_value": 0.0,
            "weight_value": 1.0,
            "updated_at": "2026-03-15T00:03:00Z",
        },
        {
            "package_run_id": "run_curr",
            "package_date": "2026-03-14",
            "week_end": "2026-03-14",
            "factor_id": "SPY",
            "factor_group": "market",
            "proxy_ric": "SPY.P",
            "proxy_ticker": "SPY",
            "return_value": 1.0,
            "weight_value": 1.0,
            "updated_at": "2026-03-15T00:03:00Z",
        },
        {
            "package_run_id": "run_curr",
            "package_date": "2026-03-14",
            "week_end": "2026-02-28",
            "factor_id": "XLF",
            "factor_group": "sector",
            "proxy_ric": "XLF.P",
            "proxy_ticker": "XLF",
            "return_value": -1.0,
            "weight_value": 1.0,
            "updated_at": "2026-03-15T00:03:00Z",
        },
        {
            "package_run_id": "run_curr",
            "package_date": "2026-03-14",
            "week_end": "2026-03-07",
            "factor_id": "XLF",
            "factor_group": "sector",
            "proxy_ric": "XLF.P",
            "proxy_ticker": "XLF",
            "return_value": -2.0,
            "weight_value": 1.0,
            "updated_at": "2026-03-15T00:03:00Z",
        },
        {
            "package_run_id": "run_curr",
            "package_date": "2026-03-14",
            "week_end": "2026-03-14",
            "factor_id": "XLF",
            "factor_group": "sector",
            "proxy_ric": "XLF.P",
            "proxy_ticker": "XLF",
            "return_value": 3.0,
            "weight_value": 1.0,
            "updated_at": "2026-03-15T00:03:00Z",
        },
        {
            "package_run_id": "run_curr",
            "package_date": "2026-03-14",
            "week_end": "2026-02-28",
            "factor_id": "XLK",
            "factor_group": "sector",
            "proxy_ric": "XLK.P",
            "proxy_ticker": "XLK",
            "return_value": -3.0,
            "weight_value": 1.0,
            "updated_at": "2026-03-15T00:03:00Z",
        },
        {
            "package_run_id": "run_curr",
            "package_date": "2026-03-14",
            "week_end": "2026-03-07",
            "factor_id": "XLK",
            "factor_group": "sector",
            "proxy_ric": "XLK.P",
            "proxy_ticker": "XLK",
            "return_value": 2.0,
            "weight_value": 1.0,
            "updated_at": "2026-03-15T00:03:00Z",
        },
        {
            "package_run_id": "run_curr",
            "package_date": "2026-03-14",
            "week_end": "2026-03-14",
            "factor_id": "XLK",
            "factor_group": "sector",
            "proxy_ric": "XLK.P",
            "proxy_ticker": "XLK",
            "return_value": 1.0,
            "weight_value": 1.0,
            "updated_at": "2026-03-15T00:03:00Z",
        },
    ]


def _proxy_transform_rows() -> list[dict[str, object]]:
    return [
        {
            "package_run_id": "run_curr",
            "package_date": "2026-03-14",
            "factor_id": "XLF",
            "factor_group": "sector",
            "proxy_ric": "XLF.P",
            "proxy_ticker": "XLF",
            "market_alpha": 0.0,
            "market_beta": 2.0,
            "updated_at": "2026-03-15T00:03:00Z",
        },
        {
            "package_run_id": "run_curr",
            "package_date": "2026-03-14",
            "factor_id": "XLK",
            "factor_group": "sector",
            "proxy_ric": "XLK.P",
            "proxy_ticker": "XLK",
            "market_alpha": 0.0,
            "market_beta": 2.0,
            "updated_at": "2026-03-15T00:03:00Z",
        },
    ]


def test_build_display_covariance_rows_uses_market_plus_residual_basis(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cpar_display_covariance, "ordered_factor_ids", lambda *, include_market=True: ("SPY", "XLF", "XLK"))
    factor_ids = ("SPY", "XLF", "XLK")
    expected_pairs = {(left, right) for left in factor_ids for right in factor_ids}
    rows = cpar_display_covariance.build_display_covariance_rows(
        proxy_return_rows=_proxy_return_rows(),
        proxy_transform_rows=_proxy_transform_rows(),
        package_run_id="run_curr",
    )
    lookup = {
        (str(row["factor_id"]), str(row["factor_id_2"])): row
        for row in rows
        if (str(row["factor_id"]), str(row["factor_id_2"])) in expected_pairs
    }

    assert lookup[("SPY", "SPY")]["correlation"] == pytest.approx(1.0)
    assert lookup[("SPY", "XLF")]["correlation"] == pytest.approx(0.0, abs=1e-9)
    assert lookup[("SPY", "XLK")]["correlation"] == pytest.approx(0.0, abs=1e-9)
    assert lookup[("XLF", "XLK")]["correlation"] == pytest.approx(-1.0, abs=1e-9)


def test_load_package_display_covariance_rows_fails_closed_when_transform_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cpar_display_covariance, "ordered_factor_ids", lambda *, include_market=True: ("SPY", "XLF", "XLK"))
    monkeypatch.setattr(
        cpar_display_covariance.cpar_outputs,
        "load_package_proxy_return_rows",
        lambda *args, **kwargs: _proxy_return_rows(),
    )
    monkeypatch.setattr(
        cpar_display_covariance.cpar_outputs,
        "load_package_proxy_transform_rows",
        lambda *args, **kwargs: _proxy_transform_rows()[:-1],
    )

    with pytest.raises(cpar_outputs.CparPackageNotReady, match="market-orthogonalization transform"):
        cpar_display_covariance.load_package_display_covariance_rows(package_run_id="run_curr")
