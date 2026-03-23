import math

from backend.cpar.returns_panel import build_weekly_return_series, select_weekly_prices


def test_select_weekly_prices_uses_latest_price_within_anchor_week() -> None:
    anchors = ("2025-01-03", "2025-01-10", "2025-01-17")
    rows = [
        {"date": "2025-01-03", "adj_close": 100.0, "close": 99.0},
        {"date": "2025-01-06", "adj_close": 101.0, "close": 101.0},
        {"date": "2025-01-08", "adj_close": None, "close": 102.0},
        {"date": "2025-01-17", "adj_close": 103.0, "close": 103.0},
    ]

    selections = select_weekly_prices(rows, price_anchors=anchors)

    assert selections[1].price_date == "2025-01-08"
    assert selections[1].price_field == "close"
    assert selections[1].price_value == 102.0


def test_build_weekly_return_series_tracks_missing_weeks_and_mixed_price_fields() -> None:
    anchors = ("2025-01-03", "2025-01-10", "2025-01-17", "2025-01-24", "2025-01-31")
    rows = [
        {"date": "2025-01-03", "adj_close": 100.0, "close": 100.0},
        {"date": "2025-01-08", "adj_close": None, "close": 102.0},
        {"date": "2025-01-17", "adj_close": 103.0, "close": 103.0},
        {"date": "2025-01-28", "adj_close": 104.0, "close": 104.0},
    ]

    series = build_weekly_return_series(rows, price_anchors=anchors)

    assert series.package_date == "2025-01-31"
    assert series.observed_weeks == 2
    assert series.longest_gap_weeks == 2
    assert series.price_field_used == "mixed_adj_close_close"
    assert math.isclose(series.returns[0], 0.02, rel_tol=0.0, abs_tol=1e-12)
    assert math.isclose(series.returns[1], (103.0 / 102.0) - 1.0, rel_tol=0.0, abs_tol=1e-12)
    assert math.isnan(float(series.returns[2]))
    assert math.isnan(float(series.returns[3]))
