import numpy as np

from backend.cpar.weekly_anchors import (
    generate_weekly_price_anchors,
    generate_weekly_return_anchors,
    package_return_weights,
    weekly_anchor_for_date,
)


def test_weekly_anchor_for_date_uses_previous_session_for_holiday_friday() -> None:
    assert weekly_anchor_for_date("2025-07-02") == "2025-07-03"
    assert weekly_anchor_for_date("2025-07-04") == "2025-07-03"
    assert weekly_anchor_for_date("2025-07-06") == "2025-07-03"


def test_generate_weekly_price_anchors_is_oldest_first_and_holiday_aware() -> None:
    anchors = generate_weekly_price_anchors("2025-07-04", lookback_weeks=3)

    assert anchors == ("2025-06-13", "2025-06-20", "2025-06-27", "2025-07-03")
    assert generate_weekly_return_anchors("2025-07-04", lookback_weeks=3) == anchors[1:]


def test_package_return_weights_are_newest_heaviest() -> None:
    weights = package_return_weights(lookback_weeks=4, half_life_weeks=2)

    assert weights.shape == (4,)
    assert np.all(weights > 0.0)
    assert weights[-1] > weights[0]
