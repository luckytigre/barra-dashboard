"""XNYS weekly anchor construction for cPAR1."""

from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd

from backend.trading_calendar import previous_or_same_xnys_session

DEFAULT_LOOKBACK_WEEKS = 52
DEFAULT_HALF_LIFE_WEEKS = 26


def _to_normalized_timestamp(value: str | date | pd.Timestamp) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tzinfo is not None:
        ts = ts.tz_convert("UTC").tz_localize(None)
    return ts.normalize()


def weekly_anchor_for_date(value: str | date | pd.Timestamp) -> str:
    """Return the XNYS week-ending anchor for the containing trading week."""
    target = _to_normalized_timestamp(value)
    weekday = int(target.weekday())
    if weekday <= 4:
        friday = target + pd.Timedelta(days=(4 - weekday))
    else:
        friday = target - pd.Timedelta(days=(weekday - 4))
    return previous_or_same_xnys_session(friday)


def generate_weekly_price_anchors(
    package_date: str | date | pd.Timestamp,
    *,
    lookback_weeks: int = DEFAULT_LOOKBACK_WEEKS,
) -> tuple[str, ...]:
    if lookback_weeks <= 0:
        raise ValueError("lookback_weeks must be positive")
    terminal_anchor = _to_normalized_timestamp(weekly_anchor_for_date(package_date))
    anchors: list[str] = []
    for offset in range(int(lookback_weeks), -1, -1):
        target = terminal_anchor - pd.Timedelta(days=7 * offset)
        anchors.append(weekly_anchor_for_date(target))
    return tuple(anchors)


def generate_weekly_return_anchors(
    package_date: str | date | pd.Timestamp,
    *,
    lookback_weeks: int = DEFAULT_LOOKBACK_WEEKS,
) -> tuple[str, ...]:
    return generate_weekly_price_anchors(package_date, lookback_weeks=lookback_weeks)[1:]


def package_return_weights(
    *,
    lookback_weeks: int = DEFAULT_LOOKBACK_WEEKS,
    half_life_weeks: int = DEFAULT_HALF_LIFE_WEEKS,
) -> np.ndarray:
    if lookback_weeks <= 0:
        raise ValueError("lookback_weeks must be positive")
    if half_life_weeks <= 0:
        raise ValueError("half_life_weeks must be positive")
    ages = np.arange(int(lookback_weeks) - 1, -1, -1, dtype=float)
    return np.exp(-np.log(2.0) * ages / float(half_life_weeks))
