"""Weekly price selection and return construction for cPAR1."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

import numpy as np
import pandas as pd

from backend.cpar.contracts import WeeklyPriceSelection, WeeklyReturnSeries
from backend.cpar.status_rules import longest_missing_gap
from backend.cpar.weekly_anchors import (
    DEFAULT_HALF_LIFE_WEEKS,
    DEFAULT_LOOKBACK_WEEKS,
    generate_weekly_price_anchors,
    package_return_weights,
    weekly_anchor_for_date,
)


def _to_timestamp(value: str | pd.Timestamp) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tzinfo is not None:
        ts = ts.tz_convert("UTC").tz_localize(None)
    return ts.normalize()


def _normalize_price_rows(price_rows: Sequence[Mapping[str, object]]) -> dict[str, dict[str, object]]:
    normalized: dict[str, dict[str, object]] = {}
    for row in price_rows:
        raw_date = row.get("date")
        if raw_date is None:
            continue
        date_key = str(_to_timestamp(str(raw_date)).date())
        normalized[date_key] = dict(row)
    return normalized


def _pick_price_from_row(row: Mapping[str, object]) -> tuple[str, float] | None:
    adj_close = row.get("adj_close")
    if adj_close is not None and str(adj_close) != "":
        value = float(adj_close)
        if np.isfinite(value):
            return ("adj_close", value)
    close = row.get("close")
    if close is not None and str(close) != "":
        value = float(close)
        if np.isfinite(value):
            return ("close", value)
    return None


def select_weekly_prices(
    price_rows: Sequence[Mapping[str, object]],
    *,
    price_anchors: Sequence[str] | None = None,
    package_date: str | None = None,
    lookback_weeks: int = DEFAULT_LOOKBACK_WEEKS,
) -> tuple[WeeklyPriceSelection, ...]:
    if price_anchors is None:
        if not package_date:
            raise ValueError("package_date is required when price_anchors is not provided")
        anchors = generate_weekly_price_anchors(package_date, lookback_weeks=lookback_weeks)
    else:
        anchors = tuple(str(anchor) for anchor in price_anchors)
    rows_by_date = _normalize_price_rows(price_rows)
    selections: list[WeeklyPriceSelection] = []
    for anchor in anchors:
        anchor_ts = _to_timestamp(anchor)
        week_start = anchor_ts - pd.Timedelta(days=int(anchor_ts.weekday()))
        selected_date: str | None = None
        selected_value: float | None = None
        selected_field: str | None = None
        for candidate in sorted(rows_by_date.keys(), reverse=True):
            candidate_ts = _to_timestamp(candidate)
            if candidate_ts > anchor_ts:
                continue
            if candidate_ts < week_start:
                break
            chosen = _pick_price_from_row(rows_by_date[candidate])
            if chosen is None:
                continue
            selected_field, selected_value = chosen
            selected_date = candidate
            break
        selections.append(
            WeeklyPriceSelection(
                anchor_date=str(anchor_ts.date()),
                week_start_date=str(week_start.date()),
                price_date=selected_date,
                price_value=selected_value,
                price_field=selected_field,
            )
        )
    return tuple(selections)


def summarize_price_field_usage(price_selections: Sequence[WeeklyPriceSelection], observed_mask: np.ndarray) -> str:
    used_fields: set[str] = set()
    for idx, observed in enumerate(observed_mask):
        if not bool(observed):
            continue
        left = price_selections[idx].price_field
        right = price_selections[idx + 1].price_field
        if left:
            used_fields.add(str(left))
        if right:
            used_fields.add(str(right))
    if not used_fields:
        for selection in price_selections:
            if selection.price_field:
                used_fields.add(str(selection.price_field))
    if used_fields == {"adj_close"}:
        return "adj_close"
    if used_fields == {"close"}:
        return "close"
    if used_fields == {"adj_close", "close"}:
        return "mixed_adj_close_close"
    return "none"


def build_weekly_return_series(
    price_rows: Sequence[Mapping[str, object]],
    *,
    price_anchors: Sequence[str] | None = None,
    package_date: str | None = None,
    lookback_weeks: int = DEFAULT_LOOKBACK_WEEKS,
    half_life_weeks: int = DEFAULT_HALF_LIFE_WEEKS,
) -> WeeklyReturnSeries:
    selections = select_weekly_prices(
        price_rows,
        price_anchors=price_anchors,
        package_date=package_date,
        lookback_weeks=lookback_weeks,
    )
    anchors = tuple(selection.anchor_date for selection in selections)
    if len(anchors) < 2:
        raise ValueError("At least two price anchors are required to compute returns")
    returns: list[float] = []
    observed_mask: list[bool] = []
    for left, right in zip(selections[:-1], selections[1:]):
        if left.price_value is None or right.price_value is None or float(left.price_value) == 0.0:
            returns.append(np.nan)
            observed_mask.append(False)
            continue
        returns.append(float(right.price_value / left.price_value) - 1.0)
        observed_mask.append(True)
    observed_mask_arr = np.asarray(observed_mask, dtype=bool)
    return WeeklyReturnSeries(
        package_date=str(weekly_anchor_for_date(package_date or anchors[-1])),
        lookback_weeks=len(returns),
        half_life_weeks=int(half_life_weeks),
        price_anchors=anchors,
        return_anchors=anchors[1:],
        price_selections=tuple(selections),
        returns=np.asarray(returns, dtype=float),
        observed_mask=observed_mask_arr,
        weights=package_return_weights(
            lookback_weeks=len(returns),
            half_life_weeks=half_life_weeks,
        ),
        price_field_used=summarize_price_field_usage(selections, observed_mask_arr),
        observed_weeks=int(np.count_nonzero(observed_mask_arr)),
        longest_gap_weeks=int(longest_missing_gap(observed_mask_arr)),
    )
