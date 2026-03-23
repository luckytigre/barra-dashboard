"""Fit-status and warning rules for cPAR1."""

from __future__ import annotations

from typing import Iterable

import numpy as np

from backend.cpar.contracts import FitStatusSummary, WeeklyReturnSeries

FIT_STATUS_OK = "ok"
FIT_STATUS_LIMITED = "limited_history"
FIT_STATUS_INSUFFICIENT = "insufficient_history"

WARNING_CONTINUITY_GAP = "continuity_gap"
WARNING_EX_US = "ex_us_caution"

DEFAULT_MIN_OBSERVATIONS = 39


def longest_missing_gap(observed_mask: Iterable[bool]) -> int:
    max_gap = 0
    running = 0
    for observed in observed_mask:
        if bool(observed):
            max_gap = max(max_gap, running)
            running = 0
        else:
            running += 1
    return max(max_gap, running)


def fit_status_for_counts(
    *,
    observed_weeks: int,
    longest_gap_weeks: int,
    min_observations: int = DEFAULT_MIN_OBSERVATIONS,
) -> str:
    if observed_weeks < int(min_observations) or longest_gap_weeks > 4:
        return FIT_STATUS_INSUFFICIENT
    if observed_weeks < 52 or longest_gap_weeks > 2:
        return FIT_STATUS_LIMITED
    return FIT_STATUS_OK


def warnings_for_inputs(
    *,
    longest_gap_weeks: int,
    hq_country_code: str | None = None,
) -> tuple[str, ...]:
    warnings: list[str] = []
    if int(longest_gap_weeks) > 2:
        warnings.append(WARNING_CONTINUITY_GAP)
    if str(hq_country_code or "").strip().upper() not in {"", "US"}:
        warnings.append(WARNING_EX_US)
    return tuple(warnings)


def summarize_fit_status(
    *,
    observed_weeks: int,
    longest_gap_weeks: int,
    lookback_weeks: int = 52,
    min_observations: int = DEFAULT_MIN_OBSERVATIONS,
    hq_country_code: str | None = None,
) -> FitStatusSummary:
    return FitStatusSummary(
        fit_status=fit_status_for_counts(
            observed_weeks=int(observed_weeks),
            longest_gap_weeks=int(longest_gap_weeks),
            min_observations=int(min_observations),
        ),
        warnings=warnings_for_inputs(
            longest_gap_weeks=int(longest_gap_weeks),
            hq_country_code=hq_country_code,
        ),
        observed_weeks=int(observed_weeks),
        lookback_weeks=int(lookback_weeks),
        longest_gap_weeks=int(longest_gap_weeks),
    )


def summarize_return_series(
    series: WeeklyReturnSeries,
    *,
    min_observations: int = DEFAULT_MIN_OBSERVATIONS,
    hq_country_code: str | None = None,
) -> FitStatusSummary:
    observed_weeks = int(np.count_nonzero(series.observed_mask))
    return summarize_fit_status(
        observed_weeks=observed_weeks,
        longest_gap_weeks=int(series.longest_gap_weeks),
        lookback_weeks=int(series.lookback_weeks),
        min_observations=int(min_observations),
        hq_country_code=hq_country_code,
    )
