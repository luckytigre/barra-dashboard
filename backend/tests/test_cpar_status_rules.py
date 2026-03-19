from backend.cpar.status_rules import (
    FIT_STATUS_INSUFFICIENT,
    FIT_STATUS_LIMITED,
    FIT_STATUS_OK,
    WARNING_CONTINUITY_GAP,
    WARNING_EX_US,
    fit_status_for_counts,
    summarize_fit_status,
)


def test_fit_status_boundaries_match_cpar1_contract() -> None:
    assert fit_status_for_counts(observed_weeks=52, longest_gap_weeks=2) == FIT_STATUS_OK
    assert fit_status_for_counts(observed_weeks=51, longest_gap_weeks=2) == FIT_STATUS_LIMITED
    assert fit_status_for_counts(observed_weeks=39, longest_gap_weeks=4) == FIT_STATUS_LIMITED
    assert fit_status_for_counts(observed_weeks=38, longest_gap_weeks=2) == FIT_STATUS_INSUFFICIENT
    assert fit_status_for_counts(observed_weeks=52, longest_gap_weeks=5) == FIT_STATUS_INSUFFICIENT


def test_warning_flags_include_continuity_and_ex_us_caution() -> None:
    summary = summarize_fit_status(
        observed_weeks=45,
        longest_gap_weeks=3,
        hq_country_code="GB",
    )

    assert summary.fit_status == FIT_STATUS_LIMITED
    assert summary.warnings == (WARNING_CONTINUITY_GAP, WARNING_EX_US)
