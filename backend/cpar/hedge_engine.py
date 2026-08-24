"""Deterministic hedge construction in raw ETF trade space for cPAR1."""

from __future__ import annotations

from collections.abc import Mapping

import numpy as np

from backend.cpar.contracts import HedgeLeg, HedgePreview, HedgeStabilityDiagnostics
from backend.cpar.factor_registry import MARKET_FACTOR_ID, factor_group_for_id

MARKET_MATERIALITY_THRESHOLD = 0.10
NON_MARKET_THRESHOLD = 0.05
MAX_HEDGE_LEGS = 5
TINY_POSITION_THRESHOLD = 0.05


def _coerce_covariance_lookup(covariance: Mapping[object, object]) -> dict[tuple[str, str], float]:
    lookup: dict[tuple[str, str], float] = {}
    for raw_key, raw_value in covariance.items():
        if isinstance(raw_key, tuple) and len(raw_key) == 2:
            left = str(raw_key[0])
            right = str(raw_key[1])
            value = float(raw_value)
            lookup[(left, right)] = value
            lookup[(right, left)] = value
            continue
        key = str(raw_key)
        if isinstance(raw_value, Mapping):
            for raw_inner_key, raw_inner_value in raw_value.items():
                inner_key = str(raw_inner_key)
                value = float(raw_inner_value)
                lookup[(key, inner_key)] = value
                lookup[(inner_key, key)] = value
    return lookup


def covariance_matrix_for_factors(
    factor_ids: tuple[str, ...],
    covariance: Mapping[object, object],
) -> np.ndarray:
    lookup = _coerce_covariance_lookup(covariance)
    missing_pairs: list[tuple[str, str]] = []
    matrix = np.zeros((len(factor_ids), len(factor_ids)), dtype=float)
    for row_idx, left in enumerate(factor_ids):
        for col_idx, right in enumerate(factor_ids):
            key = (left, right)
            if key not in lookup:
                missing_pairs.append(key)
                continue
            matrix[row_idx, col_idx] = float(lookup[key])
    if missing_pairs:
        rendered = ", ".join(f"{left}/{right}" for left, right in missing_pairs[:8])
        if len(missing_pairs) > 8:
            rendered += ", ..."
        raise ValueError(
            "Incomplete covariance coverage for hedge/risk calculation. "
            f"Missing factor pairs: {rendered}"
        )
    return matrix



def _variance_proxy(loadings: Mapping[str, float], covariance: Mapping[object, object]) -> float:
    factor_ids = tuple(sorted({str(factor_id) for factor_id in loadings.keys()}))
    if not factor_ids:
        return 0.0
    beta = np.asarray([float(loadings.get(factor_id, 0.0)) for factor_id in factor_ids], dtype=float)
    cov = covariance_matrix_for_factors(factor_ids, covariance)
    return float(beta.T @ cov @ beta)


def _sorted_loadings(loadings: Mapping[str, float]) -> dict[str, float]:
    items = sorted(loadings.items(), key=lambda item: (str(item[0]) != MARKET_FACTOR_ID, -abs(float(item[1])), str(item[0])))
    return {str(factor_id): float(value) for factor_id, value in items}


def _stability_diagnostics(
    current_weights: Mapping[str, float],
    previous_weights: Mapping[str, float] | None,
) -> HedgeStabilityDiagnostics:
    if previous_weights is None:
        return HedgeStabilityDiagnostics(
            leg_overlap_ratio=None,
            gross_hedge_notional_change=None,
            net_hedge_notional_change=None,
        )
    current_non_zero = {factor_id for factor_id, weight in current_weights.items() if abs(float(weight)) > 0.0}
    previous_non_zero = {factor_id for factor_id, weight in previous_weights.items() if abs(float(weight)) > 0.0}
    union = current_non_zero | previous_non_zero
    overlap_ratio = 1.0 if not union else float(len(current_non_zero & previous_non_zero) / len(union))
    current_gross = float(sum(abs(float(weight)) for weight in current_weights.values()))
    previous_gross = float(sum(abs(float(weight)) for weight in previous_weights.values()))
    current_net = float(sum(float(weight) for weight in current_weights.values()))
    previous_net = float(sum(float(weight) for weight in previous_weights.values()))
    return HedgeStabilityDiagnostics(
        leg_overlap_ratio=overlap_ratio,
        gross_hedge_notional_change=abs(current_gross - previous_gross),
        net_hedge_notional_change=abs(current_net - previous_net),
    )


def _build_preview(
    *,
    mode: str,
    status: str,
    reason: str | None,
    underlying_loadings: Mapping[str, float],
    hedge_weights: Mapping[str, float],
    covariance: Mapping[object, object],
    previous_hedge_weights: Mapping[str, float] | None,
    non_market_reduction_ratio: float | None,
) -> HedgePreview:
    sorted_hedge_weights = _sorted_loadings({factor_id: weight for factor_id, weight in hedge_weights.items() if abs(float(weight)) > 0.0})
    hedge_legs = tuple(
        HedgeLeg(
            factor_id=factor_id,
            factor_group=factor_group_for_id(factor_id),
            weight=float(weight),
        )
        for factor_id, weight in sorted_hedge_weights.items()
    )
    post_hedge = {
        factor_id: float(underlying_loadings.get(factor_id, 0.0)) + float(sorted_hedge_weights.get(factor_id, 0.0))
        for factor_id in sorted({*underlying_loadings.keys(), *sorted_hedge_weights.keys()})
    }
    post_hedge = _sorted_loadings(post_hedge)
    return HedgePreview(
        mode=str(mode),
        status=str(status),
        reason=(str(reason) if reason is not None else None),
        hedge_legs=hedge_legs,
        hedge_weights=sorted_hedge_weights,
        post_hedge_loadings=post_hedge,
        pre_hedge_variance_proxy=_variance_proxy(underlying_loadings, covariance),
        post_hedge_variance_proxy=_variance_proxy(post_hedge, covariance),
        gross_hedge_notional=float(sum(abs(float(weight)) for weight in sorted_hedge_weights.values())),
        net_hedge_notional=float(sum(float(weight) for weight in sorted_hedge_weights.values())),
        non_market_reduction_ratio=non_market_reduction_ratio,
        stability=_stability_diagnostics(sorted_hedge_weights, previous_hedge_weights),
    )


def build_market_neutral_hedge(
    thresholded_loadings: Mapping[str, float],
    covariance: Mapping[object, object],
    *,
    fit_status: str,
    hedge_use_status: str | None = None,
    previous_hedge_weights: Mapping[str, float] | None = None,
    market_total_beta: float | None = None,
) -> HedgePreview:
    # This package trades SPY alone, so it must size on the instrument's TOTAL
    # market beta. The trade-space market coefficient carried in
    # `thresholded_loadings` is net of the market exposure delivered by the
    # factor legs -- correct inside factor_neutral, which trades those legs, but
    # a systematic under-hedge here, where they are never traded.
    if market_total_beta is not None:
        thresholded_loadings = {**dict(thresholded_loadings), MARKET_FACTOR_ID: float(market_total_beta)}
    if str(hedge_use_status or "").strip() in {"missing_price", "insufficient_history"}:
        return _build_preview(
            mode="market_neutral",
            status="hedge_unavailable",
            reason=f"hedge_use_status_{str(hedge_use_status)}",
            underlying_loadings=_sorted_loadings(thresholded_loadings),
            hedge_weights={},
            covariance=covariance,
            previous_hedge_weights=previous_hedge_weights,
            non_market_reduction_ratio=None,
        )
    if str(fit_status) == "insufficient_history":
        return _build_preview(
            mode="market_neutral",
            status="hedge_unavailable",
            reason="fit_status_insufficient_history",
            underlying_loadings=_sorted_loadings(thresholded_loadings),
            hedge_weights={},
            covariance=covariance,
            previous_hedge_weights=previous_hedge_weights,
            non_market_reduction_ratio=None,
        )
    market_beta = float(thresholded_loadings.get(MARKET_FACTOR_ID, 0.0))
    if abs(market_beta) < MARKET_MATERIALITY_THRESHOLD:
        return _build_preview(
            mode="market_neutral",
            status="hedge_ok",
            reason="below_market_materiality_threshold",
            underlying_loadings=_sorted_loadings(thresholded_loadings),
            hedge_weights={},
            covariance=covariance,
            previous_hedge_weights=previous_hedge_weights,
            non_market_reduction_ratio=1.0,
        )
    return _build_preview(
        mode="market_neutral",
        status="hedge_ok",
        reason=None,
        underlying_loadings=_sorted_loadings(thresholded_loadings),
        hedge_weights={MARKET_FACTOR_ID: -market_beta},
        covariance=covariance,
        previous_hedge_weights=previous_hedge_weights,
        non_market_reduction_ratio=1.0,
    )


def _candidate_factor_ids(thresholded_loadings: Mapping[str, float]) -> list[str]:
    candidates = [
        factor_id
        for factor_id, beta in thresholded_loadings.items()
        if factor_id != MARKET_FACTOR_ID and abs(float(beta)) >= NON_MARKET_THRESHOLD
    ]
    if abs(float(thresholded_loadings.get(MARKET_FACTOR_ID, 0.0))) >= MARKET_MATERIALITY_THRESHOLD:
        candidates.append(MARKET_FACTOR_ID)
    return sorted(set(candidates), key=lambda factor_id: (factor_id != MARKET_FACTOR_ID, -abs(float(thresholded_loadings.get(factor_id, 0.0))), factor_id))


def _apply_leg_cap_with_limit(
    candidate_ids: list[str],
    thresholded_loadings: Mapping[str, float],
    *,
    max_hedge_legs: int,
) -> list[str]:
    """Keep the `max_hedge_legs` largest non-market legs, plus the market leg.

    The cap bounds the number of *factor* legs; the market leg does not compete
    for a slot, so a full package is `max_hedge_legs + 1` instruments. Legs are
    ranked by absolute trade-space loading and taken as-is -- weights are exact
    negation, so there is no solve and dropping a leg simply leaves that
    exposure unhedged.
    """
    keep: list[str] = []
    non_market = [factor_id for factor_id in candidate_ids if factor_id != MARKET_FACTOR_ID]
    non_market.sort(key=lambda factor_id: (-abs(float(thresholded_loadings.get(factor_id, 0.0))), factor_id))
    keep.extend(non_market[: max(0, max_hedge_legs)])
    if MARKET_FACTOR_ID in candidate_ids:
        keep.append(MARKET_FACTOR_ID)
    return keep


def build_factor_neutral_hedge(
    thresholded_loadings: Mapping[str, float],
    covariance: Mapping[object, object],
    *,
    fit_status: str,
    hedge_use_status: str | None = None,
    previous_hedge_weights: Mapping[str, float] | None = None,
    max_hedge_legs: int = MAX_HEDGE_LEGS,
) -> HedgePreview:
    underlying = _sorted_loadings(thresholded_loadings)
    if str(hedge_use_status or "").strip() in {"missing_price", "insufficient_history"}:
        return _build_preview(
            mode="factor_neutral",
            status="hedge_unavailable",
            reason=f"hedge_use_status_{str(hedge_use_status)}",
            underlying_loadings=underlying,
            hedge_weights={},
            covariance=covariance,
            previous_hedge_weights=previous_hedge_weights,
            non_market_reduction_ratio=None,
        )
    if str(fit_status) == "insufficient_history":
        return _build_preview(
            mode="factor_neutral",
            status="hedge_unavailable",
            reason="fit_status_insufficient_history",
            underlying_loadings=underlying,
            hedge_weights={},
            covariance=covariance,
            previous_hedge_weights=previous_hedge_weights,
            non_market_reduction_ratio=None,
        )
    candidates = _candidate_factor_ids(underlying)
    if not candidates:
        return _build_preview(
            mode="factor_neutral",
            status="hedge_ok",
            reason="no_material_factor_exposures",
            underlying_loadings=underlying,
            hedge_weights={},
            covariance=covariance,
            previous_hedge_weights=previous_hedge_weights,
            non_market_reduction_ratio=1.0,
        )
    capped = _apply_leg_cap_with_limit(candidates, underlying, max_hedge_legs=max_hedge_legs)
    hedge_weights = {factor_id: -float(underlying.get(factor_id, 0.0)) for factor_id in capped}
    hedge_weights = {
        factor_id: weight
        for factor_id, weight in hedge_weights.items()
        if abs(float(weight)) >= TINY_POSITION_THRESHOLD
    }
    pre_non_market = float(
        sum(abs(float(beta)) for factor_id, beta in underlying.items() if factor_id != MARKET_FACTOR_ID)
    )
    post_non_market = float(
        sum(
            abs(float(underlying.get(factor_id, 0.0)) + float(hedge_weights.get(factor_id, 0.0)))
            for factor_id in underlying.keys()
            if factor_id != MARKET_FACTOR_ID
        )
    )
    reduction_ratio = 1.0 if pre_non_market <= 0.0 else max(0.0, 1.0 - (post_non_market / pre_non_market))
    status = "hedge_ok" if reduction_ratio >= 0.50 else "hedge_degraded"
    reason = None if status == "hedge_ok" else "residual_factor_exposures_remain_after_pruning"
    return _build_preview(
        mode="factor_neutral",
        status=status,
        reason=reason,
        underlying_loadings=underlying,
        hedge_weights=hedge_weights,
        covariance=covariance,
        previous_hedge_weights=previous_hedge_weights,
        non_market_reduction_ratio=reduction_ratio,
    )


def build_factor_neutral_recommendation(
    thresholded_loadings: Mapping[str, float],
    covariance: Mapping[object, object],
    *,
    fit_status: str,
    hedge_use_status: str | None = None,
    previous_hedge_weights: Mapping[str, float] | None = None,
    max_hedge_legs: int = 10,
) -> HedgePreview:
    underlying = _sorted_loadings(thresholded_loadings)
    if str(hedge_use_status or "").strip() in {"missing_price", "insufficient_history"}:
        return _build_preview(
            mode="factor_neutral_recommendation",
            status="hedge_unavailable",
            reason=f"hedge_use_status_{str(hedge_use_status)}",
            underlying_loadings=underlying,
            hedge_weights={},
            covariance=covariance,
            previous_hedge_weights=previous_hedge_weights,
            non_market_reduction_ratio=None,
        )
    if str(fit_status) == "insufficient_history":
        return _build_preview(
            mode="factor_neutral_recommendation",
            status="hedge_unavailable",
            reason="fit_status_insufficient_history",
            underlying_loadings=underlying,
            hedge_weights={},
            covariance=covariance,
            previous_hedge_weights=previous_hedge_weights,
            non_market_reduction_ratio=None,
        )
    candidates = _candidate_factor_ids(underlying)
    if not candidates:
        return _build_preview(
            mode="factor_neutral_recommendation",
            status="hedge_ok",
            reason="no_material_factor_exposures",
            underlying_loadings=underlying,
            hedge_weights={},
            covariance=covariance,
            previous_hedge_weights=previous_hedge_weights,
            non_market_reduction_ratio=1.0,
        )
    # Same selection rule as build_factor_neutral_hedge: the cap bounds factor
    # legs and the market leg rides alongside, so the two surfaces cannot
    # recommend different instrument sets for the same loading vector.
    selected = _apply_leg_cap_with_limit(candidates, underlying, max_hedge_legs=max_hedge_legs)
    hedge_weights = {factor_id: -float(underlying.get(factor_id, 0.0)) for factor_id in selected}
    hedge_weights = {
        factor_id: weight
        for factor_id, weight in hedge_weights.items()
        if abs(float(weight)) >= TINY_POSITION_THRESHOLD
    }
    pre_magnitude = float(sum(abs(float(beta)) for beta in underlying.values()))
    post_magnitude = float(
        sum(
            abs(float(underlying.get(factor_id, 0.0)) + float(hedge_weights.get(factor_id, 0.0)))
            for factor_id in underlying.keys()
        )
    )
    reduction_ratio = 1.0 if pre_magnitude <= 0.0 else max(0.0, 1.0 - (post_magnitude / pre_magnitude))
    status = "hedge_ok" if reduction_ratio >= 0.50 else "hedge_degraded"
    reason = None if status == "hedge_ok" else "residual_trade_space_loadings_remain_after_leg_cap"
    return _build_preview(
        mode="factor_neutral_recommendation",
        status=status,
        reason=reason,
        underlying_loadings=underlying,
        hedge_weights=hedge_weights,
        covariance=covariance,
        previous_hedge_weights=previous_hedge_weights,
        non_market_reduction_ratio=reduction_ratio,
    )


def build_hedge_preview(
    *,
    mode: str,
    thresholded_loadings: Mapping[str, float],
    covariance: Mapping[object, object],
    fit_status: str,
    hedge_use_status: str | None = None,
    previous_hedge_weights: Mapping[str, float] | None = None,
    max_hedge_legs: int = MAX_HEDGE_LEGS,
    market_total_beta: float | None = None,
) -> HedgePreview:
    clean_mode = str(mode or "").strip().lower()
    if clean_mode == "market_neutral":
        return build_market_neutral_hedge(
            thresholded_loadings,
            covariance,
            fit_status=fit_status,
            hedge_use_status=hedge_use_status,
            previous_hedge_weights=previous_hedge_weights,
            market_total_beta=market_total_beta,
        )
    if clean_mode == "factor_neutral":
        return build_factor_neutral_hedge(
            thresholded_loadings,
            covariance,
            fit_status=fit_status,
            hedge_use_status=hedge_use_status,
            previous_hedge_weights=previous_hedge_weights,
            max_hedge_legs=max_hedge_legs,
        )
    raise ValueError(f"Unsupported hedge mode: {mode}")
