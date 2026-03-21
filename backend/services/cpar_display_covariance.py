"""Helpers for cPAR explanatory display covariance surfaces."""

from __future__ import annotations

from collections import defaultdict
from typing import Any

import numpy as np

from backend.cpar.factor_registry import MARKET_FACTOR_ID, ordered_factor_ids
from backend.cpar.regression import normalize_weights
from backend.data import cpar_outputs


def _package_not_ready(message: str) -> cpar_outputs.CparPackageNotReady:
    return cpar_outputs.CparPackageNotReady(message)


def _aligned_proxy_return_panel(
    proxy_return_rows: list[dict[str, Any]],
) -> tuple[tuple[str, ...], np.ndarray, np.ndarray, dict[str, np.ndarray], str | None, str | None]:
    expected_factor_ids = ordered_factor_ids(include_market=True)
    rows_by_factor: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    package_date = None
    updated_at = None

    for row in proxy_return_rows:
        factor_id = str(row.get("factor_id") or "").strip().upper()
        week_end = str(row.get("week_end") or "").strip()
        if not factor_id or not week_end:
            continue
        rows_by_factor[factor_id][week_end] = dict(row)
        package_date = str(row.get("package_date") or "") or package_date
        updated_at = max(updated_at or "", str(row.get("updated_at") or "")) or updated_at

    market_rows = rows_by_factor.get(MARKET_FACTOR_ID, {})
    ordered_week_ends = tuple(sorted(market_rows.keys()))
    if not ordered_week_ends:
        raise _package_not_ready("Active cPAR package is missing SPY proxy return rows for display covariance.")

    market_returns = np.asarray(
        [float(market_rows[week_end].get("return_value") or 0.0) for week_end in ordered_week_ends],
        dtype=float,
    )
    weights = normalize_weights(
        np.asarray(
            [float(market_rows[week_end].get("weight_value") or 0.0) for week_end in ordered_week_ends],
            dtype=float,
        )
    )

    expected_week_end_set = set(ordered_week_ends)
    raw_series_by_factor: dict[str, np.ndarray] = {MARKET_FACTOR_ID: market_returns}
    for factor_id in expected_factor_ids:
        if factor_id == MARKET_FACTOR_ID:
            continue
        factor_rows = rows_by_factor.get(factor_id, {})
        if set(factor_rows.keys()) != expected_week_end_set:
            raise _package_not_ready(
                "Active cPAR package is missing a complete proxy-return panel for explanatory display covariance "
                f"for factor_id={factor_id}."
            )
        raw_series_by_factor[factor_id] = np.asarray(
            [float(factor_rows[week_end].get("return_value") or 0.0) for week_end in ordered_week_ends],
            dtype=float,
        )

    return expected_factor_ids, market_returns, weights, raw_series_by_factor, package_date, updated_at


def _transform_lookup(proxy_transform_rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    lookup: dict[str, dict[str, Any]] = {}
    for row in proxy_transform_rows:
        factor_id = str(row.get("factor_id") or "").strip().upper()
        if not factor_id:
            continue
        lookup[factor_id] = dict(row)
    return lookup


def build_display_covariance_rows(
    *,
    proxy_return_rows: list[dict[str, Any]],
    proxy_transform_rows: list[dict[str, Any]],
    package_run_id: str,
) -> list[dict[str, Any]]:
    factor_ids, market_returns, weights, raw_series_by_factor, package_date, updated_at = _aligned_proxy_return_panel(
        proxy_return_rows
    )
    transform_by_factor = _transform_lookup(proxy_transform_rows)

    display_series_by_factor: dict[str, np.ndarray] = {MARKET_FACTOR_ID: market_returns}
    for factor_id in factor_ids:
        if factor_id == MARKET_FACTOR_ID:
            continue
        transform = transform_by_factor.get(factor_id)
        if transform is None:
            raise _package_not_ready(
                "Active cPAR package is missing a market-orthogonalization transform for explanatory display covariance "
                f"for factor_id={factor_id}."
            )
        alpha = float(transform.get("market_alpha") or 0.0)
        beta = float(transform.get("market_beta") or 0.0)
        display_series_by_factor[factor_id] = raw_series_by_factor[factor_id] - alpha - beta * market_returns

    matrix = np.column_stack([display_series_by_factor[factor_id] for factor_id in factor_ids])
    means = np.sum(matrix * weights[:, None], axis=0)
    centered = matrix - means
    covariance = (centered * weights[:, None]).T @ centered
    diag = np.clip(np.diag(covariance), a_min=0.0, a_max=None)
    vol = np.sqrt(diag)

    rows: list[dict[str, Any]] = []
    for row_idx, left in enumerate(factor_ids):
        for col_idx, right in enumerate(factor_ids):
            denom = float(vol[row_idx] * vol[col_idx])
            corr = 0.0 if denom <= 0.0 else float(covariance[row_idx, col_idx] / denom)
            rows.append(
                {
                    "package_run_id": str(package_run_id),
                    "package_date": package_date,
                    "updated_at": updated_at,
                    "factor_id": left,
                    "factor_id_2": right,
                    "covariance": float(covariance[row_idx, col_idx]),
                    "correlation": corr,
                }
            )
    return rows


def load_package_display_covariance_rows(
    *,
    package_run_id: str,
    data_db=None,
) -> list[dict[str, Any]]:
    proxy_return_rows = cpar_outputs.load_package_proxy_return_rows(
        str(package_run_id),
        data_db=data_db,
    )
    if not proxy_return_rows:
        raise _package_not_ready("Active cPAR package is missing proxy return rows for explanatory display covariance.")
    proxy_transform_rows = cpar_outputs.load_package_proxy_transform_rows(
        str(package_run_id),
        data_db=data_db,
    )
    return build_display_covariance_rows(
        proxy_return_rows=proxy_return_rows,
        proxy_transform_rows=proxy_transform_rows,
        package_run_id=str(package_run_id),
    )
