"""Supplemental cPAR factor-history payload service."""

from __future__ import annotations

import math
from pathlib import Path

import numpy as np

from backend import config
from backend.cpar import regression
from backend.cpar.factor_registry import MARKET_FACTOR_ID, factor_spec_by_id
from backend.data import cpar_source_reads
from backend.data.history_queries import load_price_history_rows
from backend.services import cpar_meta_service


class CparFactorNotFound(LookupError):
    """Raised when a requested cPAR factor is not part of the cPAR1 registry."""


def load_cpar_factor_history_payload(
    *,
    factor_id: str,
    years: int,
    mode: str = "market_adjusted",
    data_db=None,
) -> dict[str, object]:
    clean_factor_id = str(factor_id or "").strip().upper()
    clean_mode = str(mode or "").strip().lower()
    if not clean_factor_id:
        raise CparFactorNotFound("factor_id is required.")
    if clean_mode not in {"residual", "market_adjusted"}:
        raise ValueError(f"Unsupported cPAR factor history mode {mode!r}.")
    try:
        spec = factor_spec_by_id(clean_factor_id)
    except KeyError as exc:
        raise CparFactorNotFound(f"Unknown cPAR factor_id {clean_factor_id!r}.") from exc

    try:
        cpar_meta_service.require_active_package(data_db=data_db)
        proxy_rows = cpar_source_reads.resolve_factor_proxy_rows(
            [spec.ticker, factor_spec_by_id(MARKET_FACTOR_ID).ticker],
            data_db=Path(data_db or config.DATA_DB_PATH),
        )
        proxy_row = next(
            (
                row for row in proxy_rows
                if str(row.get("ticker") or "").strip().upper() == spec.ticker
            ),
            proxy_rows[0] if proxy_rows else None,
        )
        proxy_ric = str((proxy_row or {}).get("ric") or "").strip()
        if not proxy_ric:
            raise cpar_meta_service.CparReadNotReady(
                "Historical cPAR factor returns are not available yet."
            )
        latest, rows = load_price_history_rows(
            Path(data_db or config.DATA_DB_PATH),
            ric=proxy_ric,
            years=int(years),
        )
        market_proxy_row = next(
            (
                row for row in proxy_rows
                if str(row.get("ticker") or "").strip().upper() == MARKET_FACTOR_ID
            ),
            None,
        )
        market_proxy_ric = str((market_proxy_row or {}).get("ric") or "").strip()
        market_rows: list[tuple[str, float]] = []
        if spec.factor_id != MARKET_FACTOR_ID:
            if not market_proxy_ric:
                raise cpar_meta_service.CparReadNotReady(
                    "Daily cPAR factor residuals are not available yet."
                )
            _market_latest, market_rows = load_price_history_rows(
                Path(data_db or config.DATA_DB_PATH),
                ric=market_proxy_ric,
                years=int(years),
            )
    except (cpar_meta_service.CparReadNotReady, cpar_meta_service.CparReadUnavailable):
        raise
    except Exception as exc:
        raise cpar_meta_service.CparReadUnavailable(str(exc)) from exc

    if latest is None or not rows:
        raise cpar_meta_service.CparReadNotReady(
            "Historical cPAR factor returns are not available yet."
        )

    points = (
        _build_daily_cumulative_sum_points(rows)
        if spec.factor_id == MARKET_FACTOR_ID
        else _build_daily_non_market_points(rows, market_rows, mode=clean_mode)
    )

    return {
        "factor_id": spec.factor_id,
        "factor_name": spec.label,
        "history_mode": clean_mode,
        "years": int(years),
        "points": points,
        "_cached": True,
    }


def _build_daily_cumulative_sum_points(rows: list[tuple[str, float]]) -> list[dict[str, object]]:
    if len(rows) < 2:
        return []
    cumulative = 0.0
    points: list[dict[str, object]] = []
    previous_close = None
    for as_of_date, raw_close in rows:
        current_close = float(raw_close)
        if not math.isfinite(current_close) or current_close <= 0:
            continue
        if previous_close is None:
            previous_close = current_close
            continue
        current_return = (current_close / previous_close) - 1.0
        cumulative += current_return
        points.append(
            {
                "date": str(as_of_date),
                "factor_return": round(current_return, 8),
                "cum_return": round(cumulative, 8),
            }
        )
        previous_close = current_close
    return points


def _daily_return_series(rows: list[tuple[str, float]]) -> dict[str, float]:
    series: dict[str, float] = {}
    previous_close = None
    for as_of_date, raw_close in rows:
        current_close = float(raw_close)
        if not math.isfinite(current_close) or current_close <= 0:
            continue
        if previous_close is None:
            previous_close = current_close
            continue
        series[str(as_of_date)] = (current_close / previous_close) - 1.0
        previous_close = current_close
    return series


def _build_daily_non_market_points(
    factor_rows: list[tuple[str, float]],
    market_rows: list[tuple[str, float]],
    *,
    mode: str,
) -> list[dict[str, object]]:
    factor_returns = _daily_return_series(factor_rows)
    market_returns = _daily_return_series(market_rows)
    common_dates = sorted(set(factor_returns).intersection(market_returns))
    if len(common_dates) < 2:
        return []
    y = np.asarray([float(factor_returns[as_of_date]) for as_of_date in common_dates], dtype=float)
    x = np.asarray([float(market_returns[as_of_date]) for as_of_date in common_dates], dtype=float)
    result = regression.weighted_ols_with_intercept(y, x, np.ones_like(y, dtype=float))
    cumulative = 0.0
    points: list[dict[str, object]] = []
    for as_of_date, residual_value in zip(common_dates, result.residuals, strict=True):
        residual = float(residual_value)
        point_return = float(result.intercept + residual) if mode == "market_adjusted" else residual
        cumulative += point_return
        points.append(
            {
                "date": str(as_of_date),
                "factor_return": round(point_return, 8),
                "cum_return": round(cumulative, 8),
            }
        )
    return points
