"""Supplemental cPAR factor-history payload service."""

from __future__ import annotations

import math
from pathlib import Path

from backend import config
from backend.cpar.factor_registry import factor_spec_by_id
from backend.data import cpar_source_reads
from backend.data.history_queries import load_price_history_rows
from backend.services import cpar_meta_service


class CparFactorNotFound(LookupError):
    """Raised when a requested cPAR factor is not part of the cPAR1 registry."""


def load_cpar_factor_history_payload(
    *,
    factor_id: str,
    years: int,
    data_db=None,
) -> dict[str, object]:
    clean_factor_id = str(factor_id or "").strip().upper()
    if not clean_factor_id:
        raise CparFactorNotFound("factor_id is required.")
    try:
        spec = factor_spec_by_id(clean_factor_id)
    except KeyError as exc:
        raise CparFactorNotFound(f"Unknown cPAR factor_id {clean_factor_id!r}.") from exc

    try:
        cpar_meta_service.require_active_package(data_db=data_db)
        proxy_rows = cpar_source_reads.resolve_factor_proxy_rows(
            [spec.ticker],
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
    except (cpar_meta_service.CparReadNotReady, cpar_meta_service.CparReadUnavailable):
        raise
    except Exception as exc:
        raise cpar_meta_service.CparReadUnavailable(str(exc)) from exc

    if latest is None or not rows:
        raise cpar_meta_service.CparReadNotReady(
            "Historical cPAR factor returns are not available yet."
        )

    points = _build_daily_cumulative_points(rows)

    return {
        "factor_id": spec.factor_id,
        "factor_name": spec.label,
        "years": int(years),
        "points": points,
        "_cached": True,
    }


def _build_daily_cumulative_points(rows: list[tuple[str, float]]) -> list[dict[str, object]]:
    if len(rows) < 2:
        return []
    cumulative = 1.0
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
        cumulative *= (1.0 + current_return)
        points.append(
            {
                "date": str(as_of_date),
                "factor_return": round(current_return, 8),
                "cum_return": round(cumulative - 1.0, 8),
            }
        )
        previous_close = current_close
    return points
