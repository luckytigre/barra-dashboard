"""Read-only cPAR ticker detail payload service."""

from __future__ import annotations

from typing import Any

from backend.cpar.factor_registry import build_cpar1_factor_registry
from backend.data import cpar_outputs, cpar_queries, cpar_source_reads
from backend.services import cpar_meta_service


def _factor_rows(loadings: dict[str, Any]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for spec in build_cpar1_factor_registry():
        if spec.factor_id not in loadings:
            continue
        rows.append(
            {
                "factor_id": spec.factor_id,
                "label": spec.label,
                "group": spec.group,
                "display_order": int(spec.display_order),
                "beta": float(loadings[spec.factor_id]),
            }
        )
    return rows


def _clean_str(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


def _row_by_ric(rows: list[dict[str, Any]], ric: str) -> dict[str, Any] | None:
    wanted = str(ric or "").strip().upper()
    for row in rows:
        if str(row.get("ric") or "").strip().upper() == wanted:
            return row
    return None


def _common_name_payload(row: dict[str, Any] | None) -> dict[str, object] | None:
    if not row:
        return None
    value = _clean_str(row.get("common_name"))
    as_of_date = _clean_str(row.get("as_of_date"))
    if not value or not as_of_date:
        return None
    return {
        "value": value,
        "as_of_date": as_of_date,
    }


def _classification_payload(row: dict[str, Any] | None) -> dict[str, object] | None:
    if not row:
        return None
    as_of_date = _clean_str(row.get("as_of_date"))
    if not as_of_date:
        return None
    return {
        "as_of_date": as_of_date,
        "trbc_economic_sector": _clean_str(row.get("trbc_economic_sector")),
        "trbc_business_sector": _clean_str(row.get("trbc_business_sector")),
        "trbc_industry_group": _clean_str(row.get("trbc_industry_group")),
        "trbc_industry": _clean_str(row.get("trbc_industry")),
        "trbc_activity": _clean_str(row.get("trbc_activity")),
    }


def _price_context_payload(row: dict[str, Any] | None) -> dict[str, object] | None:
    if not row:
        return None
    price_date = _clean_str(row.get("date"))
    if not price_date:
        return None
    for field_name in ("adj_close", "close"):
        raw = row.get(field_name)
        if raw is None:
            continue
        return {
            "price": float(raw),
            "price_date": price_date,
            "price_field_used": field_name,
            "currency": _clean_str(row.get("currency")),
        }
    return None


def _source_context_payload(
    *,
    ric: str | None,
    package_date: str | None,
    data_db=None,
) -> dict[str, object]:
    default_payload = {
        "status": "missing",
        "reason": "missing_rows",
        "latest_common_name": None,
        "classification_snapshot": None,
        "latest_price_context": None,
    }
    clean_ric = _clean_str(ric)
    clean_package_date = _clean_str(package_date)
    if not clean_ric or not clean_package_date:
        return default_payload

    latest_common_name = None
    classification_snapshot = None
    latest_price_context = None
    has_source_failure = False
    has_missing_rows = False

    try:
        latest_common_name = _common_name_payload(
            _row_by_ric(
                cpar_source_reads.load_latest_common_name_rows(
                    [clean_ric],
                    as_of_date=clean_package_date,
                    data_db=data_db,
                ),
                clean_ric,
            )
        )
    except cpar_source_reads.CparSourceReadError:
        has_source_failure = True
    else:
        if latest_common_name is None:
            has_missing_rows = True

    try:
        classification_snapshot = _classification_payload(
            _row_by_ric(
                cpar_source_reads.load_latest_classification_rows(
                    [clean_ric],
                    as_of_date=clean_package_date,
                    data_db=data_db,
                ),
                clean_ric,
            )
        )
    except cpar_source_reads.CparSourceReadError:
        has_source_failure = True
    else:
        if classification_snapshot is None:
            has_missing_rows = True

    try:
        latest_price_context = _price_context_payload(
            _row_by_ric(
                cpar_source_reads.load_latest_price_rows(
                    [clean_ric],
                    as_of_date=clean_package_date,
                    data_db=data_db,
                ),
                clean_ric,
            )
        )
    except cpar_source_reads.CparSourceReadError:
        has_source_failure = True
    else:
        if latest_price_context is None:
            has_missing_rows = True

    available_count = sum(
        value is not None
        for value in (latest_common_name, classification_snapshot, latest_price_context)
    )
    if available_count == 3 and not has_source_failure:
        status = "ok"
        reason = None
    elif available_count == 0 and has_source_failure:
        status = "unavailable"
        reason = "shared_source_unavailable"
    elif available_count == 0:
        status = "missing"
        reason = "missing_rows"
    else:
        status = "partial"
        if has_source_failure and has_missing_rows:
            reason = "mixed"
        elif has_source_failure:
            reason = "shared_source_unavailable"
        else:
            reason = "missing_rows"

    return {
        "status": status,
        "reason": reason,
        "latest_common_name": latest_common_name,
        "classification_snapshot": classification_snapshot,
        "latest_price_context": latest_price_context,
    }


def load_cpar_ticker_payload(
    ticker: str,
    *,
    ric: str | None = None,
    data_db=None,
) -> dict[str, object]:
    package = cpar_meta_service.require_active_package(data_db=data_db)
    try:
        fit = cpar_outputs.load_package_instrument_fit(
            ticker=ticker,
            package_run_id=str(package["package_run_id"]),
            ric=ric,
            data_db=data_db,
        )
    except cpar_outputs.CparPackageNotReady as exc:
        raise cpar_meta_service.CparReadNotReady(str(exc)) from exc
    except cpar_outputs.CparAuthorityReadError as exc:
        raise cpar_meta_service.CparReadUnavailable(str(exc)) from exc
    except cpar_queries.CparAmbiguousInstrumentFit as exc:
        raise cpar_meta_service.CparTickerAmbiguous(str(exc)) from exc
    if fit is None:
        raise cpar_meta_service.CparTickerNotFound(
            f"Ticker {str(ticker).upper().strip()} was not found in the active cPAR package."
        )
    return {
        **cpar_meta_service.package_meta_payload(package),
        "ticker": fit.get("ticker"),
        "ric": fit.get("ric"),
        "display_name": fit.get("display_name"),
        "fit_status": fit.get("fit_status"),
        "warnings": list(fit.get("warnings") or []),
        "observed_weeks": int(fit.get("observed_weeks") or 0),
        "lookback_weeks": int(fit.get("lookback_weeks") or 0),
        "longest_gap_weeks": int(fit.get("longest_gap_weeks") or 0),
        "price_field_used": fit.get("price_field_used"),
        "hq_country_code": fit.get("hq_country_code"),
        "market_step_alpha": fit.get("market_step_alpha"),
        "beta_market_step1": fit.get("market_step_beta"),
        "block_alpha": fit.get("block_alpha"),
        "beta_spy_trade": fit.get("spy_trade_beta_raw"),
        "raw_loadings": _factor_rows(dict(fit.get("raw_loadings") or {})),
        "thresholded_loadings": _factor_rows(dict(fit.get("thresholded_loadings") or {})),
        "pre_hedge_factor_variance_proxy": fit.get("factor_variance_proxy"),
        "pre_hedge_factor_volatility_proxy": fit.get("factor_volatility_proxy"),
        "source_context": _source_context_payload(
            ric=str(fit.get("ric") or ""),
            package_date=str(package.get("package_date") or ""),
            data_db=data_db,
        ),
    }
