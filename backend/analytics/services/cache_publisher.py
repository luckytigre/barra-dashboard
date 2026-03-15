"""Cache staging/publish helpers for analytics refresh."""

from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
from pathlib import Path
from typing import Any

import numpy as np

from backend.analytics.contracts import (
    ComponentSharesPayload,
    CovarianceMatrixPayload,
    CovariancePayload,
    EligibilitySummaryPayload,
    ExposureModesPayload,
    FactorCatalogEntryPayload,
    FactorDetailPayload,
    ModelSanityPayload,
    PositionPayload,
    RefreshMetaPayload,
    RiskEngineMetaPayload,
    RiskEngineStatePayload,
    RiskSharesPayload,
    SnapshotBuildPayload,
    SourceDatesPayload,
    SpecificRiskPayload,
    StageRefreshSnapshotResult,
    UniverseFactorsPayload,
    UniverseLoadingsPayload,
)
from backend.analytics.health import compute_health_diagnostics
from backend.data import sqlite

logger = logging.getLogger(__name__)
ELIGIBILITY_WELL_COVERED_RATIO = 0.50
ELIGIBILITY_WELL_COVERED_MIN_N = 100
HEALTH_DIAGNOSTICS_CACHE_VERSION = "2026_03_15_v1"


def _finite_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return float(default)
    return out if np.isfinite(out) else float(default)


def build_risk_engine_state(
    *,
    risk_engine_meta: RiskEngineMetaPayload,
    recomputed_this_refresh: bool,
    recompute_reason: str,
) -> RiskEngineStatePayload:
    return {
        "status": str(risk_engine_meta.get("status") or "unknown"),
        "method_version": str(risk_engine_meta.get("method_version") or ""),
        "last_recompute_date": str(risk_engine_meta.get("last_recompute_date") or ""),
        "factor_returns_latest_date": risk_engine_meta.get("factor_returns_latest_date"),
        "cross_section_min_age_days": int(risk_engine_meta.get("cross_section_min_age_days") or 0),
        "recompute_interval_days": int(risk_engine_meta.get("recompute_interval_days") or 0),
        "lookback_days": int(risk_engine_meta.get("lookback_days") or 0),
        "specific_risk_ticker_count": int(risk_engine_meta.get("specific_risk_ticker_count") or 0),
        "recomputed_this_refresh": bool(recomputed_this_refresh),
        "recompute_reason": str(recompute_reason),
    }


def _positions_fingerprint(positions: list[PositionPayload]) -> str:
    normalized = []
    for row in positions:
        if not isinstance(row, dict):
            continue
        normalized.append(
            {
                "ticker": str(row.get("ticker") or "").upper(),
                "weight": round(_finite_float(row.get("weight"), 0.0), 10),
                "market_value": round(_finite_float(row.get("market_value"), 0.0), 4),
                "quantity": round(_finite_float(row.get("quantity"), 0.0), 6),
            }
        )
    normalized.sort(key=lambda item: item["ticker"])
    encoded = json.dumps(normalized, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _health_reuse_signature(
    *,
    source_dates: SourceDatesPayload,
    risk_engine_state: RiskEngineStatePayload,
    positions: list[PositionPayload],
    total_value: float,
) -> dict[str, Any]:
    return {
        "cache_version": HEALTH_DIAGNOSTICS_CACHE_VERSION,
        "source_dates": dict(source_dates or {}),
        "risk_engine": {
            "method_version": str(risk_engine_state.get("method_version") or ""),
            "last_recompute_date": str(risk_engine_state.get("last_recompute_date") or ""),
            "factor_returns_latest_date": str(risk_engine_state.get("factor_returns_latest_date") or ""),
            "lookback_days": int(risk_engine_state.get("lookback_days") or 0),
            "specific_risk_ticker_count": int(risk_engine_state.get("specific_risk_ticker_count") or 0),
        },
        "positions_fingerprint": _positions_fingerprint(positions),
        "total_value": round(_finite_float(total_value, 0.0), 2),
    }


def _can_reuse_cached_health_payload(
    cached_payload: Any,
    *,
    signature: dict[str, Any],
) -> bool:
    if not isinstance(cached_payload, dict):
        return False
    cached_signature = cached_payload.get("_reuse_signature")
    if not isinstance(cached_signature, dict):
        return False
    return cached_signature == signature


def build_model_sanity_report(
    *,
    risk_shares: RiskSharesPayload,
    factor_details: list[FactorDetailPayload],
    eligibility_summary: EligibilitySummaryPayload,
) -> ModelSanityPayload:
    warnings: list[str] = []

    regression_cov = _finite_float(eligibility_summary.get("regression_coverage"), 0.0)
    if regression_cov < 0.20:
        warnings.append(
            f"Low regression coverage on latest usable date: {regression_cov * 100.0:.1f}%."
        )

    drop_pct = _finite_float(eligibility_summary.get("drop_pct_from_prev"), 0.0)
    if drop_pct > 0.10:
        warnings.append(
            f"Eligible universe dropped {drop_pct * 100.0:.1f}% vs previous cross-section."
        )

    industry_pct = _finite_float(risk_shares.get("industry"), 0.0)
    market_pct = _finite_float(risk_shares.get("market"), 0.0)
    style_pct = _finite_float(risk_shares.get("style"), 0.0)
    if market_pct > 90.0:
        warnings.append(f"Market risk share is highly concentrated at {market_pct:.1f}% of total risk.")
    if industry_pct > 90.0:
        warnings.append(f"Industry risk share is highly concentrated at {industry_pct:.1f}% of total risk.")
    if style_pct > 90.0:
        warnings.append(f"Style risk share is highly concentrated at {style_pct:.1f}% of total risk.")

    sign_mismatch = 0
    for row in factor_details:
        exp = _finite_float(row.get("exposure"), 0.0)
        sens = _finite_float(row.get("sensitivity"), 0.0)
        if abs(exp) > 1e-12 and abs(sens) > 1e-12 and (exp * sens) < 0:
            sign_mismatch += 1
    if sign_mismatch > 0:
        warnings.append(
            f"{sign_mismatch} factors have exposure/sensitivity sign mismatch; expected same sign."
        )

    coverage_date = str(eligibility_summary.get("date") or "")
    latest_available_date = str(eligibility_summary.get("latest_available_date") or "")
    used_older_than_latest = bool(eligibility_summary.get("used_older_than_latest"))
    if used_older_than_latest and coverage_date and latest_available_date:
        warnings.append(
            f"Using latest well-covered date {coverage_date} (latest source date is {latest_available_date})."
        )

    return {
        "status": "warn" if warnings else "ok",
        "warnings": warnings,
        "coverage_date": coverage_date or None,
        "latest_available_date": latest_available_date or None,
        "selection_mode": str(eligibility_summary.get("selection_mode") or ""),
        "update_available": bool(used_older_than_latest),
        "checks": {
            "factor_sign_mismatch_count": int(sign_mismatch),
            "market_risk_share_pct": round(market_pct, 2),
            "latest_regression_coverage_pct": round(regression_cov * 100.0, 2),
            "latest_structural_eligible_n": int(eligibility_summary.get("structural_eligible_n", 0) or 0),
            "latest_core_structural_eligible_n": int(
                eligibility_summary.get("core_structural_eligible_n", 0) or 0
            ),
            "latest_projectable_n": int(eligibility_summary.get("projectable_n", 0) or 0),
            "latest_projected_only_n": int(eligibility_summary.get("projected_only_n", 0) or 0),
            "industry_risk_share_pct": round(industry_pct, 2),
            "style_risk_share_pct": round(style_pct, 2),
            "idio_risk_share_pct": round(_finite_float(risk_shares.get("idio"), 0.0), 2),
        },
    }


def load_latest_eligibility_summary(cache_db: Path) -> EligibilitySummaryPayload:
    conn = sqlite3.connect(str(cache_db))
    try:
        elig_cols = {
            str(row[1])
            for row in conn.execute("PRAGMA table_info(daily_universe_eligibility_summary)").fetchall()
        }
        core_structural_expr = (
            "core_structural_eligible_n" if "core_structural_eligible_n" in elig_cols else "structural_eligible_n"
        )
        projectable_expr = "projectable_n" if "projectable_n" in elig_cols else "regression_member_n"
        projected_only_expr = "projected_only_n" if "projected_only_n" in elig_cols else "0"
        projectable_coverage_expr = (
            "projectable_coverage" if "projectable_coverage" in elig_cols else "regression_coverage"
        )
        latest_any = conn.execute(
            f"""
            SELECT date, exp_date, exposure_n, structural_eligible_n, regression_member_n,
                   {core_structural_expr} AS core_structural_eligible_n,
                   {projectable_expr} AS projectable_n,
                   {projected_only_expr} AS projected_only_n,
                   structural_coverage, regression_coverage,
                   {projectable_coverage_expr} AS projectable_coverage,
                   drop_pct_from_prev, alert_level
            FROM daily_universe_eligibility_summary
            ORDER BY date DESC
            LIMIT 1
            """
        ).fetchone()
        max_row = conn.execute(
            """
            SELECT MAX(regression_member_n)
            FROM daily_universe_eligibility_summary
            """
        ).fetchone()
        max_regression_n = int(max_row[0] or 0) if max_row else 0
        coverage_threshold_n = max(
            ELIGIBILITY_WELL_COVERED_MIN_N,
            int(ELIGIBILITY_WELL_COVERED_RATIO * max_regression_n),
        )

        row = conn.execute(
            f"""
            SELECT date, exp_date, exposure_n, structural_eligible_n, regression_member_n,
                   {core_structural_expr} AS core_structural_eligible_n,
                   {projectable_expr} AS projectable_n,
                   {projected_only_expr} AS projected_only_n,
                   structural_coverage, regression_coverage,
                   {projectable_coverage_expr} AS projectable_coverage,
                   drop_pct_from_prev, alert_level
            FROM daily_universe_eligibility_summary
            WHERE regression_member_n >= ?
            ORDER BY date DESC
            LIMIT 1
            """,
            (coverage_threshold_n,),
        ).fetchone()
        selection_mode = "well_covered"
        if row is None:
            row = conn.execute(
                f"""
                SELECT date, exp_date, exposure_n, structural_eligible_n, regression_member_n,
                       {core_structural_expr} AS core_structural_eligible_n,
                       {projectable_expr} AS projectable_n,
                       {projected_only_expr} AS projected_only_n,
                       structural_coverage, regression_coverage,
                       {projectable_coverage_expr} AS projectable_coverage,
                       drop_pct_from_prev, alert_level
                FROM daily_universe_eligibility_summary
                WHERE regression_member_n > 0
                ORDER BY date DESC
                LIMIT 1
                """
            ).fetchone()
            selection_mode = "latest_positive"
        if row is None:
            row = latest_any
            selection_mode = "latest_any"
    except sqlite3.OperationalError:
        latest_any = None
        max_regression_n = 0
        coverage_threshold_n = ELIGIBILITY_WELL_COVERED_MIN_N
        selection_mode = "none"
        row = None
    finally:
        conn.close()
    if not row:
        return {
            "status": "no-data",
            "selection_mode": "none",
            "max_regression_member_n": int(max_regression_n),
            "coverage_threshold_n": int(coverage_threshold_n),
            "latest_available_date": str(latest_any[0]) if latest_any and latest_any[0] is not None else None,
        }

    latest_available_date = str(latest_any[0]) if latest_any and latest_any[0] is not None else str(row[0])
    selected_date = str(row[0])
    selected_well_covered = bool(selection_mode == "well_covered")
    used_older_than_latest = bool(latest_available_date and latest_available_date > selected_date)
    return {
        "status": "ok",
        "date": selected_date,
        "exp_date": str(row[1]) if row[1] is not None else None,
        "exposure_n": int(row[2] or 0),
        "structural_eligible_n": int(row[3] or 0),
        "regression_member_n": int(row[4] or 0),
        "core_structural_eligible_n": int(row[5] or 0),
        "projectable_n": int(row[6] or 0),
        "projected_only_n": int(row[7] or 0),
        "structural_coverage": float(row[8] or 0.0),
        "regression_coverage": float(row[9] or 0.0),
        "projectable_coverage": float(row[10] or 0.0),
        "drop_pct_from_prev": float(row[11] or 0.0),
        "alert_level": str(row[12] or ""),
        "selection_mode": selection_mode,
        "max_regression_member_n": int(max_regression_n),
        "coverage_threshold_n": int(coverage_threshold_n),
        "latest_available_date": latest_available_date,
        "selected_well_covered": selected_well_covered,
        "used_older_than_latest": used_older_than_latest,
    }


def stage_refresh_cache_snapshot(
    *,
    run_id: str,
    refresh_mode: str,
    refresh_started_at: str,
    source_dates: SourceDatesPayload,
    snapshot_build: SnapshotBuildPayload,
    risk_engine_meta: RiskEngineMetaPayload,
    recomputed_this_refresh: bool,
    recompute_reason: str,
    cov_payload: CovariancePayload,
    specific_risk_by_security: dict[str, SpecificRiskPayload],
    positions: list[PositionPayload],
    total_value: float,
    risk_shares: RiskSharesPayload,
    component_shares: ComponentSharesPayload,
    factor_details: list[FactorDetailPayload],
    cov_matrix: CovarianceMatrixPayload,
    latest_r2: float,
    universe_loadings: UniverseLoadingsPayload,
    exposure_modes: ExposureModesPayload,
    factor_catalog: list[FactorCatalogEntryPayload],
    cuse4_foundation: dict[str, Any],
    light_mode: bool,
    reuse_cached_static_payloads: bool = False,
    data_db: Path,
    cache_db: Path,
) -> StageRefreshSnapshotResult:
    """Stage all refresh payloads under a snapshot id (not yet published)."""
    snapshot_id = str(run_id)
    logger.info("Staging refresh cache snapshot: snapshot_id=%s mode=%s", snapshot_id, refresh_mode)

    def _stage_cache(key: str, value: Any) -> None:
        sqlite.cache_set(key, value, snapshot_id=snapshot_id)

    risk_engine_state = build_risk_engine_state(
        risk_engine_meta=risk_engine_meta,
        recomputed_this_refresh=bool(recomputed_this_refresh),
        recompute_reason=str(recompute_reason),
    )
    _stage_cache("risk_engine_cov", cov_payload)
    _stage_cache("risk_engine_specific_risk", specific_risk_by_security)
    _stage_cache("risk_engine_meta", risk_engine_meta)

    portfolio_data = {
        "positions": positions,
        "total_value": round(total_value, 2),
        "position_count": len(positions),
        "refresh_started_at": refresh_started_at,
        "source_dates": source_dates,
    }
    _stage_cache("portfolio", portfolio_data)
    health_reuse_signature = _health_reuse_signature(
        source_dates=source_dates,
        risk_engine_state=risk_engine_state,
        positions=positions,
        total_value=total_value,
    )

    risk_data = {
        "risk_shares": risk_shares,
        "component_shares": component_shares,
        "factor_details": factor_details,
        "factor_catalog": factor_catalog,
        "cov_matrix": cov_matrix,
        "r_squared": round(float(latest_r2), 4),
        "risk_engine": risk_engine_state,
        "refresh_started_at": refresh_started_at,
    }
    _stage_cache("risk", risk_data)

    universe_loadings["risk_engine"] = risk_engine_state
    universe_loadings["refresh_started_at"] = refresh_started_at
    universe_loadings["source_dates"] = source_dates
    _stage_cache("universe_loadings", universe_loadings)
    universe_factors: UniverseFactorsPayload = {
        "factors": universe_loadings.get("factors", []),
        "factor_vols": universe_loadings.get("factor_vols", {}),
        "factor_catalog": factor_catalog,
        "r_squared": round(float(latest_r2), 4),
        "ticker_count": universe_loadings.get("ticker_count", 0),
        "eligible_ticker_count": universe_loadings.get("eligible_ticker_count", 0),
        "core_estimated_ticker_count": universe_loadings.get("core_estimated_ticker_count", 0),
        "projected_only_ticker_count": universe_loadings.get("projected_only_ticker_count", 0),
        "ineligible_ticker_count": universe_loadings.get("ineligible_ticker_count", 0),
        "risk_engine": risk_engine_state,
        "refresh_started_at": refresh_started_at,
    }
    _stage_cache("universe_factors", universe_factors)
    _stage_cache("exposures", exposure_modes)

    eligibility_summary = (
        sqlite.cache_get("eligibility")
        if reuse_cached_static_payloads
        else None
    )
    if not isinstance(eligibility_summary, dict) or not eligibility_summary:
        eligibility_summary = load_latest_eligibility_summary(cache_db)
    _stage_cache("eligibility", eligibility_summary)
    sanity = build_model_sanity_report(
        risk_shares=risk_shares,
        factor_details=factor_details,
        eligibility_summary=eligibility_summary,
    )
    _stage_cache("model_sanity", sanity)
    _stage_cache("cuse4_foundation", cuse4_foundation)

    cached_health_payload = sqlite.cache_get("health_diagnostics") if light_mode else None
    if _can_reuse_cached_health_payload(cached_health_payload, signature=health_reuse_signature):
        health_payload = dict(cached_health_payload)
        health_refreshed = False
    else:
        health_payload = compute_health_diagnostics(
            data_db,
            cache_db,
            risk_payload=risk_data,
            portfolio_payload=portfolio_data,
            universe_payload=universe_loadings,
            covariance_payload=cov_payload,
            source_dates=source_dates,
            run_id=run_id,
            snapshot_id=snapshot_id,
        )
        health_refreshed = True
    if isinstance(health_payload, dict):
        health_payload = dict(health_payload)
        health_payload["run_id"] = str(run_id)
        health_payload["snapshot_id"] = str(snapshot_id)
        health_payload["_reuse_signature"] = health_reuse_signature
        health_payload["cache_version"] = HEALTH_DIAGNOSTICS_CACHE_VERSION
    _stage_cache("health_diagnostics", health_payload)
    logger.info(
        "Staged core payloads: positions=%s factors=%s health_refreshed=%s",
        len(positions),
        len(universe_loadings.get("factors", [])),
        bool(health_refreshed),
    )

    refresh_meta: RefreshMetaPayload = {
        "status": "ok",
        "mode": refresh_mode,
        "run_id": run_id,
        "snapshot_id": snapshot_id,
        "refresh_started_at": refresh_started_at,
        "source_dates": source_dates,
        "cross_section_snapshot": snapshot_build,
        "risk_engine": risk_engine_state,
        "model_sanity_status": sanity.get("status", "unknown"),
        "cuse4_foundation": cuse4_foundation,
        "health_refreshed": bool(health_refreshed),
    }
    _stage_cache("refresh_meta", refresh_meta)

    return {
        "snapshot_id": snapshot_id,
        "risk_engine_state": risk_engine_state,
        "sanity": sanity,
        "health_refreshed": bool(health_refreshed),
        "persisted_payloads": {
            "portfolio": portfolio_data,
            "risk": risk_data,
            "exposures": exposure_modes,
            "universe_loadings": universe_loadings,
            "universe_factors": universe_factors,
            "eligibility": eligibility_summary,
            "model_sanity": sanity,
            "health_diagnostics": health_payload,
            "refresh_meta": refresh_meta,
        },
    }
