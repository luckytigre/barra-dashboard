"""Stage implementation helpers for the model pipeline."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import numpy as np


def run_stage(
    *,
    profile: str,
    stage: str,
    as_of_date: str,
    should_run_core: bool,
    serving_mode: str,
    force_core: bool,
    core_reason: str,
    data_db: Path,
    cache_db: Path,
    raw_history_policy: str = "none",
    reset_core_cache: bool = False,
    enable_ingest: bool = False,
    prefer_local_source_archive: bool = False,
    refresh_scope: str | None = None,
    workspace_root: Path | None = None,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
    config_module,
    core_reads_module,
    sqlite_module,
    bootstrap_cuse4_source_tables_fn: Callable[..., Any],
    download_from_lseg_fn: Callable[..., Any],
    repair_price_gap_fn: Callable[..., dict[str, Any]],
    repair_pit_gap_fn: Callable[..., dict[str, Any]],
    profile_source_sync_required_fn: Callable[..., bool],
    profile_neon_readiness_required_fn: Callable[..., bool],
    run_neon_mirror_cycle_fn: Callable[..., dict[str, Any]],
    neon_authority_module,
    rebuild_raw_cross_section_history_fn: Callable[..., Any],
    rebuild_cross_section_snapshot_fn: Callable[..., Any],
    build_and_persist_estu_membership_fn: Callable[..., Any],
    reset_core_caches_fn: Callable[..., dict[str, int]],
    compute_daily_factor_returns_fn: Callable[..., Any],
    build_factor_covariance_from_cache_fn: Callable[..., Any],
    build_specific_risk_from_cache_fn: Callable[..., Any],
    latest_factor_return_date_fn: Callable[..., str | None],
    serialize_covariance_fn: Callable[..., dict[str, Any]],
    temporary_runtime_paths_cm: Callable[..., Any],
    serving_refresh_skip_risk_engine_fn: Callable[..., tuple[bool, str]],
    run_refresh_fn: Callable[..., dict[str, Any]],
    previous_or_same_xnys_session_fn: Callable[[str], str],
    risk_engine_method_version: str,
    canonical_data_db: Path,
    canonical_cache_db: Path,
) -> dict[str, Any]:
    if stage == "ingest":
        if progress_callback is not None:
            progress_callback({"message": "Bootstrapping source tables", "progress_kind": "stage"})
        bootstrap = bootstrap_cuse4_source_tables_fn(
            db_path=data_db,
        )
        if not config_module.runtime_role_allows_ingest():
            return {
                "status": "skipped",
                "mode": "bootstrap_only",
                "reason": "runtime_role_disallows_ingest",
                "bootstrap": bootstrap,
                "runtime_role": str(config_module.APP_RUNTIME_ROLE),
            }
        if not enable_ingest:
            return {
                "status": "ok",
                "mode": "bootstrap_only",
                "reason": "profile_skip_lseg_ingest",
                "bootstrap": bootstrap,
                "runtime_role": str(config_module.APP_RUNTIME_ROLE),
            }
        if not bool(config_module.ORCHESTRATOR_ENABLE_INGEST):
            return {
                "status": "ok",
                "mode": "bootstrap_only",
                "reason": "ORCHESTRATOR_ENABLE_INGEST=false",
                "bootstrap": bootstrap,
                "runtime_role": str(config_module.APP_RUNTIME_ROLE),
            }
        if progress_callback is not None:
            progress_callback({"message": "Pulling latest source data from LSEG", "progress_kind": "io"})
        latest_price_date_before_ingest = _latest_price_date(data_db)
        ingest = download_from_lseg_fn(
            db_path=data_db,
            as_of_date=as_of_date,
            shard_count=1,
            shard_index=0,
            write_fundamentals=False,
            write_prices=True,
            write_classification=False,
        )
        price_gap_repair = {"status": "skipped", "reason": "ingest_not_ok"}
        pit_gap_repair = {"status": "skipped", "reason": "ingest_not_ok"}
        if str(ingest.get("status") or "").strip().lower() == "ok":
            price_gap_repair = repair_price_gap_fn(
                data_db=data_db,
                as_of_date=as_of_date,
                latest_price_date_before_ingest=latest_price_date_before_ingest,
                progress_callback=progress_callback,
            )
            pit_gap_repair = repair_pit_gap_fn(
                data_db=data_db,
                as_of_date=as_of_date,
                progress_callback=progress_callback,
            )
        return {
            "status": str(ingest.get("status") or "ok"),
            "mode": "bootstrap_plus_lseg_ingest",
            "bootstrap": bootstrap,
            "ingest": ingest,
            "price_gap_repair": price_gap_repair,
            "pit_gap_repair": pit_gap_repair,
        }

    if stage == "source_sync":
        if not profile_source_sync_required_fn(profile):
            return {
                "status": "skipped",
                "reason": "profile_skip_source_sync",
            }
        dsn = str(config_module.NEON_DATABASE_URL or "").strip()
        if not dsn:
            raise RuntimeError("source_sync requires NEON_DATABASE_URL for Neon-authoritative profiles.")
        local_source_dates: dict[str, Any] | None = None
        neon_source_dates: dict[str, Any] | None = None
        try:
            with core_reads_module.core_read_backend("local"):
                local_source_dates = core_reads_module.load_source_dates()
            with core_reads_module.core_read_backend("neon"):
                neon_source_dates = core_reads_module.load_source_dates()
        except Exception:
            local_source_dates = local_source_dates
            neon_source_dates = neon_source_dates
        older_than_neon: list[str] = []
        ignored_newer_than_target: list[str] = []
        pit_latest_closed_anchor = _latest_closed_period_anchor(
            str(as_of_date),
            frequency=str(config_module.SOURCE_DAILY_PIT_FREQUENCY or "monthly").strip().lower(),
        )
        if isinstance(local_source_dates, dict) and isinstance(neon_source_dates, dict):
            for field in ("prices_asof", "fundamentals_asof", "classification_asof"):
                local_value = str(local_source_dates.get(field) or "").strip()
                neon_value = str(neon_source_dates.get(field) or "").strip()
                allowed_ceiling = str(as_of_date)
                if field in {"fundamentals_asof", "classification_asof"}:
                    allowed_ceiling = pit_latest_closed_anchor
                if local_value and neon_value and neon_value > allowed_ceiling:
                    ignored_newer_than_target.append(field)
                    continue
                if local_value and neon_value and local_value < neon_value:
                    older_than_neon.append(field)
        if older_than_neon:
            raise RuntimeError(
                "source_sync refused to overwrite newer Neon source tables from an older local archive: "
                + ", ".join(sorted(older_than_neon))
            )
        if progress_callback is not None:
            progress_callback({"message": "Syncing retained source/model window into Neon", "progress_kind": "io"})
        out = run_neon_mirror_cycle_fn(
            sqlite_path=data_db,
            cache_path=cache_db,
            dsn=dsn,
            mode=str(config_module.NEON_AUTO_SYNC_MODE or "incremental"),
            tables=[
                "security_master",
                "security_prices_eod",
                "security_fundamentals_pit",
                "security_classification_pit",
            ],
            parity_enabled=False,
            prune_enabled=False,
            source_years=int(config_module.NEON_SOURCE_RETENTION_YEARS),
            analytics_years=int(config_module.NEON_ANALYTICS_RETENTION_YEARS),
        )
        if str(out.get("status") or "") != "ok":
            raise RuntimeError(f"source_sync stage failed: {out}")
        return {
            "status": "ok",
            "local_source_dates": local_source_dates,
            "neon_source_dates_before_sync": neon_source_dates,
            "ignored_newer_than_target": ignored_newer_than_target,
            "source_sync": out,
        }

    if stage == "neon_readiness":
        if not profile_neon_readiness_required_fn(profile):
            return {
                "status": "skipped",
                "reason": "profile_skip_neon_readiness",
            }
        if not should_run_core:
            return {
                "status": "skipped",
                "reason": f"core_policy_skip_{core_reason}",
            }
        root = Path(workspace_root or (Path(config_module.APP_DATA_DIR) / "neon_rebuild_workspace" / "adhoc"))
        if progress_callback is not None:
            progress_callback({"message": "Preparing Neon-authoritative scratch workspace", "progress_kind": "io"})
        out = neon_authority_module.prepare_neon_rebuild_workspace(
            profile=profile,
            workspace_root=root,
            dsn=(str(config_module.NEON_DATABASE_URL).strip() or None),
            analytics_years=int(config_module.NEON_ANALYTICS_RETENTION_YEARS),
        )
        return {
            "status": "ok",
            **out,
        }

    if stage == "raw_history":
        if str(raw_history_policy or "none") == "none":
            return {
                "status": "skipped",
                "reason": "profile_skip_raw_history_rebuild",
            }
        frequency = "daily" if str(raw_history_policy) == "full-daily" else "weekly"
        out = rebuild_raw_cross_section_history_fn(
            data_db,
            frequency=frequency,
            progress_callback=progress_callback,
        )
        if str(out.get("status") or "") != "ok":
            raise RuntimeError(f"raw_history stage failed: {out}")
        if int(out.get("rows_upserted") or 0) <= 0:
            raise RuntimeError("raw_history stage produced zero rows")
        return {
            "status": "ok",
            "raw_history_policy": str(raw_history_policy),
            "raw_history": out,
        }

    if stage == "feature_build":
        if progress_callback is not None:
            progress_callback({"message": "Rebuilding cross-section snapshot", "progress_kind": "stage"})
        out = rebuild_cross_section_snapshot_fn(
            data_db,
            mode=str(config_module.CROSS_SECTION_SNAPSHOT_MODE or "current"),
        )
        return {
            "status": "ok",
            "snapshot": out,
        }

    if stage == "estu_audit":
        if progress_callback is not None:
            progress_callback({"message": "Recomputing ESTU membership", "progress_kind": "stage"})
        out = build_and_persist_estu_membership_fn(
            db_path=data_db,
            as_of_date=as_of_date,
        )
        return {
            "status": str(out.get("status") or "ok"),
            "estu": out,
        }

    if stage == "factor_returns":
        if not should_run_core:
            return {
                "status": "skipped",
                "reason": f"core_policy_skip_{core_reason}",
            }
        reset_summary = reset_core_caches_fn(cache_db) if reset_core_cache else {}
        if progress_callback is not None and reset_core_cache:
            progress_callback(
                {
                    "message": "Resetting factor-return and residual caches before rebuild",
                    "progress_kind": "stage",
                    "cache_rows_cleared": reset_summary,
                }
            )
        df = compute_daily_factor_returns_fn(
            data_db,
            cache_db,
            min_cross_section_age_days=config_module.CROSS_SECTION_MIN_AGE_DAYS,
            progress_callback=progress_callback,
        )
        if df is None or getattr(df, "empty", False):
            raise RuntimeError("factor_returns stage produced zero rows")
        return {
            "status": "ok",
            "factor_return_rows_loaded": int(len(df)),
            "core_cache_reset": bool(reset_core_cache),
            "cache_rows_cleared": reset_summary,
        }

    if stage == "risk_model":
        if not should_run_core:
            return {
                "status": "skipped",
                "reason": f"core_policy_skip_{core_reason}",
            }
        if progress_callback is not None:
            progress_callback({"message": "Building factor covariance matrix", "progress_kind": "stage"})
        cov, latest_r2 = build_factor_covariance_from_cache_fn(
            cache_db,
            lookback_days=config_module.LOOKBACK_DAYS,
        )
        if progress_callback is not None:
            progress_callback({"message": "Building specific-risk model", "progress_kind": "stage"})
        specific_risk = build_specific_risk_from_cache_fn(
            cache_db,
            lookback_days=config_module.LOOKBACK_DAYS,
        )
        if cov is None or cov.empty:
            raise RuntimeError("risk_model stage produced empty covariance matrix")
        if not isinstance(specific_risk, dict) or len(specific_risk) == 0:
            raise RuntimeError("risk_model stage produced empty specific-risk map")
        recompute_date = previous_or_same_xnys_session_fn(
            datetime.now(timezone.utc).date().isoformat()
        )
        risk_engine_meta = {
            "status": "ok",
            "method_version": risk_engine_method_version,
            "last_recompute_date": recompute_date,
            "factor_returns_latest_date": latest_factor_return_date_fn(canonical_cache_db),
            "lookback_days": int(config_module.LOOKBACK_DAYS),
            "cross_section_min_age_days": int(config_module.CROSS_SECTION_MIN_AGE_DAYS),
            "recompute_interval_days": int(config_module.RISK_RECOMPUTE_INTERVAL_DAYS),
            "latest_r2": float(latest_r2 if np.isfinite(latest_r2) else 0.0),
            "specific_risk_ticker_count": int(len(specific_risk)),
            "recompute_reason": "force_core" if force_core else core_reason,
        }
        if Path(cache_db).resolve() != canonical_cache_db.resolve():
            with temporary_runtime_paths_cm(data_db=data_db, cache_db=cache_db):
                sqlite_module.cache_set("risk_engine_cov", serialize_covariance_fn(cov))
                sqlite_module.cache_set("risk_engine_specific_risk", specific_risk)
                sqlite_module.cache_set("risk_engine_meta", risk_engine_meta)
        else:
            sqlite_module.cache_set("risk_engine_cov", serialize_covariance_fn(cov))
            sqlite_module.cache_set("risk_engine_specific_risk", specific_risk)
            sqlite_module.cache_set("risk_engine_meta", risk_engine_meta)
        if progress_callback is not None:
            progress_callback(
                {
                    "message": "Published risk-engine payloads to cache",
                    "progress_kind": "stage",
                    "factor_count": int(cov.shape[1]) if cov is not None and not cov.empty else 0,
                    "specific_risk_ticker_count": int(len(specific_risk)),
                }
            )
        return {
            "status": "ok",
            "factor_count": int(cov.shape[1]) if cov is not None and not cov.empty else 0,
            "specific_risk_ticker_count": int(len(specific_risk)),
            "risk_engine_meta": risk_engine_meta,
        }

    if stage == "serving_refresh":
        if progress_callback is not None:
            progress_callback({"message": "Publishing serving payloads", "progress_kind": "stage"})

        force_local_core_reads = bool(
            prefer_local_source_archive
            or should_run_core
            or Path(data_db).resolve() != canonical_data_db.resolve()
            or Path(cache_db).resolve() != canonical_cache_db.resolve()
        )

        def _run_refresh_inner() -> dict[str, Any]:
            today_utc = datetime.fromisoformat(
                previous_or_same_xnys_session_fn(datetime.now(timezone.utc).date().isoformat())
            ).date()
            skip_risk_engine, skip_reason = serving_refresh_skip_risk_engine_fn(
                today_utc=today_utc,
            )
            if force_local_core_reads:
                with core_reads_module.core_read_backend("local"):
                    out = run_refresh_fn(
                        mode=serving_mode,
                        force_risk_recompute=False,
                        refresh_scope=refresh_scope,
                        skip_snapshot_rebuild=True,
                        skip_cuse4_foundation=True,
                        skip_risk_engine=bool(skip_risk_engine),
                        refresh_deep_health_diagnostics=bool(should_run_core),
                    )
                    out["_skip_risk_engine_reason"] = str(skip_reason)
                    out["_skip_risk_engine"] = bool(skip_risk_engine)
                    return out
            out = run_refresh_fn(
                mode=serving_mode,
                force_risk_recompute=False,
                refresh_scope=refresh_scope,
                skip_snapshot_rebuild=True,
                skip_cuse4_foundation=True,
                skip_risk_engine=bool(skip_risk_engine),
                refresh_deep_health_diagnostics=bool(should_run_core),
            )
            out["_skip_risk_engine_reason"] = str(skip_reason)
            out["_skip_risk_engine"] = bool(skip_risk_engine)
            return out

        if Path(data_db).resolve() != canonical_data_db.resolve() or Path(cache_db).resolve() != canonical_cache_db.resolve():
            with temporary_runtime_paths_cm(data_db=data_db, cache_db=cache_db):
                out = _run_refresh_inner()
        else:
            out = _run_refresh_inner()
        return {
            "status": str(out.get("status") or "ok"),
            "serving_mode": serving_mode,
            "skip_risk_engine": bool(out.get("_skip_risk_engine")),
            "skip_risk_engine_reason": str(out.get("_skip_risk_engine_reason") or ""),
            "refresh": out,
        }

    raise ValueError(f"Unknown stage: {stage}")


def _latest_price_date(db_path: Path) -> str | None:
    import sqlite3

    conn = sqlite3.connect(str(db_path))
    try:
        row = conn.execute("SELECT MAX(date) FROM security_prices_eod WHERE date IS NOT NULL").fetchone()
    except sqlite3.OperationalError:
        row = None
    finally:
        conn.close()
    return str(row[0]) if row and row[0] is not None else None


def _latest_closed_period_anchor(as_of_date: str, *, frequency: str) -> str:
    from datetime import date, timedelta

    parsed = date.fromisoformat(str(as_of_date)[:10])
    if frequency == "quarterly":
        quarter_start_month = (((parsed.month - 1) // 3) * 3) + 1
        current_period_start = date(parsed.year, quarter_start_month, 1)
    else:
        current_period_start = date(parsed.year, parsed.month, 1)
    return _previous_or_same_xnys_session((current_period_start - timedelta(days=1)).isoformat())


def _previous_or_same_xnys_session(value: str) -> str:
    from backend.trading_calendar import previous_or_same_xnys_session

    return previous_or_same_xnys_session(value)
