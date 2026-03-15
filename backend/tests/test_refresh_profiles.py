from __future__ import annotations

import sqlite3
import importlib
from pathlib import Path

import pytest

run_model_pipeline = importlib.import_module("backend.orchestration.run_model_pipeline")
from backend.services import refresh_manager


def test_default_profile_is_local_daily_plus_core(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(refresh_manager.config, "APP_RUNTIME_ROLE", "local-ingest")
    assert refresh_manager._resolve_profile(None) == "source-daily-plus-core-if-due"


def test_default_profile_is_cloud_serve_refresh(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(refresh_manager.config, "APP_RUNTIME_ROLE", "cloud-serve")
    assert refresh_manager._resolve_profile(None) == "serve-refresh"


def test_unknown_profile_is_rejected() -> None:
    with pytest.raises(ValueError, match="Invalid profile"):
        refresh_manager._resolve_profile("daily-with-core-if-due")


def test_invalid_stage_window_is_rejected_before_worker_start() -> None:
    with pytest.raises(ValueError, match="--from-stage must be before or equal to --to-stage"):
        refresh_manager.start_refresh(
            force_risk_recompute=False,
            profile="serve-refresh",
            from_stage="risk_model",
            to_stage="ingest",
        )


def test_force_core_conflict_is_rejected_before_worker_start() -> None:
    with pytest.raises(ValueError, match="force_core requires a stage window"):
        refresh_manager.start_refresh(
            force_risk_recompute=False,
            force_core=True,
            profile="serve-refresh",
            from_stage="serving_refresh",
            to_stage="serving_refresh",
        )


def test_force_core_adds_core_stages_for_serve_refresh_defaults() -> None:
    selected = run_model_pipeline._apply_force_core_stage_selection(
        selected=["serving_refresh"],
        force_core=True,
        from_stage=None,
        to_stage=None,
    )

    assert selected == ["factor_returns", "risk_model", "serving_refresh"]


def test_force_core_rejects_explicit_stage_window_without_core_stages() -> None:
    with pytest.raises(ValueError, match="force_core requires a stage window"):
        run_model_pipeline._apply_force_core_stage_selection(
            selected=["serving_refresh"],
            force_core=True,
            from_stage="serving_refresh",
            to_stage="serving_refresh",
        )


def test_planned_stages_for_profile_rejects_invalid_force_core_window() -> None:
    with pytest.raises(ValueError, match="force_core requires a stage window"):
        run_model_pipeline.planned_stages_for_profile(
            profile="serve-refresh",
            from_stage="serving_refresh",
            to_stage="serving_refresh",
            force_core=True,
        )


def test_cold_profile_config_enables_full_rebuild_and_cache_reset() -> None:
    cfg = run_model_pipeline.PROFILE_CONFIG["cold-core"]
    assert cfg["core_policy"] == "always"
    assert cfg["serving_mode"] == "full"
    assert cfg["raw_history_policy"] == "full-daily"
    assert bool(cfg["reset_core_cache"]) is True


def test_publish_only_profile_config_reuses_cached_payloads() -> None:
    cfg = run_model_pipeline.PROFILE_CONFIG["publish-only"]
    assert cfg["core_policy"] == "never"
    assert cfg["serving_mode"] == "publish"
    assert cfg["raw_history_policy"] == "none"
    assert cfg["default_stages"] == ["serving_refresh"]


def test_source_daily_profile_enables_ingest_without_core() -> None:
    cfg = run_model_pipeline.PROFILE_CONFIG["source-daily"]
    assert cfg["core_policy"] == "never"
    assert cfg["enable_ingest"] is True
    assert cfg["default_stages"] == ["ingest", "serving_refresh"]


def test_cli_profile_choices_are_canonical_only() -> None:
    choices = sorted(run_model_pipeline.PROFILE_CONFIG.keys())
    assert "serve-refresh" in choices
    assert "daily-fast" not in choices


def test_profile_catalog_has_no_runtime_aliases() -> None:
    catalog = run_model_pipeline.profile_catalog()
    assert catalog
    assert all(item["aliases"] == [] for item in catalog)


def test_reset_core_caches_clears_core_tables(tmp_path: Path) -> None:
    cache_db = tmp_path / "cache.db"
    conn = sqlite3.connect(str(cache_db))
    conn.execute("CREATE TABLE daily_factor_returns (date TEXT, factor_name TEXT, factor_return REAL)")
    conn.execute("CREATE TABLE daily_specific_residuals (date TEXT, ric TEXT, residual REAL)")
    conn.execute("CREATE TABLE daily_universe_eligibility_summary (date TEXT, exposure_n INTEGER)")
    conn.execute("CREATE TABLE daily_factor_returns_meta (key TEXT, value TEXT)")
    conn.execute("CREATE TABLE cache (key TEXT PRIMARY KEY, value TEXT, updated_at REAL)")
    conn.execute("INSERT INTO daily_factor_returns VALUES ('2026-03-03', 'Liquidity', 0.0)")
    conn.execute("INSERT INTO daily_specific_residuals VALUES ('2026-03-03', 'AAPL.OQ', 0.01)")
    conn.execute("INSERT INTO daily_universe_eligibility_summary VALUES ('2026-03-03', 100)")
    conn.execute("INSERT INTO daily_factor_returns_meta VALUES ('method_version', 'v1')")
    conn.execute("INSERT INTO cache VALUES ('risk_engine_cov', '{}', 0)")
    conn.execute("INSERT INTO cache VALUES ('risk_engine_specific_risk', '{}', 0)")
    conn.execute("INSERT INTO cache VALUES ('risk_engine_meta', '{}', 0)")
    conn.execute("INSERT INTO cache VALUES ('unrelated', '{}', 0)")
    conn.commit()
    conn.close()

    summary = run_model_pipeline._reset_core_caches(cache_db)

    assert summary["daily_factor_returns"] == 1
    assert summary["daily_specific_residuals"] == 1
    assert summary["daily_universe_eligibility_summary"] == 1
    assert summary["daily_factor_returns_meta"] == 1
    assert summary["cache_risk_engine_keys"] == 3

    conn = sqlite3.connect(str(cache_db))
    assert conn.execute("SELECT COUNT(*) FROM daily_factor_returns").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM daily_specific_residuals").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM daily_universe_eligibility_summary").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM daily_factor_returns_meta").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM cache WHERE key='unrelated'").fetchone()[0] == 1
    conn.close()


def test_serving_refresh_skip_risk_engine_requires_current_method(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(run_model_pipeline, "_risk_cache_ready", lambda: True)
    monkeypatch.setattr(
        run_model_pipeline.sqlite,
        "cache_get_live_first",
        lambda key: {"method_version": "stale", "last_recompute_date": "2026-03-01"} if key == "risk_engine_meta" else None,
    )
    monkeypatch.setattr(
        run_model_pipeline,
        "_risk_recompute_due",
        lambda meta, *, today_utc: (True, "method_version_change"),
    )

    skip, reason = run_model_pipeline._serving_refresh_skip_risk_engine(
        today_utc=run_model_pipeline.date(2026, 3, 14)
    )

    assert skip is False
    assert reason == "core_due_method_version_change"


def test_serving_refresh_skip_risk_engine_allows_current_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(run_model_pipeline, "_risk_cache_ready", lambda: True)
    monkeypatch.setattr(
        run_model_pipeline.sqlite,
        "cache_get_live_first",
        lambda key: {"method_version": "current", "last_recompute_date": "2026-03-13"} if key == "risk_engine_meta" else None,
    )
    monkeypatch.setattr(
        run_model_pipeline,
        "_risk_recompute_due",
        lambda meta, *, today_utc: (False, "within_interval"),
    )

    skip, reason = run_model_pipeline._serving_refresh_skip_risk_engine(
        today_utc=run_model_pipeline.date(2026, 3, 14)
    )

    assert skip is True
    assert reason == "risk_cache_current"
