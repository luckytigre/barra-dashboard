from __future__ import annotations

import pytest

from backend.services import refresh_control_service


def test_start_refresh_delegates_to_refresh_manager_when_cloud_jobs_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(refresh_control_service.config, "CLOUD_RUN_JOBS_ENABLED", False)
    monkeypatch.setattr(refresh_control_service.config, "APP_RUNTIME_ROLE", "cloud-serve")

    captured: dict[str, object] = {}

    def _start_refresh(**kwargs):
        captured.update(kwargs)
        return True, {"status": "running"}

    monkeypatch.setattr(
        "backend.services.refresh_manager.start_refresh",
        _start_refresh,
    )

    started, state = refresh_control_service.start_refresh(
        force_risk_recompute=False,
        profile="serve-refresh",
    )

    assert started is True
    assert state["status"] == "running"
    assert captured["profile"] == "serve-refresh"


def test_start_refresh_dispatches_cloud_run_job_when_configured(monkeypatch: pytest.MonkeyPatch) -> None:
    state_store: dict[str, object] = {}
    started_calls: list[dict[str, object]] = []

    monkeypatch.setattr(refresh_control_service.config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(refresh_control_service.config, "CLOUD_RUN_JOBS_ENABLED", True)
    monkeypatch.setattr(refresh_control_service.config, "CLOUD_RUN_PROJECT_ID", "proj")
    monkeypatch.setattr(refresh_control_service.config, "CLOUD_RUN_REGION", "us-east4")
    monkeypatch.setattr(refresh_control_service.config, "SERVE_REFRESH_CLOUD_RUN_JOB_NAME", "ceiora-prod-serve-refresh")
    monkeypatch.setattr(refresh_control_service, "load_persisted_refresh_status", lambda: dict(state_store))
    monkeypatch.setattr(
        refresh_control_service,
        "persist_refresh_status",
        lambda state: state_store.update(state) or dict(state_store),
    )
    monkeypatch.setattr(
        refresh_control_service,
        "try_claim_refresh_status",
        lambda updates: (state_store.update(updates) or True, dict(state_store)),
    )
    monkeypatch.setattr(
        refresh_control_service,
        "mark_refresh_started",
        lambda **kwargs: started_calls.append(kwargs),
    )
    monkeypatch.setattr(
        refresh_control_service.cloud_run_jobs,
        "dispatch_serve_refresh",
        lambda **kwargs: {"execution_name": "executions/abc", "metadata": kwargs},
    )

    started, state = refresh_control_service.start_refresh(
        force_risk_recompute=False,
        profile="serve-refresh",
        refresh_scope="holdings_only",
    )

    assert started is True
    assert state["status"] == "running"
    assert state["dispatch_backend"] == "cloud_run_job"
    assert state["dispatch_id"] == "executions/abc"
    assert state["profile"] == "serve-refresh"
    assert started_calls[0]["profile"] == "serve-refresh"
    assert str(started_calls[0]["run_id"]).startswith("crj_")


def test_start_refresh_reports_cloud_run_dispatch_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    state_store: dict[str, object] = {}
    finished_calls: list[dict[str, object]] = []

    monkeypatch.setattr(refresh_control_service.config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(refresh_control_service.config, "CLOUD_RUN_JOBS_ENABLED", True)
    monkeypatch.setattr(refresh_control_service.config, "CLOUD_RUN_PROJECT_ID", "proj")
    monkeypatch.setattr(refresh_control_service.config, "CLOUD_RUN_REGION", "us-east4")
    monkeypatch.setattr(refresh_control_service.config, "SERVE_REFRESH_CLOUD_RUN_JOB_NAME", "ceiora-prod-serve-refresh")
    monkeypatch.setattr(refresh_control_service, "load_persisted_refresh_status", lambda: dict(state_store))
    monkeypatch.setattr(
        refresh_control_service,
        "persist_refresh_status",
        lambda state: state_store.update(state) or dict(state_store),
    )
    monkeypatch.setattr(
        refresh_control_service,
        "try_claim_refresh_status",
        lambda updates: (state_store.update(updates) or True, dict(state_store)),
    )
    monkeypatch.setattr(refresh_control_service, "mark_refresh_started", lambda **kwargs: None)
    monkeypatch.setattr(
        refresh_control_service,
        "mark_refresh_finished",
        lambda **kwargs: finished_calls.append(kwargs),
    )
    monkeypatch.setattr(
        refresh_control_service.cloud_run_jobs,
        "dispatch_serve_refresh",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("dispatch failed")),
    )

    started, state = refresh_control_service.start_refresh(
        force_risk_recompute=False,
        profile="serve-refresh",
    )

    assert started is False
    assert state["status"] == "failed"
    assert state["dispatch_backend"] == "cloud_run_job"
    assert state["error"]["message"] == "dispatch failed"
    assert finished_calls[0]["status"] == "failed"


def test_start_refresh_refuses_duplicate_cloud_run_dispatch_when_claim_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    current = {
        "status": "running",
        "dispatch_backend": "cloud_run_job",
        "pipeline_run_id": "crj_existing",
    }

    monkeypatch.setattr(refresh_control_service.config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(refresh_control_service.config, "CLOUD_RUN_JOBS_ENABLED", True)
    monkeypatch.setattr(refresh_control_service.config, "CLOUD_RUN_PROJECT_ID", "proj")
    monkeypatch.setattr(refresh_control_service.config, "CLOUD_RUN_REGION", "us-east4")
    monkeypatch.setattr(refresh_control_service.config, "SERVE_REFRESH_CLOUD_RUN_JOB_NAME", "ceiora-prod-serve-refresh")
    monkeypatch.setattr(
        refresh_control_service,
        "try_claim_refresh_status",
        lambda updates: (False, dict(current)),
    )
    monkeypatch.setattr(
        refresh_control_service.cloud_run_jobs,
        "dispatch_serve_refresh",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("dispatch should not be called")),
    )

    started, state = refresh_control_service.start_refresh(
        force_risk_recompute=False,
        profile="serve-refresh",
    )

    assert started is False
    assert state == current


def test_get_refresh_status_reads_persisted_state_when_cloud_jobs_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(refresh_control_service.config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(refresh_control_service.config, "CLOUD_RUN_JOBS_ENABLED", True)
    monkeypatch.setattr(refresh_control_service.config, "CLOUD_RUN_PROJECT_ID", "proj")
    monkeypatch.setattr(refresh_control_service.config, "CLOUD_RUN_REGION", "us-east4")
    monkeypatch.setattr(refresh_control_service.config, "SERVE_REFRESH_CLOUD_RUN_JOB_NAME", "ceiora-prod-serve-refresh")
    monkeypatch.setattr(
        refresh_control_service,
        "load_persisted_refresh_status",
        lambda: {"status": "running", "dispatch_backend": "cloud_run_job"},
    )

    assert refresh_control_service.get_refresh_status() == {
        "status": "running",
        "dispatch_backend": "cloud_run_job",
    }
