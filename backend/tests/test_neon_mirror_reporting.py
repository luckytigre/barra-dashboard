from __future__ import annotations

import json
from pathlib import Path

from backend.services import neon_mirror_reporting


def test_write_neon_mirror_artifact_persists_report_files(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(neon_mirror_reporting.config, "APP_DATA_DIR", str(tmp_path))

    artifact_path = neon_mirror_reporting.write_neon_mirror_artifact(
        run_id="job_1",
        profile="source-daily",
        as_of_date="2026-03-20",
        overall_status="ok",
        neon_mirror={"status": "ok", "sync": {"status": "ok"}},
    )

    payload = json.loads(Path(artifact_path).read_text(encoding="utf-8"))
    latest_path = tmp_path / "audit_reports" / "neon_parity" / "latest_neon_mirror_report.json"

    assert payload["run_id"] == "job_1"
    assert payload["profile"] == "source-daily"
    assert payload["neon_mirror"]["status"] == "ok"
    assert latest_path.exists()


def test_latest_neon_mirror_artifact_for_run_selects_newest(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(neon_mirror_reporting.config, "APP_DATA_DIR", str(tmp_path))
    reports = tmp_path / "audit_reports" / "neon_parity"
    reports.mkdir(parents=True)
    older = reports / "neon_mirror_20260322T170000Z_job_1.json"
    newer = reports / "neon_mirror_20260322T180000Z_job_1.json"
    older.write_text("{}", encoding="utf-8")
    newer.write_text("{}", encoding="utf-8")

    out = neon_mirror_reporting.latest_neon_mirror_artifact_for_run(run_id="job_1")

    assert out == newer


def test_extract_neon_mirror_error_reads_nested_step_error() -> None:
    out = neon_mirror_reporting.extract_neon_mirror_error(
        {
            "status": "mismatch",
            "parity": {
                "status": "mismatch",
                "error": {
                    "type": "RuntimeError",
                    "message": "parity mismatch",
                },
            },
        }
    )

    assert out == {
        "type": "RuntimeError",
        "message": "parity mismatch",
    }


def test_repair_neon_sync_health_from_existing_workspace_republishes_clean_parity(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(neon_mirror_reporting.config, "APP_DATA_DIR", str(tmp_path))
    reports = tmp_path / "audit_reports" / "neon_parity"
    reports.mkdir(parents=True)
    workspace = tmp_path / "neon_rebuild_workspace" / "job_1"
    workspace.mkdir(parents=True)
    (workspace / "data.db").write_text("", encoding="utf-8")
    (workspace / "cache.db").write_text("", encoding="utf-8")

    prior_artifact = reports / "neon_mirror_20260322T170000Z_job_1.json"
    prior_artifact.write_text(
        json.dumps(
            {
                "run_id": "job_1",
                "profile": "core-weekly",
                "as_of_date": "2026-03-20",
                "overall_status": "failed",
                "neon_mirror": {
                    "status": "mismatch",
                    "sync": {"status": "ok"},
                    "prune": {"status": "ok"},
                    "parity": {"status": "mismatch", "issues": ["mismatch:model_factor_returns_daily"]},
                },
            }
        ),
        encoding="utf-8",
    )

    captured: dict[str, object] = {}
    monkeypatch.setattr(
        neon_mirror_reporting.neon_mirror_service,
        "run_bounded_parity_audit",
        lambda **_kwargs: {"status": "ok", "issues": []},
    )
    monkeypatch.setattr(
        neon_mirror_reporting,
        "publish_neon_sync_health",
        lambda **kwargs: captured.update(kwargs),
    )

    out = neon_mirror_reporting.repair_neon_sync_health_from_existing_workspace(
        run_id="job_1",
        profile="core-weekly",
        as_of_date="2026-03-20",
        workspace_sqlite_path=workspace / "data.db",
        workspace_cache_path=workspace / "cache.db",
    )

    assert out["status"] == "ok"
    assert out["parity_status"] == "ok"
    assert Path(out["artifact_path"]).exists()
    assert captured["run_id"] == "job_1"
    assert captured["profile"] == "core-weekly"
    assert captured["as_of_date"] == "2026-03-20"
    assert isinstance(captured["neon_mirror"], dict)
    assert captured["neon_mirror"]["status"] == "ok"
    assert captured["neon_mirror"]["parity"] == {"status": "ok", "issues": []}


def test_publish_neon_sync_health_reports_warning_for_skipped_mirror(monkeypatch) -> None:
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        neon_mirror_reporting,
        "persist_runtime_health_payload",
        lambda key, payload: captured.update({"key": key, "payload": payload}) or {"status": "ok"},
    )

    neon_mirror_reporting.publish_neon_sync_health(
        run_id="job_1",
        profile="serve-refresh",
        as_of_date="2026-03-20",
        neon_mirror={"status": "skipped", "reason": "profile_skips_broad_neon_mirror"},
        artifact_path=None,
    )

    assert captured["key"] == "neon_sync_health"
    payload = captured["payload"]
    assert isinstance(payload, dict)
    assert payload["status"] == "warning"
    assert payload["mirror_status"] == "skipped"
    assert payload["artifact_path"] is None
