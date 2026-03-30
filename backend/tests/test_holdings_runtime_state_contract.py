from __future__ import annotations

from pathlib import Path

import pytest

from backend.services import holdings_runtime_state


def test_holdings_runtime_state_round_trip(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    recorded: dict[str, object] = {}

    def _capture(value: object) -> dict[str, object]:
        recorded["value"] = value
        return {"status": "ok"}

    monkeypatch.setattr(holdings_runtime_state, "_load_state", lambda: recorded.get("value"))
    monkeypatch.setattr(holdings_runtime_state, "_persist_state", _capture)

    dirty = holdings_runtime_state.mark_holdings_dirty(
        action="holdings_import:replace_account",
        account_id="main",
        summary="replace import applied",
        import_batch_id="batch_1",
        change_count=3,
    )
    assert dirty["pending"] is True
    assert dirty["pending_count"] == 3
    assert dirty["last_import_batch_id"] == "batch_1"

    holdings_runtime_state.mark_refresh_started(profile="serve-refresh", run_id="run_1")
    clean = holdings_runtime_state.mark_refresh_finished(
        profile="serve-refresh",
        run_id="run_1",
        status="ok",
        message="Serving outputs refreshed",
        clear_pending=True,
    )
    assert clean["pending"] is False
    assert clean["pending_count"] == 0
    assert clean["dirty_since"] is None
    assert clean["last_refresh_profile"] == "serve-refresh"


def test_holdings_runtime_state_does_not_clear_newer_dirty_revision(monkeypatch: pytest.MonkeyPatch) -> None:
    recorded: dict[str, object] = {}

    monkeypatch.setattr(holdings_runtime_state, "_load_state", lambda: recorded.get("value"))
    monkeypatch.setattr(
        holdings_runtime_state,
        "_persist_state",
        lambda value: recorded.update({"value": value}) or {"status": "ok"},
    )

    holdings_runtime_state.mark_holdings_dirty(
        action="holdings_position_edit",
        account_id="main",
        summary="first edit",
        import_batch_id="batch_1",
        change_count=1,
    )
    holdings_runtime_state.mark_refresh_started(profile="serve-refresh", run_id="run_1")
    second_dirty = holdings_runtime_state.mark_holdings_dirty(
        action="holdings_position_edit",
        account_id="main",
        summary="second edit",
        import_batch_id="batch_2",
        change_count=1,
    )
    finished = holdings_runtime_state.mark_refresh_finished(
        profile="serve-refresh",
        run_id="run_1",
        status="ok",
        message="Serving outputs refreshed",
        clear_pending=True,
    )

    assert second_dirty["dirty_revision"] == 2
    assert finished["pending"] is True
    assert finished["pending_count"] == 2
