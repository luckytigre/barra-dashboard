from __future__ import annotations

from backend.services import holdings_service


class _FakeConn:
    def close(self) -> None:
        return None

    def rollback(self) -> None:
        return None


def test_run_position_upsert_noop_skips_dirty_and_refresh(monkeypatch) -> None:
    calls = {"dirty": 0, "refresh": 0}

    monkeypatch.setattr(holdings_service, "resolve_dsn", lambda _dsn=None: "postgres://example")
    monkeypatch.setattr(holdings_service, "connect", lambda **kwargs: _FakeConn())
    monkeypatch.setattr(
        holdings_service,
        "apply_single_position_edit",
        lambda *args, **kwargs: {
            "status": "ok",
            "action": "none",
            "account_id": "main",
            "ric": "AAPL.OQ",
            "ticker": "AAPL",
            "quantity": 10.0,
            "import_batch_id": "batch_1",
        },
    )
    monkeypatch.setattr(
        holdings_service,
        "record_holdings_dirty",
        lambda **kwargs: calls.__setitem__("dirty", calls["dirty"] + 1),
    )
    monkeypatch.setattr(
        holdings_service,
        "trigger_light_refresh_if_requested",
        lambda trigger: calls.__setitem__("refresh", calls["refresh"] + int(bool(trigger))) or None,
    )

    out = holdings_service.run_position_upsert(
        account_id="main",
        ric="AAPL.OQ",
        quantity=10,
        trigger_refresh=False,
    )

    assert out["action"] == "none"
    assert calls["dirty"] == 0
    assert calls["refresh"] == 0
