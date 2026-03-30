from __future__ import annotations

import pytest

from backend.portfolio import positions_store


def test_positions_store_prefers_neon_when_dsn_present(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(positions_store.config, "DATA_BACKEND", "sqlite")
    monkeypatch.setattr(positions_store.config, "NEON_DATABASE_URL", "postgres://example")
    monkeypatch.setattr(
        positions_store,
        "_load_positions_from_neon",
        lambda: ({"AAPL": 10.0}, {"AAPL": {"account": "MAIN", "sleeve": "NEON HOLDINGS", "source": "NEON"}}),
    )

    shares, meta = positions_store._load_positions()

    assert shares == {"AAPL": 10.0}
    assert meta["AAPL"]["source"] == "NEON"


def test_positions_store_mock_fallback_without_neon(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(positions_store.config, "DATA_BACKEND", "sqlite")
    monkeypatch.setattr(positions_store.config, "NEON_DATABASE_URL", "")
    monkeypatch.setattr(
        positions_store,
        "_load_positions_from_neon",
        lambda: (_ for _ in ()).throw(RuntimeError("should not call neon")),
    )

    shares, meta = positions_store._load_positions()

    assert shares == positions_store.PORTFOLIO_POSITIONS
    assert meta == positions_store.POSITION_META


def test_positions_store_raises_when_neon_expected_but_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(positions_store.config, "DATA_BACKEND", "neon")
    monkeypatch.setattr(positions_store.config, "NEON_DATABASE_URL", "postgres://example")
    monkeypatch.setattr(
        positions_store,
        "_load_positions_from_neon",
        lambda: (_ for _ in ()).throw(RuntimeError("dsn failed")),
    )

    with pytest.raises(positions_store.HoldingsUnavailableError):
        positions_store._load_positions()
