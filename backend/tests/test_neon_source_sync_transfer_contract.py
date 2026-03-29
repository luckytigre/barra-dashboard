from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from backend.services import neon_stage2


class _DummyPgConn:
    class _Cursor:
        rowcount = 0

        class _Copy:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def write_row(self, _row) -> None:
                return None

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, _query, _params=None) -> None:
            return None

        def executemany(self, _query, _params=None) -> None:
            return None

        def fetchone(self):
            return (1, "2026-03-01", "2026-03-01")

        def copy(self, _query):
            return self._Copy()

    def cursor(self):
        return self._Cursor()

    def commit(self) -> None:
        return None

    def rollback(self) -> None:
        return None

    def close(self) -> None:
        return None


def _create_prices_sqlite(db_path: Path) -> None:
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE security_prices_eod (
            ric TEXT,
            date TEXT,
            close REAL
        )
        """
    )
    conn.executemany(
        "INSERT INTO security_prices_eod (ric, date, close) VALUES (?, ?, ?)",
        [
            ("AAA.OQ", "2026-03-01", 10.0),
            ("AAA.OQ", "2026-03-15", 11.0),
            ("BBB.OQ", "2026-03-01", 20.0),
            ("BBB.OQ", "2026-03-15", 21.0),
        ],
    )
    conn.commit()
    conn.close()


def _stub_source_sync_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(neon_stage2, "connect", lambda **_kwargs: _DummyPgConn())
    monkeypatch.setattr(neon_stage2, "resolve_dsn", lambda dsn: dsn)
    monkeypatch.setattr(neon_stage2, "_require_source_sync_metadata_tables", lambda _pg_conn: None)
    monkeypatch.setattr(neon_stage2, "_record_source_sync_run_start", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(neon_stage2, "_finalize_source_sync_run", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(neon_stage2, "_upsert_source_sync_watermarks", lambda *_args, **_kwargs: 0)
    monkeypatch.setattr(
        neon_stage2,
        "_materialize_security_source_status_current_pg",
        lambda *_args, **_kwargs: 0,
    )


def _stub_price_sync_table(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        neon_stage2,
        "ensure_target_columns_from_sqlite",
        lambda *_args, **_kwargs: {"status": "ok", "added_columns": []},
    )
    monkeypatch.setattr(neon_stage2, "_pg_columns", lambda _pg_conn, _table: ["ric", "date", "close"])
    monkeypatch.setattr(
        neon_stage2,
        "_assert_post_load_row_counts",
        lambda *_args, **_kwargs: {"status": "ok"},
    )


def test_sync_from_sqlite_to_neon_supports_plain_incremental_overlap_reload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "data.db"
    _create_prices_sqlite(db_path)
    copied_batches: list[list[tuple[object, ...]]] = []

    _stub_source_sync_metadata(monkeypatch)
    _stub_price_sync_table(monkeypatch)
    monkeypatch.setattr(neon_stage2, "_pg_max_date", lambda _pg_conn, **_kwargs: "2026-03-17")
    monkeypatch.setattr(neon_stage2, "_pg_min_date", lambda _pg_conn, **_kwargs: "2026-03-01")
    monkeypatch.setattr(
        neon_stage2,
        "_pg_entity_min_dates",
        lambda _pg_conn, **_kwargs: {"AAA.OQ": "2026-03-01", "BBB.OQ": "2026-03-01"},
    )
    monkeypatch.setattr(
        neon_stage2,
        "_delete_pg_rows_for_entities",
        lambda *_args, **_kwargs: pytest.fail("identifier backfill delete should not run"),
    )

    def _fake_copy(_pg_conn, *, table: str, columns: list[str], pk_cols: list[str], rows) -> int:
        assert table == "security_prices_eod"
        assert columns == ["ric", "date", "close"]
        assert pk_cols == ["ric", "date"]
        batch = list(rows)
        copied_batches.append(batch)
        return len(batch)

    monkeypatch.setattr(neon_stage2, "_copy_into_postgres_idempotent", _fake_copy)

    out = neon_stage2.sync_from_sqlite_to_neon(
        sqlite_path=db_path,
        dsn="postgresql://example",
        tables=["security_prices_eod"],
        mode="incremental",
    )

    table_out = out["tables"]["security_prices_eod"]
    assert table_out["action"] == "incremental_overlap_reload"
    assert table_out["identifier_backfill"] is None
    assert table_out["source_rows"] == 2
    assert table_out["rows_loaded"] == 2
    assert copied_batches == [[("AAA.OQ", "2026-03-15", 11.0), ("BBB.OQ", "2026-03-15", 21.0)]]


def test_sync_from_sqlite_to_neon_uses_target_empty_truncate_and_reload_when_neon_table_is_empty(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "data.db"
    _create_prices_sqlite(db_path)

    _stub_source_sync_metadata(monkeypatch)
    _stub_price_sync_table(monkeypatch)
    monkeypatch.setattr(neon_stage2, "_pg_max_date", lambda _pg_conn, **_kwargs: None)

    copied_batches: list[list[tuple[object, ...]]] = []

    def _fake_copy(_pg_conn, *, table: str, columns: list[str], pk_cols: list[str], rows) -> int:
        batch = list(rows)
        copied_batches.append(batch)
        return len(batch)

    monkeypatch.setattr(neon_stage2, "_copy_into_postgres_idempotent", _fake_copy)

    out = neon_stage2.sync_from_sqlite_to_neon(
        sqlite_path=db_path,
        dsn="postgresql://example",
        tables=["security_prices_eod"],
        mode="incremental",
    )

    table_out = out["tables"]["security_prices_eod"]
    assert table_out["action"] == "target_empty_truncate_and_reload"
    assert table_out["source_rows"] == 4
    assert table_out["rows_loaded"] == 4
    assert copied_batches == [[
        ("AAA.OQ", "2026-03-01", 10.0),
        ("AAA.OQ", "2026-03-15", 11.0),
        ("BBB.OQ", "2026-03-01", 20.0),
        ("BBB.OQ", "2026-03-15", 21.0),
    ]]


def test_sync_from_sqlite_to_neon_raises_on_source_row_mismatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "data.db"
    _create_prices_sqlite(db_path)

    _stub_source_sync_metadata(monkeypatch)
    _stub_price_sync_table(monkeypatch)
    monkeypatch.setattr(neon_stage2, "_pg_max_date", lambda _pg_conn, **_kwargs: "2026-03-17")
    monkeypatch.setattr(neon_stage2, "_pg_min_date", lambda _pg_conn, **_kwargs: "2026-03-01")
    monkeypatch.setattr(
        neon_stage2,
        "_pg_entity_min_dates",
        lambda _pg_conn, **_kwargs: {"AAA.OQ": "2026-03-01", "BBB.OQ": "2026-03-01"},
    )
    monkeypatch.setattr(
        neon_stage2,
        "_delete_pg_rows_for_entities",
        lambda *_args, **_kwargs: pytest.fail("identifier backfill delete should not run"),
    )
    monkeypatch.setattr(
        neon_stage2,
        "_copy_into_postgres_idempotent",
        lambda *_args, **_kwargs: 1,
    )

    with pytest.raises(RuntimeError, match="source row mismatch for security_prices_eod"):
        neon_stage2.sync_from_sqlite_to_neon(
            sqlite_path=db_path,
            dsn="postgresql://example",
            tables=["security_prices_eod"],
            mode="incremental",
        )


def test_sync_from_sqlite_to_neon_uses_mode_specific_copy_owner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "data.db"
    _create_prices_sqlite(db_path)

    _stub_source_sync_metadata(monkeypatch)
    _stub_price_sync_table(monkeypatch)
    monkeypatch.setattr(neon_stage2, "_pg_max_date", lambda _pg_conn, **_kwargs: "2026-03-17")
    monkeypatch.setattr(neon_stage2, "_pg_min_date", lambda _pg_conn, **_kwargs: "2026-03-01")
    monkeypatch.setattr(
        neon_stage2,
        "_pg_entity_min_dates",
        lambda _pg_conn, **_kwargs: {"AAA.OQ": "2026-03-01", "BBB.OQ": "2026-03-01"},
    )
    monkeypatch.setattr(
        neon_stage2,
        "_delete_pg_rows_for_entities",
        lambda *_args, **_kwargs: pytest.fail("identifier backfill delete should not run"),
    )

    incremental_calls = {"copy": 0, "idempotent": 0}

    def _fake_idempotent(_pg_conn, *, rows, **_kwargs) -> int:
        incremental_calls["idempotent"] += 1
        return len(list(rows))

    def _unexpected_copy(*_args, **_kwargs):
        pytest.fail("full-mode copy owner should not run on incremental mode")

    monkeypatch.setattr(neon_stage2, "_copy_into_postgres_idempotent", _fake_idempotent)
    monkeypatch.setattr(neon_stage2, "_copy_into_postgres", _unexpected_copy)

    out = neon_stage2.sync_from_sqlite_to_neon(
        sqlite_path=db_path,
        dsn="postgresql://example",
        tables=["security_prices_eod"],
        mode="incremental",
    )

    assert out["tables"]["security_prices_eod"]["rows_loaded"] == 2
    assert incremental_calls == {"copy": 0, "idempotent": 1}

    full_calls = {"copy": 0, "idempotent": 0}

    def _fake_copy(_pg_conn, *, rows, **_kwargs) -> int:
        full_calls["copy"] += 1
        return len(list(rows))

    def _unexpected_idempotent(*_args, **_kwargs):
        pytest.fail("incremental copy owner should not run on full mode")

    monkeypatch.setattr(neon_stage2, "_copy_into_postgres", _fake_copy)
    monkeypatch.setattr(neon_stage2, "_copy_into_postgres_idempotent", _unexpected_idempotent)

    out = neon_stage2.sync_from_sqlite_to_neon(
        sqlite_path=db_path,
        dsn="postgresql://example",
        tables=["security_prices_eod"],
        mode="full",
    )

    assert out["tables"]["security_prices_eod"]["rows_loaded"] == 4
    assert out["tables"]["security_prices_eod"]["action"] == "truncate_and_reload"
    assert full_calls == {"copy": 1, "idempotent": 0}
