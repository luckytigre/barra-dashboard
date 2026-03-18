from __future__ import annotations

import sqlite3
from pathlib import Path

from backend.services import neon_stage2


def test_canonical_tables_include_durable_model_outputs() -> None:
    tables = neon_stage2.canonical_tables()

    assert "model_factor_returns_daily" in tables
    assert "model_factor_covariance_daily" in tables
    assert "model_specific_risk_daily" in tables
    assert "model_run_metadata" in tables
    assert "projected_instrument_loadings" in tables
    assert "projected_instrument_meta" in tables


def test_canonical_schema_defines_durable_model_tables() -> None:
    schema_path = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "reference"
        / "migrations"
        / "neon"
        / "NEON_CANONICAL_SCHEMA.sql"
    )
    schema_sql = schema_path.read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS model_factor_returns_daily" in schema_sql
    assert "CREATE TABLE IF NOT EXISTS model_factor_covariance_daily" in schema_sql
    assert "CREATE TABLE IF NOT EXISTS model_specific_risk_daily" in schema_sql
    assert "CREATE TABLE IF NOT EXISTS model_run_metadata" in schema_sql
    assert "ADD COLUMN IF NOT EXISTS run_id TEXT" in schema_sql


class _DummyPgConn:
    def commit(self) -> None:
        return None

    def rollback(self) -> None:
        return None

    def close(self) -> None:
        return None


def test_sync_from_sqlite_to_neon_skips_missing_projection_tables(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "data.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE security_master (ric TEXT PRIMARY KEY)")
    conn.commit()
    conn.close()

    monkeypatch.setattr(neon_stage2, "connect", lambda **_kwargs: _DummyPgConn())
    monkeypatch.setattr(neon_stage2, "resolve_dsn", lambda dsn: dsn)

    out = neon_stage2.sync_from_sqlite_to_neon(
        sqlite_path=db_path,
        dsn="postgresql://example",
        tables=["projected_instrument_loadings"],
    )

    assert out["status"] == "ok"
    assert out["tables"]["projected_instrument_loadings"] == {
        "status": "skipped_missing_source"
    }
