from __future__ import annotations

import sqlite3
from pathlib import Path

from backend.services import neon_mirror


class _DummyPgConn:
    class _Cursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, _query, _params=None) -> None:
            return None

        def fetchone(self):
            return (1,)

        def fetchall(self):
            return []

    def cursor(self):
        return self._Cursor()

    def close(self) -> None:
        return None


def _create_sqlite_runtime(db_path: Path, cache_path: Path) -> None:
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE security_master (ric TEXT PRIMARY KEY)")
    conn.execute("INSERT INTO security_master (ric) VALUES ('ABC.N')")
    conn.execute("CREATE TABLE security_prices_eod (ric TEXT, date TEXT)")
    conn.execute("INSERT INTO security_prices_eod (ric, date) VALUES ('ABC.N', '2026-03-01')")
    conn.execute(
        "CREATE TABLE security_fundamentals_pit (ric TEXT, as_of_date TEXT, stat_date TEXT)"
    )
    conn.execute(
        "INSERT INTO security_fundamentals_pit (ric, as_of_date, stat_date) VALUES ('ABC.N', '2026-03-01', '2025-12-31')"
    )
    conn.execute(
        "CREATE TABLE security_classification_pit (ric TEXT, as_of_date TEXT)"
    )
    conn.execute(
        "INSERT INTO security_classification_pit (ric, as_of_date) VALUES ('ABC.N', '2026-03-01')"
    )
    conn.execute(
        "CREATE TABLE barra_raw_cross_section_history (ric TEXT, as_of_date TEXT)"
    )
    conn.execute(
        "INSERT INTO barra_raw_cross_section_history (ric, as_of_date) VALUES ('ABC.N', '2026-03-01')"
    )
    conn.commit()
    conn.close()

    cache = sqlite3.connect(str(cache_path))
    cache.execute(
        """
        CREATE TABLE daily_factor_returns (
            date TEXT NOT NULL,
            factor_name TEXT NOT NULL,
            factor_return REAL NOT NULL,
            robust_se REAL NOT NULL DEFAULT 0.0,
            t_stat REAL NOT NULL DEFAULT 0.0,
            r_squared REAL NOT NULL,
            residual_vol REAL NOT NULL,
            cross_section_n INTEGER NOT NULL DEFAULT 0,
            eligible_n INTEGER NOT NULL DEFAULT 0,
            coverage REAL NOT NULL DEFAULT 0.0
        )
        """
    )
    cache.executemany(
        """
        INSERT INTO daily_factor_returns (
            date, factor_name, factor_return, robust_se, t_stat, r_squared,
            residual_vol, cross_section_n, eligible_n, coverage
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("2026-03-02", "Beta", 0.01, 0.005, 2.0, 0.3, 0.2, 100, 95, 0.95),
            ("2026-03-02", "Book-to-Price", -0.02, 0.010, -2.0, 0.3, 0.2, 100, 95, 0.95),
        ],
    )
    cache.commit()
    cache.close()


def test_run_bounded_parity_audit_detects_factor_return_value_drift(
    tmp_path: Path,
    monkeypatch,
) -> None:
    sqlite_path = tmp_path / "data.db"
    cache_path = tmp_path / "cache.db"
    _create_sqlite_runtime(sqlite_path, cache_path)

    def _fake_pg_count_window(_pg_conn, *, table: str, date_col: str | None, cutoff: str | None, distinct_col: str | None = "ric"):
        if table == "model_factor_returns_daily":
            return {
                "row_count": 2,
                "min_date": "2026-03-02",
                "max_date": "2026-03-02",
                "latest_distinct": None,
            }
        return {
            "row_count": 1,
            "min_date": "2026-03-01" if date_col else None,
            "max_date": "2026-03-01" if date_col else None,
            "latest_distinct": 1 if date_col else None,
        }

    monkeypatch.setattr(neon_mirror, "connect", lambda **_kwargs: _DummyPgConn())
    monkeypatch.setattr(neon_mirror, "resolve_dsn", lambda dsn: dsn)
    monkeypatch.setattr(neon_mirror, "_pg_table_exists", lambda _pg_conn, _table: True)
    monkeypatch.setattr(
        neon_mirror,
        "_pg_columns",
        lambda _pg_conn, table: (
            ["date", "factor_name", "factor_return", "robust_se", "t_stat", "r_squared", "residual_vol", "cross_section_n", "eligible_n", "coverage"]
            if table == "model_factor_returns_daily"
            else ["ric", "date"]
        ),
    )
    monkeypatch.setattr(neon_mirror, "_pg_count_window", _fake_pg_count_window)
    monkeypatch.setattr(
        neon_mirror,
        "_pg_non_null_counts",
        lambda _pg_conn, *, table, columns, date_col=None, cutoff=None: {col: 2 for col in columns} if table == "model_factor_returns_daily" else {col: 1 for col in columns},
    )
    monkeypatch.setattr(
        neon_mirror,
        "_pg_group_count_by_date",
        lambda _pg_conn, *, table, date_col, dates: {date: 2 for date in dates},
    )
    monkeypatch.setattr(
        neon_mirror,
        "_pg_factor_return_values",
        lambda _pg_conn, *, table, dates: {
            ("2026-03-02", "Beta"): (0.99, 0.005, 2.0, 0.3, 0.2, 100.0, 95.0, 0.95),
            ("2026-03-02", "Book-to-Price"): (-0.02, 0.010, -2.0, 0.3, 0.2, 100.0, 95.0, 0.95),
        },
    )

    out = neon_mirror.run_bounded_parity_audit(
        sqlite_path=sqlite_path,
        cache_path=cache_path,
        dsn="postgresql://example",
        analytics_years=5,
    )

    assert out["status"] == "mismatch"
    assert "value_mismatch:model_factor_returns_daily" in out["issues"]
    table_out = out["tables"]["model_factor_returns_daily"]
    assert table_out["value_check_status"] == "mismatch"
    assert any("Beta" in issue for issue in table_out["value_check_issues"])


def test_run_bounded_parity_audit_reports_factor_return_inference_coverage(
    tmp_path: Path,
    monkeypatch,
) -> None:
    sqlite_path = tmp_path / "data.db"
    cache_path = tmp_path / "cache.db"
    _create_sqlite_runtime(sqlite_path, cache_path)

    def _fake_pg_count_window(_pg_conn, *, table: str, date_col: str | None, cutoff: str | None, distinct_col: str | None = "ric"):
        if table == "model_factor_returns_daily":
            return {
                "row_count": 2,
                "min_date": "2026-03-02",
                "max_date": "2026-03-02",
                "latest_distinct": None,
            }
        return {
            "row_count": 1,
            "min_date": "2026-03-01" if date_col else None,
            "max_date": "2026-03-01" if date_col else None,
            "latest_distinct": 1 if date_col else None,
        }

    monkeypatch.setattr(neon_mirror, "connect", lambda **_kwargs: _DummyPgConn())
    monkeypatch.setattr(neon_mirror, "resolve_dsn", lambda dsn: dsn)
    monkeypatch.setattr(neon_mirror, "_pg_table_exists", lambda _pg_conn, _table: True)
    monkeypatch.setattr(
        neon_mirror,
        "_pg_columns",
        lambda _pg_conn, table: (
            ["date", "factor_name", "factor_return", "robust_se", "t_stat", "r_squared", "residual_vol", "cross_section_n", "eligible_n", "coverage"]
            if table == "model_factor_returns_daily"
            else ["ric", "date"]
        ),
    )
    monkeypatch.setattr(neon_mirror, "_pg_count_window", _fake_pg_count_window)
    monkeypatch.setattr(
        neon_mirror,
        "_pg_non_null_counts",
        lambda _pg_conn, *, table, columns, date_col=None, cutoff=None: {
            **{col: 2 for col in columns},
            "robust_se": 0,
            "t_stat": 0,
        } if table == "model_factor_returns_daily" else {col: 1 for col in columns},
    )
    monkeypatch.setattr(
        neon_mirror,
        "_pg_group_count_by_date",
        lambda _pg_conn, *, table, date_col, dates: {date: 2 for date in dates},
    )
    monkeypatch.setattr(
        neon_mirror,
        "_pg_factor_return_values",
        lambda _pg_conn, *, table, dates: {
            ("2026-03-02", "Beta"): (0.01, 0.0, 0.0, 0.3, 0.2, 100.0, 95.0, 0.95),
            ("2026-03-02", "Book-to-Price"): (-0.02, 0.0, 0.0, 0.3, 0.2, 100.0, 95.0, 0.95),
        },
    )

    out = neon_mirror.run_bounded_parity_audit(
        sqlite_path=sqlite_path,
        cache_path=cache_path,
        dsn="postgresql://example",
        analytics_years=5,
    )

    assert out["status"] == "mismatch"
    assert "nonnull_mismatch:model_factor_returns_daily" in out["issues"]
    table_out = out["tables"]["model_factor_returns_daily"]
    assert table_out["source_non_null_counts"]["robust_se"] == 2
    assert table_out["target_non_null_counts"]["robust_se"] == 0
