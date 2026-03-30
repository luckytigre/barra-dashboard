from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd
import pytest

from backend.data import core_reads, source_reads
from backend.universe.schema import ensure_cuse4_schema


def test_load_latest_prices_sqlite_refreshes_latest_price_cache(monkeypatch, tmp_path: Path) -> None:
    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    conn.execute("CREATE TABLE security_master (ric TEXT PRIMARY KEY, ticker TEXT)")
    conn.execute(
        """
        CREATE TABLE security_prices_eod (
            ric TEXT NOT NULL,
            date TEXT NOT NULL,
            close REAL,
            PRIMARY KEY (ric, date)
        )
        """
    )
    conn.execute("INSERT INTO security_master (ric, ticker) VALUES ('AAPL.OQ', 'AAPL')")
    conn.executemany(
        "INSERT INTO security_prices_eod (ric, date, close) VALUES (?, ?, ?)",
        [
            ("AAPL.OQ", "2026-03-01", 100.0),
            ("AAPL.OQ", "2026-03-02", 101.0),
        ],
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(core_reads, "DATA_DB", data_db)
    monkeypatch.setattr(core_reads.core_backend, "use_neon_core_reads", lambda: False)

    first = core_reads.load_latest_prices()
    assert first.to_dict("records") == [
        {"ric": "AAPL.OQ", "ticker": "AAPL", "date": "2026-03-02", "close": 101.0}
    ]

    conn = sqlite3.connect(str(data_db))
    conn.execute(
        "INSERT INTO security_prices_eod (ric, date, close) VALUES (?, ?, ?)",
        ("AAPL.OQ", "2026-03-03", 102.5),
    )
    conn.commit()
    conn.close()

    second = core_reads.load_latest_prices()
    assert second.to_dict("records") == [
        {"ric": "AAPL.OQ", "ticker": "AAPL", "date": "2026-03-03", "close": 102.5}
    ]


def test_load_latest_prices_sqlite_prefers_registry_runtime_surfaces(monkeypatch, tmp_path: Path) -> None:
    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    conn.execute(
        """
        CREATE TABLE security_registry (
            ric TEXT PRIMARY KEY,
            ticker TEXT,
            isin TEXT,
            exchange_name TEXT,
            tracking_status TEXT NOT NULL DEFAULT 'active',
            source TEXT,
            job_run_id TEXT,
            updated_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE security_policy_current (
            ric TEXT PRIMARY KEY,
            price_ingest_enabled INTEGER NOT NULL DEFAULT 1,
            pit_fundamentals_enabled INTEGER NOT NULL DEFAULT 1,
            pit_classification_enabled INTEGER NOT NULL DEFAULT 1,
            allow_cuse_native_core INTEGER NOT NULL DEFAULT 1,
            allow_cuse_fundamental_projection INTEGER NOT NULL DEFAULT 0,
            allow_cuse_returns_projection INTEGER NOT NULL DEFAULT 0,
            allow_cpar_core_target INTEGER NOT NULL DEFAULT 1,
            allow_cpar_extended_target INTEGER NOT NULL DEFAULT 1,
            policy_source TEXT,
            job_run_id TEXT,
            updated_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE security_taxonomy_current (
            ric TEXT PRIMARY KEY,
            instrument_kind TEXT,
            vehicle_structure TEXT,
            issuer_country_code TEXT,
            listing_country_code TEXT,
            model_home_market_scope TEXT,
            is_single_name_equity INTEGER NOT NULL DEFAULT 0,
            classification_ready INTEGER NOT NULL DEFAULT 0,
            source TEXT,
            job_run_id TEXT,
            updated_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE security_prices_eod (
            ric TEXT NOT NULL,
            date TEXT NOT NULL,
            close REAL,
            PRIMARY KEY (ric, date)
        )
        """
    )
    conn.execute(
        """
        INSERT INTO security_registry (ric, ticker, tracking_status, updated_at)
        VALUES ('AAPL.OQ', 'AAPL', 'active', '2026-03-01T00:00:00Z')
        """
    )
    conn.execute(
        """
        INSERT INTO security_policy_current (
            ric, price_ingest_enabled, pit_fundamentals_enabled, pit_classification_enabled,
            allow_cuse_native_core, allow_cuse_fundamental_projection, allow_cuse_returns_projection,
            allow_cpar_core_target, allow_cpar_extended_target, updated_at
        ) VALUES ('AAPL.OQ', 1, 1, 1, 1, 0, 0, 1, 1, '2026-03-01T00:00:00Z')
        """
    )
    conn.execute(
        """
        INSERT INTO security_taxonomy_current (
            ric, instrument_kind, vehicle_structure, model_home_market_scope,
            is_single_name_equity, classification_ready, updated_at
        ) VALUES ('AAPL.OQ', 'single_name_equity', 'equity_security', 'us', 1, 1, '2026-03-01T00:00:00Z')
        """
    )
    conn.executemany(
        "INSERT INTO security_prices_eod (ric, date, close) VALUES (?, ?, ?)",
        [
            ("AAPL.OQ", "2026-03-01", 100.0),
            ("AAPL.OQ", "2026-03-03", 102.5),
        ],
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(core_reads, "DATA_DB", data_db)
    monkeypatch.setattr(core_reads.core_backend, "use_neon_core_reads", lambda: False)

    out = core_reads.load_latest_prices()

    assert out.to_dict("records") == [
        {"ric": "AAPL.OQ", "ticker": "AAPL", "date": "2026-03-03", "close": 102.5}
    ]


def test_load_latest_prices_fails_closed_without_registry_even_if_compat_current_exists(
    tmp_path: Path,
) -> None:
    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    ensure_cuse4_schema(conn)
    conn.execute(
        """
        INSERT INTO security_master (
            ric, ticker, classification_ok, is_equity_eligible, coverage_role, source, updated_at
        ) VALUES ('AAPL.OQ', 'WRONG', 1, 1, 'native_equity', 'legacy_master', '2026-03-13T00:00:00Z')
        """
    )
    conn.execute(
        """
        INSERT INTO security_master_compat_current (
            ric, ticker, isin, exchange_name, classification_ok, is_equity_eligible, coverage_role, source, job_run_id, updated_at
        ) VALUES ('AAPL.OQ', 'AAPL', 'US0378331005', 'NASDAQ', 1, 1, 'native_equity', 'compat', 'job_1', '2026-03-13T00:00:00Z')
        """
    )
    conn.execute(
        """
        INSERT INTO security_prices_eod (ric, date, close, volume, currency, source, updated_at)
        VALUES ('AAPL.OQ', '2026-03-13', 102.5, 1000.0, 'USD', 'prices', '2026-03-13T00:00:00Z')
        """
    )
    conn.commit()
    conn.close()

    def _fetch(sql: str, params=None):
        with sqlite3.connect(str(data_db)) as read_conn:
            read_conn.row_factory = sqlite3.Row
            return [dict(row) for row in read_conn.execute(sql, params or []).fetchall()]

    def _missing(*tables: str) -> list[str]:
        with sqlite3.connect(str(data_db)) as read_conn:
            return [
                table
                for table in tables
                if read_conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
                    (table,),
                ).fetchone()
                is None
            ]

    with pytest.raises(RuntimeError, match="registry-first runtime tables"):
        source_reads.load_latest_prices(
            tickers=["AAPL"],
            fetch_rows_fn=_fetch,
            missing_tables_fn=_missing,
        )


def test_load_latest_prices_fails_closed_without_registry_when_only_compat_current_exists(
    tmp_path: Path,
) -> None:
    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    ensure_cuse4_schema(conn)
    conn.execute(
        """
        INSERT INTO security_master_compat_current (
            ric, ticker, isin, exchange_name, classification_ok, is_equity_eligible, coverage_role, source, job_run_id, updated_at
        ) VALUES ('AAPL.OQ', 'AAPL', 'US0378331005', 'NASDAQ', 1, 1, 'native_equity', 'compat', 'job_1', '2026-03-13T00:00:00Z')
        """
    )
    conn.execute(
        """
        INSERT INTO security_prices_eod (ric, date, close, volume, currency, source, updated_at)
        VALUES ('AAPL.OQ', '2026-03-13', 102.5, 1000.0, 'USD', 'prices', '2026-03-13T00:00:00Z')
        """
    )
    conn.commit()
    conn.close()

    def _fetch(sql: str, params=None):
        with sqlite3.connect(str(data_db)) as read_conn:
            read_conn.row_factory = sqlite3.Row
            return [dict(row) for row in read_conn.execute(sql, params or []).fetchall()]

    def _missing(*tables: str) -> list[str]:
        with sqlite3.connect(str(data_db)) as read_conn:
            return [
                table
                for table in tables
                if read_conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
                    (table,),
                ).fetchone()
                is None
            ]

    with pytest.raises(RuntimeError, match="registry-first runtime tables"):
        source_reads.load_latest_prices(
            tickers=["AAPL"],
            fetch_rows_fn=_fetch,
            missing_tables_fn=_missing,
        )


def test_load_latest_prices_full_universe_stays_bounded_to_registry_rows_when_registry_exists(
    monkeypatch,
    tmp_path: Path,
) -> None:
    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    ensure_cuse4_schema(conn)
    now = "2026-03-13T00:00:00Z"
    conn.executemany(
        """
        INSERT INTO security_registry (
            ric, ticker, tracking_status, source, updated_at
        ) VALUES (?, ?, 'active', 'registry', ?)
        """,
        [
            ("AAPL.OQ", "AAPL", now),
        ],
    )
    conn.execute(
        """
        INSERT INTO security_policy_current (
            ric, price_ingest_enabled, pit_fundamentals_enabled, pit_classification_enabled,
            allow_cuse_native_core, allow_cuse_fundamental_projection, allow_cuse_returns_projection,
            allow_cpar_core_target, allow_cpar_extended_target, updated_at
        ) VALUES (?, 1, 1, 1, 1, 0, 0, 1, 1, ?)
        """,
        ("AAPL.OQ", now),
    )
    conn.execute(
        """
        INSERT INTO security_taxonomy_current (
            ric, instrument_kind, vehicle_structure, model_home_market_scope,
            is_single_name_equity, classification_ready, updated_at
        ) VALUES ('AAPL.OQ', 'single_name_equity', 'equity_security', 'us', 1, 1, ?)
        """,
        (now,),
    )
    conn.executemany(
        """
        INSERT INTO security_master_compat_current (
            ric, ticker, isin, exchange_name, classification_ok, is_equity_eligible, coverage_role, source, job_run_id, updated_at
        ) VALUES (?, ?, NULL, NULL, 1, 1, 'native_equity', 'compat', 'job_1', ?)
        """,
        [
            ("AAPL.OQ", "AAPL", now),
            ("ORPH.X", "ORPH", now),
        ],
    )
    conn.executemany(
        """
        INSERT INTO security_prices_eod (ric, date, close, volume, currency, source, updated_at)
        VALUES (?, '2026-03-13', ?, 1000.0, 'USD', 'prices', ?)
        """,
        [
            ("AAPL.OQ", 102.5, now),
            ("ORPH.X", 250.0, now),
        ],
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(core_reads, "DATA_DB", data_db)
    monkeypatch.setattr(core_reads.core_backend, "use_neon_core_reads", lambda: False)

    out = core_reads.load_latest_prices()

    assert out.to_dict("records") == [
        {"ric": "AAPL.OQ", "ticker": "AAPL", "date": "2026-03-13", "close": 102.5}
    ]


def test_load_latest_prices_full_universe_does_not_supplement_compat_rows(
    tmp_path: Path,
) -> None:
    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    ensure_cuse4_schema(conn)
    now = "2026-03-13T00:00:00Z"
    conn.executemany(
        """
        INSERT INTO security_registry (ric, ticker, tracking_status, updated_at)
        VALUES (?, ?, 'active', ?)
        """,
        [
            ("AAPL.OQ", "AAPL", now),
        ],
    )
    conn.execute(
        """
        INSERT INTO security_policy_current (
            ric, price_ingest_enabled, pit_fundamentals_enabled, pit_classification_enabled,
            allow_cuse_native_core, allow_cuse_fundamental_projection, allow_cuse_returns_projection,
            allow_cpar_core_target, allow_cpar_extended_target, updated_at
        ) VALUES ('AAPL.OQ', 1, 1, 1, 1, 0, 0, 1, 1, ?)
        """,
        (now,),
    )
    conn.executemany(
        """
        INSERT INTO security_master_compat_current (
            ric, ticker, isin, exchange_name, classification_ok, is_equity_eligible, coverage_role, source, job_run_id, updated_at
        ) VALUES (?, ?, NULL, NULL, 1, 1, 'native_equity', 'compat', 'job_1', ?)
        """,
        [
            ("AAPL.OQ", "AAPL", now),
            ("ORPH.X", "ORPH", now),
        ],
    )
    conn.executemany(
        """
        INSERT INTO security_prices_eod (ric, date, close, volume, currency, source, updated_at)
        VALUES (?, '2026-03-13', ?, 1000.0, 'USD', 'prices', ?)
        """,
        [
            ("AAPL.OQ", 102.5, now),
            ("ORPH.X", 250.0, now),
        ],
    )
    conn.commit()
    conn.close()

    def _fetch(sql: str, params=None):
        with sqlite3.connect(str(data_db)) as read_conn:
            read_conn.row_factory = sqlite3.Row
            return [dict(row) for row in read_conn.execute(sql, params or []).fetchall()]

    def _missing(*tables: str) -> list[str]:
        with sqlite3.connect(str(data_db)) as read_conn:
            return [
                table
                for table in tables
                if read_conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
                    (table,),
                ).fetchone()
                is None
            ]

    out = source_reads.load_latest_prices(
        tickers=None,
        fetch_rows_fn=_fetch,
        missing_tables_fn=_missing,
    )

    assert out.to_dict("records") == [
        {"ric": "AAPL.OQ", "ticker": "AAPL", "date": "2026-03-13", "close": 102.5}
    ]


def test_load_latest_prices_requested_disabled_registry_ticker_does_not_resurrect_from_compat(
    tmp_path: Path,
) -> None:
    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    ensure_cuse4_schema(conn)
    now = "2026-03-13T00:00:00Z"
    conn.execute(
        """
        INSERT INTO security_registry (ric, ticker, tracking_status, updated_at)
        VALUES ('SPY.P', 'SPY', 'disabled', ?)
        """,
        (now,),
    )
    conn.execute(
        """
        INSERT INTO security_policy_current (
            ric, price_ingest_enabled, pit_fundamentals_enabled, pit_classification_enabled,
            allow_cuse_native_core, allow_cuse_fundamental_projection, allow_cuse_returns_projection,
            allow_cpar_core_target, allow_cpar_extended_target, updated_at
        ) VALUES ('SPY.P', 1, 0, 0, 0, 0, 1, 0, 1, ?)
        """,
        (now,),
    )
    conn.execute(
        """
        INSERT INTO security_master_compat_current (
            ric, ticker, isin, exchange_name, classification_ok, is_equity_eligible, coverage_role, source, job_run_id, updated_at
        ) VALUES ('SPY.P', 'SPY', NULL, NULL, 0, 0, 'projection_only', 'compat', 'job_spy', ?)
        """,
        (now,),
    )
    conn.execute(
        """
        INSERT INTO security_prices_eod (ric, date, close, volume, currency, source, updated_at)
        VALUES ('SPY.P', '2026-03-13', 501.0, 1000.0, 'USD', 'prices', ?)
        """,
        (now,),
    )
    conn.commit()
    conn.close()

    def _fetch(sql: str, params=None):
        with sqlite3.connect(str(data_db)) as read_conn:
            read_conn.row_factory = sqlite3.Row
            return [dict(row) for row in read_conn.execute(sql, params or []).fetchall()]

    def _missing(*tables: str) -> list[str]:
        with sqlite3.connect(str(data_db)) as read_conn:
            return [
                table
                for table in tables
                if read_conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
                    (table,),
                ).fetchone()
                is None
            ]

    out = source_reads.load_latest_prices(
        tickers=["SPY"],
        fetch_rows_fn=_fetch,
        missing_tables_fn=_missing,
    )

    assert out.empty


def test_load_latest_prices_duplicate_registry_ticker_uses_active_row_for_request_scoped_compat(
    tmp_path: Path,
) -> None:
    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    ensure_cuse4_schema(conn)
    now = "2026-03-13T00:00:00Z"
    conn.executemany(
        """
        INSERT INTO security_registry (ric, ticker, tracking_status, updated_at)
        VALUES (?, 'DUPL', ?, ?)
        """,
        [
            ("DUPL.OQ", "disabled", now),
            ("DUPL.N", "active", now),
        ],
    )
    conn.execute(
        """
        INSERT INTO security_policy_current (
            ric, price_ingest_enabled, pit_fundamentals_enabled, pit_classification_enabled,
            allow_cuse_native_core, allow_cuse_fundamental_projection, allow_cuse_returns_projection,
            allow_cpar_core_target, allow_cpar_extended_target, updated_at
        ) VALUES ('DUPL.OQ', 1, 0, 0, 0, 0, 1, 0, 1, ?)
        """,
        (now,),
    )
    conn.execute(
        """
        INSERT INTO security_master_compat_current (
            ric, ticker, isin, exchange_name, classification_ok, is_equity_eligible, coverage_role, source, job_run_id, updated_at
        ) VALUES ('DUPL.N', 'DUPL', NULL, NULL, 0, 0, 'projection_only', 'compat', 'job_dupl', ?)
        """,
        (now,),
    )
    conn.execute(
        """
        INSERT INTO security_prices_eod (ric, date, close, volume, currency, source, updated_at)
        VALUES ('DUPL.N', '2026-03-13', 77.0, 1000.0, 'USD', 'prices', ?)
        """,
        (now,),
    )
    conn.commit()
    conn.close()

    def _fetch(sql: str, params=None):
        with sqlite3.connect(str(data_db)) as read_conn:
            read_conn.row_factory = sqlite3.Row
            return [dict(row) for row in read_conn.execute(sql, params or []).fetchall()]

    def _missing(*tables: str) -> list[str]:
        with sqlite3.connect(str(data_db)) as read_conn:
            return [
                table
                for table in tables
                if read_conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
                    (table,),
                ).fetchone()
                is None
            ]

    out = source_reads.load_latest_prices(
        tickers=["DUPL"],
        fetch_rows_fn=_fetch,
        missing_tables_fn=_missing,
    )

    assert out.to_dict("records") == [
        {"ric": "DUPL.N", "ticker": "DUPL", "date": "2026-03-13", "close": 77.0}
    ]


def test_load_latest_prices_does_not_fallback_to_compat_current_without_registry(
    tmp_path: Path,
) -> None:
    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    ensure_cuse4_schema(conn)
    conn.executemany(
        """
        INSERT INTO security_master (
            ric, ticker, classification_ok, is_equity_eligible, coverage_role, source, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("AAPL.OQ", "AAPL", 1, 1, "native_equity", "legacy_master", "2026-03-13T00:00:00Z"),
            ("SPY.P", "SPY", 0, 0, "projection_only", "legacy_master", "2026-03-13T00:00:00Z"),
        ],
    )
    conn.execute(
        """
        INSERT INTO security_master_compat_current (
            ric, ticker, isin, exchange_name, classification_ok, is_equity_eligible, coverage_role, source, job_run_id, updated_at
        ) VALUES (?, ?, NULL, NULL, ?, ?, ?, 'compat', ?, '2026-03-13T00:00:00Z')
        """,
        ("AAPL.OQ", "AAPL", 1, 1, "native_equity", "job_aapl"),
    )
    conn.execute(
        """
        INSERT INTO security_master_compat_current (
            ric, ticker, isin, exchange_name, classification_ok, is_equity_eligible, coverage_role, source, job_run_id, updated_at
        ) VALUES (?, ?, NULL, NULL, ?, ?, ?, 'compat', ?, '2026-03-13T00:00:00Z')
        """,
        ("SPY.P", "SPY", 0, 0, "projection_only", "job_spy"),
    )
    conn.executemany(
        """
        INSERT INTO security_prices_eod (ric, date, close, volume, currency, source, updated_at)
        VALUES (?, '2026-03-13', ?, 1000.0, 'USD', 'prices', '2026-03-13T00:00:00Z')
        """,
        [
            ("AAPL.OQ", 102.5),
            ("SPY.P", 501.0),
        ],
    )
    conn.commit()
    conn.close()

    def _fetch(sql: str, params=None):
        with sqlite3.connect(str(data_db)) as read_conn:
            read_conn.row_factory = sqlite3.Row
            return [dict(row) for row in read_conn.execute(sql, params or []).fetchall()]

    def _missing(*tables: str) -> list[str]:
        with sqlite3.connect(str(data_db)) as read_conn:
            return [
                table
                for table in tables
                if read_conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
                    (table,),
                ).fetchone()
                is None
            ]

    with pytest.raises(RuntimeError, match="registry-first runtime tables"):
        source_reads.load_latest_prices(
            tickers=["AAPL", "SPY"],
            fetch_rows_fn=_fetch,
            missing_tables_fn=_missing,
        )


def test_load_latest_prices_requested_registry_gap_does_not_supplement_compat_rows(
    tmp_path: Path,
) -> None:
    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    ensure_cuse4_schema(conn)
    now = "2026-03-13T00:00:00Z"
    conn.executemany(
        """
        INSERT INTO security_registry (ric, ticker, tracking_status, updated_at)
        VALUES (?, ?, 'active', ?)
        """,
        [
            ("AAPL.OQ", "AAPL", now),
            ("OTHER.OQ", "OTHER", now),
        ],
    )
    conn.execute(
        """
        INSERT INTO security_policy_current (
            ric, price_ingest_enabled, pit_fundamentals_enabled, pit_classification_enabled,
            allow_cuse_native_core, allow_cuse_fundamental_projection, allow_cuse_returns_projection,
            allow_cpar_core_target, allow_cpar_extended_target, updated_at
        ) VALUES ('AAPL.OQ', 1, 1, 1, 1, 0, 0, 1, 1, ?)
        """,
        (now,),
    )
    conn.executemany(
        """
        INSERT INTO security_master_compat_current (
            ric, ticker, isin, exchange_name, classification_ok, is_equity_eligible, coverage_role, source, job_run_id, updated_at
        ) VALUES (?, ?, NULL, NULL, 1, 1, 'native_equity', 'compat', 'job_1', ?)
        """,
        [
            ("AAPL.OQ", "AAPL", now),
            ("OTHER.OQ", "OTHER", now),
        ],
    )
    conn.executemany(
        """
        INSERT INTO security_prices_eod (ric, date, close, volume, currency, source, updated_at)
        VALUES (?, '2026-03-13', ?, 1000.0, 'USD', 'prices', ?)
        """,
        [
            ("AAPL.OQ", 102.5, now),
            ("OTHER.OQ", 250.0, now),
        ],
    )
    conn.commit()
    conn.close()

    def _fetch(sql: str, params=None):
        with sqlite3.connect(str(data_db)) as read_conn:
            read_conn.row_factory = sqlite3.Row
            return [dict(row) for row in read_conn.execute(sql, params or []).fetchall()]

    def _missing(*tables: str) -> list[str]:
        with sqlite3.connect(str(data_db)) as read_conn:
            return [
                table
                for table in tables
                if read_conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
                    (table,),
                ).fetchone()
                is None
            ]

    out = source_reads.load_latest_prices(
        tickers=["AAPL", "OTHER"],
        fetch_rows_fn=_fetch,
        missing_tables_fn=_missing,
    )

    assert out.to_dict("records") == [
        {"ric": "AAPL.OQ", "ticker": "AAPL", "date": "2026-03-13", "close": 102.5},
    ]


def test_load_latest_prices_registry_query_keeps_direct_price_ric_joins() -> None:
    captured: list[str] = []

    def _fetch(sql: str, params=None):
        if "registry_row_count" in sql:
            return [
                {
                    "registry_row_count": 1,
                    "active_registry_row_count": 1,
                    "active_missing_companion_count": 0,
                }
            ]
        captured.append(sql)
        return []

    out = source_reads.load_latest_prices(
        tickers=["AAPL"],
        fetch_rows_fn=_fetch,
        missing_tables_fn=lambda *tables: [],
    )

    assert out.empty
    assert any("ON rr.ric = p.ric" in sql for sql in captured)
    assert all("UPPER(p.ric)" not in sql for sql in captured if "ON rr.ric = p.ric" in sql)
    assert all("security_master_compat_current" not in sql for sql in captured)


def test_load_latest_prices_uses_registry_first_contract_for_neon_core_reads(monkeypatch) -> None:
    connect_calls = 0

    class _FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def execute(self, sql, params=None):
            self._sql = str(sql)

        def fetchall(self):
            return [{"ok": 1}]

    class _FakeConn:
        closed = False

        def cursor(self, row_factory=None):
            return _FakeCursor()

        def close(self):
            self.closed = True

    def _fake_connect(*args, **kwargs):
        nonlocal connect_calls
        connect_calls += 1
        return _FakeConn()

    def _fake_loader(*, tickers, fetch_rows_fn, missing_tables_fn):
        assert missing_tables_fn("security_prices_eod") == []
        fetch_rows_fn("SELECT 1")
        return pd.DataFrame([{"ric": "AAPL.OQ", "ticker": "AAPL", "date": "2026-03-15", "close": 100.0}])

    monkeypatch.setattr(core_reads.core_backend, "use_neon_core_reads", lambda: True)
    monkeypatch.setattr(core_reads.core_backend, "connect", _fake_connect)
    monkeypatch.setattr(core_reads.core_backend, "resolve_dsn", lambda _explicit=None: "postgres://example")
    monkeypatch.setattr(core_reads.source_reads, "load_latest_prices", _fake_loader)

    out = core_reads.load_latest_prices(tickers=["AAPL"])

    assert out.to_dict("records") == [{"ric": "AAPL.OQ", "ticker": "AAPL", "date": "2026-03-15", "close": 100.0}]
    assert connect_calls == 1
