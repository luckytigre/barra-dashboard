from __future__ import annotations

import sqlite3
from pathlib import Path

from backend.data import registry_quote_reads


def test_registry_quote_reads_caches_table_inventory(monkeypatch, tmp_path: Path) -> None:
    calls: list[str] = []
    registry_quote_reads._cached_available_tables.cache_clear()

    monkeypatch.setattr(registry_quote_reads.core_backend, "use_neon_core_reads", lambda: False)

    def _table_exists(table: str, *, fetch_rows_fn, neon_enabled: bool) -> bool:
        calls.append(table)
        return True

    monkeypatch.setattr(registry_quote_reads.core_backend, "table_exists", _table_exists)

    data_db = tmp_path / "quotes.db"
    first = registry_quote_reads._ensure_required_tables(data_db=data_db)
    second = registry_quote_reads._ensure_required_tables(data_db=data_db)

    assert first == second
    expected_probe_count = len(registry_quote_reads._REQUIRED_TABLES) + len(registry_quote_reads._OPTIONAL_TABLES)
    assert len(calls) == expected_probe_count


def test_registry_quote_reads_invalidates_cache_when_file_revision_changes(monkeypatch, tmp_path: Path) -> None:
    calls: list[str] = []
    registry_quote_reads._cached_available_tables.cache_clear()

    monkeypatch.setattr(registry_quote_reads.core_backend, "use_neon_core_reads", lambda: False)

    def _table_exists(table: str, *, fetch_rows_fn, neon_enabled: bool) -> bool:
        calls.append(table)
        return True

    monkeypatch.setattr(registry_quote_reads.core_backend, "table_exists", _table_exists)

    data_db = tmp_path / "quotes.db"
    data_db.write_text("v1", encoding="utf-8")
    registry_quote_reads._ensure_required_tables(data_db=data_db)

    first_probe_count = len(registry_quote_reads._REQUIRED_TABLES) + len(registry_quote_reads._OPTIONAL_TABLES)
    assert len(calls) == first_probe_count

    data_db.write_text("v2-more-bytes", encoding="utf-8")
    registry_quote_reads._ensure_required_tables(data_db=data_db)

    assert len(calls) == first_probe_count * 2


def test_search_registry_typeahead_rows_uses_lightweight_discovery_fields(tmp_path: Path) -> None:
    registry_quote_reads._cached_available_tables.cache_clear()
    data_db = tmp_path / "registry.db"
    conn = sqlite3.connect(data_db)
    conn.executescript(
        """
        CREATE TABLE security_registry (
            ric TEXT PRIMARY KEY,
            ticker TEXT,
            isin TEXT,
            exchange_name TEXT,
            tracking_status TEXT
        );
        CREATE TABLE security_policy_current (
            ric TEXT PRIMARY KEY,
            price_ingest_enabled INTEGER,
            pit_fundamentals_enabled INTEGER,
            pit_classification_enabled INTEGER,
            allow_cuse_native_core INTEGER,
            allow_cuse_fundamental_projection INTEGER,
            allow_cuse_returns_projection INTEGER,
            allow_cpar_core_target INTEGER,
            allow_cpar_extended_target INTEGER
        );
        CREATE TABLE security_fundamentals_pit (
            ric TEXT,
            as_of_date TEXT,
            stat_date TEXT,
            common_name TEXT,
            updated_at TEXT
        );
        """
    )
    conn.execute(
        "INSERT INTO security_registry (ric, ticker, tracking_status) VALUES (?, ?, ?)",
        ("COST.OQ", "COST", "active"),
    )
    conn.execute(
        """
        INSERT INTO security_policy_current (
            ric,
            price_ingest_enabled,
            pit_fundamentals_enabled,
            pit_classification_enabled,
            allow_cuse_native_core,
            allow_cuse_fundamental_projection,
            allow_cuse_returns_projection,
            allow_cpar_core_target,
            allow_cpar_extended_target
        ) VALUES (?, 1, 1, 1, 1, 0, 0, 1, 1)
        """,
        ("COST.OQ",),
    )
    conn.execute(
        """
        INSERT INTO security_fundamentals_pit (
            ric,
            as_of_date,
            stat_date,
            common_name,
            updated_at
        ) VALUES (?, ?, ?, ?, ?)
        """,
        ("COST.OQ", "2026-03-01", "2026-03-01", "Costco Wholesale Corp", "2026-03-01T00:00:00Z"),
    )
    conn.commit()
    conn.close()

    rows = registry_quote_reads.search_registry_typeahead_rows(
        "costco",
        limit=5,
        as_of_date="2026-03-14",
        data_db=data_db,
    )

    assert rows == [
        {
            "ric": "COST.OQ",
            "ticker": "COST",
            "isin": None,
            "exchange_name": None,
            "tracking_status": "active",
            "price_ingest_enabled": 1,
            "pit_fundamentals_enabled": 1,
            "pit_classification_enabled": 1,
            "allow_cuse_native_core": 1,
            "allow_cuse_fundamental_projection": 0,
            "allow_cuse_returns_projection": 0,
            "allow_cpar_core_target": 1,
            "allow_cpar_extended_target": 1,
            "instrument_kind": None,
            "vehicle_structure": None,
            "issuer_country_code": None,
            "listing_country_code": None,
            "model_home_market_scope": None,
            "is_single_name_equity": 0,
            "classification_ready": 0,
            "common_name": "Costco Wholesale Corp",
            "common_name_as_of_date": "2026-03-01",
            "observation_as_of_date": None,
            "has_price_history_as_of_date": 0,
            "has_fundamentals_history_as_of_date": 0,
            "has_classification_history_as_of_date": 0,
            "latest_price_date": None,
            "latest_fundamentals_as_of_date": None,
            "latest_classification_as_of_date": None,
            "classification_as_of_date": None,
            "trbc_economic_sector": None,
            "trbc_business_sector": None,
            "trbc_industry_group": None,
            "trbc_industry": None,
            "trbc_activity": None,
            "hq_country_code": None,
            "price_date": None,
            "price": None,
            "price_field_used": None,
            "price_currency": None,
            "registry_read_mode": "typeahead",
            "price_lookup_status": "not_loaded",
            "classification_lookup_status": "not_loaded",
        }
    ]


def test_search_registry_typeahead_rows_supports_required_tables_only(tmp_path: Path) -> None:
    registry_quote_reads._cached_available_tables.cache_clear()
    data_db = tmp_path / "registry_minimal.db"
    conn = sqlite3.connect(data_db)
    conn.executescript(
        """
        CREATE TABLE security_registry (
            ric TEXT PRIMARY KEY,
            ticker TEXT,
            isin TEXT,
            exchange_name TEXT,
            tracking_status TEXT
        );
        CREATE TABLE security_policy_current (
            ric TEXT PRIMARY KEY,
            price_ingest_enabled INTEGER,
            pit_fundamentals_enabled INTEGER,
            pit_classification_enabled INTEGER,
            allow_cuse_native_core INTEGER,
            allow_cuse_fundamental_projection INTEGER,
            allow_cuse_returns_projection INTEGER,
            allow_cpar_core_target INTEGER,
            allow_cpar_extended_target INTEGER
        );
        """
    )
    conn.executemany(
        "INSERT INTO security_registry (ric, ticker, tracking_status) VALUES (?, ?, ?)",
        [
            ("AAPL.OQ", "AAPL", "active"),
            ("AAPL.DE", "AAPLD", "inactive"),
        ],
    )
    conn.execute(
        """
        INSERT INTO security_policy_current (
            ric,
            price_ingest_enabled,
            pit_fundamentals_enabled,
            pit_classification_enabled,
            allow_cuse_native_core,
            allow_cuse_fundamental_projection,
            allow_cuse_returns_projection,
            allow_cpar_core_target,
            allow_cpar_extended_target
        ) VALUES (?, 1, 0, 0, 1, 0, 0, 1, 0)
        """,
        ("AAPL.OQ",),
    )
    conn.commit()
    conn.close()

    rows = registry_quote_reads.search_registry_typeahead_rows(
        "aapl",
        limit=10,
        as_of_date="2026-03-14",
        data_db=data_db,
    )

    assert [row["ric"] for row in rows] == ["AAPL.OQ"]
    assert rows[0]["common_name"] is None
    assert rows[0]["registry_read_mode"] == "typeahead"
    assert rows[0]["price_lookup_status"] == "not_loaded"
    assert rows[0]["classification_lookup_status"] == "not_loaded"
