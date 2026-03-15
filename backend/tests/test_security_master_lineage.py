from __future__ import annotations

import importlib
import sys
import sqlite3
import types
from pathlib import Path

import pandas as pd

from backend.scripts import augment_security_master_from_ric_xlsx
from backend.scripts import download_data_lseg
from backend.scripts.export_security_master_seed import export_seed
from backend.universe.bootstrap import bootstrap_cuse4_source_tables
from backend.universe.schema import ensure_cuse4_schema


def _row_by_ric(db_path: Path, ric: str) -> sqlite3.Row:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            """
            SELECT *
            FROM security_master
            WHERE ric = ?
            """,
            (ric,),
        ).fetchone()
        assert row is not None
        return row
    finally:
        conn.close()


def test_bootstrap_syncs_seed_registry_without_trusting_seed_flags(tmp_path: Path) -> None:
    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    ensure_cuse4_schema(conn)
    conn.execute(
        """
        INSERT INTO security_master (
            ric, ticker, isin, exchange_name, classification_ok, is_equity_eligible, source, job_run_id, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "AAPL.OQ",
            "AAPL",
            "US0378331005",
            "NASDAQ",
            1,
            1,
            "lseg_toolkit",
            "job_1",
            "2026-03-15T00:00:00+00:00",
        ),
    )
    conn.commit()
    conn.close()

    seed_path = tmp_path / "security_master_seed.csv"
    seed_path.write_text(
        "\n".join(
            [
                "ric,ticker,sid,permid,isin,instrument_type,asset_category_description,exchange_name,classification_ok,is_equity_eligible,source,job_run_id,updated_at",
                "AAPL.OQ,AAPL,123,456,US0378331005,Common Stock,Equity,NASDAQ,0,0,seed,row1,2026-03-01T00:00:00+00:00",
                "BABA.N,BABA,789,012,US01609W1027,Common Stock,Equity,NYSE,1,1,seed,row2,2026-03-01T00:00:00+00:00",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    out = bootstrap_cuse4_source_tables(db_path=data_db, seed_path=seed_path)

    assert out["status"] == "ok"
    assert out["mode"] == "bootstrap_only"
    assert out["seed_sync"]["status"] == "ok"

    existing = _row_by_ric(data_db, "AAPL.OQ")
    assert int(existing["classification_ok"]) == 1
    assert int(existing["is_equity_eligible"]) == 1
    assert existing["source"] == "lseg_toolkit"

    seeded = _row_by_ric(data_db, "BABA.N")
    assert seeded["ticker"] == "BABA"
    assert seeded["isin"] == "US01609W1027"
    assert seeded["exchange_name"] == "NYSE"
    assert int(seeded["classification_ok"]) == 0
    assert int(seeded["is_equity_eligible"]) == 0
    assert seeded["source"] == "security_master_seed"


def test_download_from_lseg_updates_security_master_for_pending_explicit_ric(
    monkeypatch,
    tmp_path: Path,
) -> None:
    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    ensure_cuse4_schema(conn)
    conn.execute(
        """
        INSERT INTO security_master (
            ric, ticker, classification_ok, is_equity_eligible, source, job_run_id, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        ("TEST.OQ", "TEST", 0, 0, "security_master_seed", "seed_job", "2026-03-15T00:00:00+00:00"),
    )
    conn.commit()
    conn.close()

    class FakeClient:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def get_company_data(self, batch, *, fields, as_of_date):
            assert batch == ["TEST.OQ"]
            assert "TR.TickerSymbol" in fields
            assert "TR.ISIN" in fields
            assert "TR.ExchangeName" in fields
            assert "TR.TRBCEconomicSector" in fields
            return pd.DataFrame(
                [
                    {
                        "Instrument": "TEST.OQ",
                        "Ticker Symbol": "TEST",
                        "ISIN": "US0000000001",
                        "Exchange Name": "NASDAQ",
                        "TRBC Economic Sector Name": "Technology",
                        "TRBC Business Sector Name": "Software & IT Services",
                        "TRBC Industry Group Name": "Software & IT Services",
                        "TRBC Industry Name": "Software",
                        "TRBC Activity Name": "Application Software",
                        "Country ISO Code of Headquarters": "US",
                        "Price Open": 10.0,
                        "Price High": 11.0,
                        "Price Low": 9.5,
                        "Price Close": 10.5,
                        "Volume": 1000.0,
                        "Price Close Currency": "USD",
                    }
                ]
            )

    monkeypatch.setattr(download_data_lseg, "_load_lseg_client", lambda: FakeClient)

    out = download_data_lseg.download_from_lseg(
        db_path=data_db,
        rics_csv="TEST.OQ,MISS.OQ",
        as_of_date="2026-03-10",
        write_fundamentals=False,
        write_prices=True,
        write_classification=True,
    )

    assert out["status"] == "ok"
    assert out["security_master_rows_upserted"] == 1
    assert out["price_rows_inserted"] == 1
    assert out["classification_rows_inserted"] == 1
    assert out["matched_requested_ric_count"] == 1
    assert out["missing_requested_rics"] == ["MISS.OQ"]

    master = _row_by_ric(data_db, "TEST.OQ")
    assert master["ticker"] == "TEST"
    assert master["isin"] == "US0000000001"
    assert master["exchange_name"] == "NASDAQ"
    assert int(master["classification_ok"]) == 1
    assert int(master["is_equity_eligible"]) == 1
    assert master["source"] == "lseg_toolkit"


def test_backfill_prices_allows_explicit_pending_ric(monkeypatch, tmp_path: Path) -> None:
    fake_lseg = types.ModuleType("lseg")
    fake_lseg_data = types.ModuleType("lseg.data")
    fake_lseg_data.open_session = lambda: None
    fake_lseg_data.close_session = lambda: None
    fake_lseg_data.get_data = lambda **kwargs: pd.DataFrame()
    fake_lseg.data = fake_lseg_data
    monkeypatch.setitem(sys.modules, "lseg", fake_lseg)
    monkeypatch.setitem(sys.modules, "lseg.data", fake_lseg_data)
    backfill_prices_range_lseg = importlib.import_module("backend.scripts.backfill_prices_range_lseg")

    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    ensure_cuse4_schema(conn)
    conn.execute(
        """
        INSERT INTO security_master (
            ric, ticker, classification_ok, is_equity_eligible, source, job_run_id, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        ("PEND.OQ", "PEND", 0, 0, "security_master_seed", "seed_job", "2026-03-15T00:00:00+00:00"),
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(backfill_prices_range_lseg.rd, "open_session", lambda: None)
    monkeypatch.setattr(backfill_prices_range_lseg.rd, "close_session", lambda: None)
    monkeypatch.setattr(backfill_prices_range_lseg.rd, "get_data", fake_lseg_data.get_data)
    monkeypatch.setattr(
        backfill_prices_range_lseg.rd,
        "get_data",
        lambda **kwargs: pd.DataFrame(
            [
                {
                    "Instrument": "PEND.OQ",
                    "Date": "2026-03-10",
                    "Price Open": 20.0,
                    "Price High": 21.0,
                    "Price Low": 19.5,
                    "Price Close": 20.5,
                    "Volume": 500.0,
                    "Price Close Currency": "USD",
                }
            ]
        ),
    )

    out = backfill_prices_range_lseg.backfill_prices(
        db_path=data_db,
        start_date="2026-03-10",
        end_date="2026-03-10",
        ticker_batch_size=100,
        days_per_window=1,
        max_retries=0,
        sleep_seconds=0.0,
        rics_csv="PEND.OQ,MISS.OQ",
    )

    assert out["status"] == "ok"
    assert out["rows_upserted"] == 1
    assert out["matched_requested_ric_count"] == 1
    assert out["missing_requested_rics"] == ["MISS.OQ"]

    conn = sqlite3.connect(str(data_db))
    try:
        row = conn.execute(
            """
            SELECT close, volume, currency
            FROM security_prices_eod
            WHERE ric = ? AND date = ?
            """,
            ("PEND.OQ", "2026-03-10"),
        ).fetchone()
    finally:
        conn.close()

    assert row == (20.5, 500.0, "USD")


def test_download_from_lseg_skips_price_rows_with_missing_close(monkeypatch, tmp_path: Path) -> None:
    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    ensure_cuse4_schema(conn)
    conn.execute(
        """
        INSERT INTO security_master (
            ric, ticker, classification_ok, is_equity_eligible, source, job_run_id, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        ("MISS.OQ", "MISS", 1, 1, "security_master_seed", "seed_job", "2026-03-15T00:00:00+00:00"),
    )
    conn.commit()
    conn.close()

    class FakeClient:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def get_company_data(self, batch, *, fields, as_of_date):
            return pd.DataFrame(
                [
                    {
                        "Instrument": "MISS.OQ",
                        "Ticker Symbol": "MISS",
                        "Price Open": 10.0,
                        "Price High": 11.0,
                        "Price Low": 9.5,
                        "Price Close": None,
                        "Volume": 1000.0,
                        "Price Close Currency": "USD",
                    }
                ]
            )

    monkeypatch.setattr(download_data_lseg, "_load_lseg_client", lambda: FakeClient)

    out = download_data_lseg.download_from_lseg(
        db_path=data_db,
        rics_csv="MISS.OQ",
        as_of_date="2026-03-10",
        write_fundamentals=False,
        write_prices=True,
        write_classification=False,
    )

    assert out["status"] == "ok"
    assert out["price_rows_inserted"] == 0
    assert out["price_rows_skipped_missing_close"] == 1

    conn = sqlite3.connect(str(data_db))
    try:
        row = conn.execute(
            """
            SELECT COUNT(*)
            FROM security_prices_eod
            WHERE ric = ? AND date = ?
            """,
            ("MISS.OQ", "2026-03-10"),
        ).fetchone()
    finally:
        conn.close()

    assert row == (0,)


def test_download_from_lseg_reports_missing_requested_rics_on_no_data(monkeypatch, tmp_path: Path) -> None:
    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    ensure_cuse4_schema(conn)
    conn.execute(
        """
        INSERT INTO security_master (
            ric, ticker, classification_ok, is_equity_eligible, source, job_run_id, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        ("TEST.OQ", "TEST", 1, 1, "security_master_seed", "seed_job", "2026-03-15T00:00:00+00:00"),
    )
    conn.commit()
    conn.close()

    class FakeClient:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def get_company_data(self, batch, *, fields, as_of_date):
            assert batch == ["TEST.OQ"]
            return pd.DataFrame()

    monkeypatch.setattr(download_data_lseg, "_load_lseg_client", lambda: FakeClient)

    out = download_data_lseg.download_from_lseg(
        db_path=data_db,
        rics_csv="TEST.OQ,MISS.OQ",
        as_of_date="2026-03-10",
        write_fundamentals=False,
        write_prices=True,
        write_classification=False,
    )

    assert out["status"] == "no-data"
    assert out["matched_requested_ric_count"] == 1
    assert out["missing_requested_rics"] == ["MISS.OQ"]


def test_backfill_prices_skips_rows_with_missing_close(monkeypatch, tmp_path: Path) -> None:
    fake_lseg = types.ModuleType("lseg")
    fake_lseg_data = types.ModuleType("lseg.data")
    fake_lseg_data.open_session = lambda: None
    fake_lseg_data.close_session = lambda: None
    fake_lseg_data.get_data = lambda **kwargs: pd.DataFrame(
        [
            {
                "Instrument": "MISS.OQ",
                "Date": "2026-03-10",
                "Price Open": 20.0,
                "Price High": 21.0,
                "Price Low": 19.5,
                "Price Close": None,
                "Volume": 500.0,
                "Price Close Currency": "USD",
            }
        ]
    )
    fake_lseg.data = fake_lseg_data
    monkeypatch.setitem(sys.modules, "lseg", fake_lseg)
    monkeypatch.setitem(sys.modules, "lseg.data", fake_lseg_data)
    backfill_prices_range_lseg = importlib.import_module("backend.scripts.backfill_prices_range_lseg")

    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    ensure_cuse4_schema(conn)
    conn.execute(
        """
        INSERT INTO security_master (
            ric, ticker, classification_ok, is_equity_eligible, source, job_run_id, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        ("MISS.OQ", "MISS", 1, 1, "security_master_seed", "seed_job", "2026-03-15T00:00:00+00:00"),
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(backfill_prices_range_lseg.rd, "open_session", lambda: None)
    monkeypatch.setattr(backfill_prices_range_lseg.rd, "close_session", lambda: None)
    monkeypatch.setattr(backfill_prices_range_lseg.rd, "get_data", fake_lseg_data.get_data)

    out = backfill_prices_range_lseg.backfill_prices(
        db_path=data_db,
        start_date="2026-03-10",
        end_date="2026-03-10",
        ticker_batch_size=100,
        days_per_window=1,
        max_retries=0,
        sleep_seconds=0.0,
        rics_csv="MISS.OQ",
    )

    assert out["status"] == "ok"
    assert out["rows_upserted"] == 0
    assert out["price_rows_skipped_missing_close"] == 1

    conn = sqlite3.connect(str(data_db))
    try:
        row = conn.execute(
            """
            SELECT COUNT(*)
            FROM security_prices_eod
            WHERE ric = ? AND date = ?
            """,
            ("MISS.OQ", "2026-03-10"),
        ).fetchone()
    finally:
        conn.close()

    assert row == (0,)


def test_backfill_volume_only_reports_missing_requested_rics_on_no_null_volume(monkeypatch, tmp_path: Path) -> None:
    fake_lseg = types.ModuleType("lseg")
    fake_lseg_data = types.ModuleType("lseg.data")
    fake_lseg_data.open_session = lambda: None
    fake_lseg_data.close_session = lambda: None
    fake_lseg_data.get_data = lambda **kwargs: pd.DataFrame()
    fake_lseg.data = fake_lseg_data
    monkeypatch.setitem(sys.modules, "lseg", fake_lseg)
    monkeypatch.setitem(sys.modules, "lseg.data", fake_lseg_data)
    backfill_prices_range_lseg = importlib.import_module("backend.scripts.backfill_prices_range_lseg")

    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    ensure_cuse4_schema(conn)
    conn.execute(
        """
        INSERT INTO security_master (
            ric, ticker, classification_ok, is_equity_eligible, source, job_run_id, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        ("PEND.OQ", "PEND", 1, 1, "security_master_seed", "seed_job", "2026-03-15T00:00:00+00:00"),
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(backfill_prices_range_lseg, "_load_missing_volume_pairs", lambda *args, **kwargs: pd.DataFrame())

    out = backfill_prices_range_lseg.backfill_prices(
        db_path=data_db,
        start_date="2026-03-10",
        end_date="2026-03-10",
        ticker_batch_size=100,
        days_per_window=1,
        max_retries=0,
        sleep_seconds=0.0,
        rics_csv="PEND.OQ,MISS.OQ",
        volume_only=True,
        only_null_volume=True,
    )

    assert out["status"] == "no-null-volume"
    assert out["matched_requested_ric_count"] == 1
    assert out["missing_requested_rics"] == ["MISS.OQ"]


def test_backfill_reports_missing_requested_rics_on_no_date_windows(monkeypatch, tmp_path: Path) -> None:
    fake_lseg = types.ModuleType("lseg")
    fake_lseg_data = types.ModuleType("lseg.data")
    fake_lseg_data.open_session = lambda: None
    fake_lseg_data.close_session = lambda: None
    fake_lseg_data.get_data = lambda **kwargs: pd.DataFrame()
    fake_lseg.data = fake_lseg_data
    monkeypatch.setitem(sys.modules, "lseg", fake_lseg)
    monkeypatch.setitem(sys.modules, "lseg.data", fake_lseg_data)
    backfill_prices_range_lseg = importlib.import_module("backend.scripts.backfill_prices_range_lseg")

    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    ensure_cuse4_schema(conn)
    conn.execute(
        """
        INSERT INTO security_master (
            ric, ticker, classification_ok, is_equity_eligible, source, job_run_id, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        ("PEND.OQ", "PEND", 1, 1, "security_master_seed", "seed_job", "2026-03-15T00:00:00+00:00"),
    )
    conn.commit()
    conn.close()

    out = backfill_prices_range_lseg.backfill_prices(
        db_path=data_db,
        start_date="2026-03-11",
        end_date="2026-03-10",
        ticker_batch_size=100,
        days_per_window=1,
        max_retries=0,
        sleep_seconds=0.0,
        rics_csv="PEND.OQ,MISS.OQ",
    )

    assert out["status"] == "no-date-windows"
    assert out["matched_requested_ric_count"] == 1
    assert out["missing_requested_rics"] == ["MISS.OQ"]


def test_augment_security_master_from_xlsx_keeps_new_rows_pending(monkeypatch, tmp_path: Path) -> None:
    data_db = tmp_path / "data.db"
    seed_xlsx = tmp_path / "new_universe.xlsx"
    seed_xlsx.write_bytes(b"placeholder")

    class FakeExcelFile:
        sheet_names = ["Sheet1"]

        def __init__(self, _path):
            self.sheet_names = ["Sheet1"]

    monkeypatch.setattr(augment_security_master_from_ric_xlsx.pd, "ExcelFile", FakeExcelFile)
    monkeypatch.setattr(
        augment_security_master_from_ric_xlsx.pd,
        "read_excel",
        lambda *_args, **_kwargs: pd.DataFrame({"RIC": ["NEW1.OQ", "NEW2.N"]}),
    )

    out = augment_security_master_from_ric_xlsx.run(
        db_path=data_db,
        xlsx_path=seed_xlsx,
        sheet=None,
        source="coverage_universe_xlsx",
        output_new_rics=None,
    )

    assert out["status"] == "ok"
    assert out["new_rics_inserted"] == 2

    conn = sqlite3.connect(str(data_db))
    try:
        rows = conn.execute(
            """
            SELECT ric, ticker, classification_ok, is_equity_eligible
            FROM security_master
            ORDER BY ric
            """
        ).fetchall()
    finally:
        conn.close()

    assert rows == [
        ("NEW1.OQ", "NEW1", 0, 0),
        ("NEW2.N", "NEW2", 0, 0),
    ]


def test_export_security_master_seed_is_registry_only(tmp_path: Path) -> None:
    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    ensure_cuse4_schema(conn)
    conn.execute(
        """
        INSERT INTO security_master (
            ric, ticker, isin, exchange_name, classification_ok, is_equity_eligible, source, job_run_id, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "AAPL.OQ",
            "AAPL",
            "US0378331005",
            "NASDAQ",
            1,
            1,
            "lseg_toolkit",
            "job_1",
            "2026-03-15T00:00:00+00:00",
        ),
    )
    conn.commit()
    conn.close()

    output_path = tmp_path / "security_master_seed.csv"
    exported = export_seed(data_db=data_db, output_path=output_path)

    assert exported == 1
    assert output_path.read_text(encoding="utf-8").splitlines() == [
        "ric,ticker,isin,exchange_name",
        "AAPL.OQ,AAPL,US0378331005,NASDAQ",
    ]
