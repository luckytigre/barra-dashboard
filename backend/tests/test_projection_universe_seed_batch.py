from __future__ import annotations

import csv
from pathlib import Path


DATA_REFERENCE = Path(__file__).resolve().parents[2] / "data" / "reference"
REGISTRY_SEED = DATA_REFERENCE / "security_registry_seed.csv"
MASTER_SEED = DATA_REFERENCE / "security_master_seed.csv"
REPRESENTATIVE_TICKERS = {
    "URA",
    "ARKK",
    "QQQM",
    "SMH",
    "IBIT",
    "FDVV",
    "FXAIX",
    "FSKAX",
    "FSELX",
    "MTUM",
    "VLUE",
    "QUAL",
    "USMV",
    "EEM",
    "EFA",
}


def test_projection_universe_seed_batch_is_present_in_registry_seed() -> None:
    with REGISTRY_SEED.open("r", encoding="utf-8", newline="") as handle:
        rows = {
            str(row.get("ticker") or "").strip().upper(): row
            for row in csv.DictReader(handle)
            if str(row.get("ticker") or "").strip()
        }

    missing = sorted(REPRESENTATIVE_TICKERS.difference(rows))
    assert missing == []
    for ticker in sorted(REPRESENTATIVE_TICKERS):
        row = rows[ticker]
        assert str(row.get("price_ingest_enabled") or "") == "1"
        assert str(row.get("pit_fundamentals_enabled") or "") == "0"
        assert str(row.get("pit_classification_enabled") or "") == "0"
        assert str(row.get("allow_cuse_native_core") or "") == "0"
        assert str(row.get("allow_cuse_fundamental_projection") or "") == "0"
        assert str(row.get("allow_cuse_returns_projection") or "") == "1"
        assert str(row.get("allow_cpar_core_target") or "") == "0"
        assert str(row.get("allow_cpar_extended_target") or "") == "1"


def test_projection_universe_seed_batch_is_present_in_compat_seed() -> None:
    with MASTER_SEED.open("r", encoding="utf-8", newline="") as handle:
        rows = {
            str(row.get("ticker") or "").strip().upper(): row
            for row in csv.DictReader(handle)
            if str(row.get("ticker") or "").strip()
        }

    missing = sorted(REPRESENTATIVE_TICKERS.difference(rows))
    assert missing == []
    for ticker in sorted(REPRESENTATIVE_TICKERS):
        assert str(rows[ticker].get("coverage_role") or "") == "projection_only"
