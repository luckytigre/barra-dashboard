"""Security-master sync helpers for canonical universe bootstrap and LSEG enrichment."""

from __future__ import annotations

import csv
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from backend.risk_model.eligibility import NON_EQUITY_ECONOMIC_SECTORS
from backend.universe.schema import SECURITY_MASTER_TABLE


DEFAULT_SECURITY_MASTER_SEED_PATH = Path(__file__).resolve().parents[2] / "data/reference/security_master_seed.csv"


def normalize_ric(value: str | None) -> str:
    return str(value or "").strip().upper()


def normalize_ticker(value: str | None) -> str | None:
    text = str(value or "").strip().upper()
    return text or None


def normalize_optional_text(value: str | None) -> str | None:
    text = str(value or "").strip()
    if not text or text.lower() in {"nan", "none"}:
        return None
    return text


def ticker_from_ric(ric: str | None) -> str | None:
    text = normalize_ric(ric)
    if not text:
        return None
    return text.split(".", 1)[0]


def derive_security_master_flags(
    *,
    trbc_economic_sector: str | None,
    trbc_business_sector: str | None,
    trbc_industry_group: str | None,
    trbc_industry: str | None,
    trbc_activity: str | None,
    hq_country_code: str | None,
) -> tuple[int, int]:
    sector = normalize_optional_text(trbc_economic_sector)
    has_classification = any(
        normalize_optional_text(value)
        for value in (
            sector,
            trbc_business_sector,
            trbc_industry_group,
            trbc_industry,
            trbc_activity,
            hq_country_code,
        )
    )
    classification_ok = 1 if has_classification else 0
    is_equity_eligible = 1 if classification_ok and sector not in NON_EQUITY_ECONOMIC_SECTORS else 0
    return classification_ok, is_equity_eligible


def upsert_security_master_rows(
    conn: sqlite3.Connection,
    rows: list[dict[str, Any]],
) -> int:
    if not rows:
        return 0

    sql = f"""
        INSERT INTO {SECURITY_MASTER_TABLE} (
            ric,
            ticker,
            isin,
            exchange_name,
            classification_ok,
            is_equity_eligible,
            source,
            job_run_id,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(ric) DO UPDATE SET
            ticker = COALESCE(NULLIF(excluded.ticker, ''), {SECURITY_MASTER_TABLE}.ticker),
            isin = COALESCE(NULLIF(excluded.isin, ''), {SECURITY_MASTER_TABLE}.isin),
            exchange_name = COALESCE(NULLIF(excluded.exchange_name, ''), {SECURITY_MASTER_TABLE}.exchange_name),
            classification_ok = COALESCE(excluded.classification_ok, {SECURITY_MASTER_TABLE}.classification_ok),
            is_equity_eligible = COALESCE(excluded.is_equity_eligible, {SECURITY_MASTER_TABLE}.is_equity_eligible),
            source = COALESCE(NULLIF(excluded.source, ''), {SECURITY_MASTER_TABLE}.source),
            job_run_id = COALESCE(NULLIF(excluded.job_run_id, ''), {SECURITY_MASTER_TABLE}.job_run_id),
            updated_at = COALESCE(NULLIF(excluded.updated_at, ''), {SECURITY_MASTER_TABLE}.updated_at)
    """
    payload = [
        (
            normalize_ric(row.get("ric")),
            normalize_ticker(row.get("ticker")),
            normalize_optional_text(row.get("isin")),
            normalize_optional_text(row.get("exchange_name")),
            int(row.get("classification_ok") or 0),
            int(row.get("is_equity_eligible") or 0),
            normalize_optional_text(row.get("source")),
            normalize_optional_text(row.get("job_run_id")),
            normalize_optional_text(row.get("updated_at")),
        )
        for row in rows
        if normalize_ric(row.get("ric"))
    ]
    if not payload:
        return 0
    conn.executemany(sql, payload)
    return len(payload)


def load_security_master_seed_rows(seed_path: Path) -> list[dict[str, Any]]:
    path = Path(seed_path).expanduser().resolve()
    if not path.exists():
        return []

    rows_by_ric: dict[str, dict[str, Any]] = {}
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for raw in reader:
            ric = normalize_ric(raw.get("ric"))
            if not ric:
                continue
            rows_by_ric[ric] = {
                "ric": ric,
                "ticker": normalize_ticker(raw.get("ticker")) or ticker_from_ric(ric),
                "isin": normalize_optional_text(raw.get("isin")),
                "exchange_name": normalize_optional_text(raw.get("exchange_name")),
            }
    return [rows_by_ric[ric] for ric in sorted(rows_by_ric)]


def sync_security_master_seed(
    conn: sqlite3.Connection,
    *,
    seed_path: Path = DEFAULT_SECURITY_MASTER_SEED_PATH,
    source: str = "security_master_seed",
) -> dict[str, Any]:
    seed_rows = load_security_master_seed_rows(seed_path)
    if not seed_rows:
        return {
            "status": "missing",
            "seed_path": str(Path(seed_path).expanduser().resolve()),
            "seed_rows": 0,
            "rows_upserted": 0,
        }

    now_iso = datetime.now(timezone.utc).isoformat()
    job_run_id = f"security_master_seed_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    insert_sql = f"""
        INSERT OR IGNORE INTO {SECURITY_MASTER_TABLE} (
            ric,
            ticker,
            isin,
            exchange_name,
            classification_ok,
            is_equity_eligible,
            source,
            job_run_id,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    update_sql = f"""
        UPDATE {SECURITY_MASTER_TABLE}
        SET
            ticker = COALESCE(NULLIF(ticker, ''), ?),
            isin = COALESCE(NULLIF(isin, ''), ?),
            exchange_name = COALESCE(NULLIF(exchange_name, ''), ?),
            source = COALESCE(source, ?),
            job_run_id = COALESCE(job_run_id, ?),
            updated_at = COALESCE(NULLIF(updated_at, ''), ?)
        WHERE ric = ?
    """
    before = conn.total_changes
    conn.executemany(
        insert_sql,
        [
            (
                normalize_ric(row.get("ric")),
                normalize_ticker(row.get("ticker")) or ticker_from_ric(row.get("ric")),
                normalize_optional_text(row.get("isin")),
                normalize_optional_text(row.get("exchange_name")),
                0,
                0,
                source,
                job_run_id,
                now_iso,
            )
            for row in seed_rows
            if normalize_ric(row.get("ric"))
        ],
    )
    conn.executemany(
        update_sql,
        [
            (
                normalize_ticker(row.get("ticker")) or ticker_from_ric(row.get("ric")),
                normalize_optional_text(row.get("isin")),
                normalize_optional_text(row.get("exchange_name")),
                source,
                job_run_id,
                now_iso,
                normalize_ric(row.get("ric")),
            )
            for row in seed_rows
            if normalize_ric(row.get("ric"))
        ],
    )
    rows_upserted = int(conn.total_changes - before)
    return {
        "status": "ok",
        "seed_path": str(Path(seed_path).expanduser().resolve()),
        "seed_rows": len(seed_rows),
        "rows_upserted": rows_upserted,
        "job_run_id": job_run_id,
        "updated_at": now_iso,
    }
