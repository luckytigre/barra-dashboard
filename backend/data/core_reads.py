"""Canonical source-read facade for the Barra dashboard."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandas as pd

from backend import config
from backend.data import core_read_backend as core_backend, source_dates, source_reads

DATA_DB = Path(config.DATA_DB_PATH)
logger = logging.getLogger(__name__)


def _use_neon_core_reads() -> bool:
    return core_backend.use_neon_core_reads()


def core_read_backend_name() -> str:
    return core_backend.core_read_backend_name()


def core_read_backend(backend: str):
    return core_backend.core_read_backend(backend)


def _to_pg_sql(query: str) -> str:
    return core_backend.to_pg_sql(query)


def _fetch_rows(sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
    return core_backend.fetch_rows(
        sql,
        params,
        data_db=DATA_DB,
        neon_enabled=_use_neon_core_reads(),
    )


def _table_exists(table: str) -> bool:
    return core_backend.table_exists(
        table,
        fetch_rows_fn=_fetch_rows,
        neon_enabled=_use_neon_core_reads(),
    )


def _missing_tables(*tables: str) -> list[str]:
    return core_backend.missing_tables(*tables, table_exists_fn=_table_exists)


def _load_latest_prices_sqlite(tickers: list[str] | None = None) -> pd.DataFrame:
    return source_reads.load_latest_prices_sqlite(
        data_db=DATA_DB,
        tickers=tickers,
        missing_tables_fn=_missing_tables,
    )


def _resolve_latest_barra_tuple() -> dict[str, str] | None:
    return source_dates.resolve_latest_barra_tuple(
        fetch_rows_fn=_fetch_rows,
        exposure_source_table_required_fn=_exposure_source_table_required,
    )


def _exposure_source_table_required() -> str:
    return source_reads.exposure_source_table_required(
        table_exists_fn=_table_exists,
    )


def _resolve_latest_well_covered_exposure_asof(table: str) -> str | None:
    return source_reads.resolve_latest_well_covered_exposure_asof(
        table,
        fetch_rows_fn=_fetch_rows,
    )


def load_raw_cross_section_latest(tickers: list[str] | None = None) -> pd.DataFrame:
    return source_reads.load_raw_cross_section_latest(
        tickers=tickers,
        fetch_rows_fn=_fetch_rows,
        exposure_source_table_required_fn=_exposure_source_table_required,
        resolve_latest_well_covered_exposure_asof_fn=_resolve_latest_well_covered_exposure_asof,
    )


def load_latest_fundamentals(
    tickers: list[str] | None = None,
    as_of_date: str | None = None,
) -> pd.DataFrame:
    return source_reads.load_latest_fundamentals(
        tickers=tickers,
        as_of_date=as_of_date,
        fetch_rows_fn=_fetch_rows,
        missing_tables_fn=_missing_tables,
    )


def load_latest_prices(tickers: list[str] | None = None) -> pd.DataFrame:
    if not _use_neon_core_reads():
        return _load_latest_prices_sqlite(tickers)
    return source_reads.load_latest_prices(
        tickers=tickers,
        fetch_rows_fn=_fetch_rows,
        missing_tables_fn=_missing_tables,
    )


def load_source_dates() -> dict[str, str | None]:
    return source_dates.load_source_dates(
        fetch_rows_fn=_fetch_rows,
        table_exists_fn=_table_exists,
        exposure_source_table_required_fn=_exposure_source_table_required,
    )
