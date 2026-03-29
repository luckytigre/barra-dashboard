from __future__ import annotations

import logging
import sqlite3
from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from psycopg import sql


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SourceSyncTransferDeps:
    pg_count_table: Callable[[Any, str], int]
    upsert_table_on_pk: Callable[..., int]
    pg_max_date: Callable[..., str | None]
    parse_iso_date: Callable[[str | None], Any]
    format_iso_date: Callable[[Any, str | None], str | None]
    pg_min_date: Callable[..., str | None]
    sqlite_entity_min_dates: Callable[..., dict[str, str]]
    pg_entity_min_dates: Callable[..., dict[str, str]]
    delete_pg_rows_for_entities: Callable[..., int]
    sqlite_count: Callable[..., int]
    copy_into_postgres: Callable[..., int]
    copy_into_postgres_idempotent: Callable[..., int]
    sqlite_select_rows: Callable[..., Any]
    sqlite_select_rows_for_entities_before_date: Callable[..., Any]
    assert_post_load_row_counts: Callable[..., dict[str, Any]]


@dataclass(frozen=True)
class SourceSyncTableTransferResult:
    status: str
    action: str | None = None
    source_rows: int | None = None
    rows_loaded: int | None = None
    where_sql: str | None = None
    where_params: tuple[Any, ...] = ()
    identifier_backfill: dict[str, Any] | None = None
    target_row_validation: dict[str, Any] | None = None


def _copy_rows_for_mode(
    pg_conn,
    *,
    mode: str,
    table: str,
    columns: list[str],
    pk_cols: list[str],
    rows,
    deps: SourceSyncTransferDeps,
) -> int:
    if mode == "incremental":
        return deps.copy_into_postgres_idempotent(
            pg_conn,
            table=table,
            columns=columns,
            pk_cols=pk_cols,
            rows=rows,
        )
    return deps.copy_into_postgres(
        pg_conn,
        table=table,
        columns=columns,
        rows=rows,
    )


def sync_table_from_sqlite_to_neon(
    sqlite_conn: sqlite3.Connection,
    pg_conn,
    *,
    cfg: Any,
    columns: list[str],
    mode: str,
    batch_size: int,
    deps: SourceSyncTransferDeps,
) -> SourceSyncTableTransferResult:
    table = str(cfg.name)
    action = "replace"
    where_sql = ""
    params: tuple[Any, ...] = ()
    identifier_backfill_entities: list[str] = []
    identifier_backfill_deleted = 0
    identifier_backfill_rows = 0
    identifier_backfill_from_date: str | None = None
    target_rows_before = deps.pg_count_table(pg_conn, table)
    deleted_overlap_rows = 0

    if cfg.sync_mode == "upsert":
        src_count = deps.sqlite_count(sqlite_conn, table)
        copied = deps.upsert_table_on_pk(
            sqlite_conn,
            pg_conn,
            table=table,
            columns=columns,
            pk_cols=list(cfg.pk_cols),
            batch_size=max(500, int(batch_size)),
        )
        return SourceSyncTableTransferResult(
            status="ok",
            action="upsert",
            source_rows=int(src_count),
            rows_loaded=int(copied),
        )

    if mode == "full" or not cfg.date_col:
        with pg_conn.cursor() as cur:
            cur.execute(sql.SQL("TRUNCATE TABLE {} ").format(sql.Identifier(table)))
        action = "truncate_and_reload"
    else:
        max_date = deps.pg_max_date(pg_conn, table=table, date_col=str(cfg.date_col))
        max_dt = deps.parse_iso_date(max_date)
        if max_dt is None:
            with pg_conn.cursor() as cur:
                cur.execute(sql.SQL("TRUNCATE TABLE {} ").format(sql.Identifier(table)))
            action = "target_empty_truncate_and_reload"
        else:
            cutoff = max_dt - timedelta(days=max(0, int(cfg.overlap_days)))
            cutoff_txt = deps.format_iso_date(cutoff, fallback=max_date)
            if cutoff_txt is None:
                raise RuntimeError(f"unable to derive cutoff for table {table}")
            if cfg.identifier_history_backfill and cfg.entity_col and cfg.date_col:
                target_retained_min = deps.pg_min_date(
                    pg_conn,
                    table=table,
                    date_col=str(cfg.date_col),
                )
                identifier_backfill_from_date = target_retained_min
                source_entity_min_dates = deps.sqlite_entity_min_dates(
                    sqlite_conn,
                    table=table,
                    entity_col=str(cfg.entity_col),
                    date_col=str(cfg.date_col),
                )
                target_entity_min_dates = deps.pg_entity_min_dates(
                    pg_conn,
                    table=table,
                    entity_col=str(cfg.entity_col),
                    date_col=str(cfg.date_col),
                )
                if target_retained_min:
                    for entity, source_min in source_entity_min_dates.items():
                        desired_min = max(str(source_min), str(target_retained_min))
                        target_entity_min = target_entity_min_dates.get(entity)
                        if desired_min >= cutoff_txt:
                            continue
                        if target_entity_min is None or str(target_entity_min) > desired_min:
                            identifier_backfill_entities.append(entity)
                identifier_backfill_entities = sorted(set(identifier_backfill_entities))
                if identifier_backfill_entities:
                    identifier_backfill_deleted = deps.delete_pg_rows_for_entities(
                        pg_conn,
                        table=table,
                        entity_col=str(cfg.entity_col),
                        entities=identifier_backfill_entities,
                    )
            where_sql = f"WHERE {cfg.date_col} >= ?"
            params = (cutoff_txt,)
            with pg_conn.cursor() as cur:
                cur.execute(
                    sql.SQL("DELETE FROM {} WHERE {} >= %s").format(
                        sql.Identifier(table),
                        sql.Identifier(str(cfg.date_col)),
                    ),
                    (cutoff_txt,),
                )
                deleted_overlap_rows = int(cur.rowcount or 0)
            action = (
                "incremental_overlap_plus_identifier_backfill"
                if identifier_backfill_entities
                else "incremental_overlap_reload"
            )

    src_count = deps.sqlite_count(sqlite_conn, table, where_sql, params)
    copied = _copy_rows_for_mode(
        pg_conn,
        mode=mode,
        table=table,
        columns=columns,
        pk_cols=list(cfg.pk_cols),
        rows=deps.sqlite_select_rows(
            sqlite_conn,
            table=table,
            columns=columns,
            where_sql=where_sql,
            params=params,
            batch_size=max(1_000, int(batch_size)),
        ),
        deps=deps,
    )
    if identifier_backfill_entities and cfg.entity_col and cfg.date_col:
        identifier_backfill_rows = _copy_rows_for_mode(
            pg_conn,
            mode=mode,
            table=table,
            columns=columns,
            pk_cols=list(cfg.pk_cols),
            rows=deps.sqlite_select_rows_for_entities_before_date(
                sqlite_conn,
                table=table,
                columns=columns,
                entity_col=str(cfg.entity_col),
                entities=identifier_backfill_entities,
                date_col=str(cfg.date_col),
                from_date=identifier_backfill_from_date,
                before_date=str(params[0]),
                batch_size=max(1_000, int(batch_size)),
            ),
            deps=deps,
        )
    expected_rows_loaded = int(src_count + identifier_backfill_rows)
    actual_rows_loaded = int(copied + identifier_backfill_rows)
    if actual_rows_loaded != expected_rows_loaded:
        raise RuntimeError(
            f"source row mismatch for {table}: expected {expected_rows_loaded} row(s) to load, "
            f"but copied {actual_rows_loaded}; source archive may be inconsistent"
        )
    target_row_validation = deps.assert_post_load_row_counts(
        pg_conn,
        table=table,
        action=action,
        target_rows_before=target_rows_before,
        deleted_overlap_rows=deleted_overlap_rows,
        identifier_backfill_deleted=identifier_backfill_deleted,
        rows_loaded=actual_rows_loaded,
    )
    return SourceSyncTableTransferResult(
        status="ok",
        action=action,
        source_rows=int(src_count),
        rows_loaded=actual_rows_loaded,
        where_sql=where_sql or None,
        where_params=params,
        identifier_backfill=(
            {
                "entity_col": str(cfg.entity_col),
                "count": int(len(identifier_backfill_entities)),
                "sample": identifier_backfill_entities[:10],
                "rows_loaded": int(identifier_backfill_rows),
                "rows_deleted": int(identifier_backfill_deleted),
            }
            if identifier_backfill_entities
            else None
        ),
        target_row_validation=target_row_validation,
    )
