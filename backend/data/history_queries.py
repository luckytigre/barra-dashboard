"""Read-only historical query helpers used by API routes."""

from __future__ import annotations

import sqlite3
from datetime import date, timedelta
from pathlib import Path

from psycopg.rows import dict_row

from backend import config
from backend.data.neon import connect, resolve_dsn

_FACTOR_HISTORY_SURFACE = "factor_history"
_PRICE_HISTORY_SURFACE = "price_history"


def _use_neon_surface(surface: str) -> bool:
    return bool(config.neon_surface_enabled(surface))


def _path_matches_config(path: Path, configured: str) -> bool:
    try:
        return Path(path).expanduser().resolve() == Path(configured).expanduser().resolve()
    except Exception:
        return False


def load_factor_return_history(
    cache_db: Path,
    *,
    factor: str,
    years: int,
) -> tuple[str | None, list[tuple[str, float]]]:
    """Return latest factor-return date and historical rows for a factor."""
    use_neon = _use_neon_surface(_FACTOR_HISTORY_SURFACE) and _path_matches_config(
        cache_db,
        config.SQLITE_PATH,
    )
    if use_neon:
        pg_conn = connect(dsn=resolve_dsn(None), autocommit=True)
        try:
            with pg_conn.cursor(row_factory=dict_row) as cur:
                cur.execute("SELECT MAX(date)::text AS latest FROM model_factor_returns_daily")
                latest_row = cur.fetchone() or {}
                latest = str(latest_row.get("latest") or "").strip()
                if not latest:
                    return None, []
                latest_dt = date.fromisoformat(latest)
                start_dt = latest_dt - timedelta(days=365 * max(1, int(years)))
                cur.execute(
                    """
                    SELECT date::text AS date, factor_return
                    FROM model_factor_returns_daily
                    WHERE factor_name = %s
                      AND date >= %s
                    ORDER BY date
                    """,
                    (str(factor), start_dt.isoformat()),
                )
                rows = cur.fetchall()
                out = [
                    (str(row.get("date")), float(row.get("factor_return") or 0.0))
                    for row in rows
                ]
                return latest, out
        finally:
            pg_conn.close()

    conn = sqlite3.connect(str(cache_db))
    try:
        latest_row = conn.execute("SELECT MAX(date) FROM daily_factor_returns").fetchone()
        latest = str(latest_row[0]).strip() if latest_row and latest_row[0] is not None else ""
        if not latest:
            return None, []
        latest_dt = date.fromisoformat(latest)
        start_dt = latest_dt - timedelta(days=365 * max(1, int(years)))
        rows = conn.execute(
            """
            SELECT date, factor_return
            FROM daily_factor_returns
            WHERE factor_name = ?
              AND date >= ?
            ORDER BY date
            """,
            (str(factor), start_dt.isoformat()),
        ).fetchall()
        out = [(str(dt), float(raw_ret or 0.0)) for dt, raw_ret in rows]
        return latest, out
    finally:
        conn.close()


def load_price_history_rows(
    data_db: Path,
    *,
    ric: str,
    years: int,
) -> tuple[str | None, list[tuple[str, float]]]:
    """Return latest price date and historical daily closes for a RIC."""
    use_neon = _use_neon_surface(_PRICE_HISTORY_SURFACE) and _path_matches_config(
        data_db,
        config.DATA_DB_PATH,
    )
    if use_neon:
        pg_conn = connect(dsn=resolve_dsn(None), autocommit=True)
        try:
            with pg_conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    """
                    SELECT MAX(date)::text AS latest
                    FROM security_prices_eod
                    WHERE ric = %s
                    """,
                    (str(ric),),
                )
                latest_row = cur.fetchone() or {}
                latest = str(latest_row.get("latest") or "").strip()
                if not latest:
                    return None, []

                end_dt = date.fromisoformat(latest)
                start_dt = end_dt - timedelta(days=(366 * max(1, int(years))))
                cur.execute(
                    """
                    SELECT date::text AS date, CAST(close AS DOUBLE PRECISION) AS close
                    FROM security_prices_eod
                    WHERE ric = %s
                      AND date >= %s
                      AND date <= %s
                      AND close IS NOT NULL
                    ORDER BY date ASC
                    """,
                    (str(ric), start_dt.isoformat(), end_dt.isoformat()),
                )
                rows = cur.fetchall()
                out = [
                    (str(row.get("date")), float(row.get("close")))
                    for row in rows
                    if row.get("date") is not None and row.get("close") is not None
                ]
                return latest, out
        finally:
            pg_conn.close()

    conn = sqlite3.connect(str(data_db))
    try:
        latest_row = conn.execute(
            """
            SELECT MAX(date)
            FROM security_prices_eod
            WHERE ric = ?
            """,
            (str(ric),),
        ).fetchone()
        latest = str(latest_row[0]).strip() if latest_row and latest_row[0] is not None else ""
        if not latest:
            return None, []

        end_dt = date.fromisoformat(latest)
        start_dt = end_dt - timedelta(days=(366 * max(1, int(years))))
        rows = conn.execute(
            """
            SELECT date, CAST(close AS REAL) AS close
            FROM security_prices_eod
            WHERE ric = ?
              AND date >= ?
              AND date <= ?
              AND close IS NOT NULL
            ORDER BY date ASC
            """,
            (str(ric), start_dt.isoformat(), end_dt.isoformat()),
        ).fetchall()
        out = [(str(dt), float(close)) for dt, close in rows if dt is not None and close is not None]
        return latest, out
    finally:
        conn.close()
