"""Lower serving-payload write and verification helpers."""

from __future__ import annotations

from collections.abc import Callable
import sqlite3
import time
from typing import Any


def persist_current_payloads_neon(
    rows: list[tuple[str, str, str, str, str, str]],
    *,
    replace_all: bool,
    write_mode: str,
    connect_fn: Callable[..., Any],
    resolve_dsn_fn: Callable[[Any], Any],
    ensure_postgres_schema: Callable[[Any], None],
    verify_current_payloads_neon: Callable[..., dict[str, Any]],
) -> dict[str, Any]:
    started_at = time.perf_counter()
    try:
        conn = connect_fn(dsn=resolve_dsn_fn(None), autocommit=False)
    except Exception as exc:
        return {
            "status": "error",
            "error": {"type": type(exc).__name__, "message": str(exc)},
        }
    try:
        ensure_postgres_schema(conn)
        upsert_sql = """
            INSERT INTO serving_payload_current (
                payload_name,
                snapshot_id,
                run_id,
                refresh_mode,
                payload_json,
                updated_at
            ) VALUES (%s, %s, %s, %s, %s::jsonb, %s::timestamptz)
            ON CONFLICT (payload_name) DO UPDATE SET
                snapshot_id = EXCLUDED.snapshot_id,
                run_id = EXCLUDED.run_id,
                refresh_mode = EXCLUDED.refresh_mode,
                payload_json = EXCLUDED.payload_json,
                updated_at = EXCLUDED.updated_at
            """
        with conn.cursor() as cur:
            if replace_all:
                if rows:
                    cur.execute(
                        """
                        DELETE FROM serving_payload_current
                        WHERE payload_name <> ALL(%s)
                        """,
                        ([row[0] for row in rows],),
                    )
                else:
                    cur.execute("DELETE FROM serving_payload_current")
            if write_mode == "row_by_row":
                for row in rows:
                    cur.execute(upsert_sql, row)
            else:
                cur.executemany(upsert_sql, rows)
        verification = verify_current_payloads_neon(
            conn,
            rows=rows,
            replace_all=replace_all,
        )
        if str(verification.get("status") or "") != "ok":
            conn.rollback()
            return {
                "status": "error",
                "row_count": len(rows),
                "replace_all": bool(replace_all),
                "verification": verification,
                "error": {
                    "type": "RuntimeError",
                    "message": "Neon serving payload verification failed: "
                    + ", ".join(str(issue) for issue in verification.get("issues") or []),
                },
            }
        conn.commit()
        return {
            "status": "ok",
            "row_count": len(rows),
            "replace_all": bool(replace_all),
            "write_mode": write_mode,
            "duration_seconds": round(float(time.perf_counter() - started_at), 3),
            "verification": verification,
        }
    except Exception as exc:
        conn.rollback()
        return {
            "status": "error",
            "error": {"type": type(exc).__name__, "message": str(exc)},
            "write_mode": write_mode,
            "duration_seconds": round(float(time.perf_counter() - started_at), 3),
        }
    finally:
        conn.close()


def persist_current_payloads_sqlite(
    rows: list[tuple[str, str, str, str, str, str]],
    *,
    data_db,
    replace_all: bool,
    ensure_sqlite_schema: Callable[[sqlite3.Connection], None],
) -> dict[str, Any]:
    started_at = time.perf_counter()
    conn = sqlite3.connect(str(data_db), timeout=120)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=120000")
    try:
        ensure_sqlite_schema(conn)
        if replace_all:
            if rows:
                placeholders = ",".join("?" for _ in rows)
                conn.execute(
                    f"DELETE FROM serving_payload_current WHERE payload_name NOT IN ({placeholders})",
                    [row[0] for row in rows],
                )
            else:
                conn.execute("DELETE FROM serving_payload_current")
        conn.executemany(
            """
            INSERT OR REPLACE INTO serving_payload_current (
                payload_name,
                snapshot_id,
                run_id,
                refresh_mode,
                payload_json,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.commit()
        return {
            "status": "ok",
            "row_count": len(rows),
            "replace_all": bool(replace_all),
            "duration_seconds": round(float(time.perf_counter() - started_at), 3),
        }
    except Exception as exc:
        conn.rollback()
        return {
            "status": "error",
            "error": {"type": type(exc).__name__, "message": str(exc)},
            "duration_seconds": round(float(time.perf_counter() - started_at), 3),
        }
    finally:
        conn.close()


def verify_current_payloads_neon(
    pg_conn,
    *,
    rows: list[tuple[str, str, str, str, str, str]],
    replace_all: bool,
    normalize_payload_value: Callable[[Any], Any],
    payload_semantic_hash: Callable[[Any], str],
) -> dict[str, Any]:
    expected_by_name = {
        str(row[0]): {
            "snapshot_id": str(row[1]),
            "run_id": str(row[2]),
            "refresh_mode": str(row[3]),
            "payload_value": normalize_payload_value(row[4]),
            "payload_json_sha256": payload_semantic_hash(row[4]),
        }
        for row in rows
    }
    payload_names = sorted(expected_by_name.keys())
    out: dict[str, Any] = {
        "status": "ok",
        "expected_row_count": len(expected_by_name),
        "replace_all": bool(replace_all),
        "verified_row_count": 0,
        "verified_payload_names": [],
        "issues": [],
    }

    with pg_conn.cursor() as cur:
        if replace_all:
            cur.execute(
                """
                SELECT payload_name, snapshot_id, run_id, refresh_mode, payload_json::text
                FROM serving_payload_current
                ORDER BY payload_name
                """
            )
        elif payload_names:
            cur.execute(
                """
                SELECT payload_name, snapshot_id, run_id, refresh_mode, payload_json::text
                FROM serving_payload_current
                WHERE payload_name = ANY(%s)
                ORDER BY payload_name
                """,
                (payload_names,),
            )
        else:
            cur.execute(
                """
                SELECT payload_name, snapshot_id, run_id, refresh_mode, payload_json::text
                FROM serving_payload_current
                WHERE FALSE
                """
            )
        fetched = cur.fetchall()

    observed_by_name = {
        str(row[0]): {
            "snapshot_id": str(row[1]),
            "run_id": str(row[2]),
            "refresh_mode": str(row[3]),
            "payload_value": normalize_payload_value(row[4]),
            "payload_json_sha256": payload_semantic_hash(row[4]),
        }
        for row in fetched
    }
    observed_names = sorted(observed_by_name.keys())
    out["verified_row_count"] = len(observed_names)
    out["verified_payload_names"] = observed_names

    if replace_all:
        unexpected = sorted(set(observed_names) - set(payload_names))
        missing = sorted(set(payload_names) - set(observed_names))
        if unexpected:
            out["issues"].extend(f"unexpected_payload:{name}" for name in unexpected)
        if missing:
            out["issues"].extend(f"missing_payload:{name}" for name in missing)
        if len(observed_names) != len(payload_names):
            out["issues"].append(
                f"row_count_mismatch:{len(observed_names)}!={len(payload_names)}"
            )
    else:
        missing = sorted(set(payload_names) - set(observed_names))
        if missing:
            out["issues"].extend(f"missing_payload:{name}" for name in missing)

    for payload_name in payload_names:
        expected = expected_by_name.get(payload_name) or {}
        observed = observed_by_name.get(payload_name)
        if observed is None:
            continue
        for field in ("snapshot_id", "run_id", "refresh_mode"):
            if str(observed.get(field) or "") != str(expected.get(field) or ""):
                out["issues"].append(
                    f"metadata_mismatch:{payload_name}:{field}:{observed.get(field)}!={expected.get(field)}"
                )
        if observed.get("payload_value") != expected.get("payload_value"):
            out["issues"].append(
                "metadata_mismatch:"
                f"{payload_name}:payload_json_sha256:{observed.get('payload_json_sha256')}!={expected.get('payload_json_sha256')}"
            )

    if out["issues"]:
        out["status"] = "error"
    return out
