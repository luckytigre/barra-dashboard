from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from typing import Any

from backend import config
from backend.data import serving_outputs


def _load_local_serving_payloads(data_db: Path) -> tuple[dict[str, Any], str, str, str]:
    conn = sqlite3.connect(str(data_db))
    try:
        rows = conn.execute(
            """
            SELECT payload_name, snapshot_id, run_id, refresh_mode, payload_json
            FROM serving_payload_current
            ORDER BY payload_name
            """
        ).fetchall()
    finally:
        conn.close()
    if not rows:
        raise RuntimeError(f"no local serving payloads found in {data_db}")

    snapshot_ids = {str(row[1] or "").strip() for row in rows}
    run_ids = {str(row[2] or "").strip() for row in rows}
    refresh_modes = {str(row[3] or "").strip() for row in rows}
    if len(snapshot_ids) != 1 or len(run_ids) != 1 or len(refresh_modes) != 1:
        raise RuntimeError(
            "local serving payloads are not on a single snapshot/run/mode: "
            f"snapshot_ids={sorted(snapshot_ids)} run_ids={sorted(run_ids)} refresh_modes={sorted(refresh_modes)}"
        )

    payloads = {
        str(row[0]): json.loads(str(row[4]))
        for row in rows
    }
    return payloads, next(iter(snapshot_ids)), next(iter(run_ids)), next(iter(refresh_modes))


def main() -> None:
    parser = argparse.ArgumentParser(description="Republish local serving_payload_current rows to Neon.")
    parser.add_argument(
        "--data-db",
        default=str(config.DATA_DB_PATH),
        help="Path to local data.db with serving_payload_current",
    )
    args = parser.parse_args()

    data_db = Path(args.data_db).expanduser().resolve()
    payloads, snapshot_id, run_id, refresh_mode = _load_local_serving_payloads(data_db)
    result = serving_outputs.persist_current_payloads(
        data_db=data_db,
        run_id=run_id,
        snapshot_id=snapshot_id,
        refresh_mode=refresh_mode,
        payloads=payloads,
        replace_all=True,
    )
    print(
        json.dumps(
            {
                "status": result.get("status"),
                "authority_store": result.get("authority_store"),
                "snapshot_id": snapshot_id,
                "run_id": run_id,
                "refresh_mode": refresh_mode,
                "payload_count": len(payloads),
                "neon_write": result.get("neon_write"),
                "sqlite_mirror_write": result.get("sqlite_mirror_write"),
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
