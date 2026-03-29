"""Pure serving-payload manifest helpers behind the serving facade."""

from __future__ import annotations

from typing import Any


def compare_current_payload_manifests(
    left: dict[str, Any],
    right: dict[str, Any],
) -> dict[str, Any]:
    left_payloads = dict(left.get("payloads") or {})
    right_payloads = dict(right.get("payloads") or {})
    left_names = sorted(left_payloads.keys())
    right_names = sorted(right_payloads.keys())
    common_names = sorted(set(left_names).intersection(right_names))
    issues: list[str] = []

    missing_left = sorted(set(right_names) - set(left_names))
    missing_right = sorted(set(left_names) - set(right_names))
    issues.extend(f"missing_left:{name}" for name in missing_left)
    issues.extend(f"missing_right:{name}" for name in missing_right)

    for field in ("snapshot_id", "run_id", "refresh_mode", "payload_sha256"):
        for payload_name in common_names:
            left_value = str(left_payloads.get(payload_name, {}).get(field) or "")
            right_value = str(right_payloads.get(payload_name, {}).get(field) or "")
            if left_value != right_value:
                issues.append(
                    f"mismatch:{payload_name}:{field}:{left_value}!={right_value}"
                )

    if sorted(left.get("distinct_snapshot_ids") or []) != sorted(right.get("distinct_snapshot_ids") or []):
        issues.append(
            "manifest_mismatch:distinct_snapshot_ids:"
            f"{sorted(left.get('distinct_snapshot_ids') or [])}!={sorted(right.get('distinct_snapshot_ids') or [])}"
        )
    if sorted(left.get("distinct_run_ids") or []) != sorted(right.get("distinct_run_ids") or []):
        issues.append(
            "manifest_mismatch:distinct_run_ids:"
            f"{sorted(left.get('distinct_run_ids') or [])}!={sorted(right.get('distinct_run_ids') or [])}"
        )
    if sorted(left.get("distinct_refresh_modes") or []) != sorted(right.get("distinct_refresh_modes") or []):
        issues.append(
            "manifest_mismatch:distinct_refresh_modes:"
            f"{sorted(left.get('distinct_refresh_modes') or [])}!={sorted(right.get('distinct_refresh_modes') or [])}"
        )

    return {
        "status": "ok" if not issues else "error",
        "left_store": str(left.get("store") or ""),
        "right_store": str(right.get("store") or ""),
        "issues": issues,
        "left_row_count": int(left.get("row_count") or 0),
        "right_row_count": int(right.get("row_count") or 0),
        "shared_payload_count": len(common_names),
    }


def manifest_from_rows(
    rows: list[tuple[Any, ...]],
    *,
    store: str,
    requested_payload_names: list[str],
    payload_semantic_hash,
    canonical_serving_payload_name_set: frozenset[str],
) -> dict[str, Any]:
    payloads: dict[str, dict[str, Any]] = {}
    snapshot_ids: set[str] = set()
    run_ids: set[str] = set()
    refresh_modes: set[str] = set()
    observed_names: list[str] = []
    for payload_name, snapshot_id, run_id, refresh_mode, payload_json, updated_at in rows:
        clean_name = str(payload_name or "").strip()
        if not clean_name:
            continue
        snapshot = str(snapshot_id or "").strip()
        run = str(run_id or "").strip()
        mode = str(refresh_mode or "").strip()
        payload_text = str(payload_json or "")
        payloads[clean_name] = {
            "snapshot_id": snapshot,
            "run_id": run,
            "refresh_mode": mode,
            "updated_at": str(updated_at or ""),
            "payload_sha256": payload_semantic_hash(payload_text),
            "payload_bytes": len(payload_text.encode("utf-8")),
        }
        observed_names.append(clean_name)
        if snapshot:
            snapshot_ids.add(snapshot)
        if run:
            run_ids.add(run)
        if mode:
            refresh_modes.add(mode)

    observed_name_set = set(observed_names)
    missing_requested = sorted(set(requested_payload_names) - observed_name_set)
    missing_canonical = sorted(canonical_serving_payload_name_set - observed_name_set)
    return {
        "status": "ok",
        "store": str(store),
        "row_count": len(observed_names),
        "payload_names": sorted(observed_names),
        "requested_payload_names": list(requested_payload_names),
        "missing_requested_payloads": missing_requested,
        "payloads": payloads,
        "distinct_snapshot_ids": sorted(snapshot_ids),
        "distinct_run_ids": sorted(run_ids),
        "distinct_refresh_modes": sorted(refresh_modes),
        "canonical_payload_set_complete": not missing_canonical,
        "missing_canonical_payloads": missing_canonical,
    }
