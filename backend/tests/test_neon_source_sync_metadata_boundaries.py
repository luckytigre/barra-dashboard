from __future__ import annotations

import ast
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
NEON_SOURCE_SYNC_METADATA = REPO_ROOT / "backend" / "services" / "neon_source_sync_metadata.py"
NEON_STAGE2 = REPO_ROOT / "backend" / "services" / "neon_stage2.py"
NEON_MIRROR = REPO_ROOT / "backend" / "services" / "neon_mirror.py"
STAGE_SOURCE = REPO_ROOT / "backend" / "orchestration" / "stage_source.py"


def _imported_modules(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                modules.add(str(alias.name))
        elif isinstance(node, ast.ImportFrom) and node.module:
            modules.add(str(node.module))
    return modules


def test_neon_source_sync_metadata_does_not_import_neon_stage2() -> None:
    imported = _imported_modules(NEON_SOURCE_SYNC_METADATA)
    assert "backend.services.neon_stage2" not in imported


def test_neon_stage2_keeps_public_source_sync_boundary_and_metadata_aliases() -> None:
    text = NEON_STAGE2.read_text(encoding="utf-8")
    assert "backend.services.neon_source_sync_metadata" in text
    assert "def sync_from_sqlite_to_neon(" in text
    assert "_record_source_sync_run_start" in text
    assert "_require_source_sync_metadata_tables" in text
    assert "_finalize_source_sync_run" in text
    assert "_materialize_security_source_status_current_pg" in text
    assert "_upsert_source_sync_watermarks" in text


def test_higher_layers_keep_using_existing_source_sync_boundaries() -> None:
    for path in (NEON_MIRROR, STAGE_SOURCE):
        imported = _imported_modules(path)
        assert "backend.services.neon_source_sync_metadata" not in imported
