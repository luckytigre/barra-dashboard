from __future__ import annotations

import ast
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SERVING_OUTPUTS = REPO_ROOT / "backend" / "data" / "serving_outputs.py"
SERVING_OUTPUT_WRITE_AUTHORITY = REPO_ROOT / "backend" / "data" / "serving_output_write_authority.py"
SERVING_OUTPUT_MANIFEST = REPO_ROOT / "backend" / "data" / "serving_output_manifest.py"
REPAIR_SCRIPT = REPO_ROOT / "backend" / "scripts" / "repair_serving_payloads_neon.py"
FORBIDDEN_WRITE_AUTHORITY_TOKENS = (
    "load_current_payload(",
    "load_runtime_payload(",
    "collect_current_payload_manifest",
    "compare_current_payload_manifests",
)
FORBIDDEN_MANIFEST_TOKENS = (
    "_persist_current_payloads_neon",
    "_persist_current_payloads_sqlite",
    "_verify_current_payloads_neon",
)


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


def test_serving_output_write_authority_does_not_import_public_serving_outputs() -> None:
    imported = _imported_modules(SERVING_OUTPUT_WRITE_AUTHORITY)
    assert "backend.data.serving_outputs" not in imported


def test_serving_output_manifest_does_not_import_public_serving_outputs() -> None:
    imported = _imported_modules(SERVING_OUTPUT_MANIFEST)
    assert "backend.data.serving_outputs" not in imported


def test_serving_outputs_remains_the_public_write_and_manifest_boundary() -> None:
    text = SERVING_OUTPUTS.read_text(encoding="utf-8")
    assert "serving_output_write_authority" in text
    assert "serving_output_manifest" in text
    assert "def persist_current_payloads(" in text
    assert "def collect_current_payload_manifest(" in text


def test_repair_script_continues_to_depend_on_serving_outputs_boundary_module() -> None:
    imported = _imported_modules(REPAIR_SCRIPT)
    assert "backend.data.serving_output_write_authority" not in imported
    assert "backend.data.serving_output_manifest" not in imported
    text = REPAIR_SCRIPT.read_text(encoding="utf-8")
    assert "serving_outputs" in text


def test_lower_write_authority_stays_write_only() -> None:
    text = SERVING_OUTPUT_WRITE_AUTHORITY.read_text(encoding="utf-8")
    assert all(token not in text for token in FORBIDDEN_WRITE_AUTHORITY_TOKENS)


def test_lower_manifest_module_stays_manifest_only() -> None:
    text = SERVING_OUTPUT_MANIFEST.read_text(encoding="utf-8")
    assert all(token not in text for token in FORBIDDEN_MANIFEST_TOKENS)
