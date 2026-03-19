from __future__ import annotations

import ast
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
CPAR_DIR = REPO_ROOT / "backend" / "cpar"
FORBIDDEN_IMPORT_PREFIXES = (
    "backend.api",
    "backend.services",
    "backend.orchestration",
    "backend.data",
    "frontend",
    "fastapi",
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


def test_cpar_package_stays_pure_and_does_not_import_integration_layers() -> None:
    offenders: list[str] = []
    for path in sorted(CPAR_DIR.glob("*.py")):
        imported = _imported_modules(path)
        if any(
            name == prefix or name.startswith(prefix + ".")
            for name in imported
            for prefix in FORBIDDEN_IMPORT_PREFIXES
        ):
            offenders.append(path.name)
    assert offenders == []
