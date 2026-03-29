from __future__ import annotations

from backend.scripts import cleanup_security_master_second_pass_aliases


def test_cleanup_security_master_second_pass_aliases_is_retired(capsys) -> None:
    assert cleanup_security_master_second_pass_aliases.main() == 1

    captured = capsys.readouterr()
    assert "retired" in captured.err.lower()
    assert "physical security_master cleanup" in captured.err.lower()
    assert "backend/scripts/_archive/cleanup_security_master_second_pass_aliases.py" in captured.err
    assert "docs/archive/one-time-protocols/SECURITY_MASTER_ALIAS_CLEANUP.md" in captured.err
