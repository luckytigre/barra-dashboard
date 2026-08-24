from __future__ import annotations

from backend.services import neon_holdings_store


class _Cursor:
    def __init__(self) -> None:
        self.sql = ""
        self.params = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def execute(self, sql: str, params=None) -> None:
        self.sql = sql
        self.params = params

    def fetchall(self):
        return [("acct_main", "Main", "shared", True, 2, 15.5, None)]


class _Connection:
    def __init__(self) -> None:
        self.cursor_value = _Cursor()

    def cursor(self) -> _Cursor:
        return self.cursor_value


def test_list_holdings_accounts_includes_live_account_type() -> None:
    conn = _Connection()

    rows = neon_holdings_store.list_holdings_accounts(conn, allowed_account_ids=["acct_main"])

    assert "a.account_type" in conn.cursor_value.sql
    assert "GROUP BY a.account_id, a.account_name, a.account_type, a.is_active" in conn.cursor_value.sql
    assert conn.cursor_value.params == (["acct_main"],)
    assert rows == [
        {
            "account_id": "acct_main",
            "account_name": "Main",
            "account_type": "shared",
            "is_active": True,
            "positions_count": 2,
            "gross_quantity": 15.5,
            "last_position_updated_at": None,
        }
    ]
