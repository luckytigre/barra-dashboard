from __future__ import annotations

from fastapi.testclient import TestClient

from backend.api.routes import portfolio as portfolio_routes
from backend.main import app


def test_portfolio_whatif_route_returns_preview_payload(monkeypatch) -> None:
    monkeypatch.setattr(
        portfolio_routes,
        "preview_portfolio_whatif",
        lambda scenario_rows: {
            "scenario_rows": scenario_rows,
            "holding_deltas": [],
            "current": {
                "positions": [{"ticker": "AAA", "trbc_sector": "Technology"}],
                "total_value": 100.0,
                "position_count": 1,
                "risk_shares": {"country": 1.0, "industry": 2.0, "style": 3.0, "idio": 94.0},
                "component_shares": {"country": 1.0, "industry": 2.0, "style": 3.0},
                "factor_details": [],
                "exposure_modes": {"raw": [], "sensitivity": [], "risk_contribution": []},
            },
            "hypothetical": {
                "positions": [{"ticker": "AAA", "trbc_sector": "Technology"}],
                "total_value": 120.0,
                "position_count": 1,
                "risk_shares": {"country": 2.0, "industry": 2.0, "style": 4.0, "idio": 92.0},
                "component_shares": {"country": 2.0, "industry": 2.0, "style": 4.0},
                "factor_details": [],
                "exposure_modes": {"raw": [], "sensitivity": [], "risk_contribution": []},
            },
            "diff": {
                "total_value": 20.0,
                "position_count": 0,
                "risk_shares": {"country": 1.0, "industry": 0.0, "style": 1.0, "idio": -2.0},
                "factor_deltas": {"raw": [], "sensitivity": [], "risk_contribution": []},
            },
            "source_dates": {},
            "_preview_only": True,
        },
    )

    client = TestClient(app)
    res = client.post(
        "/api/portfolio/whatif",
        json={"scenario_rows": [{"account_id": "acct_a", "ticker": "AAA", "quantity": 20}]},
    )

    assert res.status_code == 200
    body = res.json()
    assert body["_preview_only"] is True
    assert body["current"]["positions"][0]["trbc_economic_sector_short"] == "Technology"
    assert body["hypothetical"]["total_value"] == 120.0


def test_portfolio_whatif_route_rejects_missing_account_id() -> None:
    client = TestClient(app)
    res = client.post(
        "/api/portfolio/whatif",
        json={"scenario_rows": [{"ticker": "AAA", "quantity": 20}]},
    )

    assert res.status_code == 422
