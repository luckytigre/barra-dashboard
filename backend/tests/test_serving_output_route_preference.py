from __future__ import annotations

from fastapi.testclient import TestClient

from backend.main import app
from backend.api.routes import exposures as exposures_routes
from backend.api.routes import health as health_routes
from backend.api.routes import portfolio as portfolio_routes
from backend.api.routes import risk as risk_routes
from backend.api.routes import universe as universe_routes


def test_portfolio_prefers_serving_payload_over_cache(monkeypatch) -> None:
    monkeypatch.setattr(portfolio_routes, "load_current_payload", lambda name: {"positions": [], "total_value": 1, "position_count": 0} if name == "portfolio" else None)
    monkeypatch.setattr(portfolio_routes, "cache_get", lambda key: {"positions": [], "total_value": 999, "position_count": 9})

    client = TestClient(app)
    res = client.get("/api/portfolio")

    assert res.status_code == 200
    assert res.json()["total_value"] == 1


def test_exposures_prefers_serving_payload_over_cache(monkeypatch) -> None:
    monkeypatch.setattr(exposures_routes, "load_current_payload", lambda name: {"raw": [{"factor": "Size"}]} if name == "exposures" else None)
    monkeypatch.setattr(exposures_routes, "cache_get", lambda key: {"raw": [{"factor": "Momentum"}]})

    client = TestClient(app)
    res = client.get("/api/exposures?mode=raw")

    assert res.status_code == 200
    assert res.json()["factors"][0]["factor"] == "Size"


def test_risk_prefers_serving_payload_over_cache(monkeypatch) -> None:
    risk_payload = {
        "risk_shares": {"country": 1, "industry": 2, "style": 3, "idio": 94},
        "component_shares": {"country": 1, "industry": 2, "style": 3},
        "factor_details": [],
        "cov_matrix": {"factors": ["Country: US"], "correlation": [[1.0]]},
        "r_squared": 0.5,
        "risk_engine": {"specific_risk_ticker_count": 1},
    }
    monkeypatch.setattr(risk_routes, "load_current_payload", lambda name: risk_payload if name == "risk" else {"status": "ok"} if name == "model_sanity" else None)
    monkeypatch.setattr(risk_routes, "cache_get", lambda key: None)

    client = TestClient(app)
    res = client.get("/api/risk")

    assert res.status_code == 200
    assert res.json()["risk_shares"]["country"] == 1


def test_health_prefers_serving_payload_over_cache(monkeypatch) -> None:
    monkeypatch.setattr(health_routes, "require_role", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        health_routes,
        "load_current_payload",
        lambda name: {"status": "ok", "as_of": "2026-03-03", "notes": ["fresh"]} if name == "health_diagnostics" else None,
    )
    monkeypatch.setattr(
        health_routes,
        "cache_get",
        lambda key: {"status": "ok", "as_of": "2020-01-01", "notes": ["stale"]} if key == "health_diagnostics" else None,
    )

    client = TestClient(app)
    res = client.get("/api/health/diagnostics")

    assert res.status_code == 200
    assert res.json()["as_of"] == "2026-03-03"
    assert res.json()["notes"] == ["fresh"]


def test_universe_search_prefers_serving_payload_over_cache(monkeypatch) -> None:
    payload = {
        "index": [{"ticker": "JPM", "name": "JPMORGAN CHASE", "ric": "JPM.N"}],
        "by_ticker": {"JPM": {"ticker": "JPM", "ric": "JPM.N"}},
    }
    monkeypatch.setattr(universe_routes, "load_current_payload", lambda name: payload if name == "universe_loadings" else None)
    monkeypatch.setattr(universe_routes, "cache_get", lambda key: {"index": [], "by_ticker": {}})

    client = TestClient(app)
    res = client.get("/api/universe/search?q=JPM")

    assert res.status_code == 200
    assert res.json()["total"] == 1
