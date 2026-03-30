from __future__ import annotations

from backend.data import core_reads


def test_load_raw_cross_section_latest_prefers_well_covered_asof(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        core_reads.source_reads,
        "exposure_source_table_required",
        lambda **_kwargs: "barra_raw_cross_section_history",
    )

    def _fake_fetch(sql: str, params=None):
        if "COUNT(*) AS row_count" in sql:
            return [
                {"as_of_date": "2026-03-04", "row_count": 10},
                {"as_of_date": "2026-03-03", "row_count": 3681},
            ]
        captured["params"] = list(params or [])
        return [
            {
                "ric": "LAZ.N",
                "ticker": "LAZ",
                "as_of_date": "2026-03-03",
                "growth_score": 0.5,
            }
        ]

    monkeypatch.setattr(core_reads.core_backend, "fetch_rows", lambda sql, params=None, **_kwargs: _fake_fetch(sql, params))

    out = core_reads.load_raw_cross_section_latest(tickers=["laz"])

    assert captured["params"] == ["2026-03-03", "LAZ"]
    assert out.to_dict("records") == [
        {
            "ric": "LAZ.N",
            "ticker": "LAZ",
            "as_of_date": "2026-03-03",
            "growth_score": 0.5,
        }
    ]


def test_core_read_backend_override_can_force_local_or_neon(monkeypatch) -> None:
    monkeypatch.setattr(core_reads.config, "neon_surface_enabled", lambda surface: surface == "core_reads")

    assert core_reads.core_read_backend_name() == "neon"
    with core_reads.core_read_backend("local"):
        assert core_reads.core_read_backend_name() == "local"
    with core_reads.core_read_backend("neon"):
        assert core_reads.core_read_backend_name() == "neon"


def test_load_source_dates_exposes_explicit_latest_available_exposure_date(monkeypatch) -> None:
    monkeypatch.setattr(core_reads.source_dates, "_pit_latest_closed_anchor", lambda: "2026-03-02")
    monkeypatch.setattr(
        core_reads.core_backend,
        "table_exists",
        lambda table, **_kwargs: table in {
            "security_fundamentals_pit",
            "security_classification_pit",
            "security_prices_eod",
            "barra_raw_cross_section_history",
        },
    )
    monkeypatch.setattr(
        core_reads.source_reads,
        "exposure_source_table_required",
        lambda **_kwargs: "barra_raw_cross_section_history",
    )

    def _fake_fetch(sql: str, params=None):
        if "security_fundamentals_pit" in sql:
            return [{"latest": "2026-03-02"}]
        if "security_classification_pit" in sql:
            return [{"latest": "2026-03-01"}]
        if "security_prices_eod" in sql:
            return [{"latest": "2026-03-03"}]
        if "barra_raw_cross_section_history" in sql:
            return [{"latest": "2026-03-04"}]
        return []

    monkeypatch.setattr(core_reads.core_backend, "fetch_rows", lambda sql, params=None, **_kwargs: _fake_fetch(sql, params))

    out = core_reads.load_source_dates()

    assert out == {
        "fundamentals_asof": "2026-03-02",
        "classification_asof": "2026-03-01",
        "prices_asof": "2026-03-03",
        "exposures_asof": "2026-03-04",
        "exposures_latest_available_asof": "2026-03-04",
    }


def test_load_source_dates_caps_pit_recency_to_latest_closed_anchor(monkeypatch) -> None:
    monkeypatch.setattr(core_reads.source_dates, "_pit_latest_closed_anchor", lambda: "2026-02-27")
    monkeypatch.setattr(
        core_reads.core_backend,
        "table_exists",
        lambda table, **_kwargs: table in {
            "security_fundamentals_pit",
            "security_classification_pit",
            "security_prices_eod",
            "barra_raw_cross_section_history",
        },
    )
    monkeypatch.setattr(
        core_reads.source_reads,
        "exposure_source_table_required",
        lambda **_kwargs: "barra_raw_cross_section_history",
    )

    def _fake_fetch(sql: str, params=None):
        if "security_fundamentals_pit" in sql and "<=" in sql:
            return [{"latest": "2026-02-27"}]
        if "security_classification_pit" in sql and "<=" in sql:
            return [{"latest": "2026-02-27"}]
        if "security_prices_eod" in sql:
            return [{"latest": "2026-03-13"}]
        if "barra_raw_cross_section_history" in sql:
            return [{"latest": "2026-03-13"}]
        return []

    monkeypatch.setattr(core_reads.core_backend, "fetch_rows", lambda sql, params=None, **_kwargs: _fake_fetch(sql, params))

    out = core_reads.load_source_dates()

    assert out == {
        "fundamentals_asof": "2026-02-27",
        "classification_asof": "2026-02-27",
        "prices_asof": "2026-03-13",
        "exposures_asof": "2026-03-13",
        "exposures_latest_available_asof": "2026-03-13",
    }
