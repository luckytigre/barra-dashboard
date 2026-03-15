from __future__ import annotations

from backend.data import core_reads


def test_load_raw_cross_section_latest_prefers_well_covered_asof(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(core_reads, "_exposure_source_table_required", lambda: "barra_raw_cross_section_history")

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

    monkeypatch.setattr(core_reads, "_fetch_rows", _fake_fetch)

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
