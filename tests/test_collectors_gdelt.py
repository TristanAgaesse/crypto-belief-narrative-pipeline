from __future__ import annotations

from pathlib import Path

from crypto_belief_pipeline.collectors import gdelt as gd


def test_keyword_for_filters_or_list_avoids_double_quoting() -> None:
    assert gd._keyword_for_filters('"bitcoin ETF" OR "BTC ETF"') == [
        "bitcoin ETF",
        "BTC ETF",
    ]
    assert gd._keyword_for_filters("bitcoin") == "bitcoin"
    assert gd._keyword_for_filters('"single phrase"') == "single phrase"


def test_load_narratives_config_reads_yaml(tmp_path: Path) -> None:
    p = tmp_path / "narratives.yaml"
    p.write_text(
        'narratives:\n  bitcoin_etf:\n    query: \'"bitcoin ETF"\'\n    assets: ["BTC"]\n',
        encoding="utf-8",
    )
    cfg = gd.load_narratives_config(p)
    assert "narratives" in cfg
    assert cfg["narratives"]["bitcoin_etf"]["query"] == '"bitcoin ETF"'


def test_fetch_timelinevol_converts_volume_intensity_columns(monkeypatch) -> None:
    def fake_request(mode: str, query_string: str, **kwargs):  # noqa: ANN001
        assert mode == "timelinevol"
        assert "bitcoin" in query_string
        return {
            "timeline": [
                {
                    "series": "Volume Intensity",
                    "data": [
                        {"date": "2026-05-05 12:00:00", "value": 0.00042},
                        {"date": "2026-05-05 13:00:00", "value": 0.00055},
                    ],
                }
            ]
        }

    monkeypatch.setattr(gd, "_gdelt_doc_request", fake_request)

    rows = gd.fetch_timelinevol(
        narrative="bitcoin_etf",
        query='"bitcoin ETF"',
        start_date="2026-05-05",
        end_date="2026-05-06",
        min_interval_sec=0,
    )
    assert len(rows) == 2
    assert rows[0]["narrative"] == "bitcoin_etf"
    assert rows[0]["query"] == '"bitcoin ETF"'
    assert rows[0]["mention_volume"] == 0.00042
    assert rows[0]["avg_tone"] is None
    assert rows[0]["source"] == "gdelt"
    assert rows[0]["timestamp"] == "2026-05-05T12:00:00Z"


def test_fetch_timelinevol_empty_dataframe_returns_empty(monkeypatch) -> None:
    monkeypatch.setattr(gd, "_gdelt_doc_request", lambda *a, **k: {})

    rows = gd.fetch_timelinevol("n", "q", "2026-05-05", "2026-05-06", min_interval_sec=0)
    assert rows == []


def test_collect_gdelt_raw_iterates_narratives(monkeypatch, tmp_path: Path) -> None:
    cfg = tmp_path / "narratives.yaml"
    cfg.write_text(
        "narratives:\n"
        "  a:\n"
        "    query: 'foo'\n"
        '    assets: ["BTC"]\n'
        "  b:\n"
        "    query: 'bar'\n"
        '    assets: ["ETH"]\n',
        encoding="utf-8",
    )

    def fake_fetch(
        narrative: str, query: str, start_date: str, end_date: str, **kwargs: object
    ) -> list[dict]:
        return [
            {
                "timestamp": "2026-05-05T12:00:00Z",
                "narrative": narrative,
                "query": query,
                "mention_volume": 0.001,
                "avg_tone": None,
                "source": "gdelt",
                "raw": {},
            }
        ]

    monkeypatch.setattr(gd, "fetch_timelinevol", fake_fetch)

    out = gd.collect_gdelt_raw(
        start_date="2026-05-05",
        end_date="2026-05-06",
        narratives_config_path=cfg,
    )
    assert {r["narrative"] for r in out} == {"a", "b"}
