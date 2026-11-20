import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "pipeline"))

import catalog_docs as D  # noqa: E402
from catalog_metadata import COLUMN_DESCRIPTIONS, DECLARED_LINEAGE, TABLE_DESCRIPTIONS  # noqa: E402


def test_descriptions_are_safe_for_sql_and_not_empty():
    for text in [*TABLE_DESCRIPTIONS.values(), *COLUMN_DESCRIPTIONS.values()]:
        assert text.strip()
        assert "'" not in text and "\\" not in text, text


def test_every_declared_lineage_endpoint_is_a_documented_table():
    for s, t in DECLARED_LINEAGE:
        assert s in TABLE_DESCRIPTIONS and t in TABLE_DESCRIPTIONS, (s, t)


def test_layers_come_from_table_prefixes():
    assert D.layer_of("bronze_yellow_trips") == "bronze"
    assert D.layer_of("silver_yellow_quarantine") == "silver"
    assert D.layer_of("gold_cube_time") == "gold"
    assert D.layer_of("something_else") == "other"


def test_comment_statements_only_touch_existing_columns_and_report_gaps():
    stmts, missing = D.comment_statements(
        "workspace", "nyc_taxi",
        {"gold_daily_summary": ["pickup_date", "trips", "brand_new_col"]},
        {"gold_daily_summary": "Daily."}, {"pickup_date": "Date.", "trips": "Count."},
    )
    assert stmts[0] == "COMMENT ON TABLE workspace.nyc_taxi.gold_daily_summary IS 'Daily.'"
    assert "ALTER COLUMN `trips` COMMENT 'Count.'" in stmts[2]
    assert missing == [("gold_daily_summary", "brand_new_col")]
    assert not any("brand_new_col" in s for s in stmts)


def test_apostrophes_are_rejected_instead_of_breaking_sql():
    with pytest.raises(ValueError):
        D.comment_statements("c", "s", {"t": ["a"]}, {}, {"a": "driver's note"})


def test_dictionary_shape_orders_layers_and_columns():
    tables = [{"table_name": "gold_x", "comment": "g"}, {"table_name": "bronze_x", "comment": None}]
    cols = [
        {"table_name": "bronze_x", "column_name": "b", "data_type": "int", "ordinal_position": 2, "comment": None},
        {"table_name": "bronze_x", "column_name": "a", "data_type": "string", "ordinal_position": 1, "comment": "A"},
    ]
    d = D.shape_dictionary("workspace", "nyc_taxi", tables, cols, {"bronze_x": 5})
    assert [t["name"] for t in d["tables"]] == ["bronze_x", "gold_x"]
    assert [c["name"] for c in d["tables"][0]["columns"]] == ["a", "b"]
    assert d["tables"][0]["rows"] == 5 and d["tables"][1]["rows"] is None
    assert d["tables"][0]["comment"] == ""


def test_lineage_prefers_unity_catalog_and_falls_back_to_declared():
    names = list(TABLE_DESCRIPTIONS)
    uc = D.shape_lineage(names, [("bronze_yellow_trips", "silver_yellow_trips"), ("other", "gold_x")], DECLARED_LINEAGE)
    assert uc["source"].startswith("Unity Catalog")
    assert uc["edges"] == [{"from": "bronze_yellow_trips", "to": "silver_yellow_trips"}]
    fb = D.shape_lineage(names, [], DECLARED_LINEAGE)
    assert fb["source"].startswith("Declared")
    assert len(fb["edges"]) == len(DECLARED_LINEAGE)
    assert [n["layer"] for n in fb["nodes"]] == sorted((n["layer"] for n in fb["nodes"]), key=lambda x: D.LAYER_ORDER[x])


def test_flow_definitions_become_edges_with_bare_names():
    rows = [("workspace.nyc_taxi_sdp.silver_yellow_trips", '["workspace.nyc_taxi.bronze_yellow_trips"]'),
            ("gold_x", '["silver_yellow_trips", "workspace.nyc_taxi.bronze_taxi_zones"]'), (None, None)]
    assert D.edges_from_flow_definitions(rows) == [
        ("bronze_yellow_trips", "silver_yellow_trips"),
        ("silver_yellow_trips", "gold_x"),
        ("bronze_taxi_zones", "gold_x"),
    ]


def test_expectations_shape_computes_failure_rate_and_sorts_worst_first():
    r = D.shape_expectations([("a.b.t", "r1", 90, 10), ("a.b.t", "r2", 100, 0), ("a.b.t", "r3", 0, 0)])["expectations"]
    assert [x["rule"] for x in r] == ["r1", "r2", "r3"]
    assert r[0]["pct_failed"] == 10.0 and r[1]["pct_failed"] == 0.0 and r[2]["pct_failed"] == 0.0
    assert r[0]["dataset"] == "t"
