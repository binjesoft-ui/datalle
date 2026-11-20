"""The declarative pipeline repeats the quality rules (pipeline files must be self-contained).
These tests make sure the copy can never drift from pipeline/quality_rules.py."""
import ast
import sys
from pathlib import Path

PIPELINE = Path(__file__).resolve().parents[1] / "pipeline"
sys.path.insert(0, str(PIPELINE))


def _literal_rules(path: Path) -> dict:
    tree = ast.parse(path.read_text())
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(getattr(t, "id", None) == "RULES" for t in node.targets):
            return ast.literal_eval(node.value)
    raise AssertionError(f"RULES not found in {path}")


def test_sdp_rules_are_identical_to_quality_rules():
    assert _literal_rules(PIPELINE / "sdp_nyc_taxi.py") == _literal_rules(PIPELINE / "quality_rules.py")


def test_sdp_file_is_valid_python_and_declares_expected_tables():
    src = (PIPELINE / "sdp_nyc_taxi.py").read_text()
    ast.parse(src)
    for table in [
        "silver_yellow_trips", "silver_yellow_quarantine",
        "gold_hourly_demand", "gold_daily_summary", "gold_top_pickup_zones",
    ]:
        assert f'name="{table}"' in src, table
    assert "expect_all_or_drop(EXPECTATIONS)" in src


def test_null_counts_as_failure_in_expectations():
    src = (PIPELINE / "sdp_nyc_taxi.py").read_text()
    assert 'f"coalesce({sql}, false)"' in src
