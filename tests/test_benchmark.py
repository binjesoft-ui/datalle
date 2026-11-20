import ast
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytest

duckdb = pytest.importorskip("duckdb")
pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")
pytest.importorskip("pyspark")

PIPELINE = Path(__file__).resolve().parents[1] / "pipeline"
sys.path.insert(0, str(PIPELINE))

import benchmark as B  # noqa: E402
from quality_rules import RULES  # noqa: E402


def test_duckdb_rules_cover_exactly_the_same_rule_names():
    assert set(B.DUCKDB_RULES) == set(RULES)


def test_columns_match_the_bronze_notebook():
    tree = ast.parse((PIPELINE / "01_bronze.py").read_text())
    cols = next(ast.literal_eval(n.value) for n in ast.walk(tree)
                if isinstance(n, ast.Assign) and getattr(n.targets[0], "id", "") == "COLUMNS")
    assert cols == B.COLUMNS


def make_file(path, month, n, airport_name="airport_fee", passenger_type=pa.int64()):
    y, m = map(int, month.split("-"))
    base = datetime(y, m, 3, 8, 0)
    rows = []
    for i in range(n):
        pick = base + timedelta(minutes=7 * i)
        drop = pick + timedelta(minutes=[15, 15, 0, 500, 20][i % 5])      # bad: zero and 500-minute trips
        rows.append(dict(
            VendorID=1 + i % 2, tpep_pickup_datetime=pick if i % 11 else pick.replace(year=2008),   # wrong month
            tpep_dropoff_datetime=drop, passenger_count=[1, 2, 0, None, 3, 7][i % 6],
            trip_distance=[2.0, 0.0, 150.0, 5.5][i % 4], RatecodeID=1, store_and_fwd_flag="N",
            PULocationID=100 if i % 13 else None, DOLocationID=200, payment_type=1 + i % 2,
            fare_amount=[10.0, -3.0, 0.0, 25.5][i % 4], extra=0.0, mta_tax=0.5, tip_amount=[1.0, -1.0, 0.0][i % 3],
            tolls_amount=0.0, improvement_surcharge=1.0, total_amount=[14.0, -3.0, 30.0][i % 3],
            congestion_surcharge=2.5, **{airport_name: 0.0}))
    rows += rows[:5]                                                      # exact duplicates
    table = pa.Table.from_pylist(rows)
    idx = table.schema.get_field_index("passenger_count")
    table = table.set_column(idx, "passenger_count", table["passenger_count"].cast(passenger_type))
    pq.write_table(table, path)


@pytest.fixture(scope="module")
def files(tmp_path_factory):
    d = tmp_path_factory.mktemp("tlc")
    specs = [("2024-01", dict()), ("2024-02", dict(airport_name="Airport_fee", passenger_type=pa.float64())),
             ("2024-03", dict())]
    paths = []
    for month, kw in specs:
        p = str(d / f"yellow_tripdata_{month}.parquet")
        make_file(p, month, 400, **kw)
        paths.append(p)
    return paths


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession
    s = (SparkSession.builder.master("local[2]").appName("bench").config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.ansi.enabled", "true").getOrCreate())
    yield s
    s.stop()


def test_engines_agree_on_messy_multi_schema_files(files, spark):
    duck = B.run_duckdb(files)
    spk = B.run_spark(spark, files, RULES)
    assert B.results_match(duck, spk), (duck, spk)
    assert duck["total_rows"] == 3 * 400                    # the 5 appended duplicates per file were dropped
    assert 0 < duck["clean_rows"] < duck["total_rows"]
    assert sum(duck["failed"].values()) > 0 and all(v >= 0 for v in duck["failed"].values())


def test_results_match_detects_a_difference(files):
    a = B.run_duckdb(files)
    b = {**a, "failed": {**a["failed"], "fare_positive": a["failed"]["fare_positive"] + 1}}
    assert B.results_match(a, a) and not B.results_match(a, b)


def test_timed_records_errors_instead_of_hiding_them():
    secs, res = B.timed(lambda: 1 / 0, runs=3)
    assert secs == [] and "ZeroDivisionError" in res
    assert B.summarise_engine(secs, res)["ok"] is False
    secs, res = B.timed(lambda: {"x": 1}, runs=2)
    assert B.summarise_engine(secs, res)["median_s"] == pytest.approx(sorted(secs)[0] if secs[0] == secs[1] else sum(secs) / 2, abs=1)


def test_month_of():
    assert B.month_of("/Volumes/a/b/yellow_tripdata_2024-07.parquet") == "2024-07"
    with pytest.raises(ValueError):
        B.month_of("nope.parquet")
