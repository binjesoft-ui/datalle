"""Run the declarative pipeline's table logic locally, with the pipeline decorators stubbed out, and check it
produces exactly the same clean/quarantine split as the notebook version (quality_rules.py).

Databricks-only pieces (the expectation decorators themselves) cannot run locally; everything else can."""
import sys
import types
from datetime import datetime
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
PIPELINE = Path(__file__).resolve().parents[1] / "pipeline"
sys.path.insert(0, str(PIPELINE))

import pyspark  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from quality_rules import split_clean_and_quarantine  # noqa: E402

BRONZE_COLS = (
    "VendorID int, tpep_pickup_datetime timestamp, tpep_dropoff_datetime timestamp, passenger_count int, "
    "trip_distance double, RatecodeID int, store_and_fwd_flag string, PULocationID int, DOLocationID int, "
    "payment_type int, fare_amount double, extra double, mta_tax double, tip_amount double, tolls_amount double, "
    "improvement_surcharge double, total_amount double, congestion_surcharge double, airport_fee double, "
    "source_month string, source_file string, ingested_at timestamp"
)


def row(vendor=1, pickup=datetime(2024, 1, 5, 8, 0), dropoff=datetime(2024, 1, 5, 8, 20), passengers=1, miles=3.2,
        pu=100, do=200, payment=1, fare=15.0, tip=3.0, total=21.0, month="2024-01"):
    return (vendor, pickup, dropoff, passengers, miles, 1, "N", pu, do, payment, fare, 0.0, 0.5, tip, 0.0, 1.0,
            total, 2.5, 0.0, month, "f.parquet", datetime(2026, 11, 19))


ROWS = [
    row(),                                                    # clean
    row(vendor=2, pickup=datetime(2024, 1, 6, 9, 0), dropoff=datetime(2024, 1, 6, 9, 30)),   # clean
    row(vendor=2, pickup=datetime(2024, 1, 6, 9, 0), dropoff=datetime(2024, 1, 6, 9, 30)),   # exact duplicate of previous
    row(passengers=0),                                        # bad passengers
    row(passengers=None),                                     # NULL must fail
    row(fare=0.0, tip=1.0, total=5.0),                        # $0 fare (must not raise a divide-by-zero error)
    row(fare=-4.0, total=-4.0),                               # negative fare and total
    row(dropoff=datetime(2024, 1, 5, 7, 0)),                  # dropoff before pickup
    row(miles=90.0),                                          # impossible speed
    row(pickup=datetime(2008, 12, 31, 23, 0), dropoff=datetime(2008, 12, 31, 23, 30)),   # outside source month
    row(pu=None),                                             # missing zone
    row(payment=2, tip=0.0, pickup=datetime(2024, 1, 7, 10, 0), dropoff=datetime(2024, 1, 7, 10, 25)),   # cash trip, clean
]


@pytest.fixture(scope="module")
def sdp():
    spark = SparkSession.builder.master("local[1]").appName("sdp-tests").getOrCreate()
    spark.conf.set("spark.sql.ansi.enabled", "true")  # Databricks serverless runs with ANSI on

    spark.createDataFrame(ROWS, BRONZE_COLS).createOrReplaceTempView("t_bronze")
    spark.createDataFrame(
        [(100, "Manhattan", "Midtown", "Yellow Zone"), (200, "Queens", "JFK Airport", "Airports")],
        "location_id int, borough string, zone string, service_zone string",
    ).createOrReplaceTempView("t_zones")
    # the pipeline reads silver by its bare name; provide it after we build it below
    stub = types.ModuleType("pyspark.pipelines")

    def identity(*args, **kwargs):
        if len(args) == 1 and callable(args[0]) and not kwargs:
            return args[0]
        return lambda f: f

    for name in ("materialized_view", "table", "view", "expect_all_or_drop", "expect_all", "expect_or_drop"):
        setattr(stub, name, identity)

    saved_mod, saved_attr = sys.modules.get("pyspark.pipelines"), getattr(pyspark, "pipelines", None)
    sys.modules["pyspark.pipelines"] = stub
    pyspark.pipelines = stub
    ns = {"spark": spark, "__name__": "sdp_nyc_taxi"}
    try:
        exec(compile((PIPELINE / "sdp_nyc_taxi.py").read_text(), "sdp_nyc_taxi.py", "exec"), ns)
    finally:
        if saved_mod is not None:
            sys.modules["pyspark.pipelines"] = saved_mod
        else:
            sys.modules.pop("pyspark.pipelines", None)
        if saved_attr is not None:
            pyspark.pipelines = saved_attr
        elif hasattr(pyspark, "pipelines"):
            delattr(pyspark, "pipelines")

    ns["BRONZE_SOURCE"], ns["ZONES_SOURCE"] = "t_bronze", "t_zones"
    yield spark, ns
    spark.stop()


def apply_expectations(df, expectations):
    """What @dp.expect_all_or_drop does: keep only rows where every expectation is true."""
    for sql in expectations.values():
        df = df.where(F.expr(sql))
    return df


def test_declarative_split_matches_notebook_split(sdp):
    spark, ns = sdp
    silver_all = ns["silver_yellow_trips"]()                       # includes bad rows until expectations run
    clean_sdp = apply_expectations(silver_all, ns["EXPECTATIONS"])
    quarantine_sdp = ns["silver_yellow_quarantine"]()

    clean_nb, quarantine_nb = split_clean_and_quarantine(ns["_prepared"]())

    assert clean_sdp.count() == clean_nb.count() == 3              # clean, clean (dup removed), cash trip
    assert quarantine_sdp.count() == quarantine_nb.count()
    got = {tuple(sorted(r["failed_rules"])) for r in quarantine_sdp.collect()}
    want = {tuple(sorted(r["failed_rules"])) for r in quarantine_nb.collect()}
    assert got == want


def test_every_row_is_either_clean_or_quarantined(sdp):
    _, ns = sdp
    total = ns["_prepared"]().count()
    clean = apply_expectations(ns["silver_yellow_trips"](), ns["EXPECTATIONS"]).count()
    assert clean + ns["silver_yellow_quarantine"]().count() == total


def test_zero_fare_does_not_raise_under_ansi(sdp):
    _, ns = sdp
    bad = ns["silver_yellow_trips"]().where("fare_amount = 0").select("tip_pct").collect()
    assert bad and bad[0]["tip_pct"] is None   # try_divide gives NULL, not an error


def test_gold_tables_build_from_clean_silver(sdp):
    spark, ns = sdp
    clean = apply_expectations(ns["silver_yellow_trips"](), ns["EXPECTATIONS"])
    clean.createOrReplaceTempView("silver_yellow_trips")      # the pipeline reads it by bare name
    assert ns["gold_daily_summary"]().agg(F.sum("trips")).first()[0] == 3
    assert ns["gold_hourly_demand"]().agg(F.sum("trips")).first()[0] == 3
    zones = ns["gold_top_pickup_zones"]().collect()
    assert sum(r["trips"] for r in zones) == 3
