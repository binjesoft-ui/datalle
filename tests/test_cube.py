import sys
from datetime import date
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "pipeline"))

from pyspark.sql import SparkSession  # noqa: E402

from cube import build_cubes  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    s = SparkSession.builder.master("local[1]").appName("cube-tests").getOrCreate()
    s.conf.set("spark.sql.ansi.enabled", "true")  # Databricks serverless runs with ANSI on
    yield s
    s.stop()


@pytest.fixture(scope="module")
def cubes(spark):
    trips = spark.createDataFrame(
        [
            # source_month, pickup_date, dow, hour, zone, payment, fare, tip, total, miles, minutes
            ("2024-01", date(2024, 1, 5), 6, 8, 1, 1, 10.0, 2.0, 14.0, 2.0, 10.0),
            ("2024-01", date(2024, 1, 5), 6, 8, 1, 1, 20.0, 4.0, 26.0, 5.0, 20.0),
            ("2024-01", date(2024, 1, 6), 7, 9, 2, 2, 8.0, 0.0, 10.0, 1.0, 8.0),    # cash
            ("2024-02", date(2024, 2, 1), 5, 7, 99, 9, 6.0, 0.0, 7.0, 1.0, 5.0),    # unknown zone + odd payment
        ],
        "source_month string, pickup_date date, pickup_dow int, pickup_hour int, pu_location_id int, "
        "payment_type int, fare_amount double, tip_amount double, total_amount double, "
        "trip_distance double, trip_minutes double",
    )
    zones = spark.createDataFrame(
        [(1, "Manhattan", "Midtown"), (2, "Queens", "JFK Airport")],
        "location_id int, borough string, zone string",
    )
    return {k: v.toPandas() for k, v in build_cubes(trips, zones).items()}


def test_every_cube_reconciles_to_total_trips(cubes):
    for name, pdf in cubes.items():
        assert pdf["trips"].sum() == 4, name


def test_unknown_zone_and_odd_payment_are_kept_not_dropped(cubes):
    z = cubes["zone"]
    row = z[z["borough"] == "Unknown"].iloc[0]
    assert row["zone"] == "Unknown" and row["payment"] == "Other" and row["trips"] == 1


def test_card_tip_columns_only_count_card_trips(cubes):
    t = cubes["time"]
    assert t["card_tip_sum"].sum() == 6.0      # 2 + 4, cash tip excluded
    assert t["card_fare_sum"].sum() == 30.0    # 10 + 20


def test_measures_are_additive_sums_not_averages(cubes):
    d = cubes["daily"]
    jan5 = d[(d["pickup_date"].astype(str) == "2024-01-05") & (d["payment"] == "Credit card")].iloc[0]
    assert jan5["trips"] == 2 and jan5["fare_sum"] == 30.0 and jan5["distance_sum"] == 7.0
