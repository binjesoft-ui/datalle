import sys
from datetime import datetime
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "pipeline"))

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    DoubleType, IntegerType, StringType, StructField, StructType, TimestampType,
)

from quality_rules import RULES, split_clean_and_quarantine  # noqa: E402

SCHEMA = StructType([
    StructField("tpep_pickup_datetime", TimestampType()),
    StructField("tpep_dropoff_datetime", TimestampType()),
    StructField("passenger_count", IntegerType()),
    StructField("trip_distance", DoubleType()),
    StructField("fare_amount", DoubleType()),
    StructField("tip_amount", DoubleType()),
    StructField("total_amount", DoubleType()),
    StructField("pu_location_id", IntegerType()),
    StructField("do_location_id", IntegerType()),
    StructField("source_month", StringType()),
])

GOOD = (datetime(2024, 1, 5, 8, 0), datetime(2024, 1, 5, 8, 20), 1, 3.2, 15.0, 3.0, 21.0, 100, 200, "2024-01")


@pytest.fixture(scope="module")
def spark():
    s = SparkSession.builder.master("local[1]").appName("tests").getOrCreate()
    s.conf.set("spark.sql.ansi.enabled", "false")
    yield s
    s.stop()


def failed(spark, row):
    clean, quarantine = split_clean_and_quarantine(spark.createDataFrame([row], SCHEMA))
    q = quarantine.collect()
    return [] if not q else list(q[0]["failed_rules"])


def with_(**changes):
    names = [f.name for f in SCHEMA.fields]
    row = dict(zip(names, GOOD))
    row.update(changes)
    return tuple(row[n] for n in names)


def test_good_row_is_clean(spark):
    assert failed(spark, GOOD) == []


def test_dropoff_before_pickup(spark):
    row = with_(tpep_dropoff_datetime=datetime(2024, 1, 5, 7, 0))
    assert "pickup_before_dropoff" in failed(spark, row)


def test_pickup_outside_source_month(spark):
    row = with_(tpep_pickup_datetime=datetime(2008, 12, 31, 23, 0),
                tpep_dropoff_datetime=datetime(2008, 12, 31, 23, 30))
    assert "pickup_in_source_month" in failed(spark, row)


def test_impossible_speed(spark):
    row = with_(trip_distance=90.0)  # 90 miles in 20 minutes
    assert "speed_under_100_mph" in failed(spark, row)


@pytest.mark.parametrize("field,value,rule", [
    ("passenger_count", 0, "passenger_count_1_to_6"),
    ("passenger_count", 9, "passenger_count_1_to_6"),
    ("fare_amount", -5.0, "fare_positive"),
    ("total_amount", 0.0, "total_amount_positive"),
    ("tip_amount", -1.0, "tip_not_negative"),
    ("trip_distance", 0.0, "distance_positive_and_under_100_mi"),
    ("pu_location_id", None, "zones_present"),
])
def test_single_rule_failures(spark, field, value, rule):
    assert rule in failed(spark, with_(**{field: value}))


def test_null_values_fail_instead_of_passing(spark):
    assert failed(spark, with_(fare_amount=None)) != []


def test_every_rule_has_a_name_and_sql():
    assert all(isinstance(k, str) and isinstance(v, str) for k, v in RULES.items())
