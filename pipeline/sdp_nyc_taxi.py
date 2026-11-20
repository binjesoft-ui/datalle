"""Lakeflow Spark Declarative Pipeline: NYC yellow taxi silver + gold with data quality expectations.

This is the *declarative* version of notebooks 02_silver and 03_gold. Instead of writing the load logic by
hand, each table is declared and Databricks works out the order, and records how many rows passed or failed
every rule (the "expectations") in the pipeline event log.

It reads the bronze tables already built by 01_bronze and writes to its own schema (set when you create the
pipeline, e.g. workspace.nyc_taxi_sdp) so it never collides with the notebook-built tables.

This file is NOT a notebook: add it as pipeline source code.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

BRONZE_SOURCE = "workspace.nyc_taxi.bronze_yellow_trips"
ZONES_SOURCE = "workspace.nyc_taxi.bronze_taxi_zones"

# Same rules as pipeline/quality_rules.py. They are repeated here because pipeline source files are
# self-contained; tests/test_sdp_rules.py fails if the two copies ever drift apart.
RULES = {
    "pickup_before_dropoff": "tpep_dropoff_datetime > tpep_pickup_datetime",
    "pickup_in_source_month": "date_format(tpep_pickup_datetime, 'yyyy-MM') = source_month",
    "duration_1_to_360_min": (
        "(unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime)) "
        "BETWEEN 60 AND 21600"
    ),
    "distance_positive_and_under_100_mi": "trip_distance > 0 AND trip_distance < 100",
    "speed_under_100_mph": (
        "try_divide(trip_distance, "
        "(unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime)) / 3600.0) <= 100"
    ),
    "passenger_count_1_to_6": "passenger_count BETWEEN 1 AND 6",
    "fare_positive": "fare_amount > 0",
    "total_amount_positive": "total_amount > 0",
    "tip_not_negative": "tip_amount >= 0",
    "zones_present": "pu_location_id IS NOT NULL AND do_location_id IS NOT NULL",
}

# NULL must count as a failure, exactly as in the notebook version.
EXPECTATIONS = {name: f"coalesce({sql}, false)" for name, sql in RULES.items()}


def _prepared():
    """Bronze trips with standard column names and exact duplicates removed."""
    return (
        spark.read.table(BRONZE_SOURCE)
        .withColumnRenamed("PULocationID", "pu_location_id")
        .withColumnRenamed("DOLocationID", "do_location_id")
        .dropDuplicates([
            "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
            "pu_location_id", "do_location_id", "total_amount",
        ])
    )


# ---------------------------------------------------------------- silver
@dp.materialized_view(
    name="silver_yellow_trips",
    comment="Validated yellow taxi trips. Rows failing any expectation are dropped (and counted in the event log).",
)
@dp.expect_all_or_drop(EXPECTATIONS)
def silver_yellow_trips():
    return (
        _prepared()
        .withColumn(
            "trip_minutes",
            (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60,
        )
        .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
        .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
        .withColumn("pickup_dow", F.dayofweek("tpep_pickup_datetime"))  # 1 = Sunday
        # try_divide, not "/": expectations drop bad rows AFTER this query runs, so a $0 fare must not raise
        # a divide-by-zero error under serverless's ANSI mode.
        .withColumn(
            "tip_pct",
            F.when(F.col("payment_type") == 1, F.expr("try_divide(tip_amount, fare_amount) * 100")),
        )
    )


@dp.materialized_view(
    name="silver_yellow_quarantine",
    comment="Trips that failed at least one rule, with the names of the failed rules. Nothing is silently lost.",
)
def silver_yellow_quarantine():
    failed = F.filter(
        F.array(*[F.when(~F.expr(sql), F.lit(name)) for name, sql in EXPECTATIONS.items()]),
        lambda x: x.isNotNull(),
    )
    return _prepared().withColumn("failed_rules", failed).where(F.size("failed_rules") > 0)


# ---------------------------------------------------------------- gold
@dp.materialized_view(name="gold_hourly_demand", comment="Trips and average duration by weekday and pickup hour.")
def gold_hourly_demand():
    return (
        spark.read.table("silver_yellow_trips")
        .groupBy("pickup_dow", "pickup_hour")
        .agg(F.count("*").alias("trips"), F.round(F.avg("trip_minutes"), 1).alias("avg_minutes"))
    )


@dp.materialized_view(name="gold_daily_summary", comment="Trips, revenue and average distance per day.")
def gold_daily_summary():
    return (
        spark.read.table("silver_yellow_trips")
        .groupBy("pickup_date")
        .agg(
            F.count("*").alias("trips"),
            F.round(F.sum("total_amount"), 0).alias("revenue"),
            F.round(F.avg("trip_distance"), 2).alias("avg_miles"),
        )
    )


@dp.materialized_view(name="gold_top_pickup_zones", comment="The 20 pickup zones with the most trips.")
def gold_top_pickup_zones():
    trips = spark.read.table("silver_yellow_trips")
    zones = spark.read.table(ZONES_SOURCE)
    return (
        trips.join(zones, trips.pu_location_id == zones.location_id)
        .groupBy("borough", "zone")
        .agg(F.count("*").alias("trips"), F.round(F.sum("total_amount"), 0).alias("revenue"))
        .orderBy(F.desc("trips"))
        .limit(20)
    )
