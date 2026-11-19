"""Data quality rules for NYC yellow taxi trips.

Each rule is a Spark SQL expression that is TRUE when the row is VALID.
A NULL result counts as a failure, so missing values never slip through.
Keeping the rules as plain data makes them easy to document, test and report on.
"""
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

# name -> SQL expression that must be true for a valid row
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


def failed_rules_column(rules: dict = RULES) -> Column:
    """Array column listing the names of every rule a row fails (empty = clean)."""
    checks = [
        F.when(~F.coalesce(F.expr(sql), F.lit(False)), F.lit(name))
        for name, sql in rules.items()
    ]
    return F.filter(F.array(*checks), lambda x: x.isNotNull())


def split_clean_and_quarantine(df: DataFrame, rules: dict = RULES):
    """Return (clean, quarantine). Quarantine keeps the rows plus a failed_rules array."""
    flagged = df.withColumn("failed_rules", failed_rules_column(rules))
    clean = flagged.where(F.size("failed_rules") == 0).drop("failed_rules")
    quarantine = flagged.where(F.size("failed_rules") > 0)
    return clean, quarantine
