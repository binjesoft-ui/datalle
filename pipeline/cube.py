"""Aggregation cubes for the interactive dashboard.

Each cube stores SUMS and COUNTS, never averages. That keeps them re-aggregatable: the browser can
filter any dimension, add the sums up, and compute an average as sum / count for exactly that selection.

  daily : date x borough x payment          (trend over time)
  time  : month x borough x payment x weekday x hour   (demand pattern, tip by hour)
  zone  : month x borough x zone x payment  (top zones)

Borough and zone are the PICKUP location.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

PAYMENT_LABELS = {
    0: "Flex fare",
    1: "Credit card",
    2: "Cash",
    3: "No charge",
    4: "Dispute",
    5: "Unknown",
    6: "Voided",
}


def _payment_label():
    expr = F.lit("Other")
    for code, label in PAYMENT_LABELS.items():
        expr = F.when(F.col("payment_type") == code, F.lit(label)).otherwise(expr)
    return expr  # NULL payment_type falls through to "Other"


def prepare(trips: DataFrame, zones: DataFrame) -> DataFrame:
    z = zones.select("location_id", "borough", "zone")
    return (
        trips.join(z, trips.pu_location_id == z.location_id, "left")
        .withColumn("borough", F.coalesce(F.col("borough"), F.lit("Unknown")))
        .withColumn("zone", F.coalesce(F.col("zone"), F.lit("Unknown")))
        .withColumn("payment", _payment_label())
        .withColumn("month", F.col("source_month"))
        .withColumn("is_card", F.col("payment_type") == 1)
    )


def measures():
    """Additive measures only. Card-only columns exist because cash tips are not recorded."""
    card = F.col("is_card")
    return [
        F.count("*").alias("trips"),
        F.round(F.sum("fare_amount"), 2).alias("fare_sum"),
        F.round(F.sum("total_amount"), 2).alias("total_sum"),
        F.round(F.sum("trip_distance"), 2).alias("distance_sum"),
        F.round(F.sum("trip_minutes"), 1).alias("minutes_sum"),
        F.round(F.sum(F.when(card, F.col("fare_amount")).otherwise(0.0)), 2).alias("card_fare_sum"),
        F.round(F.sum(F.when(card, F.col("tip_amount")).otherwise(0.0)), 2).alias("card_tip_sum"),
    ]


def build_cubes(trips: DataFrame, zones: DataFrame) -> dict:
    df = prepare(trips, zones)
    m = measures()
    return {
        "daily": (df.groupBy("pickup_date", "borough", "payment").agg(*m)
                    .orderBy("pickup_date", "borough", "payment")),
        "time": (df.groupBy("month", "borough", "payment", "pickup_dow", "pickup_hour").agg(*m)
                   .orderBy("month", "borough", "payment", "pickup_dow", "pickup_hour")),
        "zone": (df.groupBy("month", "borough", "zone", "payment").agg(*m)
                   .orderBy("month", "borough", "zone", "payment")),
    }
