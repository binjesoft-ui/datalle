# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze: land raw TLC files
# MAGIC Downloads monthly yellow taxi parquet files into a Unity Catalog Volume and loads them
# MAGIC into a Delta table **unchanged** (only column names/types are normalised and lineage columns added).
# MAGIC Safe to re-run: each month is written with `replaceWhere`, so there are no duplicates.

# COMMAND ----------
import os
import urllib.request

from pyspark.sql import functions as F

import config as C  # works when pipeline/ is a Databricks Repo folder; else %run ./config

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {C.CATALOG}.{C.SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {C.CATALOG}.{C.SCHEMA}.{C.VOLUME}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {C.CATALOG}.{C.SCHEMA}.export")

# COMMAND ----------
def fetch(url: str, dest: str) -> bool:
    """Download url to dest unless it is already there. Returns False if blocked/missing.

    Free Edition limits outbound internet to trusted domains. If this fails, download the
    file on your PC and upload it to the Volume through the Catalog Explorer UI instead.
    """
    if os.path.exists(dest):
        return True
    try:
        urllib.request.urlretrieve(url, dest)
        return True
    except Exception as exc:  # noqa: BLE001
        print(f"Could not download {url}: {exc}")
        return False


fetch(C.ZONE_LOOKUP_URL, f"{C.VOLUME_PATH}/taxi_zone_lookup.csv")

# COMMAND ----------
# Zone lookup (small dimension table)
zones = (
    spark.read.option("header", True).csv(f"{C.VOLUME_PATH}/taxi_zone_lookup.csv")
    .select(
        F.col("LocationID").cast("int").alias("location_id"),
        F.col("Borough").alias("borough"),
        F.col("Zone").alias("zone"),
        F.col("service_zone"),
    )
)
zones.write.mode("overwrite").saveAsTable(C.BRONZE_ZONES)

# COMMAND ----------
# Explicit target types: TLC files change types between months (e.g. passenger_count
# is double in some files, long in others), so we cast everything to one schema.
COLUMNS = {
    "VendorID": "int",
    "tpep_pickup_datetime": "timestamp",
    "tpep_dropoff_datetime": "timestamp",
    "passenger_count": "int",
    "trip_distance": "double",
    "RatecodeID": "int",
    "store_and_fwd_flag": "string",
    "PULocationID": "int",
    "DOLocationID": "int",
    "payment_type": "int",
    "fare_amount": "double",
    "extra": "double",
    "mta_tax": "double",
    "tip_amount": "double",
    "tolls_amount": "double",
    "improvement_surcharge": "double",
    "total_amount": "double",
    "congestion_surcharge": "double",
    "airport_fee": "double",
}


def load_month(month: str) -> None:
    path = f"{C.VOLUME_PATH}/yellow_tripdata_{month}.parquet"
    if not fetch(f"{C.TLC_BASE_URL}/yellow_tripdata_{month}.parquet", path):
        print(f"Skipping {month}: file not available")
        return

    raw = spark.read.parquet(path)
    # Column-name casing differs between months (Airport_fee vs airport_fee).
    lower = {c.lower(): c for c in raw.columns}
    cols = []
    for name, dtype in COLUMNS.items():
        src = lower.get(name.lower())
        col = F.col(src).cast(dtype) if src else F.lit(None).cast(dtype)
        cols.append(col.alias(name))

    df = (
        raw.select(*cols)
        .withColumn("source_month", F.lit(month))
        .withColumn("source_file", F.lit(os.path.basename(path)))
        .withColumn("ingested_at", F.current_timestamp())
    )

    if spark.catalog.tableExists(C.BRONZE_TRIPS):
        (df.write.mode("overwrite")
           .option("replaceWhere", f"source_month = '{month}'")
           .saveAsTable(C.BRONZE_TRIPS))
    else:
        df.write.partitionBy("source_month").saveAsTable(C.BRONZE_TRIPS)
    print(f"Loaded {month}")


for m in C.MONTHS:
    load_month(m)

# COMMAND ----------
display(spark.table(C.BRONZE_TRIPS).groupBy("source_month").count().orderBy("source_month"))
