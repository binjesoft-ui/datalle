# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver: clean, validate, quarantine
# MAGIC Applies the rules in `quality_rules.py`. Valid rows go to `silver_yellow_trips`;
# MAGIC invalid rows go to `silver_yellow_quarantine` with the list of rules they failed.
# MAGIC Nothing is silently dropped, and re-running a month replaces that month only.

# COMMAND ----------
from pyspark.sql import functions as F

import config as C
from quality_rules import split_clean_and_quarantine

# COMMAND ----------
def build_month(month: str) -> None:
    bronze = (
        spark.table(C.BRONZE_TRIPS)
        .where(F.col("source_month") == month)
        .withColumnRenamed("PULocationID", "pu_location_id")
        .withColumnRenamed("DOLocationID", "do_location_id")
        .dropDuplicates([
            "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
            "pu_location_id", "do_location_id", "total_amount",
        ])
    )

    clean, quarantine = split_clean_and_quarantine(bronze)

    clean = (
        clean
        .withColumn("trip_minutes",
                    (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60)
        .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
        .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
        .withColumn("pickup_dow", F.dayofweek("tpep_pickup_datetime"))  # 1=Sunday
        .withColumn("tip_pct",
                    F.when(F.col("payment_type") == 1,   # card only: cash tips are not recorded
                           F.col("tip_amount") / F.col("fare_amount") * 100))
    )

    for df, table in ((clean, C.SILVER_TRIPS), (quarantine, C.SILVER_QUARANTINE)):
        if spark.catalog.tableExists(table):
            (df.write.mode("overwrite")
               .option("replaceWhere", f"source_month = '{month}'")
               .saveAsTable(table))
        else:
            df.write.partitionBy("source_month").saveAsTable(table)
    print(f"Silver built for {month}")


for m in C.MONTHS:
    build_month(m)

# COMMAND ----------
display(
    spark.table(C.SILVER_TRIPS).groupBy("source_month").count().orderBy("source_month")
)
