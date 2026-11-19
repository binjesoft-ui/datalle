# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Gold: small, query-ready aggregates
# MAGIC These tables are what the public website shows. They are tiny (KBs) by design.

# COMMAND ----------
from pyspark.sql import functions as F

import config as C

trips = spark.table(C.SILVER_TRIPS)
zones = spark.table(C.BRONZE_ZONES)

# COMMAND ----------
# Demand heatmap: day of week x hour
(trips.groupBy("pickup_dow", "pickup_hour")
      .agg(F.count("*").alias("trips"), F.round(F.avg("trip_minutes"), 1).alias("avg_minutes"))
      .write.mode("overwrite").saveAsTable(C.GOLD_HOURLY_DEMAND))

# Daily trend
(trips.groupBy("pickup_date")
      .agg(F.count("*").alias("trips"),
           F.round(F.sum("total_amount"), 0).alias("revenue"),
           F.round(F.avg("trip_distance"), 2).alias("avg_miles"))
      .orderBy("pickup_date")
      .write.mode("overwrite").saveAsTable(C.GOLD_DAILY_SUMMARY))

# Top pickup zones
(trips.join(zones, trips.pu_location_id == zones.location_id)
      .groupBy("borough", "zone")
      .agg(F.count("*").alias("trips"), F.round(F.sum("total_amount"), 0).alias("revenue"))
      .orderBy(F.desc("trips")).limit(20)
      .write.mode("overwrite").saveAsTable(C.GOLD_TOP_ZONES))

# Tip percentage by hour (card payments only)
(trips.where(F.col("tip_pct").isNotNull() & (F.col("tip_pct") < 100))
      .groupBy("pickup_hour")
      .agg(F.round(F.avg("tip_pct"), 2).alias("avg_tip_pct"), F.count("*").alias("card_trips"))
      .orderBy("pickup_hour")
      .write.mode("overwrite").saveAsTable(C.GOLD_TIP_BY_HOUR))

# COMMAND ----------
# Data quality report: how many rows failed each rule, out of everything ingested
quarantine = spark.table(C.SILVER_QUARANTINE)
total_rows = spark.table(C.SILVER_TRIPS).count() + quarantine.count()

(quarantine.select(F.explode("failed_rules").alias("rule"))
           .groupBy("rule").agg(F.count("*").alias("rows_failed"))
           .withColumn("pct_of_all_rows", F.round(F.col("rows_failed") / F.lit(total_rows) * 100, 3))
           .withColumn("total_rows", F.lit(total_rows))
           .orderBy(F.desc("rows_failed"))
           .write.mode("overwrite").saveAsTable(C.GOLD_QUALITY_REPORT))

# COMMAND ----------
display(spark.table(C.GOLD_QUALITY_REPORT))
