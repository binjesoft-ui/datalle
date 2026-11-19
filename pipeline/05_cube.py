# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Interactive cubes
# MAGIC Builds three small additive cubes from silver (see `cube.py`) and exports them as **Parquet**
# MAGIC for the website. The browser filters them with DuckDB-WASM.
# MAGIC Run after `02_silver` (it does not depend on `03_gold`).

# COMMAND ----------
import os

import config as C
from cube import build_cubes

trips = spark.table(C.SILVER_TRIPS)
zones = spark.table(C.BRONZE_ZONES)
cubes = build_cubes(trips, zones)

# COMMAND ----------
# Save as tables (for SQL access) and export Parquet files to the export Volume.
for name, df in cubes.items():
    table = f"{C.CATALOG}.{C.SCHEMA}.gold_cube_{name}"
    df.write.mode("overwrite").saveAsTable(table)

    pdf = spark.table(table).toPandas()          # small by design: thousands of rows, not millions
    path = f"{C.EXPORT_PATH}/cube_{name}.parquet"
    pdf.to_parquet(path, index=False, compression="zstd")
    print(f"cube_{name}: {len(pdf):,} rows, {os.path.getsize(path) / 1024:,.0f} KB -> {path}")

# COMMAND ----------
# Reconciliation: every cube must add up to exactly the clean silver row count.
clean_rows = trips.count()
for name in cubes:
    cube_rows = spark.table(f"{C.CATALOG}.{C.SCHEMA}.gold_cube_{name}").agg({"trips": "sum"}).first()[0]
    status = "OK" if cube_rows == clean_rows else "MISMATCH"
    print(f"{name}: {cube_rows:,} vs silver {clean_rows:,} -> {status}")
