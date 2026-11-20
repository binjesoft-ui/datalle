# Databricks notebook source
# MAGIC %md
# MAGIC # 08 - Benchmark: Spark vs DuckDB on the same cleaning job
# MAGIC Both engines read the same raw TLC parquet files from the Volume, drop exact duplicates, evaluate the 10
# MAGIC quality rules and aggregate the clean rows. The notebook first checks that the two engines **agree** (and agree
# MAGIC with the published pipeline tables); only then are the timings meaningful.
# MAGIC
# MAGIC Run on **Serverless** compute. Takes roughly 10-25 minutes (each size is run three times per engine).
# MAGIC Output: `benchmark.json` in the export Volume.

# COMMAND ----------
# MAGIC %pip install duckdb -q

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
import json
import os, sys
from datetime import datetime, timezone

sys.path.append(os.getcwd())

import duckdb

import benchmark as B
import config as C
from quality_rules import RULES

SIZES = [1, 3, 6, 12]   # number of monthly files
RUNS = 3

paths = [f"{C.VOLUME_PATH}/yellow_tripdata_{m}.parquet" for m in C.MONTHS]
missing = [p for p in paths if not os.path.exists(p)]
assert not missing, f"Missing raw files (run 01_bronze first): {missing[:2]}"
size_mb = {n: round(sum(os.path.getsize(p) for p in paths[:n]) / 1e6, 1) for n in SIZES}

def mem_gb():
    try:
        with open("/proc/meminfo") as f:
            return round(int(f.readline().split()[1]) / 1e6, 1)
    except Exception:
        return None

env = {"cpu_count": os.cpu_count(), "memory_gb": mem_gb(), "spark_version": spark.version,
       "duckdb_version": duckdb.__version__, "compute": "Databricks Free Edition serverless notebook"}
print(env)

# COMMAND ----------
# Warm-up so the first timed Spark run is not charged for session start-up
B.run_spark(spark, paths[:1], RULES)

results = []
for n in SIZES:
    subset = paths[:n]
    s_secs, s_res = B.timed(lambda: B.run_spark(spark, subset, RULES), RUNS)
    d_secs, d_res = B.timed(lambda: B.run_duckdb(subset), RUNS)
    spark_out, duck_out = B.summarise_engine(s_secs, s_res), B.summarise_engine(d_secs, d_res)
    agree = bool(spark_out["ok"] and duck_out["ok"] and B.results_match(s_res, d_res))
    results.append({"months": n, "input_mb": size_mb[n],
                    "rows": (s_res if spark_out["ok"] else d_res)["total_rows"] if (spark_out["ok"] or duck_out["ok"]) else None,
                    "spark": spark_out, "duckdb": duck_out, "engines_agree": agree})
    print(n, "months | spark", s_secs, "| duckdb", d_secs, "| agree:", agree)

# COMMAND ----------
# Cross-check the full year against the tables the real pipeline produced
full = results[-1]
matches_pipeline = None
if full["spark"]["ok"]:
    r = full["spark"]["result"]
    clean = spark.table(C.SILVER_TRIPS).count()
    quarantined = spark.table(C.SILVER_QUARANTINE).count()
    published = {x["rule"]: x["rows_failed"] for x in spark.table(C.GOLD_QUALITY_REPORT).collect()}
    matches_pipeline = (r["clean_rows"] == clean and r["total_rows"] == clean + quarantined
                        and all(r["failed"].get(k, 0) == v for k, v in published.items()))
    print("benchmark clean/total:", r["clean_rows"], r["total_rows"], "| pipeline:", clean, clean + quarantined,
          "| matches pipeline:", matches_pipeline)

payload = {
    "generated_at": datetime.now(timezone.utc).isoformat(),
    "environment": env, "runs_per_size": RUNS,
    "workload": "Read raw TLC parquet, drop exact duplicates, evaluate 10 quality rules, aggregate clean rows",
    "matches_pipeline": matches_pipeline, "sizes": results,
}
with open(f"{C.EXPORT_PATH}/benchmark.json", "w") as f:
    json.dump(payload, f)
print("benchmark.json written")
