# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Export gold tables to JSON for the website
# MAGIC Writes one JSON file per gold table to the `export` Volume. Download them from Catalog Explorer
# MAGIC and commit them to `site/public/data/` (the public site is static, so this is the hand-off point).

# COMMAND ----------
import json
from datetime import datetime, timezone

import config as C

EXPORTS = {
    "hourly_demand": C.GOLD_HOURLY_DEMAND,
    "daily_summary": C.GOLD_DAILY_SUMMARY,
    "top_zones": C.GOLD_TOP_ZONES,
    "tip_by_hour": C.GOLD_TIP_BY_HOUR,
    "quality_report": C.GOLD_QUALITY_REPORT,
}

for name, table in EXPORTS.items():
    rows = [r.asDict(recursive=True) for r in spark.table(table).collect()]
    payload = {
        "placeholder": False,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": "NYC TLC yellow taxi trip records",
        "data": rows,
    }
    with open(f"{C.EXPORT_PATH}/{name}.json", "w") as f:
        json.dump(payload, f, default=str)
    print(f"{name}: {len(rows)} rows")
