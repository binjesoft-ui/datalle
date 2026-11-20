# Databricks notebook source
# MAGIC %md
# MAGIC # 07 - Export the declarative pipeline's data quality results
# MAGIC After the pipeline (`sdp_nyc_taxi.py`) has run, its event log records, for every expectation, how many rows
# MAGIC passed and failed. This notebook reads that log for the **latest update** and exports
# MAGIC `pipeline_expectations.json`.
# MAGIC
# MAGIC Fill in the `pipeline_id` widget: open the pipeline, then Settings -> About (or copy the id from the page URL).

# COMMAND ----------
import json
import os, sys
sys.path.append(os.getcwd())

import config as C
import catalog_docs as D

dbutils.widgets.text("pipeline_id", "")
pipeline_id = dbutils.widgets.get("pipeline_id").strip()
assert pipeline_id, "Fill in the pipeline_id widget at the top of the notebook"

LATEST_UPDATE = f"""
    SELECT origin.update_id AS id FROM event_log('{pipeline_id}')
    WHERE event_type = 'create_update' ORDER BY timestamp DESC LIMIT 1
"""

# COMMAND ----------
expectation_rows = spark.sql(f"""
    SELECT exp.dataset, exp.name, SUM(exp.passed_records) AS passed, SUM(exp.failed_records) AS failed
    FROM (
      SELECT explode(from_json(
               details:flow_progress.data_quality.expectations,
               'array<struct<name:string,dataset:string,passed_records:bigint,failed_records:bigint>>')) AS exp
      FROM event_log('{pipeline_id}')
      WHERE event_type = 'flow_progress'
        AND origin.update_id = ({LATEST_UPDATE})
        AND details:flow_progress.data_quality.expectations IS NOT NULL
    )
    GROUP BY exp.dataset, exp.name
""").collect()
print("expectation rows found:", len(expectation_rows))
if not expectation_rows:
    print("No expectation results in the event log. Make sure the pipeline ran to completion and that the update "
          "was a full refresh; tell Claude what you see in the event log UI.")

# COMMAND ----------
flow_rows = spark.sql(f"""
    SELECT details:flow_definition.output_dataset AS output_dataset,
           details:flow_definition.input_datasets AS input_datasets
    FROM event_log('{pipeline_id}')
    WHERE event_type = 'flow_definition' AND origin.update_id = ({LATEST_UPDATE})
""").collect()

payload = D.shape_expectations([(r[0], r[1], r[2], r[3]) for r in expectation_rows])
payload["flows"] = [{"from": s, "to": t} for s, t in sorted(set(D.edges_from_flow_definitions([(r[0], r[1]) for r in flow_rows])))]
with open(f"{C.EXPORT_PATH}/pipeline_expectations.json", "w") as f:
    json.dump(payload, f, default=str)
print(f"pipeline_expectations.json: {len(payload['expectations'])} expectations, {len(payload['flows'])} flows")
