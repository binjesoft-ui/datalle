# Databricks notebook source
# MAGIC %md
# MAGIC # 06 - Unity Catalog documentation: data dictionary and lineage
# MAGIC 1. Writes the descriptions in `catalog_metadata.py` onto the tables and columns as Unity Catalog comments.
# MAGIC 2. Reads the dictionary back from `information_schema` (so the website shows what the catalog really holds).
# MAGIC 3. Exports table lineage from `system.access.table_lineage`; if that system table is not available on your
# MAGIC    account, falls back to the edges declared in code and says so in the file.
# MAGIC
# MAGIC Run it after `05_cube`. Outputs: `data_dictionary.json` and `lineage.json` in the export Volume.

# COMMAND ----------
import json
import os, sys
sys.path.append(os.getcwd())

import config as C
import catalog_docs as D
from catalog_metadata import COLUMN_DESCRIPTIONS, DECLARED_LINEAGE, TABLE_DESCRIPTIONS

info = f"{C.CATALOG}.information_schema"
tables = [r.asDict() for r in spark.sql(
    f"SELECT table_name, comment FROM {info}.tables WHERE table_schema = '{C.SCHEMA}' AND table_type IN ('MANAGED','EXTERNAL')"
).collect()]
existing = {r["table_name"]: [] for r in tables}
for r in spark.sql(f"SELECT table_name, column_name FROM {info}.columns WHERE table_schema = '{C.SCHEMA}' ORDER BY ordinal_position").collect():
    if r["table_name"] in existing:
        existing[r["table_name"]].append(r["column_name"])

# COMMAND ----------
# 1. Apply the descriptions
statements, undocumented = D.comment_statements(C.CATALOG, C.SCHEMA, existing, TABLE_DESCRIPTIONS, COLUMN_DESCRIPTIONS)
for s in statements:
    spark.sql(s)
print(f"applied {len(statements)} comments")
if undocumented:
    print("columns still without a description (add them to catalog_metadata.py):")
    for t, c in undocumented:
        print(f"  {t}.{c}")

# COMMAND ----------
# 2. Read the dictionary back and export it
tables = [r.asDict() for r in spark.sql(
    f"SELECT table_name, comment FROM {info}.tables WHERE table_schema = '{C.SCHEMA}' AND table_type IN ('MANAGED','EXTERNAL')"
).collect()]
columns = [r.asDict() for r in spark.sql(
    f"SELECT table_name, column_name, data_type, ordinal_position, comment FROM {info}.columns WHERE table_schema = '{C.SCHEMA}'"
).collect()]
row_counts = {t["table_name"]: spark.table(f"{C.CATALOG}.{C.SCHEMA}.{t['table_name']}").count() for t in tables}

dictionary = D.shape_dictionary(C.CATALOG, C.SCHEMA, tables, columns, row_counts)
with open(f"{C.EXPORT_PATH}/data_dictionary.json", "w") as f:
    json.dump(dictionary, f, default=str)
print(f"data_dictionary.json: {len(dictionary['tables'])} tables")

# COMMAND ----------
# 3. Lineage: prefer Unity Catalog, fall back to declared edges
uc_pairs = []
try:
    rows = spark.sql(f"""
        SELECT DISTINCT source_table_full_name, target_table_full_name
        FROM system.access.table_lineage
        WHERE source_table_catalog = '{C.CATALOG}' AND source_table_schema = '{C.SCHEMA}'
          AND target_table_catalog = '{C.CATALOG}' AND target_table_schema = '{C.SCHEMA}'
    """).collect()
    uc_pairs = [(D.bare(r[0]), D.bare(r[1])) for r in rows]
    print(f"Unity Catalog lineage rows: {len(uc_pairs)}")
except Exception as e:
    print("system.access.table_lineage not available, using declared lineage:", str(e)[:200])

lineage = D.shape_lineage([t["table_name"] for t in tables], uc_pairs, DECLARED_LINEAGE)
with open(f"{C.EXPORT_PATH}/lineage.json", "w") as f:
    json.dump(lineage, f)
print("lineage.json source:", lineage["source"], "| edges:", len(lineage["edges"]))
