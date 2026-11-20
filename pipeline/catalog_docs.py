"""Pure helpers (no Spark session needed) that turn Databricks metadata into the JSON the website reads."""
import json
from datetime import datetime, timezone

LAYER_ORDER = {"bronze": 0, "silver": 1, "gold": 2, "other": 3}


def layer_of(table_name: str) -> str:
    for layer in ("bronze", "silver", "gold"):
        if table_name.startswith(layer + "_"):
            return layer
    return "other"


def _literal(text: str) -> str:
    if "'" in text or "\\" in text:
        raise ValueError(f"Description may not contain apostrophes or backslashes: {text!r}")
    return "'" + text + "'"


def comment_statements(catalog, schema, existing_columns, table_descriptions, column_descriptions):
    """SQL that applies the descriptions. Only tables and columns that really exist are touched.

    existing_columns: {table_name: [column names]}.  Returns (statements, undocumented) where
    undocumented lists (table, column) pairs that have no description yet.
    """
    statements, undocumented = [], []
    for table, columns in sorted(existing_columns.items()):
        full = f"{catalog}.{schema}.{table}"
        if table in table_descriptions:
            statements.append(f"COMMENT ON TABLE {full} IS {_literal(table_descriptions[table])}")
        for col in columns:
            if col in column_descriptions:
                statements.append(
                    f"ALTER TABLE {full} ALTER COLUMN `{col}` COMMENT {_literal(column_descriptions[col])}"
                )
            else:
                undocumented.append((table, col))
    return statements, undocumented


def shape_dictionary(catalog, schema, tables, columns, row_counts, now=None):
    """tables: [{table_name, comment}], columns: [{table_name, column_name, data_type, ordinal_position, comment}]."""
    by_table = {}
    for c in sorted(columns, key=lambda c: (c["table_name"], int(c["ordinal_position"]))):
        by_table.setdefault(c["table_name"], []).append(
            {"name": c["column_name"], "type": c["data_type"], "comment": c.get("comment") or ""}
        )
    out = []
    for t in tables:
        name = t["table_name"]
        out.append({
            "name": name,
            "layer": layer_of(name),
            "comment": t.get("comment") or "",
            "rows": row_counts.get(name),
            "columns": by_table.get(name, []),
        })
    out.sort(key=lambda t: (LAYER_ORDER[t["layer"]], t["name"]))
    return {
        "generated_at": (now or datetime.now(timezone.utc)).isoformat(),
        "source": "Unity Catalog information_schema",
        "catalog": catalog,
        "schema": schema,
        "tables": out,
    }


def shape_lineage(table_names, system_rows=None, declared=()):
    """Edges between the given tables. Uses Unity Catalog lineage rows when there are any, else declared edges.

    system_rows: iterable of (source_table_name, target_table_name) already reduced to bare table names.
    """
    names = set(table_names)

    def keep(pairs):
        return sorted({(s, t) for s, t in pairs if s in names and t in names and s != t})

    from_uc = keep(system_rows or [])
    if from_uc:
        source, edges = "Unity Catalog lineage (system.access.table_lineage)", from_uc
    else:
        source, edges = "Declared in the pipeline code (Unity Catalog lineage was not available to query)", keep(declared)
    used = {n for e in edges for n in e}
    nodes = sorted(used, key=lambda n: (LAYER_ORDER[layer_of(n)], n))
    return {"source": source, "nodes": [{"name": n, "layer": layer_of(n)} for n in nodes],
            "edges": [{"from": s, "to": t} for s, t in edges]}


def bare(name: str) -> str:
    """workspace.schema.table -> table"""
    return name.split(".")[-1].strip("`")


def edges_from_flow_definitions(rows):
    """rows: (output_dataset, input_datasets) from the pipeline event log; input_datasets is a JSON array string."""
    pairs = []
    for output, inputs in rows:
        if not output or not inputs:
            continue
        for src in json.loads(inputs) if isinstance(inputs, str) else inputs:
            pairs.append((bare(src), bare(output)))
    return pairs


def shape_expectations(rows, now=None):
    """rows: (dataset, name, passed, failed). Returns one record per rule with its failure rate."""
    out = []
    for dataset, name, passed, failed in rows:
        passed, failed = int(passed or 0), int(failed or 0)
        total = passed + failed
        out.append({
            "dataset": bare(dataset),
            "rule": name,
            "passed": passed,
            "failed": failed,
            "pct_failed": round(failed / total * 100, 3) if total else 0.0,
        })
    out.sort(key=lambda r: (-r["failed"], r["rule"]))
    return {"generated_at": (now or datetime.now(timezone.utc)).isoformat(),
            "source": "Lakeflow pipeline event log (expectations)", "expectations": out}
