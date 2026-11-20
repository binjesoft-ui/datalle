"""Spark vs DuckDB on the same job: read the raw TLC parquet files, drop exact duplicates, evaluate the 10 quality
rules, and aggregate the clean rows. Both engines return the same result dict, so the comparison can first check that
they agree before any timing is believed.

The rules are the ones in quality_rules.py, translated to DuckDB's SQL dialect (tests check both agree)."""
import re
import statistics
import time

# Same target schema as 01_bronze (tests check they stay identical)
COLUMNS = {
    "VendorID": "int", "tpep_pickup_datetime": "timestamp", "tpep_dropoff_datetime": "timestamp",
    "passenger_count": "int", "trip_distance": "double", "RatecodeID": "int", "store_and_fwd_flag": "string",
    "PULocationID": "int", "DOLocationID": "int", "payment_type": "int", "fare_amount": "double", "extra": "double",
    "mta_tax": "double", "tip_amount": "double", "tolls_amount": "double", "improvement_surcharge": "double",
    "total_amount": "double", "congestion_surcharge": "double", "airport_fee": "double",
}

# Rules as DuckDB SQL; the Spark side uses quality_rules.RULES. In the raw files the zone columns are still
# PULocationID / DOLocationID (silver renames them), so the zones rule is written against those.
DUCKDB_RULES = {
    "pickup_before_dropoff": "tpep_dropoff_datetime > tpep_pickup_datetime",
    "pickup_in_source_month": "strftime(tpep_pickup_datetime, '%Y-%m') = source_month",
    "duration_1_to_360_min": "(epoch(tpep_dropoff_datetime) - epoch(tpep_pickup_datetime)) BETWEEN 60 AND 21600",
    "distance_positive_and_under_100_mi": "trip_distance > 0 AND trip_distance < 100",
    "speed_under_100_mph": "trip_distance / ((epoch(tpep_dropoff_datetime) - epoch(tpep_pickup_datetime)) / 3600.0) <= 100",
    "passenger_count_1_to_6": "passenger_count BETWEEN 1 AND 6",
    "fare_positive": "fare_amount > 0",
    "total_amount_positive": "total_amount > 0",
    "tip_not_negative": "tip_amount >= 0",
    "zones_present": "PULocationID IS NOT NULL AND DOLocationID IS NOT NULL",
}


def spark_rules(rules: dict) -> dict:
    """quality_rules.RULES uses the silver column names; map the zone ones back to the raw names."""
    return {n: s.replace("pu_location_id", "PULocationID").replace("do_location_id", "DOLocationID")
            for n, s in rules.items()}


def month_of(path: str) -> str:
    m = re.search(r"(\d{4}-\d{2})\.parquet$", path)
    if not m:
        raise ValueError(f"Cannot find the month in {path}")
    return m.group(1)


def _agg_exprs(rules: dict, wrap) -> list[str]:
    """SQL select-list: total rows, rows failing each rule (NULL = fail), and clean-row totals."""
    ok = {n: f"coalesce(({wrap(s)}), false)" for n, s in rules.items()}
    all_ok = " AND ".join(ok.values())
    parts = ["count(*) AS total_rows"]
    parts += [f"count(*) FILTER (WHERE NOT {e}) AS f_{n}" for n, e in ok.items()]
    parts += [f"count(*) FILTER (WHERE {all_ok}) AS clean_rows",
              f"sum(total_amount) FILTER (WHERE {all_ok}) AS clean_revenue",
              f"count(DISTINCT CAST(tpep_pickup_datetime AS DATE)) FILTER (WHERE {all_ok}) AS clean_days"]
    return parts


def _shape(row: dict, rules: dict) -> dict:
    return {
        "total_rows": int(row["total_rows"]),
        "clean_rows": int(row["clean_rows"]),
        "clean_revenue": round(float(row["clean_revenue"] or 0.0), 2),
        "clean_days": int(row["clean_days"]),
        "failed": {n: int(row[f"f_{n}"]) for n in rules},
    }


def run_duckdb(paths, memory_limit=None, threads=None, tmp_dir="/tmp/duckdb_spill") -> dict:
    import duckdb

    con = duckdb.connect()
    con.execute(f"SET temp_directory = '{tmp_dir}'")
    if memory_limit:
        con.execute(f"SET memory_limit = '{memory_limit}'")
    if threads:
        con.execute(f"SET threads = {int(threads)}")
    files = ", ".join("'" + p.replace("'", "''") + "'" for p in paths)
    casts = ", ".join(f"CAST({c} AS {'VARCHAR' if t == 'string' else t.upper()}) AS {c}" for c, t in COLUMNS.items())
    con.execute(f"""CREATE VIEW typed AS
        SELECT {casts}, regexp_extract(filename, '(\\d{{4}}-\\d{{2}})\\.parquet$', 1) AS source_month
        FROM read_parquet([{files}], union_by_name = true, filename = true)""")
    cur = con.execute(f"SELECT {', '.join(_agg_exprs(DUCKDB_RULES, lambda s: s))} FROM (SELECT DISTINCT * FROM typed)")
    cols = [d[0] for d in cur.description]
    return _shape(dict(zip(cols, cur.fetchone())), DUCKDB_RULES)


def run_spark(spark, paths, rules) -> dict:
    from functools import reduce

    from pyspark.sql import functions as F

    frames = []
    for p in paths:
        raw = spark.read.parquet(p)
        lower = {c.lower(): c for c in raw.columns}   # Airport_fee vs airport_fee
        cols = [(F.col(lower[n.lower()]) if n.lower() in lower else F.lit(None)).cast(t).alias(n) for n, t in COLUMNS.items()]
        frames.append(raw.select(*cols).withColumn("source_month", F.lit(month_of(p))))
    df = reduce(lambda a, b: a.unionByName(b), frames).dropDuplicates()
    sp_rules = spark_rules(rules)
    row = df.agg(*[F.expr(e) for e in _agg_exprs(sp_rules, lambda s: s)]).first().asDict()
    return _shape(row, sp_rules)


def results_match(a: dict, b: dict, revenue_tolerance: float = 1.0) -> bool:
    return (a["total_rows"] == b["total_rows"] and a["clean_rows"] == b["clean_rows"]
            and a["clean_days"] == b["clean_days"] and a["failed"] == b["failed"]
            and abs(a["clean_revenue"] - b["clean_revenue"]) <= revenue_tolerance)


def timed(fn, runs: int = 3):
    """Run fn `runs` times. Returns (seconds per run, last result), or an error string instead of the result."""
    seconds, result = [], None
    for _ in range(runs):
        start = time.perf_counter()
        try:
            result = fn()
        except Exception as exc:  # noqa: BLE001 - recorded in the output, never hidden
            return seconds, f"{type(exc).__name__}: {str(exc)[:300]}"
        seconds.append(round(time.perf_counter() - start, 2))
    return seconds, result


def summarise_engine(seconds, result) -> dict:
    if isinstance(result, str):
        return {"ok": False, "error": result, "runs_s": seconds}
    return {"ok": True, "runs_s": seconds, "median_s": round(statistics.median(seconds), 2),
            "best_s": min(seconds), "result": result}
