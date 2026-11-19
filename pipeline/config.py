"""Shared settings for the NYC yellow taxi pipeline (Databricks Free Edition).

Everything lives in Unity Catalog: tables in workspace.nyc_taxi, files in a Volume.
"""

CATALOG = "workspace"
SCHEMA = "nyc_taxi"
VOLUME = "raw"

VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
EXPORT_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/export"

BRONZE_TRIPS = f"{CATALOG}.{SCHEMA}.bronze_yellow_trips"
BRONZE_ZONES = f"{CATALOG}.{SCHEMA}.bronze_taxi_zones"
SILVER_TRIPS = f"{CATALOG}.{SCHEMA}.silver_yellow_trips"
SILVER_QUARANTINE = f"{CATALOG}.{SCHEMA}.silver_yellow_quarantine"

GOLD_HOURLY_DEMAND = f"{CATALOG}.{SCHEMA}.gold_hourly_demand"
GOLD_DAILY_SUMMARY = f"{CATALOG}.{SCHEMA}.gold_daily_summary"
GOLD_TOP_ZONES = f"{CATALOG}.{SCHEMA}.gold_top_pickup_zones"
GOLD_TIP_BY_HOUR = f"{CATALOG}.{SCHEMA}.gold_tip_by_hour"
GOLD_QUALITY_REPORT = f"{CATALOG}.{SCHEMA}.gold_quality_report"

# Months to process. Change this list (or make the job pass it in) to scale up.
YEAR = 2024
MONTHS = [f"{YEAR}-{m:02d}" for m in range(1, 13)]

TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
