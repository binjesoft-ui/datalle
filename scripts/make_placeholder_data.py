"""Generate clearly-labelled PLACEHOLDER json files so the site renders before the pipeline has run.

These files carry "placeholder": true and the site shows a banner. Replace them with the exports
from pipeline/04_export.py.
"""
import json
import math
import random
from datetime import date, timedelta
from pathlib import Path

OUT = Path(__file__).resolve().parents[1] / "site" / "public" / "data"
OUT.mkdir(parents=True, exist_ok=True)
random.seed(7)


def write(name, rows):
    payload = {"placeholder": True, "generated_at": None, "source": "placeholder", "data": rows}
    (OUT / f"{name}.json").write_text(json.dumps(payload))


start = date(2024, 1, 1)
write("daily_summary", [
    {"pickup_date": (start + timedelta(d)).isoformat(),
     "trips": int(100_000 + 15_000 * math.sin(d / 58) + random.randint(-8_000, 8_000)),
     "revenue": 0, "avg_miles": 3.0}
    for d in range(366)
])
write("tip_by_hour", [
    {"pickup_hour": h, "avg_tip_pct": round(18 + 3 * math.cos((h - 18) / 24 * 2 * math.pi), 2), "card_trips": 1}
    for h in range(24)
])
write("top_zones", [
    {"borough": "Manhattan", "zone": f"Sample zone {i + 1}", "trips": 300_000 - i * 20_000, "revenue": 0}
    for i in range(20)
])
write("hourly_demand", [
    {"pickup_dow": d, "pickup_hour": h,
     "trips": int(max(0, 1000 + 900 * math.sin((h - 9) / 24 * 2 * math.pi) + (150 if d in (6, 7) and h > 21 else 0))),
     "avg_minutes": 15.0}
    for d in range(1, 8) for h in range(24)
])
write("quality_report", [
    {"rule": r, "rows_failed": n, "pct_of_all_rows": round(n / 40_000_000 * 100, 3), "total_rows": 40_000_000}
    for r, n in [("passenger_count_1_to_6", 0), ("pickup_in_source_month", 0), ("fare_positive", 0)]
])
print("placeholder data written to", OUT)
