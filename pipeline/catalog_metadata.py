"""Human-written documentation for every table and column, applied to Unity Catalog by 06_catalog_docs.

Descriptions must not contain apostrophes or backslashes (they are embedded in SQL string literals);
tests/test_catalog_docs.py enforces this.
"""

TABLE_DESCRIPTIONS = {
    "bronze_yellow_trips": "Raw NYC yellow taxi trips exactly as published by the TLC, one row per trip. Only column types and names are normalised and lineage columns added. Partitioned by source_month.",
    "bronze_taxi_zones": "TLC taxi zone lookup: maps each location ID to its borough and zone name.",
    "silver_yellow_trips": "Validated yellow taxi trips. Every row passed all quality rules (see gold_quality_report) and exact duplicates were removed. Adds derived columns such as trip_minutes and pickup_hour.",
    "silver_yellow_quarantine": "Trips that failed at least one quality rule, kept with the names of the rules they failed (failed_rules). Nothing is silently deleted: raw rows = clean rows + quarantined rows.",
    "gold_hourly_demand": "Trips and average duration for each weekday and pickup hour.",
    "gold_daily_summary": "Trips, total paid and average distance for each pickup date.",
    "gold_top_pickup_zones": "The 20 pickup zones with the most trips, with total paid.",
    "gold_tip_by_hour": "Average card tip percentage by pickup hour. Card payments only because cash tips are not recorded.",
    "gold_quality_report": "How many rows failed each quality rule, out of all rows ingested.",
    "gold_cube_daily": "Additive cube for the dashboard: one row per date, pickup borough and payment type. Holds sums and counts so any filter can be recomputed.",
    "gold_cube_time": "Additive cube for the dashboard: one row per month, borough, payment type, weekday and pickup hour.",
    "gold_cube_zone": "Additive cube for the dashboard: one row per month, borough, pickup zone and payment type.",
}

# Column descriptions are shared by name across tables, so each concept is documented once.
COLUMN_DESCRIPTIONS = {
    # --- raw TLC columns
    "VendorID": "Code for the technology provider that supplied the trip record.",
    "tpep_pickup_datetime": "Date and time the meter was engaged (trip start).",
    "tpep_dropoff_datetime": "Date and time the meter was disengaged (trip end).",
    "passenger_count": "Number of passengers, entered by the driver. Often missing or zero in raw data.",
    "trip_distance": "Trip distance in miles reported by the taximeter.",
    "RatecodeID": "Final rate code for the trip, for example standard rate or airport rates.",
    "store_and_fwd_flag": "Y if the record was held in the vehicle memory before being sent because there was no connection, otherwise N.",
    "PULocationID": "Taxi zone ID where the meter was engaged (pickup).",
    "DOLocationID": "Taxi zone ID where the meter was disengaged (dropoff).",
    "pu_location_id": "Taxi zone ID of the pickup (standardised name for PULocationID).",
    "do_location_id": "Taxi zone ID of the dropoff (standardised name for DOLocationID).",
    "payment_type": "How the passenger paid: 0 flex fare, 1 credit card, 2 cash, 3 no charge, 4 dispute, 5 unknown, 6 voided trip.",
    "fare_amount": "Time and distance fare calculated by the meter, in US dollars.",
    "extra": "Miscellaneous extras and surcharges, such as rush hour and overnight charges.",
    "mta_tax": "Tax charged by the metered rate.",
    "tip_amount": "Tip in US dollars. Filled in automatically for card payments; cash tips are not included.",
    "tolls_amount": "Total of all tolls paid during the trip.",
    "improvement_surcharge": "Surcharge assessed at the start of the trip.",
    "total_amount": "Total charged to the passenger in US dollars. Cash tips are not included.",
    "congestion_surcharge": "Total collected for the New York State congestion surcharge.",
    "airport_fee": "Fee for pickups at the airports where it applies.",
    # --- lineage columns added by bronze
    "source_month": "Month of the TLC file the row came from, in YYYY-MM format.",
    "source_file": "Name of the TLC file the row came from.",
    "ingested_at": "Timestamp when the row was loaded into bronze.",
    # --- zone lookup
    "location_id": "Taxi zone ID (matches the pickup and dropoff location IDs).",
    "borough": "NYC borough of the zone. Special values include EWR (Newark airport), N/A and Unknown.",
    "zone": "Name of the taxi zone.",
    "service_zone": "Service zone grouping of the taxi zone, such as Yellow Zone or Airports.",
    # --- derived in silver
    "trip_minutes": "Trip duration in minutes, calculated as dropoff time minus pickup time.",
    "pickup_date": "Calendar date of the pickup.",
    "pickup_hour": "Hour of the day of the pickup, 0 to 23.",
    "pickup_dow": "Day of the week of the pickup, 1 = Sunday through 7 = Saturday.",
    "tip_pct": "Tip as a percentage of the fare. Card payments only; empty for other payment types.",
    "failed_rules": "List of the quality rules this row failed.",
    # --- gold aggregates
    "trips": "Number of trips.",
    "avg_minutes": "Average trip duration in minutes.",
    "avg_miles": "Average trip distance in miles.",
    "revenue": "Total paid by passengers (total_amount) in US dollars, rounded.",
    "avg_tip_pct": "Average tip percentage for card trips.",
    "card_trips": "Number of card-paid trips included in the tip average.",
    "rule": "Name of the quality rule.",
    "rows_failed": "Number of rows that failed the rule. A row can fail several rules.",
    "pct_of_all_rows": "Rows failed as a percentage of all rows ingested.",
    "total_rows": "Total rows ingested before cleaning.",
    # --- cubes
    "month": "Month of the pickup in YYYY-MM format.",
    "payment": "Payment type as a readable label, such as Credit card or Cash.",
    "fare_sum": "Sum of fare_amount over the trips in the row, in US dollars.",
    "total_sum": "Sum of total_amount over the trips in the row, in US dollars.",
    "distance_sum": "Sum of trip distance in miles over the trips in the row.",
    "minutes_sum": "Sum of trip minutes over the trips in the row.",
    "card_fare_sum": "Sum of fares for card-paid trips only (the denominator of tip percentage).",
    "card_tip_sum": "Sum of tips for card-paid trips only (the numerator of tip percentage).",
}

# Used for the lineage graph when Unity Catalog lineage is not available to query.
DECLARED_LINEAGE = [
    ("bronze_yellow_trips", "silver_yellow_trips"),
    ("bronze_yellow_trips", "silver_yellow_quarantine"),
    ("silver_yellow_trips", "gold_hourly_demand"),
    ("silver_yellow_trips", "gold_daily_summary"),
    ("silver_yellow_trips", "gold_top_pickup_zones"),
    ("bronze_taxi_zones", "gold_top_pickup_zones"),
    ("silver_yellow_trips", "gold_tip_by_hour"),
    ("silver_yellow_trips", "gold_quality_report"),
    ("silver_yellow_quarantine", "gold_quality_report"),
    ("silver_yellow_trips", "gold_cube_daily"),
    ("silver_yellow_trips", "gold_cube_time"),
    ("silver_yellow_trips", "gold_cube_zone"),
    ("bronze_taxi_zones", "gold_cube_daily"),
    ("bronze_taxi_zones", "gold_cube_time"),
    ("bronze_taxi_zones", "gold_cube_zone"),
]
