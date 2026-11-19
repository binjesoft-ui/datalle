# datalle — NYC Taxi Lakehouse

A data engineering portfolio project: one year of NYC yellow taxi trips (2024) cleaned and modelled on
**Databricks Free Edition**, with a public dashboard built in **Astro** and hosted on **Vercel**.

```
NYC TLC parquet (monthly)
   ↓
Bronze  raw Delta table, types normalised, lineage columns
   ↓
Silver  validated rows + quarantine table (rows with failed rule names)
   ↓
Gold    hourly demand · daily trend · top zones · tip % · quality report
   ↓  export to JSON
Static site on Vercel
```

## Why it is built this way

- **Databricks Apps are not public** (authenticated users only) and Free Edition is non-commercial, so the
  public site is a static build that reads small JSON exports of the gold tables.
- **Idempotent loads:** each month is written with `replaceWhere`, so re-runs never duplicate data.
- **Quarantine instead of delete:** rejected rows keep a `failed_rules` array.
- **Rules as data:** `pipeline/quality_rules.py` holds SQL expressions; `tests/` covers them.
- **Null means fail:** a missing value never passes a rule by accident.
- **Schema drift:** TLC files vary in column casing and types between months; bronze casts to one schema.

## Repo layout

```
pipeline/   Databricks notebooks (01_bronze, 02_silver, 03_gold, 04_export) + config + quality rules
tests/      pytest unit tests for the quality rules (needs pyspark locally)
scripts/    make_placeholder_data.py, sample JSON so the site renders before the pipeline runs
site/       Astro static site (deployed to Vercel)
```

## Run the pipeline

1. Create a [Databricks Free Edition](https://www.databricks.com/learn/free-edition) workspace.
2. Add this repo through **Workspace → Create → Git folder**, so notebooks can `import config` and `quality_rules`.
3. Run `pipeline/01_bronze.py` → `02_silver.py` → `03_gold.py` → `04_export.py`.
   - Free Edition only reaches trusted domains. If the TLC download fails, download the monthly parquet files
     from the [TLC trip record page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) and upload them to the
     `workspace.nyc_taxi.raw` Volume as `yellow_tripdata_YYYY-MM.parquet` (plus `taxi_zone_lookup.csv`).
   - Start with one month by editing `MONTHS` in `pipeline/config.py`.
4. Download the JSON files from the `export` Volume and commit them to `site/public/data/`.
5. Optionally, schedule the notebooks as a Lakeflow Job.

## Run the site

```bash
python scripts/make_placeholder_data.py   # only needed before real data exists
cd site
npm install
npm run dev
```

Placeholder JSON files contain `"placeholder": true` and the site shows a banner until they are replaced.

## Deploy to Vercel

Import the GitHub repo in Vercel and set **Root Directory** to `site`. The Astro framework preset is detected
automatically, with no extra settings.

## Tests

```bash
pip install pyspark pytest
pytest
```

## Data

[NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page). Cash tips are not
recorded, so tip % uses card payments only.
