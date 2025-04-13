# Group Scholar Cohort Health Sentinel

Group Scholar Cohort Health Sentinel is a lightweight C CLI that audits cohort engagement data and highlights retention risk signals. It summarizes risk mix, surfaces the highest-risk scholars, and provides cohort-level averages for touchpoints, attendance, satisfaction, and recency.

## Features
- Risk scoring based on recency, touchpoints, attendance, and satisfaction
- Top risk list with configurable limit
- Cohort summary table with average health signals
- Cohort alerting based on high-risk share thresholds
- Missing data detection for IDs and dates
- Optional cohort filtering for focused reviews
- Optional JSON output for downstream workflows
- Optional Postgres sync for cohort health snapshots

## Data format
CSV columns (header required):

```
scholar_id,cohort,last_touchpoint_date,touchpoints_last_30d,attendance_rate,satisfaction_score
```

Dates must be in `YYYY-MM-DD` format. Attendance is 0-1. Satisfaction is 1-5.

## Build

```
cc -std=c11 -O2 -o cohort-health-sentinel src/main.c
```

## Usage

```
./cohort-health-sentinel --input data/sample.csv --as-of 2026-02-01 --limit 8
```

Filter to specific cohorts:

```
./cohort-health-sentinel --input data/sample.csv --cohort "Delta,Omega" --limit 5
```

Alert on cohorts with high-risk share >= 35% and at least 8 scholars:

```
./cohort-health-sentinel --input data/sample.csv --alert-threshold 0.35 --min-cohort-size 8
```

Write JSON output:

```
./cohort-health-sentinel --input data/sample.csv --json output.json --alert-threshold 0.30
```

Sync JSON output to Postgres:

```
python3 -m venv .venv
source .venv/bin/activate
pip install -r scripts/requirements.txt

export PGHOST="db-acupinir.groupscholar.com"
export PGPORT="23947"
export PGUSER="ralph"
export PGPASSWORD="your-password"
export PGDATABASE="postgres"

python3 scripts/db_sync.py --json output.json
```

## Output
The CLI prints:
- Total valid/invalid rows
- Missing ID/date counts
- Risk mix (high/medium/low)
- Top risk entries
- Cohort-level averages and risk distribution
- Cohort alerts when high-risk share exceeds the threshold

## Postgres integration
Load JSON output into the Group Scholar Postgres database for historical tracking.

Install dependencies:

```
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Set environment variables (do not commit credentials):

```
export GSCH_DB_HOST=...
export GSCH_DB_PORT=23947
export GSCH_DB_USER=ralph
export GSCH_DB_PASSWORD=...
export GSCH_DB_NAME=postgres
```

Create schema and seed the production database with sample output:

```
python scripts/postgres_ingest.py --setup --seed
```

Ingest a fresh JSON report:

```
python scripts/postgres_ingest.py --ingest --json data/sample-output.json --source sample
```

## Tech
- C (C11)
- Standard library only
- Python (SQLAlchemy, psycopg2) for optional database ingestion
- Python (Postgres sync)
