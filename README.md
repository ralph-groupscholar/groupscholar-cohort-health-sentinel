# Group Scholar Cohort Health Sentinel

Group Scholar Cohort Health Sentinel is a lightweight C CLI that audits cohort engagement data and highlights retention risk signals. It summarizes risk mix, surfaces the highest-risk scholars, and provides cohort-level averages for touchpoints, attendance, satisfaction, and recency.

## Features
- Risk scoring based on recency, touchpoints, attendance, and satisfaction
- Top risk list with configurable limit
- Cohort summary table with average health signals
- Cohort risk index (1-3) based on risk mix weighting
- Cohort alerting based on high-risk share thresholds
- Missing data detection for IDs and dates
- Optional cohort filtering for focused reviews
- Cohort summary sorting and limiting for triage-ready dashboards
- Optional JSON output for downstream workflows
- CSV export for cohort summaries and alerts
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

Sort cohorts by high-risk share and show only the top 5:

```
./cohort-health-sentinel --input data/sample.csv --cohort-sort high --cohort-limit 5
```

Clamp out-of-range numeric values instead of marking rows invalid:

```
./cohort-health-sentinel --input data/sample.csv --clamp-ranges
```

Write JSON output:

```
./cohort-health-sentinel --input data/sample.csv --json output.json --alert-threshold 0.30
```

Write cohort summary and alert CSVs:

```
./cohort-health-sentinel --input data/sample.csv --cohort-csv cohort-summary.csv --alert-csv cohort-alerts.csv
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
- Invalid row breakdown (columns, numeric parsing, date format, range)
- Future-dated touchpoints (clamped to 0 days since)
- Clamped out-of-range numeric values
- Risk mix (high/medium/low)
- Top risk entries
- Cohort-level averages, risk distribution, high-risk share, and risk index (sorted and optionally limited)
- Cohort alerts when high-risk share exceeds the threshold

## Postgres integration
Load JSON output into the Group Scholar Postgres database for historical tracking.

Install dependencies:

```
python3 -m venv .venv
source .venv/bin/activate
pip install -r scripts/requirements.txt
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

## Tests
Run the smoke test script:

```
tests/run.sh
```

## Tech
- C (C11)
- Standard library only
- Python (SQLAlchemy, psycopg2) for optional database ingestion
- Python (Postgres sync)
