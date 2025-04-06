# Group Scholar Cohort Health Sentinel

Group Scholar Cohort Health Sentinel is a lightweight C CLI that audits cohort engagement data and highlights retention risk signals. It summarizes risk mix, surfaces the highest-risk scholars, and provides cohort-level averages for touchpoints, attendance, satisfaction, and recency.

## Features
- Risk scoring based on recency, touchpoints, attendance, and satisfaction
- Top risk list with configurable limit
- Cohort summary table with average health signals
- Cohort alerting based on high-risk share thresholds
- Missing data detection for IDs and dates
- Optional JSON output for downstream workflows

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

Alert on cohorts with high-risk share >= 35% and at least 8 scholars:

```
./cohort-health-sentinel --input data/sample.csv --alert-threshold 0.35 --min-cohort-size 8
```

Write JSON output:

```
./cohort-health-sentinel --input data/sample.csv --json output.json --alert-threshold 0.30
```

## Output
The CLI prints:
- Total valid/invalid rows
- Missing ID/date counts
- Risk mix (high/medium/low)
- Top risk entries
- Cohort-level averages and risk distribution
- Cohort alerts when high-risk share exceeds the threshold

## Tech
- C (C11)
- Standard library only
