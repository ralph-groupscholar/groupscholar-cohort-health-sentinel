# Ralph Progress Log

## 2026-02-07
- Started Group Scholar Cohort Health Sentinel, a C CLI that scores retention risk from cohort engagement data.
- Implemented CSV parsing, risk scoring, cohort summaries, and optional JSON output.
- Added sample dataset and README with build/usage instructions.
- Added cohort alerting with configurable high-risk thresholds and minimum cohort size.
- Extended JSON output and CLI docs to include alert settings and results.

## 2026-02-07
- Added cohort filtering support and fixed trimming to handle leading whitespace.
- Corrected invalid row accounting to avoid double-counting in summaries and JSON output.
- Updated README with cohort filter usage example.

## 2026-02-08
- Added Postgres sync script to persist cohort health runs, cohort metrics, top risks, and alerts.
- Documented database sync workflow and dependencies in the README.
- Seeded the production database schema with a sample run from the provided CSV data.

## 2026-02-08
- Added Postgres ingestion script using SQLAlchemy with schema/table setup and report ingestion.
- Generated sample JSON output for seeding and documented database workflow in README.
- Added Python requirements for optional database integration.

## 2026-02-08
- Added future-date detection to clamp negative recency and surface future touchpoints in CLI/JSON output.
- Extended Postgres ingestion scripts to capture future-date counts in stored reports.

## 2026-02-08
- Added cohort risk index calculations to the CLI output and JSON export.
- Extended Postgres sync/ingest schemas to store cohort and alert risk index values.
- Documented the risk index output in the README.

## 2026-02-08
- Added cohort summary sorting/limiting with high-risk share metrics in CLI and JSON output.
- Introduced alert sorting plus cohort metadata in JSON payload.
- Added smoke tests and updated README usage/testing notes.

## 2026-02-08
- Added invalid row breakdown (columns, numeric parsing, date format) to CLI output and JSON.
- Extended Postgres sync and ingestion scripts to persist invalid breakdown metrics.
- Updated tests and README to cover the new data quality metrics.
