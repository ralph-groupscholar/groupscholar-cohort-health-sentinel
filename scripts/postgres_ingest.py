#!/usr/bin/env python3
import argparse
import json
import os
from pathlib import Path
from urllib.parse import quote_plus

from sqlalchemy import create_engine, text

DEFAULT_SCHEMA = "cohort_health_sentinel"
DEFAULT_JSON = Path(__file__).resolve().parents[1] / "data" / "sample-output.json"


def build_db_url():
    host = os.environ.get("GSCH_DB_HOST")
    port = os.environ.get("GSCH_DB_PORT", "5432")
    user = os.environ.get("GSCH_DB_USER")
    password = os.environ.get("GSCH_DB_PASSWORD")
    name = os.environ.get("GSCH_DB_NAME", "postgres")

    missing = [key for key, val in {
        "GSCH_DB_HOST": host,
        "GSCH_DB_USER": user,
        "GSCH_DB_PASSWORD": password,
    }.items() if not val]
    if missing:
        raise SystemExit(f"Missing required env vars: {', '.join(missing)}")

    safe_password = quote_plus(password)
    return f"postgresql+psycopg://{user}:{safe_password}@{host}:{port}/{name}"


def load_report(path: Path):
    if not path.exists():
        raise SystemExit(f"JSON file not found: {path}")
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def setup_schema(conn, schema):
    conn.execute(text("CREATE EXTENSION IF NOT EXISTS pgcrypto"))
    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
    conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS {schema}.reports (
            report_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            reference_date TEXT NOT NULL,
            cohort_filter TEXT,
            valid_records INT NOT NULL,
            invalid_records INT NOT NULL,
            invalid_columns INT NOT NULL DEFAULT 0,
            invalid_numeric INT NOT NULL DEFAULT 0,
            invalid_date_format INT NOT NULL DEFAULT 0,
            invalid_range INT NOT NULL DEFAULT 0,
            clamped_values INT NOT NULL DEFAULT 0,
            missing_ids INT NOT NULL,
            missing_dates INT NOT NULL,
            future_dates INT NOT NULL DEFAULT 0,
            risk_high INT NOT NULL,
            risk_medium INT NOT NULL,
            risk_low INT NOT NULL,
            alert_threshold NUMERIC(5,2) NOT NULL,
            min_cohort_size INT NOT NULL,
            source_label TEXT
        )
    """))
    conn.execute(text(f"""
        ALTER TABLE {schema}.reports
        ADD COLUMN IF NOT EXISTS future_dates INT NOT NULL DEFAULT 0
    """))
    conn.execute(text(f"""
        ALTER TABLE {schema}.reports
        ADD COLUMN IF NOT EXISTS invalid_columns INT NOT NULL DEFAULT 0
    """))
    conn.execute(text(f"""
        ALTER TABLE {schema}.reports
        ADD COLUMN IF NOT EXISTS invalid_numeric INT NOT NULL DEFAULT 0
    """))
    conn.execute(text(f"""
        ALTER TABLE {schema}.reports
        ADD COLUMN IF NOT EXISTS invalid_date_format INT NOT NULL DEFAULT 0
    """))
    conn.execute(text(f"""
        ALTER TABLE {schema}.reports
        ADD COLUMN IF NOT EXISTS invalid_range INT NOT NULL DEFAULT 0
    """))
    conn.execute(text(f"""
        ALTER TABLE {schema}.reports
        ADD COLUMN IF NOT EXISTS clamped_values INT NOT NULL DEFAULT 0
    """))
    conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS {schema}.top_risks (
            report_id UUID NOT NULL REFERENCES {schema}.reports(report_id) ON DELETE CASCADE,
            scholar_id TEXT NOT NULL,
            cohort TEXT NOT NULL,
            score INT NOT NULL,
            days_since INT NOT NULL,
            touchpoints_30d INT NOT NULL,
            attendance_rate NUMERIC(5,2) NOT NULL,
            satisfaction_score NUMERIC(5,2) NOT NULL
        )
    """))
    conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS {schema}.cohort_summaries (
            report_id UUID NOT NULL REFERENCES {schema}.reports(report_id) ON DELETE CASCADE,
            cohort TEXT NOT NULL,
            count INT NOT NULL,
            high INT NOT NULL,
            medium INT NOT NULL,
            low INT NOT NULL,
            risk_index NUMERIC(6,2) NOT NULL DEFAULT 0,
            avg_touchpoints_30d NUMERIC(6,2) NOT NULL,
            avg_attendance NUMERIC(5,2) NOT NULL,
            avg_satisfaction NUMERIC(5,2) NOT NULL,
            avg_days_since NUMERIC(6,1) NOT NULL
        )
    """))
    conn.execute(text(f"""
        ALTER TABLE {schema}.cohort_summaries
        ADD COLUMN IF NOT EXISTS risk_index NUMERIC(6,2) NOT NULL DEFAULT 0
    """))
    conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS {schema}.alerts (
            report_id UUID NOT NULL REFERENCES {schema}.reports(report_id) ON DELETE CASCADE,
            cohort TEXT NOT NULL,
            high_share NUMERIC(5,2) NOT NULL,
            risk_index NUMERIC(6,2) NOT NULL DEFAULT 0,
            count INT NOT NULL,
            high INT NOT NULL,
            medium INT NOT NULL,
            low INT NOT NULL,
            avg_days_since NUMERIC(6,1) NOT NULL,
            avg_attendance NUMERIC(5,2) NOT NULL,
            avg_satisfaction NUMERIC(5,2) NOT NULL
        )
    """))
    conn.execute(text(f"""
        ALTER TABLE {schema}.alerts
        ADD COLUMN IF NOT EXISTS risk_index NUMERIC(6,2) NOT NULL DEFAULT 0
    """))
    conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_reports_created_at ON {schema}.reports(created_at)"))
    conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_top_risks_report ON {schema}.top_risks(report_id)"))
    conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_cohort_summaries_report ON {schema}.cohort_summaries(report_id)"))
    conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_alerts_report ON {schema}.alerts(report_id)"))


def ingest_report(conn, schema, report, source_label):
    reference_date = report.get("reference_date", "")
    records = report.get("records", {})
    invalid_breakdown = report.get("invalid_breakdown", {})
    missing = report.get("missing", {})
    date_anomalies = report.get("date_anomalies", {})
    risk_mix = report.get("risk_mix", {})

    cohort_filter = report.get("cohort_filter")
    cohort_filter_text = ",".join(cohort_filter) if cohort_filter else None

    result = conn.execute(
        text(f"""
            INSERT INTO {schema}.reports (
                reference_date, cohort_filter, valid_records, invalid_records,
                invalid_columns, invalid_numeric, invalid_date_format, invalid_range, clamped_values,
                missing_ids, missing_dates,
                future_dates, risk_high, risk_medium, risk_low,
                alert_threshold, min_cohort_size, source_label
            )
            VALUES (
                :reference_date, :cohort_filter, :valid_records, :invalid_records,
                :invalid_columns, :invalid_numeric, :invalid_date_format, :invalid_range, :clamped_values,
                :missing_ids, :missing_dates,
                :future_dates, :risk_high, :risk_medium, :risk_low,
                :alert_threshold, :min_cohort_size, :source_label
            )
            RETURNING report_id
        """),
        {
            "reference_date": reference_date,
            "cohort_filter": cohort_filter_text,
            "valid_records": records.get("valid", 0),
            "invalid_records": records.get("invalid", 0),
            "invalid_columns": invalid_breakdown.get("columns", 0),
            "invalid_numeric": invalid_breakdown.get("numeric", 0),
            "invalid_date_format": invalid_breakdown.get("date_format", 0),
            "invalid_range": invalid_breakdown.get("range", 0),
            "clamped_values": report.get("clamped_values", 0),
            "missing_ids": missing.get("ids", 0),
            "missing_dates": missing.get("dates", 0),
            "future_dates": date_anomalies.get("future_dates", 0),
            "risk_high": risk_mix.get("high", 0),
            "risk_medium": risk_mix.get("medium", 0),
            "risk_low": risk_mix.get("low", 0),
            "alert_threshold": report.get("alert_threshold", 0),
            "min_cohort_size": report.get("min_cohort_size", 0),
            "source_label": source_label,
        }
    )
    report_id = result.scalar_one()

    top_risks = report.get("top_risks", [])
    if top_risks:
        conn.execute(
            text(f"""
                INSERT INTO {schema}.top_risks (
                    report_id, scholar_id, cohort, score, days_since, touchpoints_30d,
                    attendance_rate, satisfaction_score
                )
                VALUES (
                    :report_id, :scholar_id, :cohort, :score, :days_since, :touchpoints_30d,
                    :attendance_rate, :satisfaction_score
                )
            """),
            [
                {
                    "report_id": report_id,
                    "scholar_id": entry.get("id"),
                    "cohort": entry.get("cohort"),
                    "score": entry.get("score"),
                    "days_since": entry.get("days_since"),
                    "touchpoints_30d": entry.get("touchpoints_30d"),
                    "attendance_rate": entry.get("attendance_rate"),
                    "satisfaction_score": entry.get("satisfaction_score"),
                }
                for entry in top_risks
            ]
        )

    cohorts = report.get("cohorts", [])
    if cohorts:
        conn.execute(
            text(f"""
                INSERT INTO {schema}.cohort_summaries (
                    report_id, cohort, count, high, medium, low, risk_index,
                    avg_touchpoints_30d, avg_attendance, avg_satisfaction, avg_days_since
                )
                VALUES (
                    :report_id, :cohort, :count, :high, :medium, :low, :risk_index,
                    :avg_touchpoints_30d, :avg_attendance, :avg_satisfaction, :avg_days_since
                )
            """),
            [
                {
                    "report_id": report_id,
                    "cohort": entry.get("cohort"),
                    "count": entry.get("count"),
                    "high": entry.get("high"),
                    "medium": entry.get("medium"),
                    "low": entry.get("low"),
                    "risk_index": entry.get("risk_index", 0),
                    "avg_touchpoints_30d": entry.get("avg_touchpoints_30d"),
                    "avg_attendance": entry.get("avg_attendance"),
                    "avg_satisfaction": entry.get("avg_satisfaction"),
                    "avg_days_since": entry.get("avg_days_since"),
                }
                for entry in cohorts
            ]
        )

    alerts = report.get("alerts", [])
    if alerts:
        conn.execute(
            text(f"""
                INSERT INTO {schema}.alerts (
                    report_id, cohort, high_share, risk_index, count, high, medium, low,
                    avg_days_since, avg_attendance, avg_satisfaction
                )
                VALUES (
                    :report_id, :cohort, :high_share, :risk_index, :count, :high, :medium, :low,
                    :avg_days_since, :avg_attendance, :avg_satisfaction
                )
            """),
            [
                {
                    "report_id": report_id,
                    "cohort": entry.get("cohort"),
                    "high_share": entry.get("high_share"),
                    "risk_index": entry.get("risk_index", 0),
                    "count": entry.get("count"),
                    "high": entry.get("high"),
                    "medium": entry.get("medium"),
                    "low": entry.get("low"),
                    "avg_days_since": entry.get("avg_days_since"),
                    "avg_attendance": entry.get("avg_attendance"),
                    "avg_satisfaction": entry.get("avg_satisfaction"),
                }
                for entry in alerts
            ]
        )

    return report_id


def parse_args():
    parser = argparse.ArgumentParser(
        description="Load Cohort Health Sentinel JSON output into Postgres."
    )
    parser.add_argument("--schema", default=DEFAULT_SCHEMA, help="Target schema name")
    parser.add_argument("--json", type=Path, help="Path to JSON output")
    parser.add_argument("--source", help="Source label for this report")
    parser.add_argument("--setup", action="store_true", help="Create schema and tables")
    parser.add_argument("--ingest", action="store_true", help="Ingest JSON into Postgres")
    parser.add_argument("--seed", action="store_true", help="Seed with sample output JSON")
    return parser.parse_args()


def main():
    args = parse_args()
    if not (args.setup or args.ingest or args.seed):
        raise SystemExit("Provide --setup, --ingest, or --seed")

    if args.seed:
        args.ingest = True
        if args.json is None:
            args.json = DEFAULT_JSON

    if args.ingest and args.json is None:
        raise SystemExit("--json is required for ingestion")

    db_url = build_db_url()
    engine = create_engine(db_url, future=True)

    with engine.begin() as conn:
        if args.setup:
            setup_schema(conn, args.schema)
        if args.ingest:
            report = load_report(args.json)
            source_label = args.source or args.json.name
            report_id = ingest_report(conn, args.schema, report, source_label)
            print(f"Ingested report {report_id}")


if __name__ == "__main__":
    main()
