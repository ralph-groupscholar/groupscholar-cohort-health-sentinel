#!/usr/bin/env python3
import argparse
import json
import os
from datetime import date
from typing import Optional

import psycopg2
from psycopg2.extras import execute_values

SCHEMA = "cohort_health_sentinel"


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise SystemExit(f"Missing required env var: {name}")
    return value


def parse_reference_date(raw: str) -> Optional[date]:
    if not raw:
        return None
    if raw.strip().lower() == "today":
        return date.today()
    try:
        year, month, day = [int(part) for part in raw.split("-")]
        return date(year, month, day)
    except ValueError:
        return None


def ensure_schema(cur) -> None:
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA};")
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA}.runs (
            id SERIAL PRIMARY KEY,
            reference_date DATE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            alert_threshold NUMERIC(6, 3) NOT NULL,
            min_cohort_size INTEGER NOT NULL,
            valid_count INTEGER NOT NULL,
            invalid_count INTEGER NOT NULL,
            missing_ids INTEGER NOT NULL,
            missing_dates INTEGER NOT NULL
        );
        """
    )
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA}.top_risks (
            id SERIAL PRIMARY KEY,
            run_id INTEGER NOT NULL REFERENCES {SCHEMA}.runs(id) ON DELETE CASCADE,
            scholar_id TEXT NOT NULL,
            cohort TEXT NOT NULL,
            score INTEGER NOT NULL,
            days_since INTEGER NOT NULL,
            touchpoints_30d INTEGER NOT NULL,
            attendance_rate NUMERIC(5, 3) NOT NULL,
            satisfaction_score NUMERIC(5, 2) NOT NULL
        );
        """
    )
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA}.cohort_metrics (
            id SERIAL PRIMARY KEY,
            run_id INTEGER NOT NULL REFERENCES {SCHEMA}.runs(id) ON DELETE CASCADE,
            cohort TEXT NOT NULL,
            count INTEGER NOT NULL,
            high INTEGER NOT NULL,
            medium INTEGER NOT NULL,
            low INTEGER NOT NULL,
            avg_touchpoints_30d NUMERIC(6, 3) NOT NULL,
            avg_attendance NUMERIC(6, 3) NOT NULL,
            avg_satisfaction NUMERIC(6, 3) NOT NULL,
            avg_days_since NUMERIC(6, 3) NOT NULL
        );
        """
    )
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA}.cohort_alerts (
            id SERIAL PRIMARY KEY,
            run_id INTEGER NOT NULL REFERENCES {SCHEMA}.runs(id) ON DELETE CASCADE,
            cohort TEXT NOT NULL,
            high_share NUMERIC(6, 3) NOT NULL,
            count INTEGER NOT NULL,
            high INTEGER NOT NULL,
            medium INTEGER NOT NULL,
            low INTEGER NOT NULL,
            avg_days_since NUMERIC(6, 3) NOT NULL,
            avg_attendance NUMERIC(6, 3) NOT NULL,
            avg_satisfaction NUMERIC(6, 3) NOT NULL
        );
        """
    )


def load_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Sync Cohort Health Sentinel JSON output into Postgres."
    )
    parser.add_argument("--json", required=True, help="Path to JSON output file")
    args = parser.parse_args()

    payload = load_json(args.json)

    connection = psycopg2.connect(
        host=require_env("PGHOST"),
        port=require_env("PGPORT"),
        user=require_env("PGUSER"),
        password=require_env("PGPASSWORD"),
        dbname=require_env("PGDATABASE"),
    )

    try:
        with connection:
            with connection.cursor() as cur:
                ensure_schema(cur)

                reference_date = parse_reference_date(payload.get("reference_date", ""))
                records = payload.get("records", {})
                missing = payload.get("missing", {})

                cur.execute(
                    f"""
                    INSERT INTO {SCHEMA}.runs
                      (reference_date, alert_threshold, min_cohort_size, valid_count, invalid_count, missing_ids, missing_dates)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    RETURNING id;
                    """,
                    (
                        reference_date,
                        payload.get("alert_threshold", 0),
                        payload.get("min_cohort_size", 0),
                        records.get("valid", 0),
                        records.get("invalid", 0),
                        missing.get("ids", 0),
                        missing.get("dates", 0),
                    ),
                )
                run_id = cur.fetchone()[0]

                top_risks = payload.get("top_risks", [])
                if top_risks:
                    execute_values(
                        cur,
                        f"""
                        INSERT INTO {SCHEMA}.top_risks
                          (run_id, scholar_id, cohort, score, days_since, touchpoints_30d, attendance_rate, satisfaction_score)
                        VALUES %s
                        """,
                        [
                            (
                                run_id,
                                item.get("id", ""),
                                item.get("cohort", ""),
                                item.get("score", 0),
                                item.get("days_since", 0),
                                item.get("touchpoints_30d", 0),
                                item.get("attendance_rate", 0),
                                item.get("satisfaction_score", 0),
                            )
                            for item in top_risks
                        ],
                    )

                cohorts = payload.get("cohorts", [])
                if cohorts:
                    execute_values(
                        cur,
                        f"""
                        INSERT INTO {SCHEMA}.cohort_metrics
                          (run_id, cohort, count, high, medium, low, avg_touchpoints_30d, avg_attendance, avg_satisfaction, avg_days_since)
                        VALUES %s
                        """,
                        [
                            (
                                run_id,
                                item.get("cohort", ""),
                                item.get("count", 0),
                                item.get("high", 0),
                                item.get("medium", 0),
                                item.get("low", 0),
                                item.get("avg_touchpoints_30d", 0),
                                item.get("avg_attendance", 0),
                                item.get("avg_satisfaction", 0),
                                item.get("avg_days_since", 0),
                            )
                            for item in cohorts
                        ],
                    )

                alerts = payload.get("alerts", [])
                if alerts:
                    execute_values(
                        cur,
                        f"""
                        INSERT INTO {SCHEMA}.cohort_alerts
                          (run_id, cohort, high_share, count, high, medium, low, avg_days_since, avg_attendance, avg_satisfaction)
                        VALUES %s
                        """,
                        [
                            (
                                run_id,
                                item.get("cohort", ""),
                                item.get("high_share", 0),
                                item.get("count", 0),
                                item.get("high", 0),
                                item.get("medium", 0),
                                item.get("low", 0),
                                item.get("avg_days_since", 0),
                                item.get("avg_attendance", 0),
                                item.get("avg_satisfaction", 0),
                            )
                            for item in alerts
                        ],
                    )

                print(f"Synced run {run_id} into schema '{SCHEMA}'.")
    finally:
        connection.close()


if __name__ == "__main__":
    main()
