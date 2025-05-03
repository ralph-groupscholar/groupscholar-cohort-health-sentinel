#!/bin/sh
set -euo pipefail

PROJECT_ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$PROJECT_ROOT"

cc -std=c11 -O2 -o cohort-health-sentinel src/main.c

output=$(./cohort-health-sentinel --input data/sample.csv --limit 3 --cohort-limit 2)
printf '%s\n' "$output" | grep -q "Cohort summary"
printf '%s\n' "$output" | grep -q "Cohort alerts"

json_tmp=$(mktemp)
./cohort-health-sentinel --input data/sample.csv --json "$json_tmp" --cohort-sort name --cohort-limit 1 > /dev/null
python3 - "$json_tmp" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, "r", encoding="utf-8") as fh:
    payload = json.load(fh)

assert payload["cohort_sort"] == "name"
assert payload["cohort_limit"] == 1
assert payload["cohort_total"] >= 1
assert payload["cohorts"], "expected at least one cohort"
for key in ("high_share", "risk_index", "avg_touchpoints_30d"):
    if key not in payload["cohorts"][0]:
        raise AssertionError(f"missing {key} in cohort summary")
invalid_breakdown = payload.get("invalid_breakdown", {})
for key in ("columns", "numeric", "date_format", "range"):
    if key not in invalid_breakdown:
        raise AssertionError(f"missing invalid_breakdown.{key}")
if "clamped_values" not in payload:
    raise AssertionError("missing clamped_values in payload")
PY

rm -f "$json_tmp"

range_csv=$(mktemp)
cat > "$range_csv" <<'CSV'
scholar_id,cohort,last_touchpoint_date,touchpoints_last_30d,attendance_rate,satisfaction_score
S-9001,RangeTest,2026-01-10,-1,1.20,6.0
S-9002,RangeTest,2026-01-12,2,0.80,4.2
CSV

range_json=$(mktemp)
./cohort-health-sentinel --input "$range_csv" --json "$range_json" > /dev/null
python3 - "$range_json" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, "r", encoding="utf-8") as fh:
    payload = json.load(fh)

assert payload["records"]["invalid"] > 0
assert payload["invalid_breakdown"]["range"] > 0
assert payload["clamped_values"] == 0
PY

clamp_json=$(mktemp)
./cohort-health-sentinel --input "$range_csv" --json "$clamp_json" --clamp-ranges > /dev/null
python3 - "$clamp_json" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, "r", encoding="utf-8") as fh:
    payload = json.load(fh)

assert payload["records"]["invalid"] == 0
assert payload["invalid_breakdown"]["range"] == 0
assert payload["clamped_values"] >= 1
PY

rm -f "$range_csv" "$range_json" "$clamp_json"

cohort_csv=$(mktemp)
alert_csv=$(mktemp)
./cohort-health-sentinel --input data/sample.csv --cohort-csv "$cohort_csv" --alert-csv "$alert_csv" --cohort-limit 2 > /dev/null
head -n 1 "$cohort_csv" | grep -q "cohort,count,high,medium,low,high_share,risk_index,avg_touchpoints_30d,avg_attendance,avg_satisfaction,avg_days_since"
head -n 1 "$alert_csv" | grep -q "cohort,high_share,risk_index,count,high,medium,low,avg_days_since,avg_attendance,avg_satisfaction"
rm -f "$cohort_csv" "$alert_csv"

echo "All tests passed."
