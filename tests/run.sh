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
PY

rm -f "$json_tmp"

echo "All tests passed."
