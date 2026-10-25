#!/usr/bin/env bash
set -euo pipefail

root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
work="$(mktemp -d)"
trap 'chmod -R u+rw "$work" 2> /dev/null; /bin/rm -rf "$work"' EXIT
mkdir -p "$work/tree/target" "$work/bin"
printf '#!/bin/sh\ncase "$*" in *%%Y%%m%%dT*) echo 20260101T000000Z ;; *) echo 2026-01-01T00:00:00Z ;; esac\n' > "$work/bin/date"
chmod +x "$work/bin/date"
export CHECK_LOGS_ROOT="$work/tree"
logs="$work/tree/target/check-logs"
xml=kotlin/build/test-results/testDebugUnitTest/TEST-x.xml
html=kotlin/build/reports/tests/testDebugUnitTest/index.html
fail() { echo "check_logs_test: $1" >&2; exit 1; }
run() { PATH="$work/bin:$PATH" CHECK_LOGS_TAG="$1" bash "$root/scripts/check_logs_driver.sh" > "$work/$1.out" 2>&1; }

code=0
CHECK_LOGS_EXIT=1 run a || code=$?
[ "$code" -eq 1 ] || fail "the failing run exited $code, expected 1"
first="$(readlink "$logs/latest" || true)"
[ -n "$first" ] || fail "the run left no latest link"
run b || fail "the passing run failed"
second="$(readlink "$logs/latest" || true)"
[ "$second" != "$first" ] || fail "two runs with the same date and lane share one run directory"
grep -qx output-a "$logs/$first/gradle-test.log" || fail "the log of the first run was lost"
grep -qx xml-a "$logs/$first/gradle-test.artifacts/$xml" || fail "the XML of the first run is not kept under its source path"
grep -qx html-a "$logs/$first/gradle-test.artifacts/$html" || fail "the HTML of the first run is not kept under its source path"
grep -qx xml-b "$logs/$second/gradle-test.artifacts/$xml" || fail "the XML of the second run is not kept under its source path"
grep -qx html-b "$logs/$second/gradle-test.artifacts/$html" || fail "the HTML of the second run is not kept under its source path"
grep -q 'FAIL(1)' "$logs/$first/summary.txt" || fail "the summary of the first run lacks the failed step"
grep -q "log: $logs/$first/gradle-test.log" "$work/a.out" || fail "the failure message does not name the log in the run directory"

code=0
CHECK_LOGS_UNREADABLE=1 run c || code=$?
[ "$code" -ne 0 ] || fail "a result file that cannot be copied gave a green run"
grep -q 'FAIL(70)' "$work/c.out" || fail "a copy failure is not reported as a failed step"

before="$(ls -1 "$logs" | wc -l)"
/bin/rm -rf "$logs"
printf 'not a directory' > "$logs"
code=0
run d || code=$?
[ "$code" -eq 4 ] || fail "an unusable log directory exited $code, expected 4"
grep -q 'cannot open a log directory' "$work/d.out" || fail "an unusable log directory is not reported"
[ "$before" -ge 3 ] || fail "the earlier runs were not kept"
echo "check_logs_test: ok"
