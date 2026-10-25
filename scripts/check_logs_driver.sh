#!/usr/bin/env bash
set -uo pipefail

source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)/check_dblayer.sh"
root="${CHECK_LOGS_ROOT:?}"
steps=("kotlin|gradle-test|60|*|mkdir -p kotlin/build/test-results/testDebugUnitTest kotlin/build/reports/tests/testDebugUnitTest; echo \"output-\$CHECK_LOGS_TAG\"; echo \"xml-\$CHECK_LOGS_TAG\" > kotlin/build/test-results/testDebugUnitTest/TEST-x.xml; echo \"html-\$CHECK_LOGS_TAG\" > kotlin/build/reports/tests/testDebugUnitTest/index.html; [ -z \"\${CHECK_LOGS_UNREADABLE:-}\" ] || chmod 000 kotlin/build/test-results/testDebugUnitTest/TEST-x.xml; exit \"\${CHECK_LOGS_EXIT:-0}\"")
run_checks --only kotlin
