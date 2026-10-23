#!/usr/bin/env bash
set -uo pipefail

root="${CHECK_LOCK_ROOT:?}"
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)/check_steps.sh"

case "${1:?}" in
  holder) steps=("cheap|holder|60|*|echo up > '$CHECK_LOCK_UP'; read -r _ < '$CHECK_LOCK_GO'") ;;
  second) steps=("cheap|second|60|*|touch '$CHECK_LOCK_SECOND_RAN'") ;;
esac
run_checks
