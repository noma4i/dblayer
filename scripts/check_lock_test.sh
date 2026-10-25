#!/usr/bin/env bash
# Self test of the per-tree check lock: while one run holds the tree, a second run exits 3 and runs no step, and a lock left by a dead run is taken over.
set -euo pipefail

root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
work="$(mktemp -d)"
holder=
trap 'echo go >&3; [ -z "$holder" ] || wait "$holder" 2> /dev/null; /bin/rm -rf "$work"' EXIT
mkdir -p "$work/tree/target"
mkfifo "$work/up" "$work/go"
exec 3<> "$work/go"
export CHECK_LOCK_ROOT="$work/tree" CHECK_LOCK_UP="$work/up" CHECK_LOCK_GO="$work/go" CHECK_LOCK_SECOND_RAN="$work/second-ran"

bash "$root/scripts/check_lock_driver.sh" holder > "$work/holder.log" 2>&1 &
holder=$!
read -r _ < "$work/up"

second_code=0
bash "$root/scripts/check_lock_driver.sh" second > "$work/second.log" 2>&1 || second_code=$?
[ "$second_code" -eq 3 ] || { echo "check_lock_test: a second run beside a live run exited $second_code, expected 3" >&2; exit 1; }
[ ! -e "$work/second-ran" ] || { echo "check_lock_test: a second run beside a live run ran its step" >&2; exit 1; }

echo go >&3
wait "$holder" || { echo "check_lock_test: the holding run failed" >&2; exit 1; }

second_code=0
bash "$root/scripts/check_lock_driver.sh" second > "$work/second.log" 2>&1 || second_code=$?
[ "$second_code" -eq 0 ] && [ -e "$work/second-ran" ] || { echo "check_lock_test: the lock was not released when the run ended" >&2; exit 1; }

/bin/rm -f "$work/second-ran"
mkdir "$work/tree/target/check.lock"
echo 999999 > "$work/tree/target/check.lock/pid"
bash "$root/scripts/check_lock_driver.sh" second > "$work/second.log" 2>&1 && [ -e "$work/second-ran" ] || { echo "check_lock_test: a lock of a dead run was not taken over" >&2; exit 1; }
echo "check_lock_test: ok"
