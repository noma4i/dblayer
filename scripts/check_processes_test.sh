#!/usr/bin/env bash
set -euo pipefail

here="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
work="$(mktemp -d)"
sleep 601.7 &
outsider=$!
trap 'kill "$outsider" 2>/dev/null || true; wait "$outsider" 2>/dev/null || true; /bin/rm -rf "$work"' EXIT
export CHECK_PROCESS_VIOLATION="$work/violation"
pkill() {
  case " $* " in
    *' -f '*) printf '%s\n' "$*" >> "$CHECK_PROCESS_VIOLATION"; return 1 ;;
    *) command pkill "$@" ;;
  esac
}
export -f pkill
ps() {
  case " $* " in
    *' -a'*) printf '%s\n' "ps $*" >> "$CHECK_PROCESS_VIOLATION" ;;
  esac
  command ps "$@"
}
export -f ps
code=0
bash "$here/check_waits_test.sh" > "$work/out" 2>&1 || code=$?
cat "$work/out"
if [ "$code" -ne 0 ] || [ -f "$work/violation" ] || ! kill -0 "$outsider" 2>/dev/null; then
  echo "FAIL selftest isolation: code=$code, outsider PID=$outsider"
  [ ! -f "$work/violation" ] || cat "$work/violation"
  exit 1
fi
source "$here/check_steps.sh"
root="$work"
steps=("a|owner|10|-|echo \$CHECK_OWNER_ID >> owners")
(run_checks) > "$work/owners.out" 2>&1 || { cat "$work/owners.out"; exit 1; }
(run_checks) >> "$work/owners.out" 2>&1 || { cat "$work/owners.out"; exit 1; }
if [ "$(sort -u "$work/owners" | wc -l | tr -d ' ')" -ne 2 ]; then
  echo "FAIL consecutive checks reused an owner ID"
  exit 1
fi
echo "check_processes_test: ok, outsider PID=$outsider remains alive"
