#!/usr/bin/env bash
set -euo pipefail

script="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)/reclaim_target.sh"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

printf '#!/bin/sh\nexit 0\n' > "$work/pgrep"
printf '#!/bin/sh\necho cargo must not run >&2\nexit 9\n' > "$work/cargo"
chmod +x "$work/pgrep" "$work/cargo"
mkdir "$work/target"

set +e
PATH="$work:$PATH" CARGO_TARGET_DIR="$work/target" bash "$script" 2> "$work/err"
code=$?
set -e

[ "$code" -eq 1 ] || { echo "reclaim_target_test: expected exit 1, got $code" >&2; exit 1; }
grep -q "cargo is running" "$work/err" || { echo "reclaim_target_test: refusal message missing" >&2; exit 1; }
echo "reclaim_target_test: ok"
