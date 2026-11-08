#!/usr/bin/env bash
# C4: every default, drop and conversion of the generator (recursive scan of gen/src, tests excluded) carries a norm ID (M-n or P-X-n) in scripts/gen_decisions_allowlist.txt (path, trimmed line, ID; tab separated). A new decision line, an ID that is not a norm, or a stale line fails.
set -euo pipefail

root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
base="${CHECK_GEN_ROOT:-$root}"
allow="${CHECK_GEN_ALLOW:-$root/scripts/gen_decisions_allowlist.txt}"
found=0
seen="$(mktemp)"
trap '/bin/rm -f "$seen"' EXIT

listed() {
  LP="$1" LT="$2" awk -F'\t' '$1 == ENVIRON["LP"] && $2 == ENVIRON["LT"] { ok = 1 } END { exit !ok }' "$3"
}

while IFS= read -r hit; do
  [ -n "$hit" ] || continue
  path="${hit%%:*}"
  rest="${hit#*:}"
  text="$(printf '%s' "${rest#*:}" | sed -E 's/^[[:space:]]+//; s/[[:space:]]+$//')"
  if listed "$path" "$text" "$allow"; then
    printf '%s\t%s\n' "$path" "$text" >> "$seen"
  else
    echo "check_gen_decisions: decision without a norm ID: $path: $text" >&2
    found=1
  fi
done < <({ rg -n --no-heading -g '*.rs' -g '!**/tests/**' -g '!*_tests.rs' 'filter_map|\.ok\(\)|unwrap_or|map_or|\.flatten\(\)' "$base/gen/src" || true; } | sed "s|^$base/||")

while IFS=$'\t' read -r path text id; do
  [ -n "$path" ] || continue
  if ! printf '%s' "$id" | rg -q '^(M|P-[A-Z]+-)[0-9]+$'; then
    echo "check_gen_decisions: not a norm ID: $path: $id" >&2
    found=1
  fi
  if ! listed "$path" "$text" "$seen"; then
    echo "check_gen_decisions: stale allowlist line: $path: $text" >&2
    found=1
  fi
done < "$allow"

if [ "$found" -eq 1 ]; then
  exit 1
fi
echo "check_gen_decisions: ok"
