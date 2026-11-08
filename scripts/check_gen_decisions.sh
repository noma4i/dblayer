#!/usr/bin/env bash
# C4: every default and refusal on values of the declaration (unwrap_or*, map_or*, ok(), filter_map; recursive scan of gen/src, tests excluded) carries a norm ID (M-n or P-X-n) in scripts/gen_decisions_allowlist.txt (path, trimmed line, ID; tab separated). A new decision line, an ID that is not a norm, or a stale line fails.
# Mechanics are no decisions and the scan skips them: iteration over an Option or Option<Vec> (flatten, flat_map), parse of a text (first element of split, chars or a find with the text length as end), and the files that read generated source or the command line (members.rs, main.rs).
set -euo pipefail

root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
base="${CHECK_GEN_ROOT:-$root}"
allow="${CHECK_GEN_ALLOW:-$root/scripts/gen_decisions_allowlist.txt}"
found=0
mechanics='\.next\(\)(\.map\(.*\))?\.unwrap_or_default\(\)|\.unwrap_or\([a-z_]+\.len\(\)\)'
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
done < <({ rg -n --no-heading -g '*.rs' -g '!**/tests/**' -g '!*_tests.rs' -g '!members.rs' -g '!main.rs' 'filter_map|\.ok\(\)|unwrap_or|map_or' "$base/gen/src" || true; } | sed "s|^$base/||" | { grep -Ev "$mechanics" || true; })

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
