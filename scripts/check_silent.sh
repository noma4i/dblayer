#!/usr/bin/env bash
# Silent rule 1: a wrapper does not swallow an error. `try?` in Swift and `runCatching {...}.getOrNull()` in Kotlin turn a failure into nil, so each use is listed with its reason in scripts/silent_allowlist.txt (path, trimmed line, reason; tab separated). A new use fails the check, and so does a listed line that no longer exists.
set -euo pipefail

root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
base="${CHECK_SILENT_ROOT:-$root}"
allow="${CHECK_SILENT_ALLOW:-$root/scripts/silent_allowlist.txt}"
found=0
seen="$(mktemp)"
trap '/bin/rm -f "$seen"' EXIT

listed() {
  awk -F'\t' -v p="$1" -v t="$2" '$1 == p && $2 == t { ok = 1 } END { exit !ok }' "$3"
}

scan() {
  local label="$1" hit path rest text
  while IFS= read -r hit; do
    [ -n "$hit" ] || continue
    path="${hit%%:*}"
    rest="${hit#*:}"
    text="$(printf '%s' "${rest#*:}" | sed -E 's/^[[:space:]]+//; s/[[:space:]]+$//')"
    if listed "$path" "$text" "$allow"; then
      printf '%s\t%s\n' "$path" "$text" >> "$seen"
    else
      echo "check_silent: $label: $path: $text" >&2
      found=1
    fi
  done
  return 0
}

swift_hits() {
  rg -n --no-heading -g '*.swift' -g '!dblayer.swift' 'try\?' "$base/swift/Sources" | sed "s|^$base/||" || true
}

kotlin_hits() {
  { rg -n --no-heading -U -g '*.kt' -g '!dblayer.kt' 'runCatching[^\n]*(\n[^\n]*){0,2}\.getOrNull\(\)' "$base/kotlin/src/main" || true; } | sed "s|^$base/||" | { rg ':[0-9]+:.*runCatching' || true; }
}

scan "swift: try? swallows the error (throw it, or list the line with a reason)" < <(swift_hits)
scan "kotlin: runCatching {...}.getOrNull() swallows the error (throw it, or list the line with a reason)" < <(kotlin_hits)

while IFS=$'\t' read -r path text _; do
  [ -n "$path" ] || continue
  if ! listed "$path" "$text" "$seen"; then
    echo "check_silent: stale allowlist line: $path: $text" >&2
    found=1
  fi
done < "$allow"

if [ "$found" -eq 1 ]; then
  exit 1
fi
echo "check_silent: ok"
