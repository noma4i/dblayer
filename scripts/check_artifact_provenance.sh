#!/usr/bin/env bash
# C8: the generated bindings are changed only by the repository generator, and no hand file owns ABI state next to them.
set -uo pipefail
root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
name="check_artifact_provenance.sh"

check() {
  local tree="$1" failed=0
  for script in scripts/build_ios.sh scripts/generate_kotlin_bindings.sh; do
    if grep -nE '(^|[^[:alnum:]_])(awk|perl|sed|python3?)[[:space:]].*(dblayer\.(swift|kt)|generated)' "$tree/$script" > /dev/null; then
      echo "$name: $script rewrites generated output by text, move the edit into bindgen/src/patch" >&2
      failed=1
    fi
  done
  for file in swift/Sources/DBlayer/RustFutureHandle.swift kotlin/src/main/kotlin/com/noma4i/dblayer/RuntimeLane.kt kotlin/src/main/kotlin/com/noma4i/dblayer/ProcessOwner.kt; do
    if [ -e "$tree/$file" ]; then
      echo "$name: $file is hand ABI state, the generator owns it" >&2
      failed=1
    fi
  done
  [ -f "$tree/bindgen/src/patch/mod.rs" ] || { echo "$name: bindgen/src/patch/mod.rs is missing" >&2; failed=1; }
  return "$failed"
}

selftest() {
  dir=""
  dir="$(mktemp -d)" || exit 1
  trap '[ -n "${dir:-}" ] && command rm -rf "$dir"' EXIT
  mkdir -p "$dir/scripts" "$dir/bindgen/src/patch"
  : > "$dir/bindgen/src/patch/mod.rs"
  printf 'cargo build\n' > "$dir/scripts/build_ios.sh"
  printf 'bindgen generate\n' > "$dir/scripts/generate_kotlin_bindings.sh"
  check "$dir" 2> /dev/null || { echo "$name: selftest: a clean tree failed" >&2; exit 1; }
  printf 'awk -f patch.awk "$generated" > out\n' >> "$dir/scripts/build_ios.sh"
  check "$dir" 2> /dev/null && { echo "$name: selftest: an awk patch of generated output passed" >&2; exit 1; }
  printf 'cargo build\n' > "$dir/scripts/build_ios.sh"
  mkdir -p "$dir/swift/Sources/DBlayer"
  : > "$dir/swift/Sources/DBlayer/RustFutureHandle.swift"
  check "$dir" 2> /dev/null && { echo "$name: selftest: a hand ABI file passed" >&2; exit 1; }
  echo "$name: selftest ok"
}

if [ "${1:-}" = "--selftest" ]; then selftest; else check "$root"; fi
