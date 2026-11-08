#!/usr/bin/env bash
# C8: the generated bindings are changed only by the repository generator, no hand file owns ABI state next to them, and a committed dblayer.swift equals the generation of the current core (--bindings <fresh> <committed>).
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

same_bindings() {
  local fresh="$1" committed="$2"
  [ -f "$fresh" ] && [ -f "$committed" ] || { echo "$name: a bindings file is missing: $fresh $committed" >&2; return 1; }
  [ "$(shasum -a 256 < "$fresh")" = "$(shasum -a 256 < "$committed")" ] || { echo "$name: the committed dblayer.swift differs from the generation of the current core" >&2; return 1; }
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
  printf 'let a = 1\n' > "$dir/fresh.swift"
  cp "$dir/fresh.swift" "$dir/committed.swift"
  same_bindings "$dir/fresh.swift" "$dir/committed.swift" 2> /dev/null || { echo "$name: selftest: equal bindings failed" >&2; exit 1; }
  printf 'let a = 2\n' > "$dir/committed.swift"
  same_bindings "$dir/fresh.swift" "$dir/committed.swift" 2> /dev/null && { echo "$name: selftest: stale committed bindings passed" >&2; exit 1; }
  echo "$name: selftest ok"
}

case "${1:-}" in
  --selftest) selftest ;;
  --bindings) same_bindings "${2:?--bindings needs the freshly generated dblayer.swift}" "${3:?--bindings needs the committed dblayer.swift}" ;;
  *) check "$root" ;;
esac
