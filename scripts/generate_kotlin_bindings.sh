#!/usr/bin/env bash
set -euo pipefail

name="generate_kotlin_bindings.sh"
usage="usage: $name --bindgen <uniffi-bindgen> --library <libdblayer.a|.so|.dylib> --config <uniffi.toml> --out <directory>"
bindgen="" library="" config="" out=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --bindgen | --library | --config | --out)
      [ "$#" -ge 2 ] || { echo "$name: $1 needs a value" >&2; exit 1; }
      case "$1" in
        --bindgen) bindgen="$2" ;;
        --library) library="$2" ;;
        --config) config="$2" ;;
        --out) out="$2" ;;
      esac
      shift 2 ;;
    *) echo "$name: unknown argument $1" >&2; echo "$usage" >&2; exit 1 ;;
  esac
done
[ -n "$bindgen" ] || { echo "$name: --bindgen is required" >&2; exit 1; }
[ -n "$library" ] || { echo "$name: --library is required" >&2; exit 1; }
[ -n "$config" ] || { echo "$name: --config is required" >&2; exit 1; }
[ -n "$out" ] || { echo "$name: --out is required" >&2; exit 1; }
[ -f "$bindgen" ] && [ -x "$bindgen" ] || { echo "$name: --bindgen is not an executable file: $bindgen" >&2; exit 1; }
[ -f "$library" ] || { echo "$name: --library is not a file: $library" >&2; exit 1; }
[ -f "$config" ] || { echo "$name: --config is not a file: $config" >&2; exit 1; }
[ -d "$out" ] || { echo "$name: --out is not a directory: $out" >&2; exit 1; }

global="$(mktemp)"
trap 'rm -f "$global"' EXIT
sed 's/^\[/[crates.dblayer./' "$config" > "$global"
"$bindgen" generate --library "$library" --config "$global" --language kotlin --no-format --out-dir "$out"

generated="$out/com/noma4i/dblayer/dblayer.kt"
[ -f "$generated" ] || { echo "$name: $generated was not generated" >&2; exit 1; }
marker='        Native.register(UniffiLib::class.java, findLibraryName(componentName = "dblayer"))'
[ "$(grep -cxF -- "$marker" "$generated")" = 1 ] || { echo "$name: expected exactly 1 UniffiLib Native.register line in dblayer.kt, the UniFFI template changed" >&2; exit 1; }
awk -v marker="$marker" '$0 == marker { print "        ProcessOwner.claim()" } { print }' "$generated" > "$generated.claimed"
mv "$generated.claimed" "$generated"
[ "$(grep -cxF -- '        ProcessOwner.claim()' "$generated")" = 1 ] || { echo "$name: ProcessOwner.claim() was not inserted into dblayer.kt" >&2; exit 1; }
