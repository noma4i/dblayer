#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -gt 1 ] || { [ "$#" -eq 1 ] && [ "$1" != "--check" ]; }; then
  echo "usage: scripts/gen_fixtures.sh [--check]" >&2
  exit 2
fi

root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
export LC_ALL=C
cd -- "$root"
swift_out="$root/swift/Tests/DBlayerTests/FixtureModels.swift"
kotlin_out="$root/kotlin/src/test/kotlin/com/noma4i/dblayer/models/FixtureModels.kt"
swift_operations_out="$root/swift/Tests/DBlayerTests/FixtureOperations.swift"
kotlin_operations_out="$root/kotlin/src/test/kotlin/com/noma4i/dblayer/models/FixtureOperations.kt"
wrapper_out="$root/core/src/ffi/selftest/wrapper_models.inc"

gen() { cargo run -p dblayer-gen -- "$1" "$root"/fixtures/*.dbl; }
gen_operations() { cargo run -p dblayer-gen -- --schema "$root/fixtures/operations/schema.graphql" "$1" "$root"/fixtures/operations/*.dbl | grep -v -e '^public let modelsJSON = ' -e '^val modelsJSON = '; }

gen_operation_specs() { cargo run -p dblayer-gen -- --schema "$root/fixtures/operations/schema.graphql" spec "$root"/fixtures/operations/*.dbl | python3 -I -c 'import json, sys; print(json.dumps(json.load(sys.stdin)["operations"], separators=(",", ":")))'; }
with_operations() { python3 -I -c 'import sys; text = sys.stdin.read(); empty = ",\"operations\":[]}"; assert text.count(empty) == 1; operations = sys.argv[1]; operations = operations.replace("$", "${\x27$\x27}") if sys.argv[2] == "kotlin" else operations; sys.stdout.write(text.replace(empty, ",\"operations\":" + operations + "}"))' "$1" "$2"; }

operation_specs="$(gen_operation_specs 2> /dev/null)"
swift="$(gen swift | with_operations "$operation_specs" swift)"
kotlin="$(gen kotlin | with_operations "$operation_specs" kotlin)"
swift_operations="$(gen_operations swift)"
kotlin_operations="$(gen_operations kotlin)"
wrapper="$(cargo run -p dblayer-gen -- spec "$root"/fixtures/*.dbl | python3 -I -c 'import json, sys; sys.stdout.write(json.dumps(json.load(sys.stdin)["models"], separators=(",", ":")))')"

if [ "$#" -eq 1 ]; then
  [ "$swift" = "$(cat "$swift_out")" ] && [ "$kotlin" = "$(cat "$kotlin_out")" ] && [ "$swift_operations" = "$(cat "$swift_operations_out")" ] && [ "$kotlin_operations" = "$(cat "$kotlin_operations_out")" ] && [ "$wrapper" = "$(cat "$wrapper_out")" ] || { echo "Fixture models differ from fixtures. Run scripts/gen_fixtures.sh."; exit 1; }
  echo "Fixture models match."
  exit 0
fi

printf '%s\n' "$swift" > "$swift_out"
printf '%s\n' "$kotlin" > "$kotlin_out"
printf '%s\n' "$swift_operations" > "$swift_operations_out"
printf '%s\n' "$kotlin_operations" > "$kotlin_operations_out"
printf '%s' "$wrapper" > "$wrapper_out"
