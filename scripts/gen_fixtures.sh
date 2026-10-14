#!/usr/bin/env bash
set -euo pipefail

root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
export PATH="/opt/homebrew/opt/rustup/bin:$PATH"
swift_out="$root/swift/Tests/DBlayerTests/FixtureModels.swift"
kotlin_out="$root/kotlin/src/test/kotlin/com/noma4i/dblayer/models/FixtureModels.kt"
swift_operations_out="$root/swift/Tests/DBlayerTests/FixtureOperations.swift"
kotlin_operations_out="$root/kotlin/src/test/kotlin/com/noma4i/dblayer/models/FixtureOperations.kt"

gen() { cargo run -q --manifest-path "$root/Cargo.toml" -p dblayer-gen -- "$1" "$root"/fixtures/*.dbl; }
gen_operations() { cargo run -q --manifest-path "$root/Cargo.toml" -p dblayer-gen -- --schema "$root/fixtures/operations/schema.graphql" "$1" "$root"/fixtures/operations/*.dbl; }

swift="$(gen swift)"
kotlin="$(gen kotlin)"
swift_operations="$(gen_operations swift)"
kotlin_operations="$(gen_operations kotlin)"

if [ "${1:-}" = "--check" ]; then
  [ "$swift" = "$(cat "$swift_out")" ] && [ "$kotlin" = "$(cat "$kotlin_out")" ] && [ "$swift_operations" = "$(cat "$swift_operations_out")" ] && [ "$kotlin_operations" = "$(cat "$kotlin_operations_out")" ] || { echo "Fixture models differ from fixtures. Run scripts/gen_fixtures.sh."; exit 1; }
  echo "Fixture models match."
  exit 0
fi

printf '%s\n' "$swift" > "$swift_out"
printf '%s\n' "$kotlin" > "$kotlin_out"
printf '%s\n' "$swift_operations" > "$swift_operations_out"
printf '%s\n' "$kotlin_operations" > "$kotlin_operations_out"
