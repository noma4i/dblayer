#!/usr/bin/env bash
set -euo pipefail

root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
export PATH="/opt/homebrew/opt/rustup/bin:$PATH"
swift_out="$root/swift/Tests/DBlayerTests/FixtureModels.swift"
kotlin_out="$root/kotlin/src/test/kotlin/com/noma4i/dblayer/models/FixtureModels.kt"

gen() { cargo run -q --manifest-path "$root/Cargo.toml" -p dblayer-gen -- "$1" "$root"/fixtures/*.dbl; }

swift="$(gen swift)"
kotlin="$(gen kotlin)"

if [ "${1:-}" = "--check" ]; then
  [ "$swift" = "$(cat "$swift_out")" ] && [ "$kotlin" = "$(cat "$kotlin_out")" ] || { echo "Fixture models differ from fixtures/*.dbl. Run scripts/gen_fixtures.sh."; exit 1; }
  echo "Fixture models match."
  exit 0
fi

printf '%s\n' "$swift" > "$swift_out"
printf '%s\n' "$kotlin" > "$kotlin_out"
