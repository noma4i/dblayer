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
snapshots=(
  "core/src/ffi/selftest/wrapper_models.inc list * fixtures"
  "core/src/ffi/selftest/upload_wrapper_models.json list Asset,Moment,User,Vibe fixtures"
  "core/tests/corpus/models.json models Moment,Notice,User,Vibe,Container fixtures corpus"
  "core/tests/corpus/models_events.json models Chat,Container,CurrentRegion,CurrentUser,Message,Record,User fixtures"
  "core/tests/corpus/matrix/kinds.spec.json spec * kinds"
  "core/tests/corpus/matrix/removed_by.spec.json spec * removed_by"
  "fixtures/media-marker.json models MediaEntry operations"
)

schema="$root/fixtures/graphql/schema.graphql"
gen() { cargo run -p dblayer-gen -- --schema "$schema" "$1" "$root"/fixtures/*.dbl; }
gen_operations() { cargo run -p dblayer-gen -- --schema "$root/fixtures/operations/schema.graphql" "$1" "$root"/fixtures/operations/*.dbl | grep -v -e '^public let modelsJSON = ' -e '^val modelsJSON = '; }

gen_operation_specs() { cargo run -p dblayer-gen -- --schema "$root/fixtures/operations/schema.graphql" spec "$root"/fixtures/operations/*.dbl | python3 -I -c 'import json, sys; print(json.dumps(json.load(sys.stdin)["operations"], separators=(",", ":")))'; }
with_operations() { python3 -I -c 'import sys; text = sys.stdin.read(); empty = ",\"operations\":[]}"; assert text.count(empty) == 1; operations = sys.argv[1]; operations = operations.replace("$", "${\x27$\x27}") if sys.argv[2] == "kotlin" else operations; sys.stdout.write(text.replace(empty, ",\"operations\":" + operations + "}"))' "$1" "$2"; }

operation_specs="$(gen_operation_specs 2> /dev/null)"
swift="$(gen swift | with_operations "$operation_specs" swift)"
kotlin="$(gen kotlin | with_operations "$operation_specs" kotlin)"
swift_operations="$(gen_operations swift)"
kotlin_operations="$(gen_operations kotlin)"
sources="$(mktemp -d)"
trap 'rm -r "$sources"' EXIT
cargo run -p dblayer-gen -- --schema "$schema" spec "$root"/fixtures/*.dbl > "$sources/fixtures"
cargo run -p dblayer-gen -- --schema "$schema" spec "$root/core/tests/corpus/container_nodes.dbl" > "$sources/corpus"
cargo run -p dblayer-gen -- spec "$root/fixtures/matrix/kinds.dbl" > "$sources/kinds"
cargo run -p dblayer-gen -- spec "$root/fixtures/matrix/removed_by.dbl" > "$sources/removed_by"
cargo run -p dblayer-gen -- --schema "$root/fixtures/operations/schema.graphql" spec "$root"/fixtures/operations/*.dbl > "$sources/operations"
snapshot() { python3 -I -c 'import json, sys; shape, keys, paths = sys.argv[1], sys.argv[2], sys.argv[3:]; specs = [json.load(open(path)) for path in paths]; found = {model["key"]: model for spec in specs for model in spec["models"]}; models = list(found.values()) if keys == "*" else [found[key] for key in keys.split(",")]; value = {"list": models, "models": {"models": models}, "spec": {"models": models, "operations": specs[0]["operations"]}}[shape]; sys.stdout.write(json.dumps(value, separators=(",", ":")))' "$@"; }
snapshot_of() { set -f; set -- $1; set +f; local shape="$2" keys="$3"; shift 3; snapshot "$shape" "$keys" "${@/#/$sources/}"; }

if [ "$#" -eq 1 ]; then
  [ "$swift" = "$(cat "$swift_out")" ] && [ "$kotlin" = "$(cat "$kotlin_out")" ] && [ "$swift_operations" = "$(cat "$swift_operations_out")" ] && [ "$kotlin_operations" = "$(cat "$kotlin_operations_out")" ] || { echo "Fixture models differ from fixtures. Run scripts/gen_fixtures.sh."; exit 1; }
  for entry in "${snapshots[@]}"; do
    snapshot_of "$entry" | python3 -I -c 'import json, sys; sys.exit(json.load(sys.stdin) != json.load(open(sys.argv[1])))' "$root/${entry%% *}" || { echo "${entry%% *} differs from its declarations. Run scripts/gen_fixtures.sh."; exit 1; }
  done
  echo "Fixture models match."
  exit 0
fi

printf '%s\n' "$swift" > "$swift_out"
printf '%s\n' "$kotlin" > "$kotlin_out"
printf '%s\n' "$swift_operations" > "$swift_operations_out"
printf '%s\n' "$kotlin_operations" > "$kotlin_operations_out"
for entry in "${snapshots[@]}"; do
  target="$root/${entry%% *}"
  fresh="$(snapshot_of "$entry")"
  printf '%s' "$fresh" | python3 -I -c 'import json, sys; sys.exit(json.load(sys.stdin) != json.load(open(sys.argv[1])))' "$target" && continue
  case "$target" in
    *.json) printf '%s' "$fresh" | python3 -I "$root/scripts/format_snapshot.py" > "$target" ;;
    *) printf '%s' "$fresh" > "$target" ;;
  esac
done
