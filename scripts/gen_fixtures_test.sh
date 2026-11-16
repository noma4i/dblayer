#!/usr/bin/env bash
set -euo pipefail

script="${GEN_FIXTURES_SCRIPT:-$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)/gen_fixtures.sh}"
work="$(mktemp -d)"
trap '/bin/rm -rf "$work"' EXIT

root="$work/repo"
mkdir -p "$root/scripts" "$root/bin" "$root/fixtures/operations" "$root/swift/Tests/DBlayerTests" "$root/kotlin/src/test/kotlin/com/noma4i/dblayer/models" "$root/core/src/ffi/selftest" "$root/core/tests/corpus/matrix"
touch "$root/Cargo.toml" "$root/fixtures/a.dbl" "$root/fixtures/operations/a.dbl" "$root/fixtures/operations/schema.graphql"
sed '/^export PATH=/d' "$script" > "$root/scripts/gen_fixtures.sh"
cp "$(dirname "$script")/format_snapshot.py" "$root/scripts/format_snapshot.py"

cat > "$root/bin/cargo" <<'CARGO'
#!/bin/sh
echo "$*" >> "$FAKE_CARGO_LOG"
kind=models
case "$*" in *fixtures/operations/schema.graphql*) kind=operations ;; esac
for arg in "$@"; do
  case "$arg" in
    swift|kotlin) if [ "$kind" = models ]; then echo "$kind-$arg-${FAKE_CARGO_TAG:-v1},\"operations\":[]}"; else echo "$kind-$arg-${FAKE_CARGO_TAG:-v1}"; fi ;;
    spec) echo '{"models":[{"key":"Asset"},{"key":"Chat"},{"key":"Container"},{"key":"CurrentRegion"},{"key":"CurrentUser"},{"key":"MediaEntry"},{"key":"Message"},{"key":"Moment"},{"key":"Notice"},{"key":"Record"},{"key":"User"},{"key":"Vibe"}],"operations":[]}' ;;
  esac
done
CARGO
chmod +x "$root/bin/cargo"

outputs=(
  "$root/swift/Tests/DBlayerTests/FixtureModels.swift"
  "$root/kotlin/src/test/kotlin/com/noma4i/dblayer/models/FixtureModels.kt"
  "$root/swift/Tests/DBlayerTests/FixtureOperations.swift"
  "$root/kotlin/src/test/kotlin/com/noma4i/dblayer/models/FixtureOperations.kt"
)
export FAKE_CARGO_LOG="$work/cargo.log"
fail=0

run() {
  : > "$FAKE_CARGO_LOG"
  set +e
  PATH="$root/bin:$PATH" bash "$root/scripts/gen_fixtures.sh" "$@" > "$work/out" 2> "$work/err"
  code=$?
  set -e
}
check() { echo "gen_fixtures_test: $*" >&2; fail=1; }
snapshot() { cat "${outputs[@]}" 2>/dev/null | cksum; }

seed() { for f in "${outputs[@]}"; do echo seed > "$f"; done; }
for args in "--bogus" "--check extra" "extra --check" "generate" ""; do
  [ -n "$args" ] || args="''"
  seed
  before="$(snapshot)"
  eval "run $args"
  [ "$code" -ne 0 ] || check "args [$args] accepted"
  [ ! -s "$FAKE_CARGO_LOG" ] || check "args [$args] reached cargo"
  [ "$(snapshot)" = "$before" ] || check "args [$args] changed outputs"
  [ -s "$work/err" ] || check "args [$args] gave no stderr"
done

seed
run
[ "$code" -eq 0 ] || check "generation failed"
[ "$(wc -l < "$FAKE_CARGO_LOG" | tr -d ' ')" -eq 10 ] || check "generation did not run cargo 10 times"
grep -qxF 'models-swift-v1,"operations":[]}' "${outputs[0]}" || check "generation did not write outputs"
[ "$(wc -l < "$root/core/tests/corpus/models.json" | tr -d ' ')" -eq 1 ] || check "a .json snapshot is not written in the committed format"
[ "$(wc -l < "$root/core/src/ffi/selftest/wrapper_models.inc" | tr -d ' ')" -eq 0 ] || check "the .inc snapshot is not written compact"

run --check
[ "$code" -eq 0 ] || check "--check failed on matching outputs"
FAKE_CARGO_TAG=v2 run --check
[ "$code" -ne 0 ] || check "--check passed on stale outputs"
echo '{"models":[]}' > "$root/core/tests/corpus/models.json"
run --check
[ "$code" -ne 0 ] || check "--check passed on a stale model snapshot"

for committed in core/tests/corpus/matrix/kinds.spec.json core/tests/corpus/matrix/removed_by.spec.json core/tests/corpus/models.json fixtures/media-marker.json; do
  source_root="$(dirname "$script")/.."
  python3 -I "$root/scripts/format_snapshot.py" < "$source_root/$committed" | cmp -s - "$source_root/$committed" || check "$committed is not what the snapshot writer emits"
done

[ "$fail" -eq 0 ] || exit 1
echo "gen_fixtures_test: ok"
