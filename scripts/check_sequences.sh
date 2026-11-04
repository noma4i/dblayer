#!/usr/bin/env bash
# check_sequences.sh <stage> [--manifest <file>] [--lane core|swift|kotlin]...
# Runs the sequence tests listed in the manifest in the core, Swift and Kotlin lanes against the staged release files.
# Fails on a missing manifest, staged bytes that differ from the built tree, a red lane, or a listed test that did not run and pass.
# Writes <stage>/sequences.tsv (lane, listed, passed, rc, log) for the release receipt.
set -uo pipefail

root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
export PATH="/opt/homebrew/opt/rustup/bin:$PATH"
export LC_ALL=C
fail() { printf 'sequences: %s\n' "$*" >&2; exit 1; }
[ "$#" -ge 1 ] || fail 'usage: check_sequences.sh <stage> [--manifest <file>] [--lane <lane>]...'
stage="$1"
shift
manifest="$root/core/src/ffi/selftest/manifest.tsv"
lanes=()
while [ "$#" -gt 0 ]; do
  case "$1" in
    --manifest) [ "$#" -ge 2 ] || fail '--manifest needs a file'; manifest="$2"; shift 2 ;;
    --lane) [ "$#" -ge 2 ] || fail '--lane needs a name'; lanes+=("$2"); shift 2 ;;
    *) fail "unknown argument: $1" ;;
  esac
done
[ "${#lanes[@]}" -gt 0 ] || lanes=(core swift kotlin)
[ -s "$manifest" ] || fail "sequence manifest missing: $manifest (the selftest sequences must be on this branch)"
[ -d "$stage" ] || fail "stage missing: $stage"
stage="$(cd "$stage" && pwd -P)"
for file in DBlayerFFI.xcframework.zip dblayer.swift dblayer-android.zip; do [ -s "$stage/$file" ] || fail "stage lacks $file"; done
head -1 "$manifest" | grep -Fxq $'version\t1' || fail 'manifest version line must be "version<TAB>1"'
sed -n 2p "$manifest" | grep -Fxq $'id\taliases\tlane\ttest\tstate\ts1\tprovenance' || fail 'manifest header line is not the expected column list'

export BUILD_RUN_ID="${BUILD_RUN_ID:-check-sequences-$$-$(date +%s)}"
slot_tool="${BUILD_SLOT:-$HOME/.claude/bin/build-slot}"
logs="$stage/sequence-logs"
mkdir -p "$logs"
tmp="$(mktemp -d)"
trap '/bin/rm -rf "$tmp"' EXIT
: > "$stage/sequences.tsv"

# bounded <limit> <name> <command...>: the command runs in a build slot when the tool exists, with a step limit inside the slot.
bounded() {
  local limit="$1" name="$2"
  shift 2
  if [ -x "$slot_tool" ]; then
    "$slot_tool" -- bash "$root/scripts/check_bounded.sh" "$limit" "$name" "$@"
  else
    bash "$root/scripts/check_bounded.sh" "$limit" "$name" "$@"
  fi
}

# The tests that ran must be built from the bytes in the stage, not from an older tree.
verify_stage_bytes() {
  mkdir -p "$tmp/ios" "$tmp/android"
  unzip -q "$stage/DBlayerFFI.xcframework.zip" -d "$tmp/ios" || fail 'cannot unzip the staged XCFramework'
  unzip -q "$stage/dblayer-android.zip" -d "$tmp/android" || fail 'cannot unzip the staged Android archive'
  cmp -s "$stage/dblayer.swift" "$root/swift/Sources/DBlayer/dblayer.swift" || fail 'staged dblayer.swift differs from swift/Sources/DBlayer/dblayer.swift'
  diff -rq "$tmp/ios/DBlayerFFI.xcframework" "$root/swift/DBlayerFFI.xcframework" > /dev/null || fail 'staged XCFramework differs from swift/DBlayerFFI.xcframework'
  cmp -s "$tmp/android/src/main/kotlin/com/noma4i/dblayer/dblayer.kt" "$root/kotlin/src/main/kotlin/com/noma4i/dblayer/dblayer.kt" || fail 'staged dblayer.kt differs from the Kotlin tree'
  local abi
  for abi in arm64-v8a armeabi-v7a x86_64 x86; do
    cmp -s "$tmp/android/src/main/jniLibs/$abi/libdblayer.so" "$root/kotlin/src/main/jniLibs/$abi/libdblayer.so" || fail "staged $abi library differs from the Kotlin tree"
  done
}

# Manifest rows of one lane: unique test identifiers, one per line.
lane_tests() { awk -F'\t' -v lane="$1" 'NR > 2 && $3 == lane {print $4}' "$manifest" | LC_ALL=C sort -u; }

record() { printf '%s\t%s\t%s\t%s\t%s\n' "$1" "$2" "$3" "$4" "$5" >> "$stage/sequences.tsv"; }

lane_core() {
  local log="$logs/core.log" filters=() name
  while IFS= read -r name; do filters+=("$name"); done < "$tmp/core.tests"
  bounded "${SEQ_CORE_LIMIT:-900}" sequences-core cargo test --manifest-path "$root/Cargo.toml" -p dblayer --lib -- "${filters[@]}" > "$log" 2>&1
}

lane_swift() {
  local log="$logs/swift.log" filters=() name
  while IFS= read -r name; do filters+=(--filter "${name/./\/}"); done < "$tmp/swift.tests"
  /bin/rm -f "$logs"/swift*.xml
  bounded "${SEQ_SWIFT_LIMIT:-900}" sequences-swift swift test --package-path "$root/swift" --xunit-output "$logs/swift.xml" "${filters[@]}" > "$log" 2>&1
}

lane_kotlin() {
  local log="$logs/kotlin.log" filters=() name gradle
  gradle="${GRADLE:-$(ls -d "$HOME"/.gradle/wrapper/dists/gradle-9.*-bin/*/gradle-9.*/bin/gradle 2> /dev/null | sort -V | tail -1)}"
  [ -x "$gradle" ] || { echo 'Gradle 9 not found: set GRADLE' > "$log"; return 1; }
  while IFS= read -r name; do filters+=(--tests "$name"); done < "$tmp/kotlin.tests"
  /bin/rm -rf "$root/kotlin/build/test-results/testDebugUnitTest"
  bounded "${SEQ_KOTLIN_LIMIT:-900}" sequences-kotlin-bindings bash "$root/scripts/build_kotlin.sh" --host > "$log" 2>&1 || return 1
  bounded "${SEQ_KOTLIN_LIMIT:-900}" sequences-kotlin "$gradle" -p "$root/kotlin" testDebugUnitTest --rerun "${filters[@]}" --console=plain --no-watch-fs >> "$log" 2>&1
}

# Counts the listed tests that ran and passed in the lane log. Prints the count.
count_passed() {
  local lane="$1" log="$logs/$1.log" passed=0 name
  case "$lane" in
    core)
      while IFS= read -r name; do grep -Eq "^test (.*::)?${name} \.\.\. ok$" "$log" && passed=$((passed + 1)); done < "$tmp/core.tests" ;;
    swift)
      passed="$(python3 -I - "$logs" "$tmp/swift.tests" <<'PY'
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

logs, listed = Path(sys.argv[1]), Path(sys.argv[2]).read_text().split()
ok = set()
for xml in logs.glob("swift*.xml"):
    for case in ET.parse(xml).getroot().iter("testcase"):
        if not any(case.find(tag) is not None for tag in ("failure", "error", "skipped")):
            suite = case.get("classname").rsplit(".", 1)[-1]
            ok.add(f"{suite}.{case.get('name').split('(')[0]}")
print(sum(1 for name in listed if name in ok))
PY
)" ;;
    kotlin)
      passed="$(python3 -I - "$root/kotlin/build/test-results/testDebugUnitTest" "$tmp/kotlin.tests" <<'PY'
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

results, listed = Path(sys.argv[1]), Path(sys.argv[2]).read_text().split()
ok = set()
for xml in results.glob("TEST-*.xml"):
    for case in ET.parse(xml).getroot().iter("testcase"):
        if not any(case.find(tag) is not None for tag in ("failure", "error", "skipped")):
            ok.add(f"{case.get('classname')}.{case.get('name').removesuffix('()')}")
print(sum(1 for name in listed if name in ok))
PY
)" ;;
  esac
  printf '%s\n' "$passed"
}

verify_stage_bytes
status=0
for lane in "${lanes[@]}"; do
  case "$lane" in core | swift | kotlin) ;; *) fail "unknown lane: $lane" ;; esac
  lane_tests "$lane" > "$tmp/$lane.tests"
  listed="$(wc -l < "$tmp/$lane.tests" | tr -d ' ')"
  if [ "$listed" = 0 ]; then
    printf 'sequences: lane %s lists no test in the manifest\n' "$lane" >&2
    record "$lane" 0 0 missing "$logs/$lane.log"
    status=1
    continue
  fi
  "lane_$lane"
  rc=$?
  passed="$(count_passed "$lane")"
  record "$lane" "$listed" "$passed" "$rc" "$logs/$lane.log"
  if [ "$rc" != 0 ] || [ "$passed" != "$listed" ]; then
    printf 'sequences: lane %s red: rc=%s, %s of %s listed tests passed (log %s)\n' "$lane" "$rc" "$passed" "$listed" "$logs/$lane.log" >&2
    status=1
  else
    printf 'sequences: lane %s green: %s of %s listed tests passed\n' "$lane" "$passed" "$listed"
  fi
done
exit "$status"
