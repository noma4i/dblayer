#!/usr/bin/env bash
# check_consumers.sh <stage> --version <x.y.z> --maven <dir> --package-swift <file> [--lane ios|android]...
# Runs the iOS and Android app test suites against a local package built from the stage, in parallel, each in a fresh
# worktree of the app repository and with its own library cache (the shared caches are never read as the candidate or written).
#   <stage>           release stage directory (DBlayerFFI.xcframework.zip, dblayer.swift, dblayer-gen-macos-universal)
#   --maven <dir>     staged Maven directory com/noma4i/dblayer/<version> with the AAR, POM and module file
#   --package-swift   the generated release Package.swift (the binary target is rewritten to the unzipped local framework)
# Environment (no machine value is built in; an unset required variable is a clear error):
#   YUPI_V3_REPO          app repository checkout (worktrees are made from the lane refs below)
#   CONSUMER_IOS_REF      app ref the iOS lane tests, default master                      (iOS lane)
#   CONSUMER_ANDROID_REF  app ref the Android lane tests, default master                  (Android lane)
#   CONSUMER_IOS_SIM      shared simulator name for sim-run                              (iOS lane)
#   BRICK_HOST            ssh target of the Android build host                           (Android lane)
#   BRICK_REMOTE_ROOT     absolute library root on that host                             (Android lane)
#   BRICK_LIBRARIES_CACHE cache directory on that host, default .cache/yupi-v3/libraries-dblayer-check
#   CONSUMER_MEDIA_CACHE  read-only source of the MediaLayer cache entries, default ~/.cache/yupi-v3/libraries
#   CONSUMER_WORK_ROOT    scratch parent for trees and caches, default $TMPDIR
#   SIM_RUN, BUILD_SLOT   tool paths, default ~/.claude/bin/sim-run and ~/.claude/bin/build-slot
#   CONSUMER_IOS_BUILD_LIMIT 1200, CONSUMER_IOS_TEST_LIMIT 600, BRICK_GRADLE_TIMEOUT 1500 (seconds)
# Writes <stage>/consumers.tsv (lane, rc, log) and exits 0 only when every requested lane is green.
set -uo pipefail

export LC_ALL=C
root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
fail() { printf 'consumers: %s\n' "$*" >&2; exit 1; }
[ "$#" -ge 1 ] || fail 'usage: check_consumers.sh <stage> --version <x.y.z> --maven <dir> --package-swift <file> [--lane ios|android]...'
stage="$1"
shift
version="" maven="" package_swift="" lanes=()
while [ "$#" -gt 0 ]; do
  [ "$#" -ge 2 ] || fail "$1 needs a value"
  case "$1" in
    --version) version="$2" ;;
    --maven) maven="$2" ;;
    --package-swift) package_swift="$2" ;;
    --lane) lanes+=("$2") ;;
    *) fail "unknown argument: $1" ;;
  esac
  shift 2
done
[ "${#lanes[@]}" -gt 0 ] || lanes=(ios android)
for lane in "${lanes[@]}"; do case "$lane" in ios | android) ;; *) fail "unknown lane: $lane" ;; esac; done
[[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || fail '--version x.y.z is required'
[ -d "$stage" ] || fail "stage missing: $stage"
stage="$(cd "$stage" && pwd -P)"
[ -d "$maven" ] || fail "--maven directory missing: $maven"
maven="$(cd "$maven" && pwd -P)"
[ -s "$package_swift" ] || fail "--package-swift file missing: $package_swift"
for file in DBlayerFFI.xcframework.zip dblayer.swift dblayer-gen-macos-universal; do [ -s "$stage/$file" ] || fail "stage lacks $file"; done
for file in "dblayer-$version.aar" "dblayer-$version.pom" "dblayer-$version.module"; do [ -s "$maven/$file" ] || fail "Maven directory lacks $file"; done

# Every missing input is listed at once.
missing=()
need() { [ -n "${!1:-}" ] || missing+=("$1 ($2)"); }
need YUPI_V3_REPO 'app repository checkout'
for lane in "${lanes[@]}"; do
  case "$lane" in
    ios) need CONSUMER_IOS_SIM 'simulator name for sim-run' ;;
    android) need BRICK_HOST 'ssh target of the Android build host'; need BRICK_REMOTE_ROOT 'absolute library root on that host' ;;
  esac
done
[ "${#missing[@]}" = 0 ] || fail "unset inputs: ${missing[*]}"
[ -d "$YUPI_V3_REPO/.git" ] || [ -f "$YUPI_V3_REPO/.git" ] || fail "YUPI_V3_REPO is not a git checkout: $YUPI_V3_REPO"

sim_run="${SIM_RUN:-$HOME/.claude/bin/sim-run}"
slot_tool="${BUILD_SLOT:-$HOME/.claude/bin/build-slot}"
ios_ref="${CONSUMER_IOS_REF:-master}"
android_ref="${CONSUMER_ANDROID_REF:-master}"
media_cache="${CONSUMER_MEDIA_CACHE:-$HOME/.cache/yupi-v3/libraries}"
brick_cache="${BRICK_LIBRARIES_CACHE:-.cache/yupi-v3/libraries-dblayer-check}"
case "$brick_cache" in */libraries | .cache/yupi-v3/libraries | *..*) fail "BRICK_LIBRARIES_CACHE must be a dedicated directory, not a shared cache: $brick_cache" ;; esac
ios_build_limit="${CONSUMER_IOS_BUILD_LIMIT:-1200}"
ios_test_limit="${CONSUMER_IOS_TEST_LIMIT:-600}"
gradle_limit="${BRICK_GRADLE_TIMEOUT:-1500}"
export BUILD_RUN_ID="${BUILD_RUN_ID:-check-consumers-$$-$(date +%s)}"

work="$(mktemp -d "${CONSUMER_WORK_ROOT:-${TMPDIR:-/tmp}}/dblayer-consumers.XXXXXX")"
logs="$stage/consumer-logs"
mkdir -p "$logs"
: > "$stage/consumers.tsv"
trees=("$work/ios-tree" "$work/android-tree")
cleanup() {
  local tree
  for tree in "${trees[@]}"; do git -C "$YUPI_V3_REPO" worktree remove --force "$tree" > /dev/null 2>&1 || true; done
  git -C "$YUPI_V3_REPO" worktree prune > /dev/null 2>&1 || true
  /bin/rm -rf "$work"
}
trap cleanup EXIT

# Local package: unzipped framework, generated bindings, tracked sources of the future release, local Package.swift.
package="$work/dblayer"
mkdir -p "$package/swift/Sources"
unzip -q "$stage/DBlayerFFI.xcframework.zip" -d "$package" || fail 'cannot unzip the staged XCFramework'
git -C "$root" archive HEAD core gen Cargo.toml Cargo.lock swift/Sources | tar -x -C "$package" || fail 'cannot export the release sources'
cp "$stage/dblayer.swift" "$package/swift/Sources/DBlayer/dblayer.swift"
python3 -I - "$package_swift" "$package/Package.swift" <<'PY' || fail 'cannot rewrite the binary target of Package.swift'
import re, sys
text = open(sys.argv[1]).read()
text, count = re.subn(r'url: "[^"]+",\s*checksum: "[^"]+"', 'path: "DBlayerFFI.xcframework"', text)
assert count == 1, "binaryTarget url not found"
open(sys.argv[2], "w").write(text)
PY
: > "$package/.fetched-$version"

new_tree() {
  local tree="$work/$1" ref="$2"
  git -C "$YUPI_V3_REPO" rev-parse --verify --quiet "$ref^{commit}" > /dev/null || { echo "app ref not found in $YUPI_V3_REPO: $ref" >&2; return 1; }
  git -C "$YUPI_V3_REPO" worktree add --quiet --detach "$tree" "$ref" || return 1
  printf '%s\n' "$tree"
}

in_slot() {
  if [ -x "$slot_tool" ]; then "$slot_tool" -- "$@"; else "$@"; fi
}

lane_ios() {
  local log="$logs/ios.log" tree
  [ -x "$sim_run" ] || { echo "sim-run not found: $sim_run (set SIM_RUN)" > "$log"; return 1; }
  tree="$(new_tree ios-tree "$ios_ref" 2> "$log")" || { echo "cannot make the iOS worktree from $ios_ref" >> "$log"; return 1; }
  {
    [ -f "$tree/ios/scripts/dblayer_path.sh" ] || { echo "the app ref $ios_ref lacks YUPI_DBLAYER_PATH support (ios/scripts/dblayer_path.sh)"; return 1; }
    [ ! -f "$YUPI_V3_REPO/ios/Configs/Secrets.xcconfig" ] || cp "$YUPI_V3_REPO/ios/Configs/Secrets.xcconfig" "$tree/ios/Configs/"
    (cd "$tree" && YUPI_DBLAYER_PATH="$package" bash ios/scripts/bootstrap.sh) || return 1
    grep -Fq "$package" "$tree/ios/Yupi.xcodeproj/project.pbxproj" || { echo "the generated project does not name the local package $package"; return 1; }
    (cd "$tree" && in_slot "$sim_run" "$CONSUMER_IOS_SIM" -- timeout --kill-after=10 "$ios_build_limit" xcodebuild build-for-testing -project ios/Yupi.xcodeproj -scheme YupiLogicTests -destination 'id=@UDID@' -derivedDataPath .orca-artifacts/DD.noindex) || return 1
    (cd "$tree" && in_slot "$sim_run" "$CONSUMER_IOS_SIM" -- env RUN_REASON="release check $version" timeout --kill-after=10 "$ios_test_limit" bash ios/scripts/run-unit-tests.sh --ios '@UDID@') || return 1
  } > "$log" 2>&1
}

lane_android() {
  local log="$logs/android.log" tree cache mirror remote_log waited=0 out
  tree="$(new_tree android-tree "$android_ref" 2> "$log")" || { echo "cannot make the Android worktree from $android_ref" >> "$log"; return 1; }
  cache="$work/android-cache"
  mirror="dblayer-check-$$"
  remote_log="$mirror.log"
  {
    grep -q BRICK_LIBRARIES_CACHE "$tree/android/scripts/brick.sh" || { echo "the app ref $android_ref lacks BRICK_LIBRARIES_CACHE support in android/scripts/brick.sh"; return 1; }
    mkdir -p "$cache/dblayer-maven/com/noma4i/dblayer" "$cache/dblayer-gen"
    mkdir -p "$cache/dblayer" && cp -R "$package/." "$cache/dblayer/"
    cp -R "$maven" "$cache/dblayer-maven/com/noma4i/dblayer/$version"
    git -C "$cache/dblayer-maven" init -q
    cp "$stage/dblayer-gen-macos-universal" "$cache/dblayer-gen/dblayer-gen"
    : > "$cache/dblayer-gen/.version-$version"
    local entry
    for entry in medialayer medialayer-maven medialayer-gen; do
      [ -d "$media_cache/$entry" ] || { echo "MediaLayer cache entry missing: $media_cache/$entry (set CONSUMER_MEDIA_CACHE)"; return 1; }
      cp -R "$media_cache/$entry" "$cache/$entry"
    done
    sed -i.bak "s/^DBLAYER=.*/DBLAYER=$version/" "$tree/libraries.versions" && /bin/rm -f "$tree/libraries.versions.bak"
    export BRICK_HOST BRICK_REMOTE_ROOT YUPI_LIBRARIES_CACHE="$cache" BRICK_LIBRARIES_CACHE="$brick_cache" BRICK_GRADLE_TIMEOUT="$gradle_limit"
    ssh "$BRICK_HOST" "rm -f $BRICK_REMOTE_ROOT/$remote_log" || return 1
    (cd "$tree" && in_slot bash android/scripts/brick.sh sync "$mirror") || return 1
    (cd "$tree" && in_slot bash android/scripts/brick.sh gradle "$mirror" "$remote_log" :app:testDebugUnitTest :app:testDebugNativeSandboxUnitTest -PyupiV3ServerKey=stub -PyupiV3ServerPin=stub --continue) || return 1
    until ssh "$BRICK_HOST" "grep -q '^rc=' $BRICK_REMOTE_ROOT/$remote_log" 2> /dev/null; do
      waited=$((waited + 15))
      [ "$waited" -le $((gradle_limit + 300)) ] || { echo "TIMEOUT $((gradle_limit + 300)) waiting for rc= in $remote_log"; return 1; }
      sleep 15
    done
    out="$(cd "$tree" && bash android/scripts/brick.sh result "$remote_log" "$mirror")" || return 1
    printf '%s\n' "$out"
    printf '%s\n' "$out" | grep -Fxq 'rc=0' || { echo 'Gradle did not finish with rc=0'; return 1; }
    printf '%s\n' "$out" | grep -q '^CACHED-RUN' && { echo 'Gradle reused cached test results: no test executed against the candidate'; return 1; }
    printf '%s\n' "$out" | grep -Eq '^jvm tests: [1-9][0-9]* \(failures and errors: 0 0\)$' || { echo 'the result shows no executed JVM tests, or failures'; return 1; }
    ssh "$BRICK_HOST" "grep '^> Task :app:testDebugNativeSandboxUnitTest' $BRICK_REMOTE_ROOT/$remote_log" | grep -Evq 'NO-SOURCE|UP-TO-DATE|FROM-CACHE|SKIPPED' || { echo 'the native sandbox task did not execute'; return 1; }
  } > "$log" 2>&1
  local rc=$?
  ssh "$BRICK_HOST" "rm -rf $BRICK_REMOTE_ROOT/$mirror $BRICK_REMOTE_ROOT/$remote_log $brick_cache" > /dev/null 2>&1 || true
  return "$rc"
}

pids=()
for lane in "${lanes[@]}"; do
  ( "lane_$lane"; echo "$?" > "$work/$lane.rc" ) &
  pids+=("$!")
done
wait "${pids[@]}"
status=0
for lane in "${lanes[@]}"; do
  rc="$(cat "$work/$lane.rc" 2> /dev/null || echo missing)"
  printf '%s\t%s\t%s\n' "$lane" "$rc" "$logs/$lane.log" >> "$stage/consumers.tsv"
  if [ "$rc" = 0 ]; then
    printf 'consumers: lane %s green\n' "$lane"
  else
    printf 'consumers: lane %s red: rc=%s (log %s)\n' "$lane" "$rc" "$logs/$lane.log" >&2
    status=1
  fi
done
exit "$status"
