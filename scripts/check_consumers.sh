#!/usr/bin/env bash
# check_consumers.sh <stage> --version <x.y.z> --maven <dir> --package-swift <file> [--lane ios|android]...
# Runs the consumer test commands against a local package built from the stage, in parallel, each in a fresh worktree of the
# consumer repository. The commands live in the consumer repository, this script only prepares the package and the trees.
#   <stage>           release stage directory (DBlayerFFI.xcframework.zip, dblayer.swift, dblayer-gen-macos-universal)
#   --maven <dir>     staged Maven directory com/noma4i/dblayer/<version> with the AAR, POM and module file
#   --package-swift   the generated release Package.swift (the binary target is rewritten to the unzipped local framework)
# Environment (no machine value is built in; an unset required variable is a clear error):
#   CONSUMER_REPO         consumer repository checkout (worktrees are made from the lane refs below)
#   CONSUMER_IOS_REF      ref the iOS lane tests, default master
#   CONSUMER_ANDROID_REF  ref the Android lane tests, default master
#   CONSUMER_IOS_COMMAND      command of the iOS lane, run in its worktree        (iOS lane)
#   CONSUMER_ANDROID_COMMAND  command of the Android lane, run in its worktree    (Android lane)
#   CONSUMER_WORK_ROOT    scratch parent for trees, default $TMPDIR
#   BUILD_SLOT            tool path of a wrapper that serialises builds, default none
# A lane command receives DBLAYER_PACKAGE (local Swift package), DBLAYER_MAVEN (staged Maven directory),
# DBLAYER_GEN (staged generator), DBLAYER_VERSION and DBLAYER_STAGE. Only exit code 0 is a pass.
# Writes <stage>/consumers.tsv (lane, rc, log, source sha, run id) and exits 0 only when every requested lane is green.
set -uo pipefail

export LC_ALL=C
root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
fail() { printf 'consumers: %s\n' "$*" >&2; exit 1; }
source_sha="${RELEASE_SOURCE_SHA:-$(git -C "$root" rev-parse HEAD 2> /dev/null || echo unknown)}"
run_id="${RELEASE_RUN_ID:-standalone}"
[ "$#" -ge 1 ] || fail 'usage: check_consumers.sh <stage> --version <x.y.z> --maven <dir> --package-swift <file> [--lane ios|android]...'
stage="$1"
shift
version="" maven="" package_swift="" lanes=()
lane_limit="${CONSUMER_LANE_LIMIT:-300}"
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
need CONSUMER_REPO 'consumer repository checkout'
for lane in "${lanes[@]}"; do
  case "$lane" in
    ios) need CONSUMER_IOS_COMMAND 'command of the iOS lane' ;;
    android) need CONSUMER_ANDROID_COMMAND 'command of the Android lane' ;;
  esac
done
[ "${#missing[@]}" = 0 ] || fail "unset inputs: ${missing[*]}"
[ -d "$CONSUMER_REPO/.git" ] || [ -f "$CONSUMER_REPO/.git" ] || fail "CONSUMER_REPO is not a git checkout: $CONSUMER_REPO"

slot_tool="${BUILD_SLOT:-}"
ios_ref="${CONSUMER_IOS_REF:-master}"
android_ref="${CONSUMER_ANDROID_REF:-master}"
export BUILD_RUN_ID="${BUILD_RUN_ID:-check-consumers-$$-$(date +%s)}"

work="$(mktemp -d "${CONSUMER_WORK_ROOT:-${TMPDIR:-/tmp}}/dblayer-consumers.XXXXXX")"
logs="$stage/consumer-logs"
mkdir -p "$logs"
: > "$stage/consumers.tsv"
trees=("$work/ios-tree" "$work/android-tree")
cleanup() {
  local tree
  for tree in "${trees[@]}"; do git -C "$CONSUMER_REPO" worktree remove --force "$tree" > /dev/null 2>&1 || true; done
  git -C "$CONSUMER_REPO" worktree prune > /dev/null 2>&1 || true
  /bin/rm -rf "$work"
}
trap cleanup EXIT

# Local package: unzipped framework, generated bindings, tracked sources of the future release, local Package.swift.
package="$work/dblayer"
mkdir -p "$package/swift/Sources"
unzip -q "$stage/DBlayerFFI.xcframework.zip" -d "$package" || fail 'cannot unzip the staged XCFramework'
git -C "$root" archive HEAD core gen Cargo.toml Cargo.lock swift/Sources | tar -x -C "$package" || fail 'cannot export the release sources'
cp "$stage/dblayer.swift" "$package/swift/Sources/DBlayer/dblayer.swift"
target_name="$(python3 -I - "$package_swift" "$package/Package.swift" <<'PY'
import re, sys
text = open(sys.argv[1]).read()
pattern = r'(\.binaryTarget\(\s*name:\s*"([^"]+)",\s*)url: "[^"]+",\s*checksum: "[^"]+"'
match = re.search(pattern, text)
assert match, "binaryTarget not found"
text = text.replace(match.group(0), match.group(1) + 'path: "%s.xcframework"' % match.group(2), 1)
open(sys.argv[2], "w").write(text)
print(match.group(2))
PY
)" || fail 'cannot rewrite the binary target of Package.swift'
mv "$package/DBlayerFFI.xcframework" "$package/.unzipped-xcframework" && mv "$package/.unzipped-xcframework" "$package/$target_name.xcframework" || fail 'cannot name the XCFramework after its binary target'
: > "$package/.fetched-$version"

new_tree() {
  local tree="$work/$1" ref="$2"
  git -C "$CONSUMER_REPO" rev-parse --verify --quiet "$ref^{commit}" > /dev/null || { echo "ref not found in $CONSUMER_REPO: $ref" >&2; return 1; }
  git -C "$CONSUMER_REPO" worktree add --quiet --detach "$tree" "$ref" || return 1
  printf '%s\n' "$tree"
}

in_slot() {
  if [ -x "$slot_tool" ]; then "$slot_tool" -- "$@"; else "$@"; fi
}

run_lane() {
  local lane="$1" ref="$2" command="$3" log="$logs/$1.log" tree code=0
  tree="$(new_tree "$lane-tree" "$ref" 2> "$log")" || { echo "cannot make the $lane worktree from $ref" >> "$log"; return 1; }
  DBLAYER_PACKAGE="$package" DBLAYER_MAVEN="$maven" DBLAYER_GEN="$stage/dblayer-gen-macos-universal" DBLAYER_VERSION="$version" DBLAYER_STAGE="$stage" \
    in_slot timeout --kill-after=10 "$lane_limit" bash -c "git -C \"$tree\" rev-parse HEAD > /dev/null && cd \"$tree\" && $command" >> "$log" 2>&1 || code=$?
  [ "$code" -ne 124 ] || echo "TIMEOUT $lane_limit lane $lane" >> "$log"
  return "$code"
}

lane_ios() { run_lane ios "$ios_ref" "$CONSUMER_IOS_COMMAND"; }
lane_android() { run_lane android "$android_ref" "$CONSUMER_ANDROID_COMMAND"; }

pids=()
for lane in "${lanes[@]}"; do
  ( "lane_$lane"; echo "$?" > "$work/$lane.rc" ) &
  pids+=("$!")
done
wait "${pids[@]}"
status=0
: > "$stage/consumers.tsv" || fail 'cannot write consumers.tsv'
for lane in "${lanes[@]}"; do
  rc="$(cat "$work/$lane.rc" 2> /dev/null || echo missing)"
  printf '%s\t%s\t%s\t%s\t%s\n' "$lane" "$rc" "$logs/$lane.log" "$source_sha" "$run_id" >> "$stage/consumers.tsv" || fail 'cannot write consumers.tsv'
  if [ "$rc" = 0 ]; then
    printf 'consumers: lane %s green\n' "$lane"
  else
    printf 'consumers: lane %s red: rc=%s (log %s)\n' "$lane" "$rc" "$logs/$lane.log" >&2
    status=1
  fi
done
exit "$status"
