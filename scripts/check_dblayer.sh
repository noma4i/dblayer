#!/usr/bin/env bash
set -euo pipefail

root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
export PATH="/opt/homebrew/opt/rustup/bin:$PATH"
export ANDROID_HOME="${ANDROID_HOME:-$HOME/Library/Android/sdk}"
gradle="${GRADLE:-$(ls -d "$HOME"/.gradle/wrapper/dists/gradle-9.*-bin/*/gradle-9.*/bin/gradle | sort -V | tail -1)}"
limit="${STEP_LIMIT_SECONDS:-1200}"

step() {
  timeout "$limit" "$@" || { echo "check_dblayer: step failed or passed $limit s: $*" >&2; return 1; }
}

echo "== rust: cargo test"
step cargo test --manifest-path "$root/core/Cargo.toml"

echo "== ios: xcframework, swift test, iOS simulator compile"
step bash "$root/scripts/build_ios.sh"
step swift test --package-path "$root/swift"
step bash -c 'cd -- "$1/swift" && exec xcodebuild build -scheme DBlayer -destination "generic/platform=iOS Simulator" -derivedDataPath "$1/target/dd-swift" -quiet' _ "$root"

echo "== kotlin: libs for 4 Android ABI, bindings, gradle unit tests"
step bash "$root/scripts/build_kotlin.sh"
step bash "$root/scripts/check_android_artifacts.sh"
step "$gradle" -p "$root/kotlin" testDebugUnitTest --console=plain

echo "== fixtures: generated models match fixtures/*.dbl"
step bash "$root/scripts/gen_fixtures.sh" --check

echo "== zoo: one mechanism per question"
step bash "$root/scripts/check_zoo.sh"

echo "== docs: paths, links, wording"
step bash "$root/scripts/check_docs.sh"

echo "== invariants: traceability report (not strict)"
step bash "$root/scripts/check_invariants.sh"

echo "check_dblayer: ok"
