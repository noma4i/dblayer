#!/usr/bin/env bash
set -euo pipefail

root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
export PATH="/opt/homebrew/opt/rustup/bin:$PATH"
export ANDROID_HOME="${ANDROID_HOME:-$HOME/Library/Android/sdk}"
gradle="${GRADLE:-$(ls -d "$HOME"/.gradle/wrapper/dists/gradle-9.*-bin/*/gradle-9.*/bin/gradle | sort -V | tail -1)}"

echo "== rust: cargo test"
cargo test --manifest-path "$root/core/Cargo.toml"

echo "== ios: xcframework, swift test, iOS simulator compile"
bash "$root/scripts/build_ios.sh"
swift test --package-path "$root/swift"
(cd -- "$root/swift" && xcodebuild build -scheme DBlayer -destination 'generic/platform=iOS Simulator' \
  -derivedDataPath "$root/target/dd-swift" -quiet)

echo "== kotlin: libs for 4 Android ABI, bindings, gradle unit tests"
bash "$root/scripts/build_kotlin.sh"
bash "$root/scripts/check_android_artifacts.sh"
"$gradle" -p "$root/kotlin" testDebugUnitTest --console=plain

echo "== fixtures: generated models match fixtures/*.dbl"
bash "$root/scripts/gen_fixtures.sh" --check

echo "== zoo: one mechanism per question"
bash "$root/scripts/check_zoo.sh"

echo "== docs: paths, links, wording"
bash "$root/scripts/check_docs.sh"

echo "== invariants: traceability report (not strict)"
bash "$root/scripts/check_invariants.sh"

echo "check_dblayer: ok"
