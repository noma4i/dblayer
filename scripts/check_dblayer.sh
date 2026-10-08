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

echo "== android: libs for 4 ABI, kotlin bindings, gradle unit tests"
bash "$root/scripts/build_android.sh"
"$gradle" -p "$root/android" testDebugUnitTest --console=plain

echo "check_dblayer: ok"
