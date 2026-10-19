#!/usr/bin/env bash
# The whole DBlayer check. Cheap checks run first, then the cargo, swift, kotlin and android lanes one after another: the check
# holds one build slot command at a time and the first red lane stops the run.
# Every step has a time limit and its process group is killed past it. --fast runs the cheap lane plus the lanes of
# the touched paths. --list prints the steps. --only <lane> runs one lane.
set -uo pipefail

root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
export PATH="/opt/homebrew/opt/rustup/bin:$PATH"
export ANDROID_HOME="${ANDROID_HOME:-$HOME/Library/Android/sdk}"
gradle="${GRADLE:-$(ls -d "$HOME"/.gradle/wrapper/dists/gradle-9.*-bin/*/gradle-9.*/bin/gradle 2> /dev/null | sort -V | tail -1)}"
slot="$HOME/.claude/bin/build-slot -- bash $root/scripts/check_bounded.sh {limit}"
[ -x "$HOME/.claude/bin/build-slot" ] || slot="bash $root/scripts/check_bounded.sh {limit}"

# shellcheck source=check_steps.sh
source "$root/scripts/check_steps.sh"

steps=(
  "cheap|fixtures|120|*|bash scripts/gen_fixtures.sh --check"
  "cheap|zoo|120|*|bash scripts/check_zoo.sh"
  "cheap|protocol-config|120|*|bash scripts/check_protocol_config.sh && bash scripts/check_protocol_config.sh --selftest"
  "cheap|docs|120|*|bash scripts/check_docs.sh"
  "cheap|invariants|120|*|bash scripts/check_invariants.sh"
  "cheap|invariants-selftest|120|*|bash scripts/check_invariants_selftest.sh"
  "cheap|waits|120|*|bash scripts/check_waits.sh"
  "cheap|waits-selftest|120|*|bash scripts/check_waits_test.sh"
  "features|cargo-check-features|600|core/ Cargo.|$slot cargo-step cargo check --manifest-path core/Cargo.toml --all-features --all-targets"
  "cargo|cargo-test-core|600|core/ Cargo.|$slot cargo-step cargo test --manifest-path core/Cargo.toml"
  "cargo|cargo-test-gen|600|gen/ Cargo.|$slot cargo-step cargo test --manifest-path Cargo.toml -p dblayer-gen"
  "swift|ios-slices|600|swift/|$slot bash-step bash scripts/build_ios.sh --check"
  "swift|swift-test|600|swift/|$slot swift-step swift test --package-path swift --enable-code-coverage -Xswiftc -Xllvm -Xswiftc -instrprof-atomic-counter-update-all"
  "swift|ios-sim-compile|600|swift/|cd swift && $slot xcodebuild-step xcodebuild build -scheme DBlayer -destination 'generic/platform=iOS Simulator' ARCHS=arm64 -derivedDataPath ../target/dd-swift.noindex -quiet"
  "kotlin|kotlin-bindings|600|kotlin/|$slot bash-step bash scripts/build_kotlin.sh --host"
  "kotlin|gradle-test|900|kotlin/|$slot gradle-step $gradle -p kotlin testDebugUnitTest --rerun createDebugUnitTestCoverageReport --console=plain --no-watch-fs -Dorg.gradle.daemon.idletimeout=600000 -Dorg.gradle.jvmargs=\"-Xmx1g -XX:MaxMetaspaceSize=512m -XX:+HeapDumpOnOutOfMemoryError -XX:ErrorFile=/tmp/check-owner-\$CHECK_OWNER_ID-%p.log\" -Pkotlin.compiler.execution.strategy=in-process"
  "final|coverage|120|-|bash scripts/check_coverage.sh"
  "ship|android-abis|900|-|$slot bash-step bash scripts/build_kotlin.sh --abis && bash scripts/check_android_artifacts.sh"
  "ship|ios-device-check|600|-|$slot ios-check bash -c 'cargo check --manifest-path core/Cargo.toml --target aarch64-apple-ios && cargo check --manifest-path core/Cargo.toml --target x86_64-apple-ios'"
)

run_checks "$@"
