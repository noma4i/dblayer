#!/usr/bin/env bash
# Cheap checks precede sequential builds, each with a time limit.
set -uo pipefail

root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
export ANDROID_HOME="${ANDROID_HOME:-$HOME/Library/Android/sdk}"
export BUILD_RUN_ID="${BUILD_RUN_ID:-check-dblayer-$$-$(date +%s)}"
gradle="${GRADLE:-$(ls -d "$HOME"/.gradle/wrapper/dists/gradle-9.*-bin/*/gradle-9.*/bin/gradle 2> /dev/null | sort -V | tail -1)}"
slot="bash $root/scripts/check_bounded.sh {limit}"
[ -n "${BUILD_SLOT:-}" ] && [ -x "$BUILD_SLOT" ] && slot="$BUILD_SLOT -- $slot"

source "$root/scripts/check_steps.sh"

steps=(
  "cheap|zoo|120|*|bash scripts/check_zoo.sh"
  "cheap|zoo-selftest|120|*|bash scripts/check_zoo_test.sh"
  "cheap|protocol-config|120|*|bash scripts/check_protocol_config.sh && bash scripts/check_protocol_config.sh --selftest"
  "cheap|build-modes|120|*|bash scripts/check_build_modes.sh"
  "cheap|release-modes|120|*|bash scripts/check_release_modes.sh"
  "cheap|testing-doubles|120|swift/ kotlin/|bash scripts/check_testing_doubles.sh && bash scripts/check_testing_doubles.sh --selftest"
  "cheap|docs|120|*|bash scripts/check_docs.sh"
  "cheap|waits|120|*|bash scripts/check_waits.sh"
  "cheap|waits-selftest|120|*|bash scripts/check_waits_test.sh"
  "cheap|steps-selftest|120|*|bash scripts/check_steps_test.sh"
  "cheap|check-lock-selftest|120|*|bash scripts/check_lock_test.sh"
  "cheap|check-logs-selftest|120|*|bash scripts/check_logs_test.sh"
  "cheap|record-parity|120|core/ specs/ scripts/|python3 -I scripts/check_record_parity.py"
  "cheap|record-parity-selftest|120|*|python3 -I scripts/check_record_parity_test.py"
  "cheap|projection-parity-selftest|120|*|python3 -I scripts/check_projection_parity_test.py"
  "cheap|artifact-provenance|120|scripts/ core/ swift/ kotlin/|bash scripts/check_artifact_provenance.sh"
  "cheap|artifact-provenance-selftest|120|*|bash scripts/check_artifact_provenance.sh --selftest"
  "cheap|silent|120|*|bash scripts/check_silent.sh"
  "cheap|silent-selftest|120|*|bash scripts/check_silent_test.sh"
  "cheap|gen-decisions|120|*|bash scripts/check_gen_decisions.sh"
  "cheap|gen-decisions-selftest|120|*|bash scripts/check_gen_decisions_test.sh"
  "cheap|coverage-selftest|120|*|python3 -I scripts/check_coverage_test.py"
  "cheap|reclaim-selftest|120|*|bash scripts/reclaim_target_test.sh"
  "cheap|docs-selftest|120|*|python3 -I scripts/check_docs_test.py"
  "cheap|markers-selftest|120|*|python3 -I scripts/check_markers_test.py"
  "cheap|android-artifacts-selftest|120|*|python3 -I scripts/check_android_artifacts_test.py"
  "cheap|consumers-selftest|120|*|python3 -I scripts/check_consumers_test.py"
  "cheap|sequences-selftest|120|*|python3 -I scripts/check_sequences_test.py"
  "cheap|bounded-selftest|120|*|python3 -I scripts/check_bounded_test.py"
  "cargo|cargo-test-core|300|core/ Cargo.|$slot cargo-step cargo test -p dblayer"
  "cargo|cargo-test-gen|300|gen/ Cargo.|$slot cargo-step cargo test -p dblayer-gen"
  "cargo|cargo-test-bindgen|300|bindgen/ Cargo.|$slot cargo-step cargo test -p dblayer-bindgen"
  "cargo|fixtures|120|core/ gen/ Cargo. fixtures/|bash scripts/gen_fixtures.sh --check"
  "cargo|fixtures-args|120|core/ gen/ Cargo. fixtures/|bash scripts/gen_fixtures_test.sh"
  "cargo|consumer-corpus|120|core/ gen/ Cargo. fixtures/|bash scripts/gen_consumer_corpus.sh --check"
  "swift|ios-slices|600|swift/|$slot bash-step bash scripts/build_ios.sh --check"
  "swift|swift-test|300|swift/|$slot swift-step swift test --package-path swift --no-parallel --enable-code-coverage -Xswiftc -Xllvm -Xswiftc -instrprof-atomic-counter-update-all"
  "swift|ios-sim-compile|600|swift/|cd swift && $slot xcodebuild-step xcodebuild build -scheme DBlayer -destination 'generic/platform=iOS Simulator' ARCHS=arm64 -derivedDataPath ../target/dd-swift.noindex -quiet"
  "kotlin|kotlin-bindings|600|kotlin/|$slot bash-step bash scripts/build_kotlin.sh --host"
  "kotlin|projection-parity|120|core/ specs/ scripts/ swift/ kotlin/|python3 -I scripts/check_projection_parity.py"
  "kotlin|gradle-r8|300|kotlin/|$slot gradle-step $gradle -p kotlin/r8-check probe --console=plain --no-watch-fs -Dorg.gradle.jvmargs=\"-Xmx1g -XX:MaxMetaspaceSize=768m\" -Pkotlin.compiler.execution.strategy=in-process"
  "kotlin|gradle-test|300|kotlin/|$slot gradle-step $gradle -p kotlin testDebugUnitTest :consumer-check:compileDebugKotlin --rerun createDebugUnitTestCoverageReport --console=plain --no-watch-fs -Dorg.gradle.daemon.idletimeout=600000 -Dorg.gradle.jvmargs=\"-Xmx1g -XX:MaxMetaspaceSize=512m -XX:+HeapDumpOnOutOfMemoryError -XX:ErrorFile=/tmp/check-owner-\$CHECK_OWNER_ID-%p.log\" -Pkotlin.compiler.execution.strategy=in-process"
  "final|coverage|120|-|bash scripts/check_coverage.sh"
  "ship|android-abis|900|-|$slot bash-step bash scripts/build_kotlin.sh --release && bash scripts/check_android_artifacts.sh --release"
  "ship|ios-device-check|600|-|bash scripts/native_source_hash.sh --fresh target/ios-device-check.SOURCE_HASH ios-device-check aarch64-apple-ios || { PATH=/opt/homebrew/opt/rustup/bin:\$PATH $slot ios-check cargo check --manifest-path core/Cargo.toml --target aarch64-apple-ios && bash scripts/native_source_hash.sh ios-device-check aarch64-apple-ios > target/ios-device-check.SOURCE_HASH; }"
)

step_artifacts() {
  case "$1" in
    gradle-test) printf '%s\n' kotlin/build/test-results/testDebugUnitTest kotlin/build/reports/tests/testDebugUnitTest ;;
  esac
}

if [ "${BASH_SOURCE[0]}" = "$0" ]; then
  cap=300
  case " $* " in *" --only "*) cap=1500 ;; esac
  run_bounded "$cap" check-dblayer run_checks "$@"
fi
