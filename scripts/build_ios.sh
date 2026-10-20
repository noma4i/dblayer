#!/usr/bin/env bash
set -euo pipefail

root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
bridge="$root/swift"
build="$root/target/ios-build"
targets=(aarch64-apple-darwin aarch64-apple-ios aarch64-apple-ios-sim)
[ "$#" -le 1 ] || { echo "usage: build_ios.sh [--check | --device]" >&2; exit 2; }
case "${1:-}" in
  --check) targets=(aarch64-apple-darwin aarch64-apple-ios-sim) ;;
  --device) targets=(aarch64-apple-ios) ;;
  "") ;;
  *) echo "usage: build_ios.sh [--check | --device]" >&2; exit 2 ;;
esac
if [ "${DBLAYER_BUILD_TARGETS_ONLY:-0}" = 1 ]; then
  printf '%s\n' "${targets[@]}"
  exit 0
fi

export PATH="/opt/homebrew/opt/rustup/bin:$PATH"
export CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-$(nproc 2>/dev/null || sysctl -n hw.ncpu)}" RUST_TEST_THREADS="${RUST_TEST_THREADS:-$(nproc 2>/dev/null || sysctl -n hw.ncpu)}"
export IPHONEOS_DEPLOYMENT_TARGET=17.0

for target in "${targets[@]}"; do
  cargo build --release --manifest-path "$root/core/Cargo.toml" --target "$target"
done

bash "$root/scripts/build_bindgen.sh"
rm -rf "$build" "$bridge/DBlayerFFI.xcframework"
mkdir -p "$build/headers" "$bridge/Sources/DBlayer"

(cd -- "$root/core" && "$root/target/debug/uniffi-bindgen" generate \
  --library "$root/target/${targets[0]}/release/libdblayer.a" --language swift --out-dir "$build/swift")

cp "$build/swift/dblayerFFI.h" "$build/headers/"
cp "$build/swift/dblayerFFI.modulemap" "$build/headers/module.modulemap"
cp "$build/swift/dblayer.swift" "$bridge/Sources/DBlayer/dblayer.swift"

arguments=()
for target in "${targets[@]}"; do
  arguments+=(-library "$root/target/$target/release/libdblayer.a" -headers "$build/headers")
done
xcodebuild -create-xcframework "${arguments[@]}" -output "$bridge/DBlayerFFI.xcframework"
echo "xcframework: $bridge/DBlayerFFI.xcframework"
