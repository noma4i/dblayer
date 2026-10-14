#!/usr/bin/env bash
set -euo pipefail

root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
export PATH="/opt/homebrew/opt/rustup/bin:$PATH"
export CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-$(sysctl -n hw.ncpu)}" RUST_TEST_THREADS="${RUST_TEST_THREADS:-$(sysctl -n hw.ncpu)}"
export IPHONEOS_DEPLOYMENT_TARGET=17.0
bridge="$root/swift"
build="$root/target/ios-build"
targets=(aarch64-apple-darwin aarch64-apple-ios aarch64-apple-ios-sim x86_64-apple-ios)
case "${1:-}" in
  --check) targets=(aarch64-apple-darwin aarch64-apple-ios-sim) ;;
  "") ;;
  *) echo "usage: build_ios.sh [--check]" >&2; exit 2 ;;
esac
check=$([ "${1:-}" = --check ] && echo 1 || echo 0)

for target in "${targets[@]}"; do
  cargo build --release --manifest-path "$root/core/Cargo.toml" --target "$target"
done

cargo build --manifest-path "$root/core/Cargo.toml" --features cli
rm -rf "$build" "$bridge/DBlayerFFI.xcframework"
mkdir -p "$build/headers" "$bridge/Sources/DBlayer"

(cd -- "$root/core" && "$root/target/debug/uniffi-bindgen" generate \
  --library "$root/target/debug/libdblayer.dylib" --language swift --out-dir "$build/swift")

cp "$build/swift/dblayerFFI.h" "$build/headers/"
cp "$build/swift/dblayerFFI.modulemap" "$build/headers/module.modulemap"
cp "$build/swift/dblayer.swift" "$bridge/Sources/DBlayer/dblayer.swift"

mkdir -p "$build/simulator"
if [ "$check" = 1 ]; then
  cp "$root/target/aarch64-apple-ios-sim/release/libdblayer.a" "$build/simulator/libdblayer.a"
  libraries=("$root/target/aarch64-apple-darwin/release/libdblayer.a" "$build/simulator/libdblayer.a")
else
  lipo -create "$root/target/aarch64-apple-ios-sim/release/libdblayer.a" "$root/target/x86_64-apple-ios/release/libdblayer.a" \
    -output "$build/simulator/libdblayer.a"
  libraries=("$root/target/aarch64-apple-darwin/release/libdblayer.a" "$root/target/aarch64-apple-ios/release/libdblayer.a" "$build/simulator/libdblayer.a")
fi

arguments=()
for library in "${libraries[@]}"; do
  arguments+=(-library "$library" -headers "$build/headers")
done
xcodebuild -create-xcframework "${arguments[@]}" -output "$bridge/DBlayerFFI.xcframework"
echo "xcframework: $bridge/DBlayerFFI.xcframework"
