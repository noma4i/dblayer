#!/usr/bin/env bash
set -euo pipefail

root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
bridge="$root/swift"
build="$root/target/ios-build"
targets=(aarch64-apple-darwin aarch64-apple-ios aarch64-apple-ios-sim x86_64-apple-darwin x86_64-apple-ios)
[ "$#" -le 1 ] || { echo "usage: build_ios.sh [--check | --device | --ios]" >&2; exit 2; }
case "${1:-}" in
  --check) targets=(aarch64-apple-darwin aarch64-apple-ios-sim) ;;
  --device) targets=(aarch64-apple-ios) ;;
  --ios) targets=(aarch64-apple-ios aarch64-apple-ios-sim) ;;
  "") ;;
  *) echo "usage: build_ios.sh [--check | --device | --ios]" >&2; exit 2 ;;
esac
if [ "${DBLAYER_BUILD_TARGETS_ONLY:-0}" = 1 ]; then
  printf '%s\n' "${targets[@]}"
  exit 0
fi

host_path="$PATH"
export PATH="/opt/homebrew/opt/rustup/bin:$PATH"
export CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-$(nproc 2>/dev/null || sysctl -n hw.ncpu)}" RUST_TEST_THREADS="${RUST_TEST_THREADS:-$(nproc 2>/dev/null || sysctl -n hw.ncpu)}"
export IPHONEOS_DEPLOYMENT_TARGET=17.0
export MACOSX_DEPLOYMENT_TARGET=14.0

stamp="$build/SOURCE_HASH"
outputs=("$bridge/DBlayerFFI.xcframework" "$bridge/Sources/DBlayer/dblayer.swift")
for target in "${targets[@]}"; do outputs+=("$root/target/$target/release/libdblayer.a"); done
if [ "$#" -eq 0 ]; then outputs+=("$build/macos/libdblayer.a" "$build/simulator/libdblayer.a"); fi
if bash "$root/scripts/native_source_hash.sh" --fresh "$stamp" ios "${targets[@]}" -- "${outputs[@]}"; then
  exit 0
fi
hash="$(bash "$root/scripts/native_source_hash.sh" ios "${targets[@]}")"

target_args=()
for target in "${targets[@]}"; do target_args+=(--target "$target"); done
cargo build --release --manifest-path "$root/core/Cargo.toml" "${target_args[@]}"

PATH="$host_path" bash "$root/scripts/build_bindgen.sh"
/bin/rm -rf "$build" "$bridge/DBlayerFFI.xcframework"
mkdir -p "$build/headers" "$bridge/Sources/DBlayer"

(cd -- "$root/core" && "$root/target/debug/uniffi-bindgen" generate \
  --library "$root/target/${targets[0]}/release/libdblayer.a" --language swift --out-dir "$build/swift")

cp "$build/swift/dblayerFFI.h" "$build/headers/"
cp "$build/swift/dblayerFFI.modulemap" "$build/headers/module.modulemap"
cp "$build/swift/dblayer.swift" "$bridge/Sources/DBlayer/dblayer.swift"

arguments=()
if [ "$#" -eq 0 ]; then
  mkdir -p "$build/macos" "$build/simulator"
  lipo -create "$root/target/aarch64-apple-darwin/release/libdblayer.a" "$root/target/x86_64-apple-darwin/release/libdblayer.a" -output "$build/macos/libdblayer.a"
  lipo -create "$root/target/aarch64-apple-ios-sim/release/libdblayer.a" "$root/target/x86_64-apple-ios/release/libdblayer.a" -output "$build/simulator/libdblayer.a"
  libraries=("$build/macos/libdblayer.a" "$root/target/aarch64-apple-ios/release/libdblayer.a" "$build/simulator/libdblayer.a")
else
  libraries=()
  for target in "${targets[@]}"; do libraries+=("$root/target/$target/release/libdblayer.a"); done
fi
for library in "${libraries[@]}"; do arguments+=(-library "$library" -headers "$build/headers"); done
xcodebuild -create-xcframework "${arguments[@]}" -output "$bridge/DBlayerFFI.xcframework"
printf '%s\n' "$hash" > "$stamp"
echo "xcframework: $bridge/DBlayerFFI.xcframework"
