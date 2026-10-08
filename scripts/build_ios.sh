#!/usr/bin/env bash
set -euo pipefail

root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
export PATH="/opt/homebrew/opt/rustup/bin:$PATH"
bridge="$root/swift"
build="$root/target/ios-build"
targets=(aarch64-apple-darwin aarch64-apple-ios aarch64-apple-ios-sim)

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

arguments=()
for target in "${targets[@]}"; do
  arguments+=(-library "$root/target/$target/release/libdblayer.a" -headers "$build/headers")
done
xcodebuild -create-xcframework "${arguments[@]}" -output "$bridge/DBlayerFFI.xcframework"
echo "xcframework: $bridge/DBlayerFFI.xcframework"
