#!/usr/bin/env bash
set -euo pipefail

root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
module="$root/kotlin"
part="${1:---release}"
abis=(arm64-v8a armeabi-v7a x86_64 x86)
[ "$#" -le 1 ] || { echo "usage: build_kotlin.sh [--host | --abis | --release]" >&2; exit 2; }
case "$part" in
  --host | --abis) ;;
  --release) abis=(arm64-v8a armeabi-v7a) ;;
  *) echo "usage: build_kotlin.sh [--host | --abis | --release]" >&2; exit 2 ;;
esac
if [ "${DBLAYER_BUILD_TARGETS_ONLY:-0}" = 1 ]; then
  if [ "$part" = --host ]; then printf '%s\n' host; else printf '%s\n' "${abis[@]}"; fi
  exit 0
fi

export PATH="/opt/homebrew/opt/rustup/bin:$PATH"
export CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-$(nproc 2>/dev/null || sysctl -n hw.ncpu)}" RUST_TEST_THREADS="${RUST_TEST_THREADS:-$(nproc 2>/dev/null || sysctl -n hw.ncpu)}"
export ANDROID_NDK_HOME="${ANDROID_NDK_HOME:-$(ls -d "$HOME"/Library/Android/sdk/ndk/* | sort -V | tail -1)}"

if [ "$part" = --host ]; then
  cargo build --manifest-path "$root/core/Cargo.toml" --features cli
  library="$root/target/debug/libdblayer.dylib"
else
  bash "$root/scripts/build_bindgen.sh"
fi

if [ "$part" != --host ]; then
  arguments=()
  for abi in "${abis[@]}"; do arguments+=(-t "$abi"); done
  rm -rf "$module/src/main/jniLibs"
  (cd -- "$root/core" && cargo ndk "${arguments[@]}" \
    -o "$module/src/main/jniLibs" build --release)
  bash "$root/scripts/android_source_hash.sh" > "$module/src/main/jniLibs/SOURCE_HASH"
  for abi in "${abis[@]}"; do printf '%s\n' "$module/src/main/jniLibs/$abi/libdblayer.so"; done
  library="$root/target/aarch64-linux-android/release/libdblayer.a"
fi

(cd -- "$root/core" && "$root/target/debug/uniffi-bindgen" generate \
  --library "$library" --language kotlin --no-format --out-dir "$module/src/main/kotlin")
