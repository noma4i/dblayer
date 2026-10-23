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
  kind=host
  targets=(host)
  stamp="$root/target/debug/libdblayer.dylib.SOURCE_HASH"
  outputs=("$root/target/debug/libdblayer.dylib")
else
  kind=android
  targets=("${abis[@]}")
  stamp="$module/src/main/jniLibs/SOURCE_HASH"
  outputs=()
  for abi in "${abis[@]}"; do outputs+=("$module/src/main/jniLibs/$abi/libdblayer.so"); done
fi
outputs+=("$module/src/main/kotlin/com/noma4i/dblayer/dblayer.kt")
if bash "$root/scripts/native_source_hash.sh" --fresh "$stamp" "$kind" "${targets[@]}" -- "${outputs[@]}"; then
  [ "$part" = --host ] || for abi in "${abis[@]}"; do printf '%s\n' "$module/src/main/jniLibs/$abi/libdblayer.so"; done
  exit 0
fi
hash="$(bash "$root/scripts/native_source_hash.sh" "$kind" "${targets[@]}")"

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
  (cd -- "$root/core" && cargo ndk "${arguments[@]}" --platform 33 \
    -o "$module/src/main/jniLibs" build --release)
  for abi in "${abis[@]}"; do printf '%s\n' "$module/src/main/jniLibs/$abi/libdblayer.so"; done
  library="$root/target/aarch64-linux-android/release/libdblayer.a"
fi

bash "$root/scripts/generate_kotlin_bindings.sh" --bindgen "$root/target/debug/uniffi-bindgen" \
  --library "$library" --config "$root/core/uniffi.toml" --out "$module/src/main/kotlin"
printf '%s\n' "$hash" > "$stamp"
