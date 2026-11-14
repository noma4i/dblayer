#!/usr/bin/env bash
set -euo pipefail

root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
module="$root/kotlin"
part="${1:---release}"
abis=(arm64-v8a armeabi-v7a x86_64 x86)
[ "$#" -le 1 ] || { echo "usage: build_kotlin.sh [--host | --jvm | --abis | --release]" >&2; exit 2; }
case "$part" in
  --host | --jvm | --abis) ;;
  --release) abis=(arm64-v8a armeabi-v7a) ;;
  *) echo "usage: build_kotlin.sh [--host | --jvm | --abis | --release]" >&2; exit 2 ;;
esac
jvm_targets=(aarch64-apple-darwin x86_64-apple-darwin)
android_triple() {
  case "$1" in
    arm64-v8a) echo aarch64-linux-android ;;
    armeabi-v7a) echo armv7-linux-androideabi ;;
    x86_64) echo x86_64-linux-android ;;
    x86) echo i686-linux-android ;;
  esac
}
if [ "${DBLAYER_BUILD_TARGETS_ONLY:-0}" = 1 ]; then
  if [ "$part" = --host ]; then printf '%s\n' host; elif [ "$part" = --jvm ]; then printf '%s\n' "${jvm_targets[@]}"; else printf '%s\n' "${abis[@]}"; fi
  exit 0
fi

host_path="$PATH"
export PATH="/opt/homebrew/opt/rustup/bin:$PATH"
export CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-$(nproc 2>/dev/null || sysctl -n hw.ncpu)}" RUST_TEST_THREADS="${RUST_TEST_THREADS:-$(nproc 2>/dev/null || sysctl -n hw.ncpu)}"
if [ "$part" != --host ] && [ "$part" != --jvm ]; then
  export ANDROID_NDK_HOME="${ANDROID_NDK_HOME:-$(ls -d "$HOME"/Library/Android/sdk/ndk/* | sort -V | tail -1)}"
fi

if [ "$part" = --host ]; then
  kind=host
  targets=(host)
  stamp="$root/target/debug/libdblayer.dylib.SOURCE_HASH"
  outputs=("$root/target/debug/libdblayer.dylib")
elif [ "$part" = --jvm ]; then
  kind=jvm
  targets=("${jvm_targets[@]}")
  stamp="$root/target/jvm-build/SOURCE_HASH"
  outputs=("$root/target/jvm-build/libdblayer.dylib")
  for target in "${targets[@]}"; do outputs+=("$root/target/$target/release/libdblayer.dylib"); done
else
  kind=android
  targets=("${abis[@]}")
  stamp="$module/src/main/jniLibs/SOURCE_HASH"
  outputs=()
  for abi in "${abis[@]}"; do outputs+=("$module/src/main/jniLibs/$abi/libdblayer.so"); done
fi
if [ "$part" != --jvm ]; then outputs+=("$module/src/main/kotlin/com/noma4i/dblayer/dblayer.kt"); fi
if bash "$root/scripts/native_source_hash.sh" --fresh "$stamp" "$kind" "${targets[@]}" -- "${outputs[@]}"; then
  if [ "$part" = --jvm ]; then echo "$root/target/jvm-build/libdblayer.dylib"; elif [ "$part" != --host ]; then for abi in "${abis[@]}"; do printf '%s\n' "$module/src/main/jniLibs/$abi/libdblayer.so"; done; fi
  exit 0
fi
hash="$(bash "$root/scripts/native_source_hash.sh" "$kind" "${targets[@]}")"

if [ "$part" = --jvm ]; then
  target_args=()
  for target in "${targets[@]}"; do target_args+=(--target "$target"); done
  cargo build --release --manifest-path "$root/core/Cargo.toml" "${target_args[@]}"
  mkdir -p "$root/target/jvm-build"
  lipo -create "${outputs[1]}" "${outputs[2]}" -output "${outputs[0]}"
  printf '%s\n' "$hash" > "$stamp"
  echo "${outputs[0]}"
  exit 0
fi

if [ "$part" = --host ]; then
  (cd -- "$root" && PATH="$host_path" cargo build -p dblayer)
  library="$root/target/debug/libdblayer.dylib"
fi
PATH="$host_path" bash "$root/scripts/build_bindgen.sh"

if [ "$part" != --host ]; then
  ndk_bin="$ANDROID_NDK_HOME/toolchains/llvm/prebuilt/darwin-x86_64/bin"
  target_args=()
  for abi in "${abis[@]}"; do
    triple="$(android_triple "$abi")"
    clang="$ndk_bin/${triple/armv7-/armv7a-}33-clang"
    export "CARGO_TARGET_$(printf '%s' "$triple" | tr 'a-z-' 'A-Z_')_LINKER=$clang" "CC_${triple//-/_}=$clang" "AR_${triple//-/_}=$ndk_bin/llvm-ar"
    target_args+=(--target "$triple")
  done
  cargo build --release --manifest-path "$root/core/Cargo.toml" "${target_args[@]}"
  /bin/rm -rf "$module/src/main/jniLibs"
  for abi in "${abis[@]}"; do
    mkdir -p "$module/src/main/jniLibs/$abi"
    cp "$root/target/$(android_triple "$abi")/release/libdblayer.so" "$module/src/main/jniLibs/$abi/libdblayer.so"
    printf '%s\n' "$module/src/main/jniLibs/$abi/libdblayer.so"
  done
  library="$root/target/aarch64-linux-android/release/libdblayer.a"
fi

bash "$root/scripts/generate_kotlin_bindings.sh" --bindgen "$root/target/bindgen-bin/uniffi-bindgen" \
  --library "$library" --config "$root/core/uniffi.toml" --out "$module/src/main/kotlin"
printf '%s\n' "$hash" > "$stamp"
