#!/usr/bin/env bash
set -euo pipefail

root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
export PATH="/opt/homebrew/opt/rustup/bin:$PATH"
export ANDROID_NDK_HOME="${ANDROID_NDK_HOME:-$(ls -d "$HOME"/Library/Android/sdk/ndk/* | sort -V | tail -1)}"
out="$root/target/android-libs"

cd -- "$root/core"
cargo ndk -t arm64-v8a -t armeabi-v7a -t x86_64 -t x86 -o "$out" build --release
find "$out" -name 'libdblayer.so' | sort
