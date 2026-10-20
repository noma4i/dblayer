#!/usr/bin/env bash
set -euo pipefail

root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
if [ "$#" -gt 1 ] || { [ "${1:---release}" != --release ] && [ "${1:---release}" != --abis ]; }; then
  echo "usage: check_android_artifacts.sh [--release | --abis]" >&2
  exit 2
fi
abis="$(DBLAYER_BUILD_TARGETS_ONLY=1 bash "$root/scripts/build_kotlin.sh" "${1:---release}")"
while IFS= read -r abi; do
  if [ ! -f "$root/kotlin/src/main/jniLibs/$abi/libdblayer.so" ]; then
    echo "android artifact missing: $abi/libdblayer.so" >&2
    exit 1
  fi
done <<< "$abis"
stamp="$root/kotlin/src/main/jniLibs/SOURCE_HASH"
current="$(bash "$root/scripts/android_source_hash.sh")"
if [[ ! -f "$stamp" || "$(cat "$stamp")" != "$current" ]]; then
  echo "android artifacts are stale: libdblayer.so was not built from the current core sources. Run scripts/build_kotlin.sh" >&2
  exit 1
fi
echo "android artifacts match the core sources"
