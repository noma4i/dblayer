#!/usr/bin/env bash
set -euo pipefail

root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
stamp="$root/kotlin/src/main/jniLibs/SOURCE_HASH"
current="$(bash "$root/scripts/android_source_hash.sh")"
if [[ ! -f "$stamp" || "$(cat "$stamp")" != "$current" ]]; then
  echo "android artifacts are stale: libdblayer.so was not built from the current core sources. Run scripts/build_kotlin.sh" >&2
  exit 1
fi
echo "android artifacts match the core sources"
