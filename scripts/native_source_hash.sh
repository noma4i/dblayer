#!/usr/bin/env bash
# native_source_hash.sh <kind> [target...] prints the fingerprint of everything a native build reads.
# native_source_hash.sh --fresh <stamp> <kind> [target...] [-- <artifact>...] exits 0 and prints the skip line
# when the stamp holds the current fingerprint and every artifact exists.
set -euo pipefail

root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd -- "$root"
stamp=""
if [ "${1:-}" = --fresh ]; then
  stamp="${2:?--fresh needs a stamp file}"
  shift 2
fi
args=()
artifacts=()
while [ "$#" -gt 0 ]; do
  if [ "$1" = -- ]; then
    shift
    artifacts=("$@")
    break
  fi
  args+=("$1")
  shift
done
kind="${args[0]:-}"

fingerprint() {
  git ls-files -z --cached --others --exclude-standard -- core/src core/Cargo.toml core/uniffi.toml gen Cargo.toml Cargo.lock .cargo \
    scripts/build_ios.sh scripts/build_kotlin.sh scripts/build_bindgen.sh scripts/generate_kotlin_bindings.sh scripts/native_source_hash.sh \
    | LC_ALL=C sort -z -u \
    | while IFS= read -r -d '' source; do
        if [ -f "$source" ]; then printf '%s\0' "$source"; fi
      done | xargs -0 shasum -a 256 || return
  printf 'arg %s\n' ${args[@]+"${args[@]}"}
  rustc -Vv 2>&1 || true
  cargo -V 2>&1 || true
  case "$kind" in
    android*) cargo ndk --version 2>&1 || true; echo "ndk ${ANDROID_NDK_HOME:-}" | sed 's|/.*/||' ;;
    ios*|jvm*) xcodebuild -version 2>&1 || true ;;
  esac
  env | grep -E '^(RUSTFLAGS|CARGO_ENCODED_RUSTFLAGS|CARGO_PROFILE_|CARGO_BUILD_TARGET|CARGO_TARGET_|IPHONEOS_DEPLOYMENT_TARGET|MACOSX_DEPLOYMENT_TARGET)' | sort || true
}

current="$(fingerprint | shasum -a 256 | cut -d' ' -f1)"
if [ -z "$stamp" ]; then
  echo "$current"
  exit 0
fi
[ -f "$stamp" ] && [ "$(cat "$stamp")" = "$current" ] || exit 1
for artifact in ${artifacts[@]+"${artifacts[@]}"}; do
  [ -e "$artifact" ] || exit 1
done
echo "native build skipped: inputs unchanged, fingerprint $current ($stamp)"
