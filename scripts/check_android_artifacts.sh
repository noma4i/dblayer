#!/usr/bin/env bash
set -euo pipefail

root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
[ "$#" -eq 0 ] || { echo "usage: check_android_artifacts.sh" >&2; exit 2; }
abis="$(DBLAYER_BUILD_TARGETS_ONLY=1 bash "$root/scripts/build_kotlin.sh" --release)"
declared="$(sed -n 's/^ *abiFilters += listOf(\(.*\))$/\1/p' "$root/kotlin/build.gradle.kts" | tr -d '" ' | tr ',' '\n' | sort)"
if [ "$declared" != "$(printf '%s\n' "$abis" | sort)" ]; then
  echo "abiFilters in kotlin/build.gradle.kts differ from the release ABIs of build_kotlin.sh: $(echo $declared) vs $(echo $abis)" >&2
  exit 1
fi
echo "abiFilters match the release ABIs"
