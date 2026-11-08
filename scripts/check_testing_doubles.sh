#!/usr/bin/env bash
# bridge_testing_doubles_parity: the public types of DBlayerTesting (Swift) and testFixtures (Kotlin) have the same names. Platform-only types are listed here with the platform that has no counterpart.
set -euo pipefail

root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
swift_dir="${CHECK_DOUBLES_SWIFT:-$root/swift/Sources/DBlayerTesting}"
kotlin_dir="${CHECK_DOUBLES_KOTLIN:-$root/kotlin/src/testFixtures}"
swift_only="ScriptedURLProtocol"
kotlin_only="RequestPurpose"

swift_names() {
  rg --no-filename -o '^public (final |indirect )?(class|struct|enum|protocol|actor) +\w+' "$swift_dir" | awk '{print $NF}' | sort -u
}

kotlin_names() {
  rg --no-filename -o '^(data |enum |sealed |abstract |open )*(class|interface|object) +\w+' "$kotlin_dir" | awk '{print $NF}' | sort -u
}

without() {
  local listed="$1" name
  while read -r name; do
    case " $listed " in *" $name "*) ;; *) echo "$name" ;; esac
  done
}

compare() {
  local swift kotlin missing_kotlin missing_swift
  swift="$(swift_names | without "$swift_only")"
  kotlin="$(kotlin_names | without "$kotlin_only")"
  missing_kotlin="$(comm -23 <(echo "$swift") <(echo "$kotlin"))"
  missing_swift="$(comm -13 <(echo "$swift") <(echo "$kotlin"))"
  [ -z "$swift" ] && { echo "check_testing_doubles: no Swift public types found in $swift_dir" >&2; return 1; }
  [ -z "$kotlin" ] && { echo "check_testing_doubles: no Kotlin public types found in $kotlin_dir" >&2; return 1; }
  [ -z "$missing_kotlin" ] || echo "check_testing_doubles: only in DBlayerTesting: $missing_kotlin" >&2
  [ -z "$missing_swift" ] || echo "check_testing_doubles: only in testFixtures: $missing_swift" >&2
  [ -z "$missing_kotlin$missing_swift" ]
}

selftest() {
  local dir
  dir="$(mktemp -d)"
  mkdir "$dir/swift" "$dir/kotlin"
  printf 'public final class SealedServer {}\npublic protocol CablePeer {}\n' > "$dir/swift/a.swift"
  printf 'class SealedServer\ninterface CablePeer\n' > "$dir/kotlin/a.kt"
  CHECK_DOUBLES_SWIFT="$dir/swift" CHECK_DOUBLES_KOTLIN="$dir/kotlin" bash "$0" || { echo "selftest: equal sets must pass" >&2; exit 1; }
  printf 'public final class Extra {}\n' >> "$dir/swift/a.swift"
  if CHECK_DOUBLES_SWIFT="$dir/swift" CHECK_DOUBLES_KOTLIN="$dir/kotlin" bash "$0" 2> /dev/null; then echo "selftest: a Swift-only type must fail" >&2; exit 1; fi
  printf 'class Other\n' >> "$dir/kotlin/a.kt"
  if CHECK_DOUBLES_SWIFT="$dir/swift" CHECK_DOUBLES_KOTLIN="$dir/kotlin" bash "$0" 2> /dev/null; then echo "selftest: a type missing on one side must fail" >&2; exit 1; fi
  /bin/rm -rf "$dir"
  echo "check_testing_doubles selftest: ok"
}

if [ "${1:-}" = "--selftest" ]; then selftest; else compare && echo "bridge_testing_doubles_parity: ok"; fi
