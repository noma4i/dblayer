#!/usr/bin/env bash
# Counts legacy markers and references to development files in tracked files.
#   check_legacy.sh count <tree> <ref>  prints "<legacy> <dev>"
#   check_legacy.sh check               exits 1 when either number is above 0 on HEAD
set -euo pipefail

LEGACY_RE='[Ll]egacy|LEGACY|[Cc]ompat([^i]|$)|Leases|Admitted|Origin::|DeliveryUnknown|DELIVERY_UNKNOWN|deliveryUnknown|delivery_unknown|_for_testing|reach_stream|resync_stream|reachStream|resyncStream|session_resume|sessionResume|set_listening|setListening|retry_all|retryAll|batch_outcome|batchOutcome|refresh_preloads|refreshPreloads|resume_uploads|resumeUploads|round_(begin|next|leave|abandon)|round(Begin|Next|Leave|Abandon)|awaitingSnapshot|mutate_wait|forget_credential|start_session_in_tests|fn flush\(|fn resume\(|fn maintain\(|fn warm\(|crate::sync::|ffi::stream|ffi/stream|ffi/live'
LEGACY_PATHS='core/src gen/src swift/Sources swift/Tests kotlin/src scripts docs README.md fixtures Makefile'
DEV_RE='specs/|CLAUDE\.md|AGENTS\.md|\.orca-artifacts'

count() {
  local tree="$1" ref="$2" pattern="$3"
  shift 3
  { git -C "$tree" grep -E -c "$pattern" "$ref" -- "$@" || true; } | awk -F: '{s += $NF} END {print s + 0}'
}

counts() {
  local tree="$1" ref="$2"
  # shellcheck disable=SC2086
  printf '%s %s\n' \
    "$(count "$tree" "$ref" "$LEGACY_RE" $LEGACY_PATHS ':(exclude)scripts/check_legacy.sh' ':(exclude)scripts/release.sh' ':(exclude)scripts/check_release_modes.sh')" \
    "$(count "$tree" "$ref" "$DEV_RE" . ':(exclude)scripts/check_legacy.sh')"
}

case "${1:-}" in
  count)
    [ $# -eq 3 ] || { echo "usage: check_legacy.sh count <tree> <ref>" >&2; exit 2; }
    counts "$2" "$3"
    ;;
  check)
    root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
    read -r legacy dev <<< "$(counts "$root" HEAD)"
    if [ "$legacy" -ne 0 ] || [ "$dev" -ne 0 ]; then
      echo "check_legacy: $legacy legacy lines, $dev development-file references (both must be 0)" >&2
      exit 1
    fi
    echo "check_legacy: ok"
    ;;
  *)
    echo "usage: check_legacy.sh count <tree> <ref> | check" >&2
    exit 2
    ;;
esac
