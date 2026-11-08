#!/usr/bin/env bash
# Counts legacy markers, legacy-named files, development files and references to them in tracked files.
#   check_legacy.sh count <tree> <ref>  prints "<legacy> <dev>"
#   check_legacy.sh check               exits 1 when either number is above 0 on HEAD, 2 when git fails
set -euo pipefail

LEGACY_RE='[Ll]egacy|LEGACY|[Cc]ompat([^i]|$|i[^b]|ib[^l]|ibl[^e])|COMPAT|Leases|Admitted|(^|[^A-Za-z])Origin::|DeliveryUnknown|DELIVERY_UNKNOWN|deliveryUnknown|delivery_unknown|_for_testing|reach_stream|resync_stream|reachStream|resyncStream|session_resume|sessionResume|set_listening|setListening|retry_all|retryAll|batch_outcome|batchOutcome|refresh_preloads|refreshPreloads|resume_uploads|resumeUploads|round_(begin|next|leave|abandon)|round(Begin|Next|Leave|Abandon)|awaitingSnapshot|mutate_wait|forget_credential|start_session_in_tests|fn[[:space:]]+(flush|resume|maintain|warm)[[:space:]]*\(|crate::sync([^_[:alnum:]]|$)|ffi::stream|ffi/stream|ffi/live|Authority::|WriteOrigin|CommitEnvelope|commit_as|lifecycle::authority|Quarantined|StreamTable|sync::(send|upload|load|subscribe|live|invalidate|model_event|entity_fetch|preload|online)|upload::machine|resume_machine|db::(upload_round|dispatch|observe|watch)|migrate_transfer_identity|state\.cached|_in_tests([^_[:alnum:]]|$)|write_store|mutate_wait|after_publish: Mutex|settle_after_epoch|release_held|set_ending|wait_store_closed|cable::lifecycle|listening|(^|[^A-Za-z])(RX|NB|BR)[0-9]|INV-|\.flush\(|wrapper-glue|OnlineRelay|needsAttention|roundNext|roundLeave|roundAbandon|opened = false|reported = false'
LEGACY_PATHS='core/src gen/src swift/Sources swift/Tests kotlin/src scripts docs README.md fixtures :(exclude)fixtures/consumer Makefile'
DEV_RE='specs/|CLAUDE\.md|AGENTS\.md|\.orca-artifacts|(^|[^/])\.claude/'
LEGACY_NAME_RE='(^|[/._-])(legacy|compat|old|v1|shim|adapter)([/._-]|$)'
DEV_NAME_RE='(^|/)(specs|\.claude|\.orca-artifacts)/|(^|/)(CLAUDE|AGENTS)\.md$|\.log$|(^|/)api_gap[^/]*\.md$'

die() { echo "check_legacy: $*" >&2; exit 2; }

# Prints the number of matching lines. git grep rc 1 is "no match", any other nonzero rc is a git failure.
count() {
  local tree="$1" ref="$2" pattern="$3" out rc=0
  shift 3
  out="$(git -C "$tree" grep -E -c "$pattern" "$ref" -- "$@")" || rc=$?
  [ "$rc" -le 1 ] || die "git grep failed (rc=$rc) in $tree at $ref"
  printf '%s\n' "$out" | awk -F: 'NF {s += $NF} END {print s + 0}'
}

# Prints the number of tracked paths of the revision whose name matches.
count_names() {
  local tree="$1" ref="$2" pattern="$3" files rc=0
  files="$(git -C "$tree" ls-tree -r --name-only "$ref")" || die "git ls-tree failed in $tree at $ref"
  printf '%s\n' "$files" | grep -vx 'scripts/check_legacy.sh' | grep -ciE "$pattern" || rc=$?
  [ "$rc" -le 1 ] || die "name scan failed"
}

counts() {
  local tree="$1" ref="$2" lines names_legacy dev_lines names_dev
  # shellcheck disable=SC2086
  lines="$(count "$tree" "$ref" "$LEGACY_RE" $LEGACY_PATHS ':(exclude)scripts/check_legacy.sh' ':(exclude)scripts/release.sh' ':(exclude)scripts/check_release_modes.sh' ':(exclude)scripts/check_markers_test.py')" || return $?
  names_legacy="$(count_names "$tree" "$ref" "$LEGACY_NAME_RE")" || return $?
  dev_lines="$(count "$tree" "$ref" "$DEV_RE" . ':(exclude)fixtures/consumer' ':(exclude)scripts/check_legacy.sh' ':(exclude)scripts/check_release_modes.sh' ':(exclude)scripts/check_markers_test.py')" || return $?
  names_dev="$(count_names "$tree" "$ref" "$DEV_NAME_RE")" || return $?
  printf '%s %s\n' "$((lines + names_legacy))" "$((dev_lines + names_dev))"
}

case "${1:-}" in
  count)
    [ $# -eq 3 ] || { echo "usage: check_legacy.sh count <tree> <ref>" >&2; exit 2; }
    counts "$2" "$3"
    ;;
  check)
    root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
    result="$(counts "$root" HEAD)" || exit $?
    read -r legacy dev <<< "$result"
    if [ "$legacy" -ne 0 ] || [ "$dev" -ne 0 ]; then
      echo "check_legacy: $legacy legacy lines or files, $dev development-file references or files (both must be 0)" >&2
      exit 1
    fi
    echo "check_legacy: ok"
    ;;
  *)
    echo "usage: check_legacy.sh count <tree> <ref> | check" >&2
    exit 2
    ;;
esac
