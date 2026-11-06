#!/usr/bin/env bash
# Sourced by check_<library>.sh and the self test. run_bounded <seconds> <name> <command...>:
# runs the command in its own process group and, past the limit, kills the group and every descendant
# (a test host that left the group included; the tree is frozen before it is listed, so no process escapes between the list and the kill). Returns the command code, or 124 on the limit.

descendants() {
  local child
  for child in $(pgrep -P "$1" 2>/dev/null); do
    echo "$child"
    descendants "$child"
  done
}

freeze_tree() {
  local pid="$1" known="unlisted" now
  kill -STOP -- "-$pid" 2>/dev/null || true
  kill -STOP "$pid" 2>/dev/null || true
  while :; do
    now="$(descendants "$pid" | sort -u | tr '\n' ' ')"
    [ "$now" = "$known" ] && break
    kill -STOP $now 2>/dev/null || true
    known="$now"
  done
  echo "$known"
}

kill_tree() {
  local pid="$1" tree
  tree="$(freeze_tree "$pid")"
  kill -TERM -- "-$pid" $tree 2>/dev/null || true
  kill -TERM "$pid" 2>/dev/null || true
  kill -CONT -- "-$pid" $tree 2>/dev/null || true
  kill -CONT "$pid" 2>/dev/null || true
  sleep 2
  kill -KILL -- "-$pid" $tree 2>/dev/null || true
  kill -KILL "$pid" 2>/dev/null || true
  survivors "$pid" $tree
}

# Every process that was in the tree must be gone (a zombie waits for its parent and counts as gone). A survivor is killed again, up to 50 times, then the step fails loudly.
survivors() {
  local left tries=0 one
  while :; do
    left=""
    for one in "$@"; do
      case "$(ps -o state= -p "$one" 2>/dev/null)" in "" | Z*) ;; *) left="$left $one" ;; esac
    done
    [ -z "$left" ] && return 0
    if [ "$tries" -ge 50 ]; then
      echo "check: kill_tree could not remove:$left" >&2
      return 1
    fi
    kill -KILL $left 2>/dev/null || true
    tries=$((tries + 1))
    sleep 0.1
  done
}

run_bounded() {
  local limit="$1" name="$2" pid watcher code marker
  shift 2
  marker="$(mktemp)"
  set -m
  "$@" &
  pid=$!
  set +m
  ( sleep "$limit"; echo expired > "$marker"; kill_tree "$pid" ) &
  watcher=$!
  code=0
  wait "$pid" || code=$?
  kill "$watcher" 2>/dev/null || true
  pkill -P "$watcher" 2>/dev/null || true
  wait "$watcher" 2>/dev/null || true
  if [ -s "$marker" ]; then
    /bin/rm -f "$marker"
    echo "check: step '$name' passed $limit s, its process group was killed" >&2
    return 124
  fi
  /bin/rm -f "$marker"
  return "$code"
}

# Step table engine. A step with a build slot gets its limit inside the slot ({limit} in the command, see check_bounded.sh)
# and the limit plus CHECK_QUEUE_ALLOWANCE (3600 s) outside, so the wait for a slot never counts as test time.
# The library script defines `steps` (lines "lane|name|limit seconds|paths|command"), then calls
# run_checks "$@". Order: cheap first, then every lane except cheap, ship and final at the same time (steps of one lane run one after another and each takes its own build slot),
# then ship, then final. A red lane does not stop the other lanes of its group, ship and final run only when every earlier lane passed.
# The output of a lane group is printed after the group ends, lanes in table order.
# --fast keeps the steps whose paths prefix a changed file ("*" always): the same commands, a subset of the full run.
# Lane "final" runs last, only when every lane passed (checks of their outputs). --list prints the selected steps. --only <lane> runs one lane.

# CPU seconds of processes the waited-children counter (`times`) cannot see, sampled once per second: processes below a
# java process of this check (single-use daemon, test executors) and the Gradle and Kotlin daemons that carry this
# run's check-owner-<id> tag in a JVM option (-XX:ErrorFile, Gradle drops plain -D options from the daemon command line), with their children. A daemon of another run carries another id and is
# not counted. The highest CPU time seen per pid counts, the CPU a daemon had before the run is subtracted.
counted_processes_cpu() {
  local root_pid="$CHECK_ROOT_PID" pid pids tagged=""
  [ -z "${CHECK_OWNER_ID:-}" ] || tagged="$(pgrep -f "check-owner-${CHECK_OWNER_ID}-" || true)"
  pids="$(
    echo "$root_pid"
    descendants "$root_pid"
    for pid in $tagged; do
      echo "$pid"
      descendants "$pid"
    done
  )"
  ps -p "$(printf '%s\n' "$pids" | sort -nu | paste -sd, -)" -o pid=,ppid=,cputime=,command= | awk -v root="$root_pid" -v owner="check-owner-${CHECK_OWNER_ID:-none}-" '
    { parent[$1] = $2; n = split($3, a, ":"); sec[$1] = a[n] + 60 * a[n - 1] + (n > 2 ? 3600 * a[n - 2] : 0)
      isjava[$1] = ($0 ~ /\/java( |$)/) ? 1 : 0
      tagged[$1] = ($0 ~ /GradleDaemon|KotlinCompileDaemon/ && index($0, owner) > 0) ? 1 : 0 }
    END {
      for (pid in parent) {
        inside = 0; below = 0; under_tagged = 0; up = parent[pid]
        for (hops = 0; up > 1 && hops < 30; hops++) { if (up == root) inside = 1; if (isjava[up]) below = 1; if (tagged[up]) under_tagged = 1; up = parent[up] }
        if ((inside && below) || tagged[pid] || under_tagged) print pid, sec[pid]
      }
    }'
}

sample_java_cpu() {
  local file="$1"
  while :; do
    counted_processes_cpu >> "$file"
    sleep 1
  done
}

changed_files() {
  local base
  base="$(git -C "$root" merge-base HEAD master 2>/dev/null || echo HEAD)"
  { git -C "$root" diff --name-only "$base"; git -C "$root" ls-files -m -o --exclude-standard; } | sort -u
}

step_selected() {
  local paths="$1" prefix file prefixes
  [ "$fast" -eq 0 ] && return 0
  read -ra prefixes <<< "$paths"
  for prefix in "${prefixes[@]}"; do
    [ "$prefix" = "*" ] && return 0
    while IFS= read -r file; do
      case "$file" in "$prefix"*) return 0 ;; esac
    done <<< "$changed"
  done
  return 1
}

keep_step_artifacts() {
  local name="$1" path kept=0
  declare -F step_artifacts > /dev/null || return 0
  while IFS= read -r path; do
    [ -n "$path" ] || continue
    if [ ! -e "$root/$path" ]; then
      echo "check: step $name left no $path" >&2
    elif ! { mkdir -p "$logdir/$name.artifacts/$(dirname "$path")" && cp -R "$root/$path" "$logdir/$name.artifacts/$path"; }; then
      echo "check: could not keep $path of step $name" >&2
      kept=1
    fi
  done < <(step_artifacts "$name")
  return "$kept"
}

run_step() {
  local lane name limit paths command log start code waited
  IFS='|' read -r lane name limit paths command <<< "$1"
  log="$logdir/$name.log"
  start=$SECONDS
  code=0
  command="${command//\{limit\}/$limit}"
  outer="$limit"
  [[ "$command" == *build-slot* ]] && outer=$((limit + ${CHECK_QUEUE_ALLOWANCE:-3600}))
  run_bounded "$outer" "$name" bash -c 'cd -- "$1" && shift && eval "$1"' _ "$root" "$command" > "$log" 2>&1 || code=$?
  waited="$(sed -n 's/.*got slot[0-9]* after \([0-9]*\)s.*/\1/p' "$log" | paste -sd+ - | bc 2> /dev/null)"
  keep_step_artifacts "$name" || [ "$code" -ne 0 ] || code=70
  printf '%-8s %-20s %-8s %4ss (queue for a build slot %ss)\n' "$lane" "$name" "$([ "$code" -eq 0 ] && echo ok || echo "FAIL($code)")" "$((SECONDS - start))" "${waited:-0}"
  grep -h 'native build skipped' "$log" | sed 's/^/    | /'
  if [ "$code" -ne 0 ]; then
    tail -25 "$log" | sed 's/^/    | /'
    echo "    log: $log"
  fi
  return "$code"
}

run_lane() {
  local lane="$1" line step_lane name rest
  for line in "${selected[@]}"; do
    IFS='|' read -r step_lane name rest <<< "$line"
    [ "$step_lane" = "$lane" ] || continue
    run_step "$line" || return 1
  done
}

in_list() {
  local wanted="$1" one
  shift
  for one in "$@"; do [ "$one" = "$wanted" ] && return 0; done
  return 1
}

# Lanes of a group start together, each prints into its own file, the files are printed in the order given.
run_lane_group() {
  local group=() pids=() lane code=0 index
  for lane in "$@"; do
    in_list "$lane" ${lanes[@]+"${lanes[@]}"} && group+=("$lane")
  done
  [ "${#group[@]}" -gt 0 ] || return 0
  if [ "${#group[@]}" -eq 1 ]; then
    run_lane "${group[0]}" 2>&1 | tee -a "$logdir/summary.txt"
    return
  fi
  for lane in "${group[@]}"; do
    ( run_lane "$lane" ) > "$logdir/lane-$lane.out" 2>&1 &
    pids+=("$!")
  done
  trap 'for lane in "${group[@]}"; do tee -a "$logdir/summary.txt" < "$logdir/lane-$lane.out"; done; exit 143' TERM
  for index in "${!group[@]}"; do
    wait "${pids[$index]}" || code=1
  done
  trap - TERM
  for lane in "${group[@]}"; do
    tee -a "$logdir/summary.txt" < "$logdir/lane-$lane.out"
  done
  return "$code"
}

open_run_logs() {
  local base="$root/target/check-logs" sha
  sha="$(git -C "$root" rev-parse --short HEAD 2> /dev/null || echo nogit)"
  [ -z "$(git -C "$root" status --porcelain 2> /dev/null)" ] || sha="$sha-dirty"
  mkdir -p "$base" || return 1
  logdir="$(mktemp -d "$base/$(date -u +%Y%m%dT%H%M%SZ)-$sha-${only:-all}-XXXXXX")" || return 1
  ln -sfn "$(basename "$logdir")" "$base/latest" || return 1
  printf 'started %s\ntree %s\nsha %s\nlane %s\nfast %s\n' "$(date -u +%FT%TZ)" "$root" "$sha" "${only:-all}" "$fast" > "$logdir/run.txt"
}

# One check run per tree: two runs share target/ and kotlin/build and corrupt each other (a duplicate build_ios.sh, two Gradle test runs, doubled load).
# The lock is a directory taken by mkdir, with the owner pid inside; a lock of a dead owner is renamed away first, so only one taker wins.
take_tree_lock() {
  local lock="$root/target/check.lock" owner
  mkdir -p "$root/target"
  until mkdir "$lock" 2> /dev/null; do
    owner="$(cat "$lock/pid" 2> /dev/null || true)"
    if [ -n "$owner" ] && kill -0 "$owner" 2> /dev/null; then
      echo "check: another check run (pid $owner) holds $root, not starting a second one" >&2
      return 3
    fi
    mv "$lock" "$lock.stale.$$" 2> /dev/null && /bin/rm -rf "$lock.stale.$$"
  done
  echo "$$" > "$lock/pid"
  trap '/bin/rm -rf "'"$lock"'"' EXIT
}

run_checks() {
  fast=0; only=""; local list=0 line lane lane_name paths
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --fast) fast=1 ;;
      --list) list=1 ;;
      --only) only="${2:?--only needs a lane}"; shift ;;
      *) echo "usage: $0 [--fast] [--list] [--only <lane>]" >&2; return 2 ;;
    esac
    shift
  done
  changed=""
  [ "$fast" -eq 1 ] && changed="$(changed_files)"
  selected=()
  for line in "${steps[@]}"; do
    IFS='|' read -r lane lane_name _ paths _ <<< "$line"
    [ -n "$only" ] && [ "$lane" != "$only" ] && continue
    step_selected "$paths" && selected+=("$line")
  done
  if [ "$list" -eq 1 ]; then
    printf '%s\n' "${selected[@]}"
    return 0
  fi
  take_tree_lock || return 3
  open_run_logs || { echo "check: cannot open a log directory under $root/target/check-logs" >&2; return 4; }
  local start=$SECONDS lanes=() parallel=() failed=0 cpu sampled before sampler cpu_children daemon
  sampled="$(mktemp)"
  CHECK_OWNER_ID="$(basename "$sampled" | tr -cd '[:alnum:]')"
  CHECK_ROOT_PID="$(exec sh -c 'echo "$PPID"')"
  export CHECK_OWNER_ID CHECK_ROOT_PID
  before="$(mktemp)"
  counted_processes_cpu > "$before"
  export CHECK_SAMPLES_FILE="$sampled"
  sample_java_cpu "$sampled" &
  sampler=$!
  while IFS= read -r lane; do lanes+=("$lane"); done < <(printf '%s\n' "${selected[@]}" | cut -d'|' -f1 | awk '!seen[$0]++')
  for lane in "${lanes[@]}"; do
    case "$lane" in cheap | ship | final) ;; *) parallel+=("$lane") ;; esac
  done
  { run_lane_group cheap && run_lane_group ${parallel[@]+"${parallel[@]}"} && run_lane_group ship && run_lane_group final; } || failed=1
  times > "$logdir/times.txt"
  cpu_children="$(tail -1 "$logdir/times.txt" | tr 'ms' '  ' | awk '{printf "%d", $1 * 60 + $2 + $3 * 60 + $4}')"
  kill "$sampler" 2> /dev/null; wait "$sampler" 2> /dev/null
  daemon="$(awk -v before="$before" 'BEGIN { while ((getline line < before) > 0) { split(line, p, " "); was[p[1]] = p[2] } }
    { if ($2 > best[$1]) best[$1] = $2 }
    END { for (pid in best) total += best[pid] - (pid in was ? was[pid] : 0); printf "%d", total }' "$sampled")"
  /bin/rm -f "$sampled" "$before"
  cpu=$((cpu_children + daemon))
  echo "check: $([ "$failed" -eq 0 ] && echo ok || echo FAILED), at least $cpu CPU seconds, a lower bound from 1 s samples ($cpu_children in waited children, $daemon in daemons and processes below java), ${#selected[@]} steps selected, wall $((SECONDS - start)) s, load $(uptime | sed 's/.*averages*: //')" | tee -a "$logdir/summary.txt"
  return "$failed"
}
