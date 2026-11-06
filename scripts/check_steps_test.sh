#!/usr/bin/env bash
# Self test of the step runner (check_steps.sh): process kill at the limit, build slot allowance, concurrent lanes, CPU counter.
# Runs in seconds, takes no build slot.
set -uo pipefail

here="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
# shellcheck source=check_steps.sh
source "$here/check_steps.sh"

work="$(mktemp -d)"
cleanup_processes() {
  local file pid
  for file in "$work"/pid-*; do
    [ -f "$file" ] || continue
    read -r pid < "$file"
    kill "$pid" 2>/dev/null || true
    /bin/rm -f "$file"
  done
}
trap 'cleanup_processes; /bin/rm -rf "$work"' EXIT
failed=0

wait_until_gone() {
  local pid="$1" tries=0
  while kill -0 "$pid" 2>/dev/null && [ "$tries" -lt 200 ]; do
    sleep 0.05
    tries=$((tries + 1))
  done
  ! kill -0 "$pid" 2>/dev/null
}

expect_red_step_killed() {
  if [ "$1" -ne 124 ] || [ -z "$2" ] || ! wait_until_gone "$2"; then
    echo "FAIL step code $1, leftover PID: ${2:-none}"
    failed=1
  fi
}

step_runner_a_step_past_its_limit_leaves_no_process() {
  local code=0 pid=""
  mkfifo "$work/go" "$work/born"
  cat > "$work/subject.sh" <<'SUBJECT'
set -m
exec 3<> "$1/go"
read -r -u 3 _
sleep 601.7 &
echo $! > "$1/born"
wait
SUBJECT
  (
    sleep() { if [ "$1" = 7777 ]; then return 0; fi; command sleep "$@"; }
    # The first list of descendants is taken before the subject forks. The fork happens right after it, as in a
    # process that was still running when the stop signal was sent, and its child leaves the group (set -m).
    pgrep() {
      local found
      found="$(command pgrep "$@")"
      if [ ! -e "$work/listed" ]; then
        : > "$work/listed"
        exec 4<> "$work/born"
        kill -CONT -- "-$2" 2> /dev/null || true
        echo go > "$work/go"
        read -r -t 30 -u 4 pid < /dev/null || true
        echo "$pid" > "$work/pid-sleep"
      fi
      [ -z "$found" ] || echo "$found"
    }
    run_bounded 7777 selftest bash "$work/subject.sh" "$work" 2> /dev/null
  ) || code=$?
  [ -s "$work/pid-sleep" ] && read -r pid < "$work/pid-sleep"
  expect_red_step_killed "$code" "$pid"
  [ -z "$pid" ] || kill -KILL "$pid" 2> /dev/null || true
  /bin/rm -f "$work/pid-sleep" "$work/go" "$work/born" "$work/listed"
}

step_runner_the_wait_for_a_build_slot_is_not_test_time() {
  root="$work/engine"
  mkdir -p "$root/target"
  changed_files() { echo "$CHANGED"; }
  steps=("final|queued|1|-|echo build-slot > /dev/null; sleep 2" "cheap|plain|1|*|sleep 2")
  (run_checks --only final) > "$work/engine.out" 2>&1 \
    || { echo "FAIL expected green: a step with a build slot must get the limit plus the queue allowance"; failed=1; }
  steps=("cheap|plain|1|*|sleep 2")
  (run_checks) > "$work/engine.out" 2>&1 && { echo "FAIL expected red: a step without a slot must stop at its limit"; failed=1; }
}

engine() {
  root="$work/engine"
  mkdir -p "$root/target"
  /bin/rm -f "$root"/ran-*
  changed_files() { echo "$CHANGED"; }
  steps=(
    "cheap|c1|20|*|$c1"
    "core|k1|20|core/|touch ran-k1 && $k1"
    "swift|s1|20|swift/|touch ran-s1 && $s1"
    "final|f1|20|-|touch ran-f1 && $f1"
  )
  (run_checks "$@") > "$work/engine.out" 2>&1
}

expect_red_engine() {
  local label="$1"
  shift
  if engine "$@"; then echo "FAIL expected red: $label"; cat "$work/engine.out"; failed=1; fi
}

expect_green_engine() {
  local label="$1"
  shift
  engine "$@" || { echo "FAIL expected green: $label"; cat "$work/engine.out"; failed=1; }
}

step_runner_fast_and_full_fail_on_the_same_step_and_cheap_runs_first() {
  c1=true; k1=true; s1=true; f1=true; CHANGED="core/a.rs"
  expect_green_engine "all steps pass in full"
  [ -e "$work/engine/ran-f1" ] || { echo "FAIL expect_green: the final lane did not run"; failed=1; }
  f1=false
  expect_red_engine "final step fails in full"
  f1=true; k1=false
  expect_red_engine "lane step fails, final must not mask it"
  [ ! -e "$work/engine/ran-f1" ] || { echo "FAIL expect_red: final ran after a failed lane"; failed=1; }
  k1=true
  expect_green_engine "all steps pass again"
  expect_green_engine "all steps pass in fast" --fast
  c1=false
  expect_red_engine "cheap step fails in full"
  expect_red_engine "cheap step fails in fast" --fast
  [ -e "$work/engine/ran-k1" ] && { echo "FAIL expect_red: a build step ran after a cheap failure"; failed=1; }
  c1=true; k1=false
  expect_red_engine "core step fails in full"
  expect_red_engine "core step fails in fast" --fast
  k1=true; s1=false; CHANGED="swift/b.swift"
  expect_red_engine "swift step fails in full"
  expect_red_engine "swift step fails in fast" --fast
  local full fast_list
  full="$(engine --list; cat "$work/engine.out")"
  fast_list="$(engine --fast --list; cat "$work/engine.out")"
  [ -n "$fast_list" ] && [ -z "$(comm -13 <(echo "$full" | sort) <(echo "$fast_list" | sort))" ] \
    || { echo "FAIL expect_green: a fast step is not a full step"; failed=1; }
}

step_runner_lanes_run_at_the_same_time_and_a_red_lane_does_not_hide_the_others() {
  root="$work/engine"
  mkdir -p "$root/target"
  /bin/rm -f "$root"/met-* "$root"/ran-*
  changed_files() { echo "$CHANGED"; }
  local meet='touch met-NAME; n=0; until [ -e met-OTHER ] || [ "$n" -ge 100 ]; do sleep 0.1; n=$((n + 1)); done; [ -e met-OTHER ]'
  steps=(
    "a|a1|20|-|${meet//NAME/a}"
    "b|b1|20|-|${meet//NAME/b}"
    "c|c1|20|-|touch ran-c1; false"
    "c|c2|20|-|touch ran-c2"
    "d|d1|20|-|sleep 1; touch ran-d1"
    "final|f1|20|-|touch ran-f1"
  )
  steps[0]="${steps[0]//OTHER/b}"
  steps[1]="${steps[1]//OTHER/a}"
  (run_checks) > "$work/engine.out" 2>&1
  [ -e "$root/met-a" ] && [ -e "$root/met-b" ] && ! grep -q 'FAIL' <(grep -E '^(a|b) ' "$work/engine.out") || { echo "FAIL expect_green: lanes a and b did not run at the same time"; failed=1; }
  [ -e "$root/ran-c1" ] && [ ! -e "$root/ran-c2" ] || { echo "FAIL expect_red: a step ran after a red step of its lane"; failed=1; }
  [ -e "$root/ran-d1" ] || { echo "FAIL expect_red: a red lane hid the lane d"; failed=1; }
  [ ! -e "$root/ran-f1" ] || { echo "FAIL expect_red: final ran after a red lane"; failed=1; }
  [ "$(grep -E '^[a-d] ' "$work/engine.out" | awk '{print $1 $2}' | paste -sd, -)" = "aa1,bb1,cc1,dd1" ] || { echo "FAIL the summary is not in table order"; cat "$work/engine.out"; failed=1; }
  grep -q '^check: FAILED' "$work/engine.out" || { echo "FAIL the run did not end red"; failed=1; }
}

step_runner_the_limit_kills_the_processes_of_every_lane() {
  root="$work/engine"
  mkdir -p "$root/target"
  /bin/rm -rf "$root/target/check.lock"
  changed_files() { echo "$CHANGED"; }
  steps=("a|a1|280|-|sleep 603 & echo \$! > $work/pid-lane-a; wait" "b|b1|280|-|sleep 604 & echo \$! > $work/pid-lane-b; wait")
  local code=0 pid
  (run_bounded 3 selftest run_checks) > "$work/engine.out" 2>&1 || code=$?
  [ "$code" -eq 124 ] || { echo "FAIL expected the limit (124), got $code"; failed=1; }
  for pid in "$work"/pid-lane-a "$work"/pid-lane-b; do
    [ -s "$pid" ] && wait_until_gone "$(cat "$pid")" || { echo "FAIL a lane left a process after the limit: $pid"; failed=1; }
  done
}

expect_green_counter() {
  [ "$1" -ge "$(($2 > 1 ? $2 - 1 : 1))" ] || { echo "FAIL the counter printed $1 CPU seconds for a child that burned $2"; failed=1; }
}

step_runner_the_cpu_counter_sees_a_build_daemon() {
  root="$work/engine"
  mkdir -p "$root/target" "$work/jdk"
  /bin/rm -f "$work/marker-java"
  changed_files() { echo "$CHANGED"; }
  cp "$(command -v perl)" "$work/jdk/java"
  local child='open(P, q(>), q(PIDFILE)); print P $$; close(P); while ((times)[0] + (times)[1] < 3) {} open(F, q(>), q(MARKER)); print F $$, q( ), int((times)[0] + (times)[1]); close(F); sleep 60' waiting='until [ -s MARKER ] && awk -v pid=$(cut -d" " -f1 MARKER) -v cpu=$(cut -d" " -f2 MARKER) "\$1 == pid && \$2 >= cpu - 1 { found = 1 } END { exit !found }" "$CHECK_SAMPLES_FILE"; do sleep 1; done'
  child="${child//PIDFILE/$work/pid-java}"
  child="${child//MARKER/$work/marker-java}"
  waiting="${waiting//MARKER/$work/marker-java}"
  /bin/rm -f "$work/release"
  steps=("a|a1|280|-|$work/jdk/java -e 'if (!fork) { exec(q(perl), q(-e), q($child)) } until (-e q($work/release)) { sleep 1 }' & $waiting; touch $work/release; wait")
  (run_checks) > "$work/engine.out" 2>&1
  cleanup_processes
  local counted
  counted="$(sed -n 's/^check: ok, at least \([0-9]*\) CPU seconds.*/\1/p' "$work/engine.out")"
  expect_green_counter "${counted:-0}" "$(cut -d' ' -f2 "$work/marker-java" 2> /dev/null || echo 0)"
}

expect_green_run_daemon() {
  [ "$1" -ge "$(($2 > 1 ? $2 - 1 : 1))" ] && [ "$1" -le $(($2 + 2)) ] || { echo "FAIL the counter printed $1 CPU seconds: the daemon of this run burned $2, the daemon of another run kept burning"; failed=1; }
}

step_runner_a_daemon_that_outlives_the_step_is_counted_and_another_runs_daemon_is_not() {
  root="$work/engine"
  mkdir -p "$root/target" "$work/jdk"
  /bin/rm -f "$work/marker-mine"
  changed_files() { echo "$CHANGED"; }
  cp "$(command -v perl)" "$work/jdk/java"
  local mine other waiting='until [ -s MARKER ] && awk -v pid=$(cut -d" " -f1 MARKER) -v cpu=$(cut -d" " -f2 MARKER) "\$1 == pid && \$2 >= cpu - 1 { found = 1 } END { exit !found }" "$CHECK_SAMPLES_FILE"; do sleep 1; done'
  mine='if (!fork) { open(P, q(>), q(PIDFILE)); print P $$; close(P); exec(q(perl), q(-e), q(while ((times)[0] + (times)[1] < 3) {} open(F, q(>), q(MARKER)); print F $$, q( ), int((times)[0] + (times)[1]); close(F); sleep 60), q(GradleDaemon), q(-XX:ErrorFile=/tmp/check-owner-) . $ENV{CHECK_OWNER_ID} . q(-x.log)) }'
  mine="${mine//PIDFILE/$work/pid-mine}"
  other='if (!fork) { open(F, q(>), q(PIDFILE)); print F $$; close(F); exec(q(perl), q(-e), q(1 while 1), q(GradleDaemon), q(-XX:ErrorFile=/tmp/check-owner-other-run-x.log)) }'
  other="${other//PIDFILE/$work/pid-other}"
  mine="${mine//MARKER/$work/marker-mine}"
  waiting="${waiting//MARKER/$work/marker-mine}"
  steps=("a|a1|280|-|$work/jdk/java -e '$mine'; $work/jdk/java -e '$other'; $waiting")
  (run_checks) > "$work/engine.out" 2>&1
  cleanup_processes
  local counted
  counted="$(sed -n 's/^check: ok, at least \([0-9]*\) CPU seconds.*/\1/p' "$work/engine.out")"
  expect_green_run_daemon "${counted:-0}" "$(cut -d' ' -f2 "$work/marker-mine" 2> /dev/null || echo 0)"
}

for test_name in $(declare -F | awk '{print $3}' | grep '^step_runner_'); do
  "$test_name"
  echo "$test_name: $([ "$failed" -eq 0 ] && echo ok || echo FAILED)"
done
[ "$failed" -eq 0 ] || exit 1
echo "check_steps_test: ok"
