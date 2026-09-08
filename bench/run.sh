#!/usr/bin/env bash
# Does compilation actually pay?
set -euo pipefail
SF_LIBS="$(./build/swordfish-config --libs)"
cd "$(dirname "$0")/.."
N=${N:-3000000}

# Bounds and cleanup, matching bench/vs_reference.sh. Nothing here runs
# unbounded: a hung compile or a hung pipeline is indistinguishable from a slow
# one until something puts a limit on it.
STEP_TIMEOUT=${STEP_TIMEOUT:-600}
exec 3>&2
kill_tree() {
  local p=$1 c
  # Children before parent: killing a `timeout` wrapper alone re-parents the
  # process it was bounding, which then keeps running unsupervised.
  for c in $(pgrep -P "$p" 2>/dev/null); do kill_tree "$c"; done
  kill -9 "$p" 2>/dev/null
}
bench_cleanup() {
  local rc=$?
  trap - EXIT INT TERM
  local c
  for c in $(pgrep -P $$ 2>/dev/null); do kill_tree "$c"; done
  true
  exit $rc
}
trap bench_cleanup EXIT INT TERM
bounded() {  # bounded <what> <command...>
  local what=$1; shift
  timeout -k 10 "$STEP_TIMEOUT" "$@"
  local rc=$?
  if [ "$rc" -eq 124 ] || [ "$rc" -eq 137 ]; then
    echo "HUNG: $what exceeded ${STEP_TIMEOUT}s and was killed" >&3
    return 99
  fi
  return $rc
}

MAPPING=${MAPPING:-'root = this
root.sq = this.n * this.n
root.tag = "n=%d".format(this.n)'}

echo "mapping:"; echo "$MAPPING" | sed 's/^/    /'
echo "messages: $N"; echo

t0=$(date +%s.%N)
bounded "sfslice emit" ./build/sfslice emit "$MAPPING" > build/blobl_gen.cc
bounded "compile" g++ -std=c++23 -O2 -Iinclude -c build/blobl_gen.cc -o build/blobl_gen.o
bounded "link" g++ -std=c++23 -O2 -Iinclude bench/bench_main.cc build/blobl_gen.o \
    $SF_LIBS -o build/bench_compiled
t1=$(date +%s.%N)
printf 'build (emit + compile + link): %.2f s\n\n' "$(echo "$t1 - $t0" | bc)"

bounded "interpreted bench" ./build/sfslice bench "$MAPPING" -n "$N"
bounded "compiled bench" ./build/bench_compiled -n "$N"
