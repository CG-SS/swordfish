#!/usr/bin/env bash
# Swordfish against the
# real Redpanda Connect, same config, same box.
#
#   bench/vs_reference.sh [path-to-redpanda-connect]
#
# What makes this a fair comparison rather than a flattering one:
#
#   * The SAME config text drives every run, and the outputs are diffed at a
#     small count before anything is timed. A throughput number for two programs
#     doing different work is worse than no number.
#   * The headline metric is **CPU-seconds per million messages**, not wall
#     clock. Wall clock silently rewards whichever implementation happens to
#     spread across more cores: measured here, Redpanda Connect used 2.6 cores
#     for a pipeline configured with `threads: 1`, because the Go runtime is
#     free to. CPU time is what both are actually spending.
#   * Redpanda Connect is measured BATCHED as well as unbatched, and the batched
#     row is its own best configuration. Unbatched, it spends 172 CPU-seconds
#     per million messages; at `batch_size: 500` that falls to 17, because its
#     per-message cost is dominated by per-batch pipeline overhead rather than
#     by the work. Reporting only the unbatched row would be measuring its worst
#     case -- nobody deploys it that way. Swordfish has no batching at all yet,
#     so it appears once.
#   * One warm-up run is discarded, then three are measured and the MEDIAN is
#     reported. This box has 64 shared cores and a single run varies by ~15%.
#   * N is sized for the SLOWEST implementation. The first version of this
#     script used 5,000,000, which is two and a half minutes per reference run.
#   * NOTHING here runs unbounded. Every invocation is wrapped in `timeout`,
#     because both implementations have hung in practice: the reference CLI
#     hangs outright on some inputs, and the note above about `interval` is a
#     documented two-day run one typo away. A benchmark that hangs looks
#     identical to a benchmark that is merely slow, and both look identical to
#     one still warming up -- so the bound is what turns "stuck" into a report.
set -uo pipefail
cd "$(dirname "$0")/.."

RC=${1:-../redpanda-connect}
[ -x "$RC" ] || { echo "no redpanda-connect binary at $RC" >&2; exit 2; }
SF_RUN=build/swordfish-run
SF_BUILD=build/swordfish-build
export SWORDFISH_CONFIG="$PWD/build/swordfish-config"

N=${N:-200000}
CORES=${CORES:-"1 8"}

# Bounds, all overridable. These catch a HANG, not slowness: a measured run of
# the default N takes ~35s for the slowest implementation, so 600s is ~17x
# headroom. Raise RUN_TIMEOUT along with N rather than removing the bound.
RUN_TIMEOUT=${RUN_TIMEOUT:-600}
BUILD_TIMEOUT=${BUILD_TIMEOUT:-600}
CHECK_TIMEOUT=${CHECK_TIMEOUT:-120}

WORK=$(mktemp -d /tmp/sfvs.XXXXXX)

# Kill children BEFORE their parent. `timeout` is a wrapper: killing it alone
# re-parents the process it was bounding to init, which keeps running and keeps
# burning the cores this script is trying to measure. That exact mistake left a
# "killed" member alive in the Kafka chaos harness and produced three runs of
# meaningless results before it was noticed.
# A diagnostics channel that per-call redirection cannot swallow. The measured
# commands are noisy, so their invocations carry `>/dev/null 2>&1` -- which also
# silenced this script's OWN messages about them, and a hang reported only
# "warm-up failed", losing the one word that distinguishes a hang from a crash.
exec 3>&2

kill_tree() {
  local p=$1 c
  for c in $(pgrep -P "$p" 2>/dev/null); do kill_tree "$c"; done
  kill -9 "$p" 2>/dev/null
}
cleanup() {
  local rc=$?
  trap - EXIT INT TERM
  local c
  for c in $(pgrep -P $$ 2>/dev/null); do kill_tree "$c"; done
  rm -rf "$WORK"
  exit $rc
}
# INT and TERM as well as EXIT: with EXIT alone, a SIGTERM from an outer
# `timeout` or a CI runner kills the script and orphans whatever it was running.
trap cleanup EXIT INT TERM

# Every external command goes through this. Distinguishes a timeout from an
# ordinary failure, because they call for different responses: one means the
# thing hung, the other means it broke.
bounded() {  # bounded <seconds> <what> <command...>
  local secs=$1 what=$2; shift 2
  timeout -k 10 "$secs" "$@"
  local rc=$?
  if [ "$rc" -eq 124 ] || [ "$rc" -eq 137 ]; then
    echo "HUNG: $what exceeded ${secs}s and was killed" >&3
    return 99
  fi
  return $rc
}

MAPPING=${MAPPING:-'root = this
root.sq = this.n * this.n
root.tag = "n=%d".format(this.n)'}

# `interval` must be set: the reference defaults it to 1s, which would turn this
# into a two-day run. `count` is a MESSAGE count, not a batch count.
mkcfg() {  # mkcfg <count> <threads> <batch> <output-block>
  cat <<YAML
input:
  generate:
    count: $1
    interval: ""
    batch_size: $3
    mapping: 'root.n = counter()'
pipeline:
  threads: $2
  processors:
    - mapping: |
$(printf '%s\n' "$MAPPING" | sed 's/^/        /')
output:
  $4
YAML
}

median() { printf '%s\n' "$@" | sort -n | awk '{a[NR]=$1} END{print a[int((NR+1)/2)]}'; }

# Wall and CPU seconds for one run. CPU is user+sys, which is the figure that
# does not depend on how many cores the runtime decided to use.
#
# `time -o` rather than capturing stderr: /usr/bin/time reports on stderr, so
# the obvious `{ time cmd >/dev/null 2>/dev/null; } 2>&1` discards the
# measurement along with the command's own noise and every row reads 0.0000.
measure() {
  # Removed first: /usr/bin/time only writes this file if it actually ran. If
  # the command is missing or is killed by the timeout, a leftover file from the
  # PREVIOUS row would be read and reported as this row's numbers -- a wrong
  # answer that looks entirely plausible, which is worse than a visible failure.
  rm -f "$WORK/t"
  # `timeout` goes INSIDE `/usr/bin/time`, not outside, and the nesting is not
  # cosmetic. GNU time does not forward signals to its child: with timeout on
  # the outside a hang kills /usr/bin/time and leaves the benchmark process it
  # was wrapping re-parented to init -- still running, still burning the cores
  # this script exists to measure, and no longer a descendant the cleanup trap
  # can even find. Inverted, timeout owns the benchmark process directly and
  # kills it. Verified with a deliberately hanging command; the outer form left
  # an orphan every time.
  /usr/bin/time -f "%e %U %S" -o "$WORK/t" \
      timeout -k 10 "$RUN_TIMEOUT" "$@" >/dev/null 2>&1
  local rc=$?
  if [ "$rc" -eq 124 ] || [ "$rc" -eq 137 ]; then
    echo "HUNG: exceeded ${RUN_TIMEOUT}s and was killed: $*" >&3; return 1
  fi
  [ "$rc" -eq 0 ] || { echo "failed (exit $rc): $*" >&3; return 1; }
  [ -s "$WORK/t" ] || { echo "no measurement was written for: $*" >&3; return 1; }
  awk '{printf "%.4f %.4f", $1, $2 + $3}' "$WORK/t"
}

# ---- correctness first -------------------------------------------------------
mkcfg 20 1 1 'stdout: {}' > "$WORK/check.yaml"
bounded "$CHECK_TIMEOUT" "reference lint" "$RC" lint "$WORK/check.yaml" >/dev/null 2>&1 ||
    { echo "reference rejects the config" >&2; exit 1; }
# Piping into sort would hide a hang in the producer as a sort that never
# returns, so each side is bounded on its own and written to a file first.
bounded "$CHECK_TIMEOUT" "reference correctness run" \
    "$RC" run --disable-telemetry --log.level off "$WORK/check.yaml" \
    > "$WORK/ref.raw" 2>/dev/null || { echo "reference run failed" >&2; exit 1; }
bounded "$CHECK_TIMEOUT" "swordfish correctness run" \
    $SF_RUN --config "$WORK/check.yaml" --smp 1 --memory 512M --overprovisioned \
    > "$WORK/sf.raw" 2>/dev/null || { echo "swordfish run failed" >&2; exit 1; }
grep '^{' "$WORK/ref.raw" | sort > "$WORK/ref.out"
grep '^{' "$WORK/sf.raw"  | sort > "$WORK/sf.out"
n_ref=$(wc -l < "$WORK/ref.out")
[ "$n_ref" -eq 20 ] || { echo "reference emitted $n_ref of 20 messages" >&2; exit 1; }
if ! diff -q "$WORK/ref.out" "$WORK/sf.out" >/dev/null; then
  echo "outputs differ; refusing to report a throughput number" >&2
  diff "$WORK/ref.out" "$WORK/sf.out" | head -10 >&2
  exit 1
fi
echo "correctness: identical output on 20 messages, both implementations"
echo "workload:    $N messages, generate -> mapping -> drop"
echo "bounds:      ${RUN_TIMEOUT}s per run, ${BUILD_TIMEOUT}s per build (raise with N)"
echo

printf '%-6s %-30s %10s %10s %12s\n' cores implementation "wall (s)" "cpu (s)" "cpu-s / 1M"
run_row() {  # run_row <label> <cores> <command...>
  local label=$1 cores=$2; shift 2
  # The warm-up is bounded too: it runs the same work as a measured pass, so it
  # can hang in exactly the same way, and an unbounded warm-up would stall the
  # run before a single number was produced.
  bounded "$RUN_TIMEOUT" "$label warm-up" "$@" >/dev/null 2>&1 || {
      echo "warm-up failed for $label" >&2; exit 1; }
  local walls=() cpus=() w c r
  for _ in 1 2 3; do
    r=$(measure "$@") || { echo "measurement failed for $label" >&2; exit 1; }
    walls+=("${r%% *}"); cpus+=("${r##* }")
  done
  w=$(median "${walls[@]}"); c=$(median "${cpus[@]}")
  # A zero here means the measurement failed, not that the work was free.
  # Printing it would look like a result.
  awk -v w="$w" 'BEGIN{exit !(w+0 > 0)}' || {
      echo "measurement failed for $label (wall=$w)" >&2; exit 1; }
  printf '%-6s %-30s %10s %10s %12s\n' "$cores" "$label" "$w" "$c" \
      "$(awk -v c="$c" -v n="$N" 'BEGIN{printf "%.1f", c/n*1000000}')"
}

for cores in $CORES; do
  mkcfg "$N" "$cores" 1   'drop: {}' > "$WORK/b1.yaml"
  mkcfg "$N" "$cores" 500 'drop: {}' > "$WORK/b500.yaml"
  bounded "$BUILD_TIMEOUT" "swordfish build" \
      $SF_BUILD --no-cache "$WORK/b1.yaml" -o "$WORK/compiled" >/dev/null 2>&1 ||
      { echo "swordfish build failed" >&2; exit 1; }

  run_row "redpanda-connect"            "$cores" env GOMAXPROCS=$cores "$RC" run --disable-telemetry --log.level off "$WORK/b1.yaml"
  run_row "redpanda-connect (batch 500)" "$cores" env GOMAXPROCS=$cores "$RC" run --disable-telemetry --log.level off "$WORK/b500.yaml"
  run_row "swordfish (interpreted)"     "$cores" $SF_RUN --config "$WORK/b1.yaml" --smp "$cores" --memory 2G --overprovisioned
  run_row "swordfish (compiled)"        "$cores" "$WORK/compiled" --smp "$cores" --memory 2G --overprovisioned
  echo
done
