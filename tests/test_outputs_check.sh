#!/usr/bin/env bash
# The composite outputs -- broker, switch, fallback, reject -- and an output's
# own `processors:`, compared against the REFERENCE binary when it is present.
#
# These cannot go in tests/fixtures/pipelines: `swordfish test` exercises a
# processor chain, and an output is only observable by running a pipeline. The
# same reasoning, and the same shape, as tests/test_inputs_check.sh.
#
# Two things this script is careful about, both of which have produced a
# confident false pass on this project before:
#   - an expectation of "" is never accepted. Two implementations that both
#     rejected the config agree on nothing useful.
#   - the reference's output is compared only when it produced some. A
#     reference that refused the config would otherwise look like agreement.
set -uo pipefail
cd "$(dirname "$0")/.."
SF=${1:-build/swordfish-run}
RC=${2:-../redpanda-connect}
export SWORDFISH_CONFIG="$PWD/build/swordfish-config"
WORK=$(mktemp -d /tmp/sfoutputs.XXXXXX)
trap 'rm -rf "$WORK"' EXIT

# Its own band, below ip_local_port_range, because ctest runs these scripts
# concurrently -- see tests/ports.sh for what sharing one cost.
SF_PORT_BAND=21500
# shellcheck source=tests/ports.sh
. "$(dirname "${BASH_SOURCE[0]}")/ports.sh"

fail=0
# Several of these fan one batch out to two outputs at once. Where both would be
# `stdout`, the REFERENCE interleaves them -- two goroutines writing fd 1
# concurrently, producing a line like `{"n":1}{"n":2}` -- which made the
# comparison flaky in a way that had nothing to do with either implementation.
# Those cases write to one file per child instead and the files are compared,
# which is deterministic and additionally proves each child got its own copy.
# <name> <config> <expected, |-separated, sorted>
check_files() {
  local got ref
  rm -f "$WORK"/o*.txt
  timeout 30 "$SF" --config "$2" --smp 1 --memory 512M --overprovisioned >/dev/null 2>&1
  got=$(cat "$WORK"/o*.txt 2>/dev/null | sort | tr '\n' '|')
  if [ -z "$3" ]; then
    echo "FAIL $1: the test itself expects nothing" >&2; fail=1; return
  fi
  if [ "$got" != "$3" ]; then
    echo "FAIL $1: expected '$3' got '$got'" >&2; fail=1; return
  fi
  if [ -x "$RC" ]; then
    rm -f "$WORK"/o*.txt
    timeout 30 "$RC" run --log.level off "$2" >/dev/null 2>&1
    ref=$(cat "$WORK"/o*.txt 2>/dev/null | sort | tr '\n' '|')
    if [ -z "$ref" ]; then
      echo "FAIL $1: the reference produced nothing, so it did not run this config" >&2
      fail=1; return
    fi
    if [ "$ref" != "$got" ]; then
      echo "FAIL $1: reference produced '$ref', swordfish '$got'" >&2; fail=1; return
    fi
  fi
  echo "ok   $1"
}

# <name> <config> <expected, |-separated, sorted>
check() {
  local got
  got=$(timeout 30 "$SF" --config "$2" --smp 1 --memory 512M --overprovisioned 2>/dev/null \
        | sort | tr '\n' '|')
  if [ -z "$3" ]; then
    echo "FAIL $1: the test itself expects nothing, which no run can distinguish "\
"from a rejected config" >&2; fail=1; return
  fi
  if [ "$got" != "$3" ]; then
    echo "FAIL $1: expected '$3' got '$got'" >&2; fail=1; return
  fi
  if [ -x "$RC" ]; then
    local ref
    ref=$(timeout 30 "$RC" run --log.level off "$2" 2>/dev/null | sort | tr '\n' '|')
    if [ -z "$ref" ]; then
      echo "FAIL $1: the reference produced nothing, so it did not run this config" >&2
      fail=1; return
    fi
    if [ "$ref" != "$got" ]; then
      echo "FAIL $1: reference produced '$ref', swordfish '$got'" >&2; fail=1; return
    fi
  fi
  echo "ok   $1"
}

# <name> <config> <expected substring of the error>
refuses() {
  local err
  err=$(timeout 30 "$SF" --config "$2" --smp 1 --memory 512M --overprovisioned 2>&1 >/dev/null)
  if ! grep -qF "$3" <<<"$err"; then
    echo "FAIL $1: expected an error containing '$3', got: $(tail -1 <<<"$err")" >&2
    fail=1; return
  fi
  echo "ok   $1"
}

gen() {  # <count> -- a source of {"n":1}..{"n":N}
  cat <<YAML
input:
  generate: { count: $1, interval: 0s, mapping: 'root.n = counter()' }
YAML
}

{ gen 3; cat <<YAML
output:
  broker:
    pattern: fan_out
    outputs:
      - file: { path: $WORK/o1.txt }
      - file: { path: $WORK/o2.txt }
YAML
} > "$WORK/fan_out.yaml"

# Sequential, so both children may share stdout without racing for it.
{ gen 2; cat <<'YAML'
output:
  broker:
    pattern: fan_out_sequential
    outputs: [ { stdout: {} }, { stdout: {} } ]
YAML
} > "$WORK/fan_out_sequential.yaml"

{ gen 4; cat <<YAML
output:
  broker:
    pattern: round_robin
    outputs:
      - file: { path: $WORK/o1.txt }
      - file: { path: $WORK/o2.txt }
YAML
} > "$WORK/round_robin.yaml"

{ gen 4; cat <<YAML
output:
  broker:
    pattern: greedy
    outputs:
      - file: { path: $WORK/o1.txt }
      - file: { path: $WORK/o2.txt }
YAML
} > "$WORK/greedy.yaml"

# `copies` repeats ONE child, so every copy would open the same file and clobber
# the others. Sequential and on stdout instead, which is race-free because only
# one copy writes at a time.
{ gen 2; cat <<'YAML'
output:
  broker:
    copies: 3
    pattern: fan_out_sequential
    outputs: [ { stdout: {} } ]
YAML
} > "$WORK/copies.yaml"

# Per-output processors: only the first child's copy is reshaped, which is what
# distinguishes an output's own processors from the pipeline's.
{ gen 2; cat <<YAML
output:
  broker:
    pattern: fan_out
    outputs:
      - file: { path: $WORK/o1.txt }
        processors:
          - mapping: 'root.tagged = this.n * 10'
      - file: { path: $WORK/o2.txt }
YAML
} > "$WORK/out_processors.yaml"

{ gen 4; cat <<YAML
output:
  switch:
    cases:
      - check: 'this.n % 2 == 0'
        output:
          file: { path: $WORK/o1.txt }
          processors: [ { mapping: 'root.even = this.n' } ]
      - output:
          file: { path: $WORK/o2.txt }
          processors: [ { mapping: 'root.odd = this.n' } ]
YAML
} > "$WORK/switch.yaml"

# `continue: true` sends a matching message on to the next case as well.
{ gen 3; cat <<YAML
output:
  switch:
    cases:
      - check: 'this.n >= 2'
        continue: true
        output:
          file: { path: $WORK/o1.txt }
          processors: [ { mapping: 'root.big = this.n' } ]
      - output:
          file: { path: $WORK/o2.txt }
          processors: [ { mapping: 'root.all = this.n' } ]
YAML
} > "$WORK/switch_continue.yaml"

# A message matching no case is dropped, and that is a SUCCESS rather than a
# nack -- so the run terminates rather than replaying for ever.
{ gen 3; cat <<'YAML'
output:
  switch:
    cases:
      - check: 'this.n > 100'
        output: { drop: {} }
      - check: 'this.n == 1'
        output: { stdout: {} }
YAML
} > "$WORK/switch_unmatched.yaml"

# The first tier always fails, so every message reaches the second one carrying
# `fallback_error`.
{ gen 2; cat <<'YAML'
output:
  fallback:
    - reject: "tier one refused it"
    - stdout: {}
      processors:
        - mapping: |
            root.n = this.n
            root.why = meta("fallback_error")
YAML
} > "$WORK/fallback.yaml"

# Nesting: a switch whose case is a broker whose child is a fallback.
{ gen 2; cat <<YAML
output:
  switch:
    cases:
      - check: 'this.n == 1'
        output:
          broker:
            pattern: fan_out
            outputs:
              - fallback:
                  - reject: "no"
                  - file: { path: $WORK/o1.txt }
              - drop: {}
      - output:
          file: { path: $WORK/o2.txt }
YAML
} > "$WORK/nested.yaml"

check_files "broker fan_out writes to every child"     "$WORK/fan_out.yaml"            '{"n":1}|{"n":1}|{"n":2}|{"n":2}|{"n":3}|{"n":3}|'
check       "broker fan_out_sequential writes to every child" "$WORK/fan_out_sequential.yaml" '{"n":1}|{"n":1}|{"n":2}|{"n":2}|'
check_files "broker round_robin writes each batch once" "$WORK/round_robin.yaml"       '{"n":1}|{"n":2}|{"n":3}|{"n":4}|'
check_files "broker greedy writes each batch once"      "$WORK/greedy.yaml"            '{"n":1}|{"n":2}|{"n":3}|{"n":4}|'
check       "broker copies duplicates the child list"   "$WORK/copies.yaml"            '{"n":1}|{"n":1}|{"n":1}|{"n":2}|{"n":2}|{"n":2}|'
check_files "an output's processors apply to it alone"  "$WORK/out_processors.yaml"    '{"n":1}|{"n":2}|{"tagged":10}|{"tagged":20}|'
check_files "switch routes by check"                    "$WORK/switch.yaml"            '{"even":2}|{"even":4}|{"odd":1}|{"odd":3}|'
check_files "switch continue tests the next case too"   "$WORK/switch_continue.yaml"   '{"all":1}|{"all":2}|{"all":3}|{"big":2}|{"big":3}|'
check       "switch drops what matches no case"         "$WORK/switch_unmatched.yaml"  '{"n":1}|'
check       "fallback moves on and records why"         "$WORK/fallback.yaml"          '{"n":1,"why":"tier one refused it"}|{"n":2,"why":"tier one refused it"}|'
check_files "composites nest"                           "$WORK/nested.yaml"            '{"n":1}|{"n":2}|'

# ---- batching -----------------------------------------------------------------
#
# `archive: json_array` after the policy is what makes batch STRUCTURE visible:
# stdout flattens batches, so without it a batch of three and three batches of
# one print identically -- and "every output matched" while the policy did
# nothing is precisely the false pass this file exists to avoid.
#
# `broker` is used as the carrier because it is one of only three outputs the
# reference lets carry a policy at all; `stdout` is refused there, and here.
bt_cfg() {  # <count> <policy lines>
  cat <<YAML
input:
  generate: { count: $1, interval: 0s, mapping: 'root.n = counter()' }
output:
  broker:
    outputs: [ { stdout: {} } ]
    batching:
$2
      processors:
        - archive: { format: json_array }
YAML
}

bt_cfg 6 '      count: 3'                > "$WORK/bt_count.yaml"
bt_cfg 6 '      byte_size: 20'           > "$WORK/bt_size.yaml"
bt_cfg 6 '      check: this.n % 3 == 0'  > "$WORK/bt_check.yaml"
bt_cfg 6 '      period: 800ms'           > "$WORK/bt_period.yaml"

check "batching by count flushes at the count"        "$WORK/bt_count.yaml" \
      '[{"n":1},{"n":2},{"n":3}]|[{"n":4},{"n":5},{"n":6}]|'
# 20 bytes is reached by the third {"n":N}, and the message that crosses the
# threshold belongs to the batch it completed -- not to the next one.
check "batching by byte_size counts the crossing message" "$WORK/bt_size.yaml" \
      '[{"n":1},{"n":2},{"n":3}]|[{"n":4},{"n":5},{"n":6}]|'
check "batching by check ends the batch on a match"   "$WORK/bt_check.yaml" \
      '[{"n":1},{"n":2},{"n":3}]|[{"n":4},{"n":5},{"n":6}]|'
# Six messages, no count and no size trigger, so the PERIOD is the only thing
# that can flush them -- and it does, 800ms after the first arrives. What this
# pins is that a policy with a period alone still delivers, which is not
# obvious: nothing in the message stream ever triggers it.
check "a period-only policy flushes on its timer" "$WORK/bt_period.yaml" \
      '[{"n":1},{"n":2},{"n":3},{"n":4},{"n":5},{"n":6}]|'

# The other half, and it is a WARNING rather than a feature: a policy whose only
# triggers are `count`, `byte_size` or `check` cannot flush a LEFTOVER on its
# own, and a pipeline that ends with one held back never terminates by itself.
#
# The chain is: the last partial batch is released by the output's drain; the
# drain runs once the input reports end-of-input; and an auto-replaying input --
# the default -- does not report end-of-input while an ack is outstanding, which
# the held batch's is. redpanda-connect 4.107.2 does not terminate on the
# identical config either, so the non-termination is FIDELITY rather than a
# swordfish defect. The advice for a real config is the same in both: pair
# `count` with a `period`.
#
# The two do differ on what a SIGNAL does, and that difference is asserted
# rather than smoothed over. `timeout` sends SIGTERM at the deadline: swordfish
# treats it as a drain, so the held batch is written before it goes; the
# reference discards it. Swordfish's behaviour is the better one, so it is
# pinned HERE -- if it ever regressed to the reference's, this is what would
# say so.
bt_cfg 5 '      count: 2' > "$WORK/bt_leftover.yaml"
bt_done='[{"n":1},{"n":2}]|[{"n":3},{"n":4}]|'
bt_name="a leftover batch with no period does not terminate on its own"
# `timeout` exits 124 when it fires, and that IS the expected outcome here. The
# completed batches must still have been written, so a run that hung without
# producing them would not pass either.
got=$(timeout 6 "$SF" --config "$WORK/bt_leftover.yaml" \
        --smp 1 --memory 512M --overprovisioned 2>/dev/null | tr '\n' '|')
rc=$?
if [ "$rc" -eq 124 ] && [ "$got" = "${bt_done}"'[{"n":5}]|' ]; then
  echo "ok   $bt_name, and SIGTERM drains it rather than dropping it"
else
  echo "FAIL $bt_name: exit $rc, got '$got'" >&2; fail=1
fi
if [ -x "$RC" ]; then
  ref=$(timeout 6 "$RC" run --log.level off "$WORK/bt_leftover.yaml" 2>/dev/null | tr '\n' '|')
  refrc=$?
  if [ "$refrc" -eq 124 ] && [ "$ref" = "$bt_done" ]; then
    echo "ok   the reference also does not terminate, and drops the held batch"
  else
    echo "FAIL the reference also does not terminate, and drops the held batch: exit $refrc, got '$ref'" >&2
    fail=1
  fi
fi

cat > "$WORK/bt_stdout.yaml" <<'YAML'
input: { generate: { count: 1, interval: 0s, mapping: 'root = {}' } }
output:
  stdout:
    batching:
      count: 2
YAML
refuses "batching on an output that does not take one is refused" \
        "$WORK/bt_stdout.yaml" "invalid when the output type is 'stdout'"

# The POSITION is the reference's too, and it was the opposite of this until
# 2026-09-06: `batching` goes inside the component's body, and the sibling
# position -- which swordfish used to require -- is refused there.
cat > "$WORK/bt_beside.yaml" <<'YAML'
input: { generate: { count: 1, interval: 0s, mapping: 'root = {}' } }
output:
  broker:
    outputs: [ { drop: {} } ]
  batching:
    count: 2
YAML
refuses "batching beside the kind rather than inside it is refused" \
        "$WORK/bt_beside.yaml" "belongs inside the 'broker' body"

# `output.file.path` INTERPOLATES, which is the main reason to use this output:
# it splits a stream into a file per key. It was taken literally until
# 2026-09-06, writing a single file actually named `o${! json("id") }.txt`.
#
# The assertion is on the FILE NAMES, not the contents: check_files concatenates
# and sorts what it finds, so one file holding all three lines gives byte-identical
# output to three files holding one each -- the broken state would have passed it.
cat > "$WORK/file_interp.yaml" <<YAML
input:
  generate:
    count: 3
    interval: 0s
    mapping: 'root.id = ["a","b","c"].index(counter() - 1)'
output:
  file:
    path: '$WORK/oi_\${! json("id") }.txt'
YAML
names_written() {  # <runner...>  -> sorted basenames of what appeared
  rm -f "$WORK"/oi_*.txt
  "$@" >/dev/null 2>&1
  (cd "$WORK" && ls oi_*.txt 2>/dev/null | sort | tr '\n' ' ')
}
sf_names=$(names_written timeout 30 "$SF" --config "$WORK/file_interp.yaml" --smp 1 \
                         --memory 512M --overprovisioned)
if [ "$sf_names" != "oi_a.txt oi_b.txt oi_c.txt " ]; then
  echo "FAIL an interpolated file path writes one file per value: got '$sf_names'" >&2
  fail=1
elif [ -x "$RC" ]; then
  rc_names=$(names_written timeout 30 "$RC" run --log.level off "$WORK/file_interp.yaml")
  if [ "$rc_names" != "$sf_names" ]; then
    echo "FAIL an interpolated file path writes one file per value: reference wrote"\
" '$rc_names', swordfish '$sf_names'" >&2; fail=1
  else
    echo "ok   an interpolated file path writes one file per value"
  fi
else
  echo "ok   an interpolated file path writes one file per value"
fi

# Configuration that must be refused by name rather than approximated. Each of
# these is a case where guessing would produce plausible, wrong behaviour.
cat > "$WORK/bad_pattern.yaml" <<'YAML'
input: { generate: { count: 1, interval: 0s, mapping: 'root = {}' } }
output:
  broker:
    pattern: fan_ouy
    outputs: [ { stdout: {} } ]
YAML
cat > "$WORK/empty_reject.yaml" <<'YAML'
input: { generate: { count: 1, interval: 0s, mapping: 'root = {}' } }
output:
  reject: ""
YAML
cat > "$WORK/one_case.yaml" <<'YAML'
input: { generate: { count: 1, interval: 0s, mapping: 'root = {}' } }
output:
  switch:
    cases:
      - output: { stdout: {} }
YAML
cat > "$WORK/retry_reject.yaml" <<'YAML'
input: { generate: { count: 1, interval: 0s, mapping: 'root = {}' } }
output:
  switch:
    retry_until_success: true
    cases:
      - check: 'true'
        output: { stdout: {} }
      - output: { reject: "no" }
YAML
cat > "$WORK/case_typo.yaml" <<'YAML'
input: { generate: { count: 1, interval: 0s, mapping: 'root = {}' } }
output:
  switch:
    cases:
      - checks: 'true'
        output: { stdout: {} }
      - output: { stdout: {} }
YAML

refuses "an unknown broker pattern is named"     "$WORK/bad_pattern.yaml"  "must be one of"
refuses "an empty reject message is refused"     "$WORK/empty_reject.yaml" "requires an error message"
refuses "a one-case switch is refused"           "$WORK/one_case.yaml"     "at least two cases"
refuses "reject under retry_until_success"       "$WORK/retry_reject.yaml" "retry_until_success"
refuses "a typo in a switch case is named"       "$WORK/case_typo.yaml"    "has no field 'checks'"

# ---- stdout: a failed write must not be reported as a delivery ---------------
#
# `stdout_output::write_batch` used to `break` out of its write loop on ANY error
# and return a ready future -- a success. EPIPE, ENOSPC and a short write all
# reported delivery, so stream::do_write counted an ack and every composite above
# was disarmed: `fallback` never reached its next tier, `retry_output` never
# retried, `fan_out` never nacked. Measured before the fix: 5000 messages into
# `fallback: [stdout, file]` piped to `head -c 100` exited 0 claiming acks=5000,
# with 100 bytes delivered and the fallback file EMPTY.
#
# Benthos's own stdout output returns the write error
# (internal/impl/io/output_stdout.go), so propagating is the upstream behaviour.
# The reference BINARY cannot show it: Go kills the process with SIGPIPE (exit
# 141) before the component's error path runs, so its fallback tier stays empty.
# That is asserted below rather than smoothed over.
cat > "$WORK/so_fb.yaml" <<YAML
input:
  generate:
    count: 5000
    interval: ""
    mapping: |
      root.n = counter()
      root.pad = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
output:
  fallback:
    - stdout: {}
    - file: { path: $WORK/so_fb.jsonl, codec: lines }
YAML
rm -f "$WORK/so_fb.jsonl"
timeout 90 "$SF" --config "$WORK/so_fb.yaml" --smp 1 --memory 512M --overprovisioned \
    2>/dev/null | head -c 100 > /dev/null
so_lines=$( { wc -l < "$WORK/so_fb.jsonl"; } 2>/dev/null || echo 0)
# Not "> 0": the point is that essentially EVERYTHING the pipe refused reached
# the next tier, and a handful of messages fit in the pipe buffer before the
# reader closed it.
if [ "$so_lines" -ge 4900 ]; then
  echo "ok   a stdout write that fails falls through to the next tier ($so_lines/5000)"
else
  echo "FAIL a stdout write that fails falls through to the next tier: only $so_lines of 5000" >&2
  fail=1
fi
if [ -x "$RC" ]; then
  rm -f "$WORK/so_fb.jsonl"
  timeout 90 "$RC" run --log.level off "$WORK/so_fb.yaml" 2>/dev/null | head -c 100 > /dev/null
  ref_lines=$( { wc -l < "$WORK/so_fb.jsonl"; } 2>/dev/null || echo 0)
  if [ "$ref_lines" -eq 0 ]; then
    echo "ok   and the reference cannot: SIGPIPE kills it before the fallback runs"
  else
    echo "NOTE the reference now falls through too ($ref_lines lines); the divergence" \
         "documented above no longer holds and the comment needs revisiting" >&2
  fi
fi

# ---- stdout must not block the reactor --------------------------------------
#
# `::write(1, ...)` ran directly on the shard, so a reader that stopped consuming
# put the reactor in the kernel for as long as it liked. Measured before the fix:
# `/ready` never answered (curl timed out at 5s against an endpoint that had
# logged itself as listening) and a SIGTERM was not seen for 3.0s. Worse, with a
# reader that stopped for good, SIGTERM left the process alive through a 25s poll
# and it needed SIGKILL -- `shutdown_timeout` could not help, because the forcing
# timer is itself a reactor task. The write is on a dedicated OS thread now.
SO_PORT=$(free_port)
cat > "$WORK/so_stall.yaml" <<YAML
shutdown_timeout: 3s
input:
  generate:
    count: 2000000
    interval: 1ms
    mapping: 'root.n = counter()'
output: { stdout: {} }
http:
  enabled: true
  address: 127.0.0.1:$SO_PORT
YAML
rm -f "$WORK/so_fifo"; mkfifo "$WORK/so_fifo"
# Reads 200 bytes and then stops for good, holding the descriptor open.
( exec 3< "$WORK/so_fifo"; head -c 200 <&3 > /dev/null; sleep 600 ) &
SO_READER=$!
"$SF" --config "$WORK/so_stall.yaml" --smp 1 --memory 512M --overprovisioned \
    > "$WORK/so_fifo" 2>/dev/null &
SO_PID=$!
sleep 6
so_code=$(curl -s -o /dev/null -w '%{http_code}' -m 5 "http://127.0.0.1:$SO_PORT/ready" 2>/dev/null)
so_t0=$(date +%s%N)
kill -TERM "$SO_PID" 2>/dev/null
for _i in $(seq 1 250); do kill -0 "$SO_PID" 2>/dev/null || break; sleep 0.1; done
so_ms=$(( ($(date +%s%N) - so_t0) / 1000000 ))
so_alive=no; kill -0 "$SO_PID" 2>/dev/null && so_alive=yes
kill -9 "$SO_PID" "$SO_READER" 2>/dev/null; wait "$SO_PID" "$SO_READER" 2>/dev/null
rm -f "$WORK/so_fifo"
if [ "$so_code" = 200 ]; then
  echo "ok   the reactor stays free while stdout is backpressured (/ready answered 200)"
else
  echo "FAIL the reactor stays free while stdout is backpressured: /ready gave '$so_code'" >&2
  fail=1
fi
# 5s is far outside the ~0.1s both implementations take and far inside the 25s a
# blocked reactor produced, so it cannot be failed by scheduler noise.
if [ "$so_alive" = no ] && [ "$so_ms" -lt 5000 ]; then
  echo "ok   SIGTERM stops a pipeline whose reader has stopped for good (${so_ms}ms)"
else
  echo "FAIL SIGTERM stops a pipeline whose reader has stopped for good:" \
       "alive=$so_alive after ${so_ms}ms" >&2
  fail=1
fi

# ---- the end-of-run summary must not race across shards ----------------------
#
# It was `invoke_on_all([&total](...){ total += s.stats(); })`: the lambda runs on
# every shard but `total` lives in shard 0's frame, and `stats::operator+=` is a
# run of non-atomic read-modify-writes, so whole shards' counts were lost.
# Measured before the fix at --smp 16: 3 runs in 30 reported in=18750, exactly one
# shard's 1250 missing, every counter dropping together. --smp 1 was always right,
# which is why it survived. Ten runs at a high shard count is enough to catch a
# regression: the old code failed roughly one run in eight here.
cat > "$WORK/so_many.yaml" <<'YAML'
input: { generate: { count: 20000, interval: "", mapping: 'root.n = counter()' } }
output: { drop: {} }
YAML
so_smp=8
[ "$(nproc)" -ge 16 ] && so_smp=16
so_bad=0
for _i in $(seq 1 10); do
  got=$(timeout 60 "$SF" --config "$WORK/so_many.yaml" --smp "$so_smp" --memory 2G \
          --overprovisioned 2>&1 | grep -oE 'in=[0-9]+' | tail -1)
  [ "$got" = "in=20000" ] || { so_bad=$((so_bad + 1)); so_last=$got; }
done
if [ "$so_bad" -eq 0 ]; then
  echo "ok   the end-of-run summary is exact at --smp $so_smp (10 runs)"
else
  echo "FAIL the end-of-run summary is exact at --smp $so_smp: $so_bad of 10 wrong," \
       "last '${so_last:-}'" >&2
  fail=1
fi

# ---- error_handling.strict reaches the OUTPUT ---------------------------------
#
# `strict_errors` was read in exactly one place -- the main pipeline -- so every
# other path that can mark a message ignored it. Three of them, all measured:
# an output's own `processors:`, a `batching.processors:` list, and a message
# marked before the output by an INPUT processor when the pipeline has no
# processors of its own to trigger the check. In each case swordfish wrote the
# message and acked it (`in=2 out=2 acks=2 nacks=0`) where the reference wrote
# nothing and rejected it by name.
#
# Each case runs the SAME config twice, once with strict on and once off, and
# requires empty output under strict AND full output without it. An
# expectation of "" on its own is worthless here: a config swordfish refuses
# produces exactly the same empty stdout, and the first draft of this gate did
# pass that way -- its `batching` block was in a position all three
# implementations reject. The non-strict half is what proves the config runs.
#
# Both implementations then RETRY the nacked batch for ever, because
# auto_replay_nacks defaults to true, so these are bounded and judged on stdout
# rather than on exit status.
strict_case() {  # <name> <body-file>
  local name=$1 body=$2 on off ref
  { echo 'error_handling: { strict: true }';  cat "$body"; } > "$WORK/sc_on.yaml"
  { echo 'error_handling: { strict: false }'; cat "$body"; } > "$WORK/sc_off.yaml"
  on=$(timeout 12 "$SF" --config "$WORK/sc_on.yaml" --smp 1 --memory 512M \
         --overprovisioned 2>/dev/null | sort | tr '\n' '|')
  off=$(timeout 12 "$SF" --config "$WORK/sc_off.yaml" --smp 1 --memory 512M \
          --overprovisioned 2>/dev/null | sort | tr '\n' '|')
  if [ "$off" != '{"i":1}|{"i":2}|' ]; then
    echo "FAIL $name: the non-strict control wrote '$off', so the config does not run" \
         "and the strict result proves nothing" >&2
    fail=1
    return
  fi
  if [ -n "$on" ]; then
    echo "FAIL $name: swordfish wrote '$on'; a strict rejection must write nothing" >&2
    fail=1
    return
  fi
  if [ -x "$RC" ]; then
    ref=$(timeout 12 "$RC" run --log.level off "$WORK/sc_on.yaml" 2>/dev/null | tr '\n' '|')
    if [ -n "$ref" ]; then
      echo "FAIL $name: the reference wrote '$ref' under strict, so the expectation" \
           "here is wrong and needs revisiting" >&2
      fail=1
      return
    fi
  fi
  echo "ok   $name"
}
cat > "$WORK/sb_out" <<'YAML'
input: { generate: { count: 2, interval: "", mapping: 'root.i = counter()' } }
output:
  stdout: {}
  processors:
    - mapping: 'root = throw("bad")'
YAML
strict_case "strict rejects a message its output.processors failed" "$WORK/sb_out"
cat > "$WORK/sb_in" <<'YAML'
input:
  generate: { count: 2, interval: "", mapping: 'root.i = counter()' }
  processors:
    - mapping: 'root = throw("bad-from-input")'
output: { stdout: {} }
YAML
strict_case "strict rejects one marked upstream with no output processors" "$WORK/sb_in"
# `batching` belongs INSIDE a component's body; beside a bare `stdout` all three
# implementations refuse it, which is how the vacuous first draft was caught.
cat > "$WORK/sb_batch" <<'YAML'
input: { generate: { count: 2, interval: "", mapping: 'root.i = counter()' } }
output:
  broker:
    outputs: [ { stdout: {} } ]
    batching:
      count: 2
      processors:
        - mapping: 'root = throw("bad-from-batching")'
YAML
strict_case "strict rejects a message its batching.processors failed" "$WORK/sb_batch"

# ---- a splitting processor over a batching policy -----------------------------
#
# `processed_output` awaited each sub-batch before issuing the next. That did not
# merely serialise them, it DEADLOCKED: a batched output cannot resolve a write
# until its policy triggers, and the messages that would trigger it sit in the
# sub-batches the loop has not issued yet. Measured: swordfish printed nothing
# and had to be SIGKILLed where the reference printed six lines and exited 0.
# They go out concurrently now, as the pipeline and fan_out already did.
cat > "$WORK/sp.yaml" <<'YAML'
input:
  broker:
    inputs:
      - generate: { count: 6, interval: "", mapping: 'root.n = counter()' }
    batching: { count: 3 }
output:
  broker:
    pattern: round_robin
    outputs: [ { stdout: {} }, { stdout: {} } ]
    batching: { count: 3 }
  processors:
    - split: { size: 1 }
YAML
sp_got=$(timeout 25 "$SF" --config "$WORK/sp.yaml" --smp 1 --memory 512M \
           --overprovisioned 2>/dev/null | sort | tr '\n' '|')
sp_want='{"n":1}|{"n":2}|{"n":3}|{"n":4}|{"n":5}|{"n":6}|'
if [ "$sp_got" = "$sp_want" ]; then
  echo "ok   a splitting processor over a batching policy still completes"
else
  echo "FAIL a splitting processor over a batching policy still completes: got '$sp_got'" >&2
  fail=1
fi

# The other half of that fix, and the easier one to get wrong: the concurrency
# must be BOUNDED by what the inner output permits. Seastar's output_stream is
# documented "all methods must be called sequentially [...] no method may be
# invoked before the previous method's returned future is resolved", and
# file_output holds a std::map iterator across its write, so a terminal output
# whose max_in_flight() is 1 must still receive one sub-batch at a time. Sixty
# messages split to one each: interleaved writes show up as short, duplicated or
# malformed lines rather than as a crash, which is why this counts them.
cat > "$WORK/fsp.yaml" <<YAML
input:
  broker:
    inputs:
      - generate: { count: 60, interval: "", mapping: 'root.n = counter()' }
    batching: { count: 20 }
output:
  file: { path: $WORK/fsp.out, codec: lines }
  processors:
    - split: { size: 1 }
YAML
rm -f "$WORK/fsp.out"
timeout 60 "$SF" --config "$WORK/fsp.yaml" --smp 1 --memory 512M --overprovisioned \
    >/dev/null 2>&1
fsp_lines=$( { wc -l < "$WORK/fsp.out"; } 2>/dev/null || echo 0)
fsp_distinct=$( { sort -u "$WORK/fsp.out" | wc -l; } 2>/dev/null || echo 0)
fsp_bad=$(grep -cvE '^\{"n":[0-9]+\}$' "$WORK/fsp.out" 2>/dev/null || true)
if [ "$fsp_lines" = 60 ] && [ "$fsp_distinct" = 60 ] && [ "${fsp_bad:-0}" = 0 ]; then
  echo "ok   concurrent sub-batches stay within the inner output's max_in_flight"
else
  echo "FAIL concurrent sub-batches stay within the inner output's max_in_flight:" \
       "$fsp_lines lines, $fsp_distinct distinct, ${fsp_bad:-0} malformed" >&2
  fail=1
fi

# ---- a flush during the graceful window gets the HARD-stop source -------------
#
# batched_output discarded the source write_batch was handed and flushed with the
# connect-time one, which is `_drain` -- aborted at the START of shutdown. So the
# batching processors and the inner write ran against an already-aborted source
# during the phase whose whole purpose is to let outstanding work finish.
#
# A and B are identical but for WHERE the 5s sleep sits, and the assertion is
# that they now behave the SAME. Judged on timing because the sleep processor
# forwards its batch even when aborted, so the message count alone cannot see it:
# before the fix A came back in ~0.1s and B in ~3.7s.
graceful() {  # <config> -> milliseconds from SIGTERM to exit
  local cfg=$1 pid t0
  "$SF" --config "$cfg" --smp 1 --memory 512M --overprovisioned >/dev/null 2>&1 &
  pid=$!
  sleep 1.5
  t0=$(date +%s%N)
  kill -TERM "$pid" 2>/dev/null
  for _i in $(seq 1 300); do kill -0 "$pid" 2>/dev/null || break; sleep 0.1; done
  kill -9 "$pid" 2>/dev/null; wait "$pid" 2>/dev/null
  echo $(( ($(date +%s%N) - t0) / 1000000 ))
}
cat > "$WORK/gA.yaml" <<'YAML'
input: { generate: { count: 2, interval: "", mapping: 'root.n = counter()' } }
output:
  broker:
    pattern: fan_out
    outputs: [ { stdout: {} }, { drop: {} } ]
    batching:
      count: 2
      processors: [ { sleep: { duration: 5s } } ]
YAML
cat > "$WORK/gB.yaml" <<'YAML'
input: { generate: { count: 2, interval: "", mapping: 'root.n = counter()' } }
output:
  broker:
    pattern: fan_out
    outputs: [ { stdout: {} }, { drop: {} } ]
  processors: [ { sleep: { duration: 5s } } ]
YAML
ga_ms=$(graceful "$WORK/gA.yaml")
gb_ms=$(graceful "$WORK/gB.yaml")
# Both must run the sleep out. 2000ms is far above the ~100ms an aborted sleep
# returns in and far below the ~3700ms a completed one takes.
if [ "$ga_ms" -gt 2000 ] && [ "$gb_ms" -gt 2000 ]; then
  echo "ok   a flush during the graceful window is not pre-aborted (${ga_ms}ms vs ${gb_ms}ms)"
else
  echo "FAIL a flush during the graceful window is not pre-aborted:" \
       "batching.processors ${ga_ms}ms, output.processors ${gb_ms}ms" >&2
  fail=1
fi

exit $fail
