#!/usr/bin/env bash
# The `buffer` block: `none` and `memory`.
#
# A buffer is not a transformation, so "the same messages came out" is nearly
# worthless as a check -- a buffer that did nothing at all would pass it. What
# actually defines `memory` is WHEN the source is acknowledged: on arrival at the
# buffer rather than on delivery to the output. That is what these cases pin,
# using a `reject` output as the instrument: without a buffer an unacknowledged
# message is replayed for ever, and with one it is not, because it was already
# acked. The difference is observable as whether the process terminates.
set -uo pipefail
cd "$(dirname "$0")/.."
SF=${1:-build/swordfish-run}
RC=${2:-../redpanda-connect}
LINT=${3:-build/sfconfig}
export SWORDFISH_CONFIG="$PWD/build/swordfish-config"
WORK=$(mktemp -d /tmp/sfbuf.XXXXXX)
SEA_ARGS="--smp 1 --memory 512M --overprovisioned"
trap 'rm -rf "$WORK"' EXIT
trap 'rm -rf "$WORK"; exit 143' INT TERM

fail=0
note() { echo "ok   $1"; }
bad()  { echo "FAIL $1: $2" >&2; fail=1; }

# <name> <config> — output must match the reference, and neither may be empty.
cmp_ref() {
  local sf ref
  # shellcheck disable=SC2086
  sf=$(timeout 60 "$SF" --config "$2" $SEA_ARGS 2>/dev/null | sort | tr '\n' '|')
  if [ -z "$sf" ]; then bad "$1" "swordfish produced nothing"; return; fi
  if [ ! -x "$RC" ]; then note "$1 (no reference to compare against)"; return; fi
  ref=$(timeout 60 "$RC" run --log.level off "$2" 2>/dev/null | sort | tr '\n' '|')
  if [ -z "$ref" ]; then
    bad "$1" "the reference produced nothing, so it did not run this config"; return
  fi
  [ "$sf" = "$ref" ] && note "$1" || bad "$1" "swordfish '$sf' reference '$ref'"
}

cat > "$WORK/none.yaml" <<'YAML'
input: { generate: { count: 4, interval: 0s, mapping: 'root.n = counter()' } }
buffer: { none: {} }
output: { stdout: {} }
YAML
cat > "$WORK/mem.yaml" <<'YAML'
input: { generate: { count: 4, interval: 0s, mapping: 'root.n = counter()' } }
buffer:
  memory:
    limit: 100000
output: { stdout: {} }
YAML
cmp_ref "the none buffer passes messages through unchanged" "$WORK/none.yaml"
cmp_ref "the memory buffer passes messages through unchanged" "$WORK/mem.yaml"

# ---- the defining property ------------------------------------------------------
#
# `reject` nacks everything, and `auto_replay_nacks` (on by default) replays a
# nacked message for ever -- so WITHOUT a buffer neither implementation can ever
# finish. WITH one the source was acknowledged on arrival, the nack has nowhere
# to go, and both terminate. A memory buffer that forwarded the source's ack
# instead of acking early would hang here, and would pass every other check in
# this file.
cat > "$WORK/reject_nobuf.yaml" <<'YAML'
input: { generate: { count: 2, interval: 0s, mapping: 'root.n = counter()' } }
output: { reject: "always" }
YAML
cat > "$WORK/reject_buf.yaml" <<'YAML'
input: { generate: { count: 2, interval: 0s, mapping: 'root.n = counter()' } }
buffer:
  memory:
    limit: 100000
output: { reject: "always" }
YAML
terminates() {  # <config> -> 0 when the run ended on its own
  # shellcheck disable=SC2086
  timeout 8 "$SF" --config "$1" $SEA_ARGS >/dev/null 2>&1
  [ $? -ne 124 ]
}
ref_terminates() {
  timeout 8 "$RC" run --log.level off "$1" >/dev/null 2>&1
  [ $? -ne 124 ]
}
if ! terminates "$WORK/reject_nobuf.yaml"; then
  note "without a buffer a rejected message is replayed for ever"
else
  bad "without a buffer a rejected message is replayed for ever" "it terminated"
fi
if terminates "$WORK/reject_buf.yaml"; then
  note "with a memory buffer the source is acked on arrival, so it terminates"
else
  bad "with a memory buffer the source is acked on arrival, so it terminates" \
      "it never terminated"
fi
if [ -x "$RC" ]; then
  if ! ref_terminates "$WORK/reject_nobuf.yaml" && ref_terminates "$WORK/reject_buf.yaml"; then
    note "the reference draws the same distinction"
  else
    bad "the reference draws the same distinction" "it behaved differently"
  fi
fi

# ---- batch_policy ----------------------------------------------------------------
#
# `archive` after the buffer is what makes batch STRUCTURE visible: stdout
# flattens batches, so without it two batches of three and six batches of one
# print identically.
cat > "$WORK/batch.yaml" <<'YAML'
input: { generate: { count: 6, interval: 0s, mapping: 'root.n = counter()' } }
buffer:
  memory:
    limit: 100000
    batch_policy:
      enabled: true
      count: 3
output:
  stdout: {}
  processors:
    - archive: { format: json_array }
YAML
cmp_ref "the buffer's batch_policy groups messages on the way out" "$WORK/batch.yaml"

# A policy that is configured but NOT enabled must not batch -- the reference
# keeps the two separate and so must this.
sed 's/      enabled: true/      enabled: false/' "$WORK/batch.yaml" > "$WORK/unbatched.yaml"
cmp_ref "a batch_policy that is not enabled does not batch" "$WORK/unbatched.yaml"

# ---- an oversize batch -----------------------------------------------------------
#
# A batch larger than the whole buffer can never fit, so waiting for room would
# block for ever. It is dropped with a logged error rather than nacked: a nack
# is replayed straight back into the same impossible wait, which livelocked the
# first version -- one message retried 75 times in ten seconds. The reference
# drops it too, silently; swordfish says so.
cat > "$WORK/toobig.yaml" <<'YAML'
input:
  generate:
    count: 2
    interval: 0s
    mapping: 'root.pad = "0123456789012345678901234567890123456789"'
buffer:
  memory:
    limit: 10
output: { stdout: {} }
YAML
# shellcheck disable=SC2086
out=$(timeout 20 "$SF" --config "$WORK/toobig.yaml" $SEA_ARGS 2>&1); rc=$?
dropped=$(printf '%s\n' "$out" | grep -c 'cannot fit a limit')
emitted=$(printf '%s\n' "$out" | grep -c '^{')
if [ "$rc" -ne 124 ] && [ "$dropped" -eq 2 ] && [ "$emitted" -eq 0 ]; then
  note "an oversize batch is dropped once with an error, not retried for ever"
else
  bad "an oversize batch is dropped once with an error, not retried for ever" \
      "exit $rc, $dropped errors, $emitted messages"
fi
if [ -x "$RC" ]; then
  timeout 20 "$RC" run --log.level off "$WORK/toobig.yaml" >/dev/null 2>&1
  [ $? -ne 124 ] && note "the reference also runs to completion on it" \
                 || bad "the reference also runs to completion on it" "it hung"
fi

# ---- refusals ---------------------------------------------------------------------
refuses() {  # <name> <buffer block> <expected substring>
  local out
  printf 'input: { generate: { count: 1, interval: 0s, mapping: "root = 1" } }\n%s\noutput: { stdout: {} }\n' \
         "$2" > "$WORK/r.yaml"
  out=$(timeout 30 "$LINT" lint "$WORK/r.yaml" 2>&1)
  case "$out" in
    *"$3"*) note "$1" ;;
    *)      bad "$1" "expected '$3', got '$out'" ;;
  esac
}
refuses "an unimplemented buffer kind is named" \
        'buffer: { sqlite: { path: /tmp/x } }' \
        "buffer 'sqlite' is not implemented by swordfish"
refuses "a typo in the memory buffer is named" \
        'buffer: { memory: { limit: 10, nonsense: 1 } }' \
        "has no field 'nonsense'"
refuses "a zero limit is refused" \
        'buffer: { memory: { limit: 0 } }' \
        "must be greater than zero"
# The reference rejects this too, at start-up rather than at lint: "batch policy
# must have at least one active trigger". Catching it earlier is the only
# difference.
refuses "a batch_policy enabled with no trigger is refused" \
        'buffer: { memory: { batch_policy: { enabled: true } } }' \
        "sets no trigger"

# ---- the compiled path -------------------------------------------------------------
if [ -x build/swordfish-build ]; then
  # shellcheck disable=SC2086
  interp=$(timeout 60 "$SF" --config "$WORK/batch.yaml" $SEA_ARGS 2>/dev/null | sort | tr '\n' '|')
  if timeout 900 build/swordfish-build "$WORK/batch.yaml" -o "$WORK/batch.bin" \
        >"$WORK/build.log" 2>&1; then
    # shellcheck disable=SC2086
    compiled=$(timeout 60 "$WORK/batch.bin" $SEA_ARGS 2>/dev/null | sort | tr '\n' '|')
    [ -n "$interp" ] && [ "$interp" = "$compiled" ] \
      && note "a compiled binary buffers and batches identically" \
      || bad "a compiled binary buffers and batches identically" \
             "interpreted '$interp' compiled '$compiled'"
  else
    bad "a compiled binary buffers and batches identically" \
        "swordfish build failed: $(tail -3 "$WORK/build.log" | tr '\n' ' ')"
  fi
  # And the ack semantics survive compilation, which is the part that would be
  # easiest to lose: the buffer is a wrapper the emitter has to remember to write.
  if timeout 900 build/swordfish-build "$WORK/reject_buf.yaml" -o "$WORK/rb.bin" \
        >"$WORK/build2.log" 2>&1; then
    # shellcheck disable=SC2086
    timeout 20 "$WORK/rb.bin" $SEA_ARGS >/dev/null 2>&1
    [ $? -ne 124 ] \
      && note "and a compiled binary acks on arrival too" \
      || bad "and a compiled binary acks on arrival too" "it never terminated"
  else
    bad "and a compiled binary acks on arrival too" \
        "swordfish build failed: $(tail -3 "$WORK/build2.log" | tr '\n' ' ')"
  fi
fi

exit $fail
