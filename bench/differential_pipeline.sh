#!/usr/bin/env bash
# The differential gate at PIPELINE level:
# the same config, run interpreted and compiled, must produce byte-identical
# output. This is what makes "compilation is an optimisation, never a change in
# behaviour" enforceable rather than aspirational.
set -uo pipefail
cd "$(dirname "$0")/.."
export SWORDFISH_CONFIG="$PWD/build/swordfish-config"
SEA_ARGS="--smp ${SMP:-1} --memory 512M --overprovisioned"
WORK=$(mktemp -d /tmp/sfdiff.XXXXXX)

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
  rm -rf "$WORK"
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


printf '{"user":"ada","score":95,"tier":"gold"}\n{"user":"bob","score":40}\nnot json\n' > "$WORK/in.jsonl"
# A batch of three, for the from()/from_all() case: those methods are meaningless
# outside one, and a single-message batch cannot tell a rebinding of `this` from
# a correct one.
printf '[{"id":10},{"id":20},{"id":30}]\n' > "$WORK/from.json"
# Three files for the `paths` case: one named directly and two matched by a glob,
# so both halves of the expansion are compared between the backends.
printf '{"f":"a1"}\n{"f":"a2"}\n' > "$WORK/dp_a.txt"
printf '{"f":"b1"}\n'             > "$WORK/dp_b1.txt"
printf '{"f":"b2"}\n'             > "$WORK/dp_b2.txt"

configs=()

mk() { local n=$1; shift; printf '%s\n' "$*" > "$WORK/$n.yaml"; configs+=("$n"); }

mk simple 'input: { generate: { count: 5, interval: 0s, mapping: "root.id = counter()" } }
pipeline:
  processors:
    - mapping: |
        root = this
        root.sq = this.id * this.id
output: { stdout: {} }'

mk strings 'input: { generate: { count: 4, interval: 0s, mapping: "root.n = counter()" } }
pipeline:
  processors:
    - mapping: |
        root.label = "row-%d".format(this.n)
        root.tag = this.n.string().re_replace_all("[0-9]", "#")
output: { stdout: {} }'

mk combinators "input: { file: { paths: [ $WORK/in.jsonl ] } }
pipeline:
  processors:
    - try:
        - mapping: |
            root = this
            root.grade = if this.score >= 90 { \"A\" } else { \"B\" }
    - catch:
        - mapping: 'root.error = \"bad\"'
    - switch:
        - check: 'this.exists(\"tier\")'
          processors:
            - mapping: |
                root = this
                root.badge = this.tier.uppercase()
    - for_each:
        - mapping: |
            root = this
            root.seen = true
output: { stdout: {} }"

# `input.file.paths` -- the REFERENCE's spelling, a list, with globs. This case
# exists because the emitter rendered the swordfish-only singular `path`
# instead, so a config written the reference's way ran correctly interpreted and
# compiled to a binary that opened "" and died. Nothing else in this corpus used
# the plural form: `combinators` above uses `path:`, which is why two backends
# disagreeing about the input went unnoticed. A scanner is named explicitly for
# the same reason -- the scanner name is emitted now rather than a constructor
# call chosen per backend.
mk file_paths "input:
  file:
    paths: [ $WORK/dp_a.txt, $WORK/dp_b*.txt ]
    scanner: { lines: {} }
output: { stdout: {} }"

mk file_to_the_end "input:
  file:
    paths: [ $WORK/dp_a.txt ]
    scanner: { to_the_end: {} }
pipeline:
  processors:
    - mapping: 'root.whole = content().string().length()'
output: { stdout: {} }"

# A configured, NESTED scanner. The emitter renders a scanner_spec as C++ that
# rebuilds it, so a field it forgot -- or a child it failed to recurse into --
# shows up here as a different split and nowhere else. `switch` is the deepest
# of them and carries a per-candidate pattern beside the child.
printf 'name,age\nada,36\n' > "$WORK/dp_c.csv"
mk file_scanner_switch "input:
  file:
    paths: [ $WORK/dp_c.csv, $WORK/dp_a.txt ]
    scanner:
      switch:
        - re_match_name: '[.]csv'
          scanner: { csv: { parse_header_row: true } }
        - scanner: { json_documents: {} }
output: { stdout: {} }"

# The `memory` buffer wraps the INPUT in the interpreted path and has to be
# written by the emitter as the same wrapper. Its batch policy carries a check
# that becomes a generated function, so a backend that skipped the wrap or the
# check would split the stream differently -- visible here through `archive` and
# nowhere else.
mk buffer_memory 'input: { generate: { count: 6, interval: 0s, mapping: "root.n = counter()" } }
buffer:
  memory:
    limit: 100000
    batch_policy:
      enabled: true
      count: 4
      check: this.n == 2
output:
  stdout: {}
  processors:
    - archive: { format: json_array }'

# A NESTED input. The emitter had no case for `broker` or `sequence` at all --
# both fell through to the file branch and compiled to `make_files_input({},...)`
# -- and this corpus could not see it, because every case above has a top-level
# input. Its `batching` rides along: the acks fan IN there, several sources to
# one assembled batch, which is the mirror of the output side.
mk input_broker_batching "input:
  broker:
    inputs:
      - file: { paths: [ $WORK/dp_a.txt ] }
      - file: { paths: [ $WORK/dp_b1.txt ] }
    batching:
      count: 2
      period: 300ms
output:
  stdout: {}
  processors:
    - archive: { format: json_array }"

# A `batching` policy NESTED inside a composite output. The stream sizes its
# in-flight semaphore once from the outermost output, and the composites returned
# a flat 1, so the inner batcher could never reach its count: twenty messages at
# `count: 5` produced nothing and the process could not be stopped, where
# redpanda-connect wrote all twenty. Both backends build the same tree, so both
# are pinned.
mk nested_batching 'input: { generate: { count: 20, interval: 0s, mapping: "root.n = counter()" } }
output:
  broker:
    pattern: fan_out
    outputs:
      - broker:
          pattern: round_robin
          outputs: [ { stdout: {} } ]
          batching: { count: 5 }
      - drop: {}'

# A `resource:` processor that is ITSELF a dedupe or a rate_limit. The inlined
# component used to be carried past the resolution walk, so its own cache and
# rate-limit references were never looked up: the config lint-ed clean and then
# aborted at start-up with "cache 'c' was not resolved against cache_resources",
# on a config the reference runs. Both backends resolve before construction.
#
# Single-quoted, like the rate_limit case below: `mk` passes its argument through
# the shell, and a double-quoted `${! ... }` is a bad substitution there.
mk resource_inlining 'cache_resources:
  - label: c
    memory: {}
rate_limit_resources:
  - label: slow
    local: { count: 100, interval: 1s }
processor_resources:
  - label: dd
    dedupe: { cache: c, key: "${! content() }" }
  - label: rl
    rate_limit: { resource: slow }
input:
  generate: { count: 4, interval: 0s, mapping: "root = [\"a\",\"a\",\"b\",\"a\"].index(counter() - 1)" }
pipeline:
  processors:
    - resource: dd
    - resource: rl
output:
  stdout: {}'

# `processors:` beside an input's kind, which the reference gives every input and
# which swordfish refused outright ("`input` must be a single-key object") until
# 2026-09-06. It is a DECORATOR in both backends and the wrapper order matters --
# outside auto_replay_nacks, outside a broker's batching -- so the two are pinned
# against each other here. `split` makes the processors fan one read out into
# several batches, which is the path the ack_group exists for.
mk input_processors "input:
  label: source
  generate: { count: 3, interval: 0s, mapping: 'root.n = counter()' }
  processors:
    - mapping: 'root.doubled = this.n * 2'
    - split: { size: 1 }
output:
  stdout: {}"

mk input_sequence "input:
  sequence:
    inputs:
      - file: { paths: [ $WORK/dp_b1.txt ] }
      - file: { paths: [ $WORK/dp_b2.txt ] }
output: { stdout: {} }"

# A cache is emitted as a spec that generated code hands to the same factory, so
# a field the emitter forgot shows up here as different deduplication and
# nowhere else. `lru` with a cap and init_values exercises both halves.
mk cache_lru 'input: { generate: { count: 6, interval: 0s, mapping: "root.k = counter() % 3" } }
pipeline:
  processors:
    - dedupe: { cache: c, key: "${! json(\"k\") }" }
output: { stdout: {} }
cache_resources:
  - label: c
    lru: { cap: 2, init_values: { "1": x } }'

# A nested input whose output NACKS. The emitter used to wrap broker/sequence in
# `make_auto_retry_input` while build_input_factory never did, so the compiled
# binary replayed the nacked batch for ever where `swordfish run` exited. Nothing
# else here nacks, which is why an extra retry layer was invisible; the loop's
# hang detection is what turns it into a failure rather than two empty outputs.
mk broker_nack_terminates 'input:
  broker:
    inputs:
      - generate: { count: 1, interval: 0s, mapping: "root.id = 1", auto_replay_nacks: false }
output: { reject: "boom" }'

mk noop_sleep 'input: { generate: { count: 3, interval: 0s, mapping: "root.i = counter()" } }
pipeline:
  processors:
    - noop: {}
    - mapping: "root = this"
output: { stdout: {} }'

# The composite outputs. They are emitted by a different code path from the
# built-in three -- emit_output() recurses, and so does build_output_factory --
# so a divergence between the two modes would live HERE and nowhere the
# Bloblang-level differential can see it.
mk out_switch 'input: { generate: { count: 4, interval: 0s, mapping: "root.n = counter()" } }
output:
  switch:
    cases:
      - check: "this.n % 2 == 0"
        output:
          stdout: {}
          processors: [ { mapping: "root.even = this.n" } ]
      - output:
          stdout: {}
          processors: [ { mapping: "root.odd = this.n" } ]'

mk out_broker_sequential 'input: { generate: { count: 2, interval: 0s, mapping: "root.n = counter()" } }
output:
  broker:
    pattern: fan_out_sequential
    outputs: [ { stdout: {} }, { stdout: {} } ]'

# `workflow` is the processor whose two backends were allowed to disagree the
# longest: `swordfish build` refused it outright while `swordfish run` accepted
# it, which is the one thing the compiler thesis cannot tolerate. The emitter
# renders the branch tree as nested factories, and this is what holds it to the
# interpreter -- groups, ordering, a conditional branch and the outcome record
# all at once.
mk workflow 'input: { generate: { count: 3, interval: 0s, mapping: "root.n = counter()" } }
pipeline:
  processors:
    - workflow:
        meta_path: audit.ran
        order:
          - [ double, tag ]
          - [ total ]
        branches:
          double:
            request_map: "root = this.n"
            processors: [ { mapping: "root = {\"v\": this * 2}" } ]
            result_map: "root.doubled = this.v"
          tag:
            request_map: "root = if this.n % 2 == 0 { this.n } else { deleted() }"
            processors: [ { mapping: "root = {\"v\": \"even\"}" } ]
            result_map: "root.tag = this.v"
          total:
            request_map: "root = this"
            processors: [ { mapping: "root = {\"v\": this.doubled}" } ]
            result_map: "root.total = this.v"
output: { stdout: {} }'

# Batching is emitted as a wrapper around the output expression, and its `check`
# becomes a generated function like any other query. `archive` after it is what
# makes the batch STRUCTURE visible on stdout -- without it a batch of three and
# three batches of one print identically, which is the trap this gate exists to
# avoid.
#
# `period` is REQUIRED here, and its absence is what made this case hang for
# 600s in both modes until the loop above learned to notice. A policy whose only
# triggers are `count` and `check` cannot flush a leftover: the last partial
# batch is released by the output's drain, the drain runs when the input reports
# end-of-input, and an auto-replaying input does not report it while an ack is
# outstanding -- which the held batch is. redpanda-connect 4.107.2 hangs on the
# identical config, so this is fidelity rather than a swordfish defect, and the
# fix for a real config is the same: pair `count` with a `period`.
mk out_batching 'input: { generate: { count: 6, interval: 0s, mapping: "root.n = counter()" } }
output:
  broker:
    outputs: [ { stdout: {} } ]
    batching:
      count: 2
      check: this.n == 5
      period: 250ms
      processors:
        - archive: { format: json_array }'

# A rate limit is resolved from a DOCUMENT-level block, so the emitter has to
# carry the resolved count and interval into generated code rather than the
# label alone -- a path nothing else in this corpus exercises. Both modes must
# also agree on SHARING: the two processors below name one label, so a backend
# that gave each its own limit would let twice as much through. Kept wide enough
# that it never actually blocks, because this gate compares output rather than
# time.
mk rate_limit 'input: { generate: { count: 4, interval: 0s, mapping: "root.n = counter()" } }
pipeline:
  processors:
    - rate_limit: { resource: wide }
    - mapping: "root.a = this.n"
    - rate_limit: { resource: wide }
    - mapping: "root.b = this.a * 10"
output: { stdout: {} }
rate_limit_resources:
  - label: wide
    local: { count: 1000, interval: 1s }'

mk out_fallback 'input: { generate: { count: 2, interval: 0s, mapping: "root.n = counter()" } }
output:
  fallback:
    - reject: "first tier always refuses"
    - stdout: {}
      processors:
        - mapping: |
            root.n = this.n
            root.why = meta("fallback_error")'

# A lambda's parameter must not leak into the caller's scope, and must be put
# back when the body throws. The emitted binding used `ctx.vars[p]`, whose
# operator[] INSERTS a null when the name was unbound and then wrote that null
# back, and restored on a straight-line statement a throwing body skipped
# entirely. Compiled answered `null` where interpreted answered `"unset"`, and
# clobbered a caller's `$zz` with the last loop element. Both shapes are here
# because they fail differently.
mk lambda_scope 'input: { generate: { count: 1, interval: 0s, mapping: "root.v = 1" } }
pipeline:
  processors:
    - mapping: |
        let zz = "orig"
        root.a = [1,2].map_each(zz -> zz)
        root.unbound = $nothing_here.catch("unset")
        root.thrown = [1,2].map_each(zz -> throw("boom")).catch("caught")
        root.after = $zz
output: { stdout: {} }'

# `from()` and `from_all()` move the batch INDEX and nothing else. The
# reference sets only ctx.Index (benthos query/methods.go) and its docs say the
# supporting functions are content, json and meta -- so `this.id.from_all()`
# repeats the CURRENT message id, while `json("id").from_all()` collects each
# message's. The interpreter rebound `this` as well and was the side that
# disagreed with both the compiled backend and the reference.
mk from_all_this "input: { file: { paths: [ $WORK/from.json ] } }
pipeline:
  processors:
    - unarchive: { format: json_array }
    - mapping: |
        root.mine = this.id
        root.first = this.id.from(0)
        root.all = this.id.from_all()
        root.jall = json(\"id\").from_all()
output: { stdout: {} }"

# Evaluation ORDER, where the two backends had drifted apart because C++ does
# not sequence call arguments. Both operands of a binary operator were passed
# straight as arguments and GCC evaluated them right to left, so when both sides
# failed the compiled binary reported the RIGHT operand's error and the
# interpreter the left's -- and unspecified order is not even stable across
# compilers. A computed object key sat inside the o_.set() call, so it ran after
# the value and not at all when the value was omitted.
mk eval_order 'input: { generate: { count: 1, interval: 0s, mapping: "root.v = 1" } }
pipeline:
  processors:
    - mapping: |
        root.plus = (throw("LEFT") + throw("RIGHT")).catch(e -> e)
        root.less = (throw("LT") < throw("RT")).catch(e -> e)
        root.keyside = {throw("KEYSIDE"): throw("VALSIDE")}.catch(e -> e)
output: { stdout: {} }'

# A SPLITTING processor in an output's `processors:`, over a `batching` policy.
# processed_output awaited each sub-batch before issuing the next, which did not
# merely serialise them but deadlocked: a batched output cannot resolve a write
# until its policy triggers, and the messages that would trigger it sit in the
# sub-batches the loop has not issued yet. Both modes printed nothing and had to
# be killed, so this case would have shown as a HANG rather than a mismatch --
# which is precisely why the harness above refuses to call two hangs agreement.
mk out_split_batching 'input:
  broker:
    inputs:
      - generate: { count: 6, interval: 0s, mapping: "root.n = counter()" }
    batching: { count: 3 }
output:
  broker:
    pattern: round_robin
    outputs: [ { stdout: {} }, { stdout: {} } ]
    batching: { count: 3 }
  processors:
    - split: { size: 1 }'

# `error_handling.strict` reaching an output's own processors, in the direction
# that still produces output. The strict direction cannot go here: a strict
# rejection nacks, an auto-replaying input retries for ever, and both modes then
# hang -- correctly, and identically to the reference, but a hang is not a
# comparison. The strict half lives in tests/test_outputs_check.sh, which judges
# it against the reference and against a non-strict control.
mk out_proc_marks 'input: { generate: { count: 2, interval: 0s, mapping: "root.i = counter()" } }
output:
  stdout: {}
  processors:
    - mapping: |
        root = if this.i == 1 { throw("marked") } else { this }'

# Runs one side and leaves the comparable lines in <outfile>, returning the
# BOUNDED status rather than the pipeline's.
#
# This exists because `x=$(bounded ...)` throws the status away, and that is not
# a cosmetic loss: a config that HUNG in both modes was once reported as
# "identical", because two runs killed at the timeout produced the same
# truncated output. Two hangs are not agreement, and a gate that says they are
# is worse than no gate at all.
run_side() {  # <label> <outfile> <command...>
  local label=$1 out=$2; shift 2
  # shellcheck disable=SC2086
  bounded "$label" "$@" >"$out.raw" 2>/dev/null
  local rc=$?
  grep '^[{"]' "$out.raw" 2>/dev/null | sort > "$out"
  return $rc
}

pass=0; fail=0
for c in "${configs[@]}"; do
  # shellcheck disable=SC2086
  run_side "interpreted $c" "$WORK/$c.interp" \
           ./build/swordfish-run --config "$WORK/$c.yaml" $SEA_ARGS
  # 99 is `bounded`'s "I killed it". Any other non-zero status is left to the
  # comparison below, because a config may legitimately exit non-zero on both
  # sides and still agree.
  if [ $? -eq 99 ]; then
    fail=$((fail+1)); echo "HUNG: $c (interpreted) -- not counted as agreement"; continue
  fi
  interp=$(cat "$WORK/$c.interp")
  if ! bounded "build $c" ./build/swordfish-build --no-cache "$WORK/$c.yaml" -o "$WORK/$c.bin" >/dev/null 2>"$WORK/$c.err"; then
    fail=$((fail+1)); echo "BUILD FAILED: $c"; sed 's/^/    /' "$WORK/$c.err" | head -5; continue
  fi
  # shellcheck disable=SC2086
  run_side "compiled $c" "$WORK/$c.compiled" "$WORK/$c.bin" $SEA_ARGS
  if [ $? -eq 99 ]; then
    fail=$((fail+1)); echo "HUNG: $c (compiled) -- not counted as agreement"; continue
  fi
  compiled=$(cat "$WORK/$c.compiled")
  if [[ "$interp" == "$compiled" ]]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    echo "MISMATCH: $c"
    diff <(echo "$interp") <(echo "$compiled") | head -8 | sed 's/^/    /'
  fi
done
echo "pipeline differential: $pass identical, $fail mismatched"
[[ $fail -eq 0 ]]
