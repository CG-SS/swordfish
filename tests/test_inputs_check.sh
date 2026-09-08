#!/usr/bin/env bash
# broker / sequence / file-paths, compared against the REFERENCE binary when it
# is present. These cannot go in tests/fixtures/pipelines: `swordfish test`
# exercises processors, and an input is only observable by running a pipeline.
set -uo pipefail
cd "$(dirname "$0")/.."
SF=${1:-build/swordfish-run}
RC=${2:-../redpanda-connect}
export SWORDFISH_CONFIG="$PWD/build/swordfish-config"
WORK=$(mktemp -d /tmp/sfinputs.XXXXXX)
trap 'rm -rf "$WORK"' EXIT
printf 'alpha\nbeta\n' > "$WORK/f1.txt"
printf 'gamma\n'       > "$WORK/f2.txt"

fail=0
check() {  # <name> <config> <expected, |-separated, sorted>
  local got
  got=$("$SF" --config "$2" --smp 1 --memory 512M --overprovisioned 2>/dev/null \
        | grep -v '^INFO' | sort | tr '\n' '|')
  if [ "$got" != "$3" ]; then
    echo "FAIL $1: expected '$3' got '$got'" >&2; fail=1; return
  fi
  # When the reference is available, it decides what correct means.
  if [ -x "$RC" ]; then
    local ref
    ref=$(timeout 30 "$RC" run --log.level off "$2" 2>/dev/null | sort | tr '\n' '|')
    if [ -n "$ref" ] && [ "$ref" != "$got" ]; then
      echo "FAIL $1: reference produced '$ref', swordfish '$got'" >&2; fail=1; return
    fi
  fi
  echo "ok   $1"
}

cat > "$WORK/seq.yaml" <<YAML
input:
  sequence:
    inputs:
      - file: { paths: [ $WORK/f1.txt ] }
      - file: { paths: [ $WORK/f2.txt ] }
output:
  stdout: {}
YAML
cat > "$WORK/brk.yaml" <<YAML
input:
  broker:
    inputs:
      - file: { paths: [ $WORK/f1.txt ] }
      - file: { paths: [ $WORK/f2.txt ] }
output:
  stdout: {}
YAML
cat > "$WORK/glob.yaml" <<YAML
input:
  file:
    paths: [ "$WORK/f*.txt" ]
output:
  stdout: {}
YAML
cat > "$WORK/copies.yaml" <<YAML
input:
  broker:
    copies: 2
    inputs:
      - file: { paths: [ $WORK/f2.txt ] }
output:
  stdout: {}
YAML
check "sequence reads inputs in order"   "$WORK/seq.yaml"    'alpha|beta|gamma|'
check "broker reads every input"         "$WORK/brk.yaml"    'alpha|beta|gamma|'
check "file paths expands globs"         "$WORK/glob.yaml"   'alpha|beta|gamma|'
check "broker copies duplicates a child" "$WORK/copies.yaml" 'gamma|gamma|'

# A broker of FINITE sources that all become ready at once, which is the case
# the file-based cases above cannot reach: a `file` yields one message at a time,
# so the broker's ready queue never held more than the batch it was returning.
#
# It held them here, and threw them away. `read_batch` returned end-of-input the
# moment every child was exhausted, without draining what it had already read --
# so this config delivered THREE messages of four, deterministically, and exited
# 0 reporting success. Every message had been read and acked; one was dropped
# between the read and the return. Found on 2026-09-07 while testing templates,
# which is to say: not by any of the cases written to test the broker.
cat > "$WORK/brk_finite.yaml" <<'YAML'
input:
  broker:
    inputs:
      - generate: { count: 2, interval: 0s, mapping: 'root.k = "a"' }
      - generate: { count: 2, interval: 0s, mapping: 'root.k = "b"' }
output:
  stdout: {}
YAML
check "a broker of finite sources loses nothing" "$WORK/brk_finite.yaml" \
      '{"k":"a"}|{"k":"a"}|{"k":"b"}|{"k":"b"}|'

# ---- scanners -----------------------------------------------------------------
#
# Two of the reference's twelve scanners are implemented, and the other ten used
# to fall through to `lines` in SILENCE -- in both backends identically, so the
# differential gate could never see it. Asking for `csv` and being given raw
# lines changes the shape of the data, which is worse than any missing feature,
# so each way of getting it wrong is pinned here by its message.
cat > "$WORK/tte.yaml" <<YAML
input:
  file:
    paths: [ $WORK/f1.txt ]
    scanner: { to_the_end: {} }
pipeline:
  processors:
    - mapping: 'root.n = content().string().length()'
output:
  stdout: {}
YAML
# "alpha\nbeta\n" is eleven bytes as ONE message; as `lines` it would be two
# messages of five and four. The length is what makes the difference visible.
check "the to_the_end scanner reads the whole file as one message" \
      "$WORK/tte.yaml" '{"n":11}|'

refuses() {  # <name> <config body> <expected substring>
  local out
  printf '%s\n' "$2" > "$WORK/sc.yaml"
  out=$(build/sfconfig lint "$WORK/sc.yaml" 2>&1)
  case "$out" in
    *"$3"*) echo "ok   $1" ;;
    *)      echo "FAIL $1: expected '$3', got '$out'" >&2; fail=1 ;;
  esac
}

# `avro` used to stand here as the one unimplemented scanner, proving that an
# unimplemented name and a misspelt one are different errors. All twelve are
# built now, so the unimplemented half has no name left to test in THIS build --
# it is still reachable, and still tested, in a tree configured with
# -DSWORDFISH_ENABLE_AVRO=OFF. What is left to assert here is that the scanner
# the case used to reject is now accepted.
if build/sfconfig lint /dev/stdin <<YAML >/dev/null 2>&1
input: { file: { paths: [ $WORK/f1.txt ], scanner: { avro: {} } } }
output: { stdout: {} }
YAML
then
  echo "ok   the avro scanner is accepted, not refused as unimplemented"
else
  echo "FAIL the avro scanner is accepted, not refused as unimplemented" >&2; fail=1
fi

refuses "a scanner name that is not a scanner at all says so differently" \
  "input: { file: { paths: [ $WORK/f1.txt ], scanner: { nonsense: {} } } }
output: { stdout: {} }" \
  "scanner 'nonsense' is not a scanner"

# The reference's `scanner` field is a component, so it takes a single-key
# object. A bare string is rejected there too -- at start-up rather than at lint
# -- and used to be silently ignored here, leaving `lines` in place.
refuses "a bare-string scanner is refused rather than ignored" \
  "input: { file: { paths: [ $WORK/f1.txt ], scanner: to_the_end } }
output: { stdout: {} }" \
  "must be a single-key object"

# `lines` takes options now; `to_the_end` genuinely takes none, so a field under
# it would be silently dropped and is refused instead.
refuses "options under a scanner that takes none are refused" \
  "input: { file: { paths: [ $WORK/f1.txt ], scanner: { to_the_end: { nonsense: 1 } } } }
output: { stdout: {} }" \
  "has no field 'nonsense'"

refuses "a typo in a scanner's own options is named" \
  "input: { file: { paths: [ $WORK/f1.txt ], scanner: { csv: { parse_headers: true } } } }
output: { stdout: {} }" \
  "has no field 'parse_headers'"

# ---- input-side batching --------------------------------------------------------
#
# `input.broker.batching` combines several source batches into one, the mirror of
# the output side -- and the acks run the other way: several sources fan IN to one
# assembled batch, so each source is acked only once every batch it fed has
# landed. `archive` is what makes the grouping visible; without it stdout flattens
# the batches and a policy that did nothing would look identical.
printf '1\n2\n3\n4\n5\n6\n' > "$WORK/nums.txt"

cat > "$WORK/ibatch.yaml" <<YAML
input:
  broker:
    inputs:
      - file: { paths: [ $WORK/nums.txt ] }
    batching:
      count: 2
output:
  stdout: {}
  processors:
    - archive: { format: json_array }
YAML
check "input.broker batching groups source batches" "$WORK/ibatch.yaml" '[1,2]|[3,4]|[5,6]|'

# A `period` alone, with nothing else able to trigger: this is what proves the
# timer runs, since no count is ever reached.
cat > "$WORK/iperiod.yaml" <<YAML
input:
  broker:
    inputs:
      - file: { paths: [ $WORK/nums.txt ] }
    batching:
      period: 400ms
output:
  stdout: {}
  processors:
    - archive: { format: json_array }
YAML
check "input.broker batching flushes on its period" "$WORK/iperiod.yaml" '[1,2,3,4,5,6]|'

# A leftover with no period hangs, exactly as it does on the OUTPUT side and
# exactly as the reference does: the flush waits for end of input, and an
# auto-replaying source will not report that while the held batch is unacked.
# Pinned so neither the hang nor a silent divergence from it goes unnoticed.
cat > "$WORK/ileft.yaml" <<YAML
input:
  broker:
    inputs:
      - file: { paths: [ $WORK/nums.txt ] }
    batching:
      count: 4
output:
  stdout: {}
  processors:
    - archive: { format: json_array }
YAML
got=$(timeout 6 "$SF" --config "$WORK/ileft.yaml" --smp 1 --memory 512M --overprovisioned \
        2>/dev/null | tr '\n' '|')
rc=$?
if [ "$rc" -eq 124 ] && [ "$got" = '[1,2,3,4]|' ]; then
  echo "ok   an input-side leftover with no period hangs, as it does in the reference"
else
  echo "FAIL an input-side leftover with no period hangs: exit $rc, got '$got'" >&2
  fail=1
fi
if [ -x "$RC" ]; then
  ref=$(timeout 6 "$RC" run --log.level off "$WORK/ileft.yaml" 2>/dev/null | tr '\n' '|')
  refrc=$?
  if [ "$refrc" -eq 124 ] && [ "$ref" = "$got" ]; then
    echo "ok   the reference hangs on it identically"
  else
    echo "FAIL the reference hangs on it identically: exit $refrc, got '$ref'" >&2
    fail=1
  fi
fi

# ---- broker and sequence must COMPILE --------------------------------------------
#
# The emitter had no case for either, so both fell through to the file branch and
# compiled to `make_files_input({}, ...)` -- a binary that read nothing and said
# nothing about it, while `swordfish run` on the same config worked. Nothing in
# the pipeline differential's corpus used a nested input, which is why it went
# unseen.
if [ -x build/swordfish-build ]; then
  for c in seq brk ibatch; do
    exp=$(timeout 60 "$SF" --config "$WORK/$c.yaml" --smp 1 --memory 512M \
            --overprovisioned 2>/dev/null | sort | tr '\n' '|')
    if [ -z "$exp" ]; then
      echo "FAIL compiled $c matches the interpreter: the interpreter produced nothing" >&2
      fail=1; continue
    fi
    if timeout 900 build/swordfish-build "$WORK/$c.yaml" -o "$WORK/$c.bin" \
          >"$WORK/$c.buildlog" 2>&1; then
      got=$(timeout 60 "$WORK/$c.bin" --smp 1 --memory 512M --overprovisioned \
              2>/dev/null | sort | tr '\n' '|')
      if [ "$got" = "$exp" ]; then
        echo "ok   compiled $c matches the interpreter"
      else
        echo "FAIL compiled $c matches the interpreter: expected '$exp' got '$got'" >&2
        fail=1
      fi
    else
      echo "FAIL compiled $c: build failed: $(tail -3 "$WORK/$c.buildlog" | tr '\n' ' ')" >&2
      fail=1
    fi
  done
fi

# ---- the hand-parsed built-ins validate their keys ---------------------------------
#
# `generate`, `file`, `stdin`, `stdout`, `file` out and `drop` bypass spec_of and
# are parsed by hand, so nothing checked their keys: a misspelt `mapping` left
# the DEFAULT `root = {}` running and the user's mapping never ran at all. That
# is the worst shape of silent approximation -- it changes what the pipeline
# does, not merely what it accepts -- and the reference names every one of these.
#
# Fields the reference HAS and swordfish does not are refused by their own name,
# separately from a typo: "not implemented" and "no such field" send the reader
# to different places.
mk_typo() {  # <name> <yaml> <expected substring>
  local out
  printf '%s\n' "$2" > "$WORK/typo.yaml"
  out=$(timeout 30 build/sfconfig lint "$WORK/typo.yaml" 2>&1)
  case "$out" in
    *"$3"*) echo "ok   $1" ;;
    *)      echo "FAIL $1: expected '$3', got '$out'" >&2; fail=1 ;;
  esac
}

# The one that motivated all of this: swordfish used to print {} for this config.
mk_typo "a misspelt generate mapping is named, not silently defaulted" \
  'input: { generate: { count: 1, interval: 0s, mapping: "root = 1", mappping: x } }
output: { drop: {} }' \
  "input.generate has no field 'mappping'"
mk_typo "generate batch_size is named as unimplemented, not as a typo" \
  'input: { generate: { count: 1, interval: 0s, mapping: "root = 1", batch_size: 4 } }
output: { drop: {} }' \
  "input.generate \`batch_size\` is not implemented"
mk_typo "file delete_on_finish is named as unimplemented" \
  "input: { file: { paths: [ $WORK/f1.txt ], delete_on_finish: true } }
output: { drop: {} }" \
  "input.file \`delete_on_finish\` is not implemented"
# `path` singular was a swordfish-only alias the reference lints as unrecognised
# -- the same trap `poll_period` and `reconnect_period` were. It is gone.
mk_typo "the swordfish-only singular file path is gone" \
  "input: { file: { path: $WORK/f1.txt } }
output: { drop: {} }" \
  "input.file has no field 'path'"
mk_typo "a typo on stdin is named" \
  'input: { stdin: { bogus: 1 } }
output: { drop: {} }' \
  "input.stdin has no field 'bogus'"
mk_typo "a typo on stdout is named" \
  'input: { generate: { count: 1, interval: 0s, mapping: "root = 1" } }
output: { stdout: { bogus: 1 } }' \
  "output.stdout has no field 'bogus'"
# swordfish writes newline-delimited messages and nothing else, so any other
# codec would change the bytes on the wire for a config that asked otherwise.
mk_typo "a codec swordfish does not write is refused" \
  'input: { generate: { count: 1, interval: 0s, mapping: "root = 1" } }
output: { stdout: { codec: "delim:;" } }' \
  "is not implemented by swordfish"
# ---- the deprecated `codec` field ---------------------------------------------------
#
# `codec` is what `scanner` replaced. The reference still ACCEPTS it -- its own
# test corpus uses `codec: lines` -- so refusing it rejected real Benthos
# configs, which is how this was found: the 105-file corpus went from 3 clean to
# 1 the moment the built-ins started validating their keys.
#
# It is a small chained language, and it chains the way the scanners nest, so it
# translates onto them rather than needing a second implementation. Each form is
# compared against the reference on the same bytes.
printf 'name,age\nada,36\n' > "$WORK/cd.csv"
printf 'a\nb\n' > "$WORK/cd.txt"
gzip -c "$WORK/cd.txt" > "$WORK/cd.txt.gz"
codec_case() {  # <name> <codec> <file> <expected>
  cat > "$WORK/cd.yaml" <<YAML
input:
  file:
    paths: [ $3 ]
    codec: "$2"
output: { stdout: {} }
YAML
  local got ref
  got=$(timeout 60 "$SF" --config "$WORK/cd.yaml" --smp 1 --memory 512M \
          --overprovisioned 2>/dev/null | sort | tr '\n' '|')
  if [ "$got" != "$4" ]; then
    echo "FAIL $1: expected '$4' got '$got'" >&2; fail=1; return
  fi
  if [ -x "$RC" ]; then
    ref=$(timeout 60 "$RC" run --log.level off "$WORK/cd.yaml" 2>/dev/null | sort | tr '\n' '|')
    if [ -z "$ref" ] || [ "$ref" != "$got" ]; then
      echo "FAIL $1: reference produced '$ref'" >&2; fail=1; return
    fi
  fi
  echo "ok   $1"
}
codec_case "codec lines"      lines        "$WORK/cd.txt"    'a|b|'
codec_case "codec all-bytes"  all-bytes    "$WORK/cd.txt"    'a|b|'
codec_case "codec csv"        csv          "$WORK/cd.csv"    '{"age":"36","name":"ada"}|'
codec_case "codec delim:x"    'delim:,'    "$WORK/cd.csv"    '36|ada|age|name|'
codec_case "codec chunker:x"  'chunker:2'  "$WORK/cd.txt"    'a|b|'
# The chaining prefixes map onto the wrapping scanners, which is the whole reason
# the translation is a translation rather than a table.
codec_case "codec gzip/lines chains"  'gzip/lines' "$WORK/cd.txt.gz" 'a|b|'

mk_typo "a codec form with no scanner behind it is named" \
  "input: { file: { paths: [ $WORK/cd.txt ], codec: multipart } }
output: { drop: {} }" \
  "\`codec: multipart\` form is not implemented"
mk_typo "setting both codec and scanner is refused" \
  "input: { file: { paths: [ $WORK/cd.txt ], codec: lines, scanner: { lines: {} } } }
output: { drop: {} }" \
  "sets both \`scanner\` and the deprecated \`codec\`"

# An empty expectation would match anything, so acceptance gets its own check
# rather than a mk_typo with an empty needle -- that is the "expected nothing"
# false pass this project has been caught by before.
printf 'input: { generate: { count: 1, interval: 0s, mapping: "root = 1" } }\noutput: { file: { path: %s/codec_ok.txt, codec: lines } }\n' \
       "$WORK" > "$WORK/codec_ok.yaml"
if out=$(timeout 30 build/sfconfig lint "$WORK/codec_ok.yaml" 2>&1) && [ -z "$out" ]; then
  echo "ok   the default codec is still accepted"
else
  echo "FAIL the default codec is still accepted: lint said '$out'" >&2; fail=1
fi

# ---- broker and sequence validate their keys ----------------------------------------
#
# The only two built-in inputs that validated nothing. `auto_replay_nacks` here
# looked accepted, did nothing in `swordfish run`, and made the compiled binary
# wrap the broker in an auto-retry the interpreter never built.
mk_typo "input.broker rejects an unknown key" \
  "input: { broker: { inputs: [ { generate: { count: 1, interval: 0s, mapping: 'root = 1' } } ], copiez: 4 } }
output: { drop: {} }" \
  "input.broker has no field 'copiez'"
mk_typo "input.broker rejects auto_replay_nacks, as the reference does" \
  "input: { broker: { inputs: [ { generate: { count: 1, interval: 0s, mapping: 'root = 1' } } ], auto_replay_nacks: false } }
output: { drop: {} }" \
  "input.broker has no field 'auto_replay_nacks'"
mk_typo "input.sequence rejects an unknown key" \
  "input: { sequence: { inputs: [ { generate: { count: 1, interval: 0s, mapping: 'root = 1' } } ], nonsense: 1 } }
output: { drop: {} }" \
  "input.sequence has no field 'nonsense'"

# ---- resources resolve inside every nested processor list ----------------------------
#
# component_config keeps nested processor trees in THREE slots -- children, cases
# and branches -- and resolution walked two, so a resource inside a workflow
# branch reached construction unresolved and died with the registry's internal
# error on a config the reference runs. An input's own batching processors were
# reached by neither walk.
resolves() {  # <name> <yaml>
  printf '%s\n' "$2" > "$WORK/res.yaml"
  local got
  got=$(timeout 30 "$SF" --config "$WORK/res.yaml" --smp 1 --memory 512M \
          --overprovisioned 2>&1 | grep -c "internal error")
  [ "$got" -eq 0 ] && echo "ok   $1" \
                   || { echo "FAIL $1: it reported an internal error" >&2; fail=1; }
}
resolves "a rate limit inside a workflow branch resolves" \
  "input: { generate: { count: 1, interval: 0s, mapping: 'root.id = 1' } }
pipeline:
  processors:
    - workflow:
        meta_path: m
        order: [[a]]
        branches:
          a:
            request_map: 'root = this'
            processors: [ { rate_limit: { resource: rl } }, { mapping: 'root = {\"v\":1}' } ]
            result_map: 'root.a = this.v'
output: { drop: {} }
rate_limit_resources: [ { label: rl, local: { count: 10, interval: 1s } } ]"
resolves "a rate limit inside input.broker.batching.processors resolves" \
  "input:
  broker:
    inputs: [ { generate: { count: 2, interval: 0s, mapping: 'root.n = counter()' } } ]
    batching:
      count: 2
      processors: [ { rate_limit: { resource: rl } } ]
output: { drop: {} }
rate_limit_resources: [ { label: rl, local: { count: 10, interval: 1s } } ]"

exit $fail
