#!/usr/bin/env bash
# The document-level blocks that are not components: `shutdown_timeout`,
# `shutdown_delay`, `error_handling`, `buffer`, `logger`, `metrics`, `tracer`,
# and `pipeline.threads`.
#
# Every one of these was listed in the root schema and read by NOBODY. A config
# asking for `error_handling.strict: true` ran non-strict, and one asking for
# `shutdown_delay: 30s` exited at once while the reference waited -- silently,
# in both cases, which is the failure mode the project's named-error rule exists
# to prevent. This gate exists so that no block can quietly return to being
# accepted and ignored: each one is either OBSERVED to change behaviour or
# asserted to be refused by name.
#
# "It lints clean" is never the whole assertion for an implemented block. A
# block that parses and then does nothing lints clean too, which is exactly the
# state this replaced.
set -uo pipefail
cd "$(dirname "$0")/.."
SF=${1:-build/swordfish-run}
RC=${2:-../redpanda-connect}
LINT=${3:-build/sfconfig}
export SWORDFISH_CONFIG="$PWD/build/swordfish-config"
WORK=$(mktemp -d /tmp/sfengine.XXXXXX)
SEA_ARGS="--smp 1 --memory 512M --overprovisioned"
trap 'rm -rf "$WORK"' EXIT
trap 'rm -rf "$WORK"; exit 143' INT TERM

fail=0
note() { echo "ok   $1"; }
bad()  { echo "FAIL $1: $2" >&2; fail=1; }

# <name> <yaml body> <expected substring of the lint error>
refuses() {
  local out
  printf '%s\n' "$2" > "$WORK/c.yaml"
  out=$(timeout 30 "$LINT" lint "$WORK/c.yaml" 2>&1)
  case "$out" in
    *"$3"*) note "$1" ;;
    *)      bad "$1" "expected '$3', got '$out'" ;;
  esac
}

# <name> <yaml body>
accepts() {
  local out
  printf '%s\n' "$2" > "$WORK/c.yaml"
  if out=$(timeout 30 "$LINT" lint "$WORK/c.yaml" 2>&1) && [ -z "$out" ]; then
    note "$1"
  else
    bad "$1" "lint said: $out"
  fi
}

BASE='input: { generate: { count: 1, interval: 0s, mapping: "root.n = 1" } }
output: { stdout: {} }'

# ---- refused by name ----------------------------------------------------------
#
# Each of these names a component or setting swordfish does not implement. The
# message must distinguish "not built here" from "you made a typo": they are
# different mistakes and lead the reader to different places.
# `memory` used to be the unimplemented example here; it is built now, so this
# moved to a kind that still is. The buffer's own behaviour lives in
# tests/test_buffer_check.sh.
refuses "an unimplemented buffer is refused by name" \
  "$BASE
buffer: { sqlite: { path: /tmp/x } }" \
  "buffer 'sqlite' is not implemented by swordfish"

refuses "an unimplemented metrics backend is refused by name" \
  "$BASE
metrics: { aws_cloudwatch: {} }" \
  "metrics backend 'aws_cloudwatch' is not implemented by swordfish"

refuses "an unimplemented tracer is refused by name" \
  "$BASE
tracer: { open_telemetry_collector: {} }" \
  "tracer 'open_telemetry_collector' is not implemented by swordfish"

refuses "the logger block is refused, and says what to use instead" \
  "$BASE
logger: { level: DEBUG }" \
  "logging is configured with seastar's own --default-log-level"

# Swordfish runs a pipeline per SHARD, so honouring `threads` would mean either
# ignoring --smp or multiplying by it. Refused rather than dropped.
refuses "pipeline.threads is refused rather than ignored" \
  'input: { generate: { count: 1, interval: 0s, mapping: "root = 1" } }
pipeline: { threads: 4, processors: [ { noop: {} } ] }
output: { stdout: {} }' \
  "\`pipeline\` \`threads\` is not implemented"

refuses "a typo inside error_handling is named" \
  "$BASE
error_handling: { strickt: true }" \
  "\`error_handling\` has no field 'strickt'"

refuses "a shutdown_timeout that is not a duration is named" \
  "$BASE
shutdown_timeout: yesterday" \
  "shutdown_timeout is not a duration"

# `/ready` reads the components' connection_status, which it did not until
# 2026-09-06: it answered 200 and "connected":true unconditionally, so a probe
# passed for a pipeline whose input had never connected -- Kubernetes would route
# traffic to a pod consuming nothing and never evict one whose source was lost.
# The two sides are reported SEPARATELY, as the reference reports them.
ready_case() {  # <name> <config-file> <port> <expected code> <expected substring>
  local name=$1 cfg=$2 port=$3 want=$4 sub=$5 code body
  # shellcheck disable=SC2086
  "$SF" --config "$cfg" $SEA_ARGS >/dev/null 2>&1 &
  local pid=$!
  local i
  for i in $(seq 1 60); do
    curl -s -o /dev/null --max-time 1 "http://127.0.0.1:$port/ready" && break
    sleep 0.2
  done
  body=$(curl -s --max-time 3 "http://127.0.0.1:$port/ready")
  code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 3 "http://127.0.0.1:$port/ready")
  kill -KILL "$pid" 2>/dev/null; wait "$pid" 2>/dev/null
  if [ "$code" != "$want" ]; then
    bad "$name" "expected HTTP $want, got $code ($body)"; return
  fi
  case "$body" in
    *"$sub"*) note "$name" ;;
    *)        bad "$name" "expected '$sub' in '$body'" ;;
  esac
}
cat > "$WORK/ready_ok.yaml" <<YAML
http: { enabled: true, address: 127.0.0.1:14252 }
input:
  socket_server:
    network: tcp
    address: 127.0.0.1:20976
    scanner: { lines: {} }
output: { drop: {} }
YAML
cat > "$WORK/ready_bad.yaml" <<YAML
http: { enabled: true, address: 127.0.0.1:14253 }
input:
  websocket:
    url: "ws://127.0.0.1:20977/ws"
    connection: { max_retries: 1000 }
output: { drop: {} }
YAML
ready_case "a connected pipeline answers /ready with 200" \
           "$WORK/ready_ok.yaml"  14252 200 '"path":"input","connected":true'
ready_case "a disconnected input answers 503 and names the input" \
           "$WORK/ready_bad.yaml" 14253 503 '"path":"input","connected":false'
ready_case "and it does not blame the output for it" \
           "$WORK/ready_bad.yaml" 14253 503 '"path":"output","connected":true'

# `shutdown_timeout` bounds the SIGNAL path, which it did not until 2026-09-06:
# SIGINT/SIGTERM only requested a drain and the wait behind it was unbounded, so
# a pipeline blocked in a processor stayed alive for ever. The assertion is that
# the process EXITS, and exits cleanly -- the first version of the fix left a
# signal handler pointing at a freed coroutine frame and segfaulted here every
# time, which a test that only measured elapsed time would have passed.
cat > "$WORK/sig.yaml" <<'YAML'
shutdown_timeout: 3s
input:
  generate: { count: 100, interval: 100ms, mapping: 'root = 1' }
pipeline:
  processors:
    - sleep: { duration: 120s }
output: { drop: {} }
YAML
signal_case() {  # <name> <signal> <runner...>
  local name=$1 sig=$2; shift 2
  "$@" >/dev/null 2>&1 &
  local pid=$!
  sleep 2
  local start=$SECONDS
  kill -"$sig" "$pid" 2>/dev/null
  local gone=0 i
  for i in $(seq 1 300); do
    kill -0 "$pid" 2>/dev/null || { gone=1; break; }
    sleep 0.1
  done
  if [ "$gone" != 1 ]; then
    kill -9 "$pid" 2>/dev/null; wait "$pid" 2>/dev/null
    bad "$name" "still running 30s after SIG$sig"; return
  fi
  wait "$pid" 2>/dev/null
  local rc=$?
  local took=$(( SECONDS - start ))
  if [ "$rc" -ne 0 ]; then
    bad "$name" "exited with $rc (139 is a segfault) after SIG$sig"; return
  fi
  if [ "$took" -gt 12 ]; then
    bad "$name" "took ${took}s to stop, well past the 3s shutdown_timeout"; return
  fi
  note "$name"
}
signal_case "SIGTERM stops a blocked pipeline within shutdown_timeout" TERM \
            "$SF" --config "$WORK/sig.yaml" --smp 1 --memory 512M --overprovisioned
signal_case "SIGINT does the same" INT \
            "$SF" --config "$WORK/sig.yaml" --smp 1 --memory 512M --overprovisioned
signal_case "and it holds on more than one shard" TERM \
            "$SF" --config "$WORK/sig.yaml" --smp 4 --memory 1G --overprovisioned

# ---- the `http` block -----------------------------------------------------------
#
# This block was parsed with three finds and no validation, so `cert_file`,
# `key_file`, `cors` and `basic_auth` were accepted and read by nobody: a config
# asking for TLS and authentication on the observability port got a plaintext,
# unauthenticated one and no warning. That is the rule the connectors are already
# held to -- "a config asking for TLS fails to lint rather than connecting in the
# clear" -- applied here at last.
refuses "http cert_file is refused rather than serving in the clear" \
  "$BASE
http: { enabled: true, address: '127.0.0.1:0', cert_file: /x.pem }" \
  "\`cert_file\` (TLS on the observability server) is not implemented"
refuses "http basic_auth is refused rather than serving unauthenticated" \
  "$BASE
http: { enabled: true, address: '127.0.0.1:0', basic_auth: { enabled: true, username: a } }" \
  "\`basic_auth\` is not implemented"
refuses "http cors is refused" \
  "$BASE
http: { enabled: true, address: '127.0.0.1:0', cors: { enabled: true } }" \
  "\`cors\` is not implemented"
refuses "http debug_endpoints is refused" \
  "$BASE
http: { enabled: true, address: '127.0.0.1:0', debug_endpoints: true }" \
  "\`debug_endpoints\` is not implemented"
refuses "a typo in the http block is named" \
  "$BASE
http: { enabled: true, nonsense_field: 1 }" \
  "the \`http\` block has no field 'nonsense_field'"

# The DISABLED forms are what swordfish actually does, so they are accepted --
# a `cors: {enabled: false}` asks for nothing that is missing. Without this the
# refusals above would be free to reject every real config too.
accepts "the switched-off forms of those fields are accepted" "$BASE
http:
  enabled: true
  # A REAL address: port 0 is refused by name here (the reference binds an
  # ephemeral port instead), and this case is about the other fields being
  # switched off, not about the address.
  address: '127.0.0.1:18771'
  root_path: /x
  debug_endpoints: false
  cert_file: ''
  key_file: ''
  cors: { enabled: false }
  basic_auth: { enabled: false }"

# ---- accepted, because swordfish genuinely does them --------------------------
#
# `none` is a real no-op in each family, and swordfish already serves /metrics in
# Prometheus text format from the `http` block, so `prometheus` is not an
# approximation either.
accepts "buffer none is accepted"      "$BASE
buffer: { none: {} }"
accepts "buffer memory is accepted"    "$BASE
buffer: { memory: { limit: 1000 } }"
accepts "tracer none is accepted"      "$BASE
tracer: { none: {} }"
accepts "metrics none is accepted"     "$BASE
metrics: { none: {} }"
accepts "metrics prometheus is accepted, and is what /metrics already serves" "$BASE
metrics: { prometheus: {} }"

# ---- observed to change behaviour ---------------------------------------------
#
# shutdown_delay. Measured, not linted: the whole point is that it was accepted
# and did nothing. The floor is what proves it waited; the ceiling keeps a
# genuinely stuck process from passing.
cat > "$WORK/delay.yaml" <<'YAML'
input: { generate: { count: 1, interval: 0s, mapping: 'root.n = 1' } }
output: { stdout: {} }
shutdown_delay: 1500ms
YAML
cat > "$WORK/nodelay.yaml" <<'YAML'
input: { generate: { count: 1, interval: 0s, mapping: 'root.n = 1' } }
output: { stdout: {} }
YAML
ms_of() {  # <config>
  local start end
  start=$(date +%s%N)
  # shellcheck disable=SC2086
  timeout 60 "$SF" --config "$1" $SEA_ARGS >/dev/null 2>&1
  end=$(date +%s%N)
  echo $(( (end - start) / 1000000 ))
}
with=$(ms_of "$WORK/delay.yaml")
without=$(ms_of "$WORK/nodelay.yaml")
# The DIFFERENCE is the assertion, not the absolute time: start-up cost varies
# between machines and would otherwise have to be guessed at.
diff_ms=$(( with - without ))
if [ "$diff_ms" -ge 1200 ] && [ "$diff_ms" -le 4000 ]; then
  note "shutdown_delay holds the process open for what it asked for"
else
  bad "shutdown_delay holds the process open for what it asked for" \
      "with=${with}ms without=${without}ms difference=${diff_ms}ms"
fi

# error_handling.strict. A failing mapping marks its message rather than
# throwing -- that is the non-strict behaviour and it matches the reference --
# so strict mode is only observable in whether the message is WRITTEN.
cat > "$WORK/strict.yaml" <<'YAML'
input:
  generate: { count: 2, interval: 0s, mapping: 'root.n = counter()', auto_replay_nacks: false }
pipeline:
  processors:
    - mapping: 'root = this.nope.uppercase()'
output: { stdout: {} }
error_handling:
  strict: true
YAML
sed 's/  strict: true/  strict: false/' "$WORK/strict.yaml" > "$WORK/nonstrict.yaml"
# shellcheck disable=SC2086
strict_out=$(timeout 60 "$SF" --config "$WORK/strict.yaml" $SEA_ARGS 2>/dev/null | tr '\n' '|')
# shellcheck disable=SC2086
loose_out=$(timeout 60 "$SF" --config "$WORK/nonstrict.yaml" $SEA_ARGS 2>/dev/null | tr '\n' '|')
[ -z "$strict_out" ] \
  && note "error_handling.strict rejects a failed message instead of writing it" \
  || bad "error_handling.strict rejects a failed message instead of writing it" \
         "it wrote '$strict_out'"
# The control. Without it, a build that wrote nothing under EITHER setting would
# pass the check above while having broken the ordinary path.
[ "$loose_out" = '{"n":1}|{"n":2}|' ] \
  && note "and without it the same message is written, as the reference does" \
  || bad "and without it the same message is written, as the reference does" \
         "got '$loose_out'"

if [ -x "$RC" ]; then
  ref=$(timeout 60 "$RC" run --log.level off "$WORK/nonstrict.yaml" 2>/dev/null | sort | tr '\n' '|')
  [ "$ref" = '{"n":1}|{"n":2}|' ] \
    && note "the reference agrees on the non-strict case" \
    || bad "the reference agrees on the non-strict case" "got '$ref'"
  # Under strict the reference nacks and then RETRIES for ever, ignoring
  # `auto_replay_nacks: false` -- it never terminates. Only the essential
  # agreement is asserted: neither writes the failed message.
  refout=$(timeout 6 "$RC" run --log.level off "$WORK/strict.yaml" 2>/dev/null | tr '\n' '|')
  [ -z "$refout" ] \
    && note "and neither implementation writes it under strict" \
    || bad "and neither implementation writes it under strict" "the reference wrote '$refout'"
fi

# ---- the engine settings survive compilation ----------------------------------
#
# A compiled binary that ignored `error_handling.strict` while the interpreted
# one honoured it would be the two modes disagreeing about what a config means.
if [ -x build/swordfish-build ]; then
  if timeout 900 build/swordfish-build "$WORK/strict.yaml" -o "$WORK/strict.bin" \
        >"$WORK/build.log" 2>&1; then
    # shellcheck disable=SC2086
    cout=$(timeout 60 "$WORK/strict.bin" $SEA_ARGS 2>/dev/null | tr '\n' '|')
    [ -z "$cout" ] \
      && note "a compiled binary honours error_handling.strict too" \
      || bad "a compiled binary honours error_handling.strict too" "it wrote '$cout'"
  else
    bad "a compiled binary honours error_handling.strict too" \
        "swordfish build failed: $(tail -3 "$WORK/build.log" | tr '\n' ' ')"
  fi
fi

# ---- Bloblang inside a connector's config is checked at load time -------------------
#
# `processor_def::bloblangs` collected only fields typed `cfg::bloblang`, never
# `cfg::interpolation` -- which is equally Bloblang, since every consumer runs it
# through interpolation_to_query -- and `input_def`/`output_def` had no such hook
# at all. So a syntax error inside `${! ... }` was not a config error but a
# failure on the first message, with no line and no field name.
refuses "a broken interpolation in a processor is a config error" \
  'input: { generate: { count: 1, interval: 0s, mapping: "root = 1" } }
pipeline: { processors: [ { insert_part: { index: -1, content: "x${! json(\"a\" }y" } } ] }
output: { drop: {} }' \
  "expected"
refuses "a broken interpolation in a dedupe key is a config error" \
  'input: { generate: { count: 1, interval: 0s, mapping: "root = 1" } }
pipeline: { processors: [ { dedupe: { cache: c, key: "${! json(\"id\" }" } } ] }
output: { drop: {} }
cache_resources: [ { label: c, memory: {} } ]' \
  "expected"
# The connector hook is the half that did not exist at all.
refuses "a broken interpolation in a connector config is a config error" \
  'input: { http_client: { url: "http://127.0.0.1:9/x", payload: "x${! json(\"a\" }y" } }
output: { drop: {} }' \
  "expected"
accepts "a valid interpolation still lints" 'input: { generate: { count: 1, interval: 0s, mapping: "root = 1" } }
pipeline: { processors: [ { insert_part: { index: -1, content: "x${! json(\"a\") }y" } } ] }
output: { drop: {} }'

# ---- `addresses` is a legacy alias for `seed_brokers` -------------------------
#
# The reference ships TWO Kafka connectors: `kafka` (stable, broker list
# `addresses`) and `kafka_franz` (beta, `seed_brokers`). Swordfish implements the
# franz-go shape under the name `kafka`, so before this alias NO config linted on
# both implementations, in either direction -- for the project's central
# connector. `seed_brokers` remains the real field and the preferred spelling;
# `addresses` is accepted so an existing Redpanda Connect config runs unchanged,
# and when both are given `seed_brokers` is the one that takes effect.
#
# "It lints clean" is not the whole assertion here, per this file's own rule: the
# precedence case reads the GENERATED SOURCE, because an alias that lints and is
# then ignored would pass a lint-only check.
ka_cfg() {  # ka_cfg <broker-lines>
  cat > "$WORK/ka.yaml" <<YAML
input:
  kafka:
$1
    topics: [ t ]
    consumer_group: g
output: { drop: {} }
YAML
}
ka_lints() {  # ka_lints <name> <broker-lines>
  ka_cfg "$2"
  if timeout 30 "$LINT" lint "$WORK/ka.yaml" >/dev/null 2>&1; then
    note "$1"
  else
    bad "$1" "sfconfig lint refused it"
  fi
}
ka_lints "seed_brokers lints"                   '    seed_brokers: [ h:9092 ]'
ka_lints "addresses lints as its legacy alias"  '    addresses: [ h:9092 ]'
ka_lints "and both together lint"               '    seed_brokers: [ good:9092 ]
    addresses: [ bad:9092 ]'

# Neither spelling is an error, and at LINT time rather than when the pipeline
# starts.
ka_cfg '    client_id: x'
if timeout 30 "$LINT" lint "$WORK/ka.yaml" >/dev/null 2>&1; then
  bad "neither broker field is a named lint error" "it linted clean"
else
  note "neither broker field is a named lint error"
fi

# Precedence, read off the emitted C++ rather than inferred from a clean lint.
ka_cfg '    seed_brokers: [ good:9092 ]
    addresses: [ bad:9092 ]'
rm -rf "$WORK/kasrc"
if timeout 240 build/swordfish-build "$WORK/ka.yaml" --emit-source "$WORK/kasrc" \
       -o "$WORK/kabin" >/dev/null 2>&1; then
  ka_got=$(grep -rhoE '"(good|bad):9092"' "$WORK/kasrc"/*.cc 2>/dev/null | sort -u | tr '\n' ' ')
  if [ "$ka_got" = '"good:9092" ' ]; then
    note "seed_brokers wins over addresses, in the generated source"
  else
    bad "seed_brokers wins over addresses, in the generated source" "found $ka_got"
  fi
else
  bad "seed_brokers wins over addresses, in the generated source" "the build failed"
fi

# The compatibility claim itself: an `addresses` config must lint on BOTH. The
# `seed_brokers` one is expected to lint here only -- it is kafka_franz's field
# name, which the reference's `kafka` does not accept -- and that asymmetry is
# the reason the alias exists rather than a defect.
if [ -x "$RC" ]; then
  ka_cfg '    addresses: [ h:9092 ]'
  if timeout 30 "$RC" lint "$WORK/ka.yaml" >/dev/null 2>&1; then
    note "an addresses config lints on the reference too"
  else
    bad "an addresses config lints on the reference too" \
        "the reference refused it, so the alias buys no compatibility"
  fi
fi

# ---- auto_replay_nacks reaches EVERY input ------------------------------------
#
# The wrapper that replays a rejected batch existed and was applied in exactly
# one branch of build_input_kind -- the three built-ins. A registered connector's
# factory was returned untouched, so `kafka` never got it: a nacked batch was
# read once, rejected once, and never seen again, leaving its partition's commit
# pinned at that offset for the life of the process. `http_client` and the socket
# inputs only worked because they wrapped themselves inside their own factories,
# which is now removed so there is genuinely one place that decides what a nack
# means.
#
# Measured with the output always rejecting: `generate` replayed 65 times in four
# seconds while `kafka` managed zero in eight. The observable here is the nack
# COUNT, because `in=` does not distinguish a replay and a delivery-attempt count
# needs an output that both prints and fails -- and `fan_out` wraps its children
# in an indefinite retry, so a failure there never reaches the input at all.
replay_nacks() {  # replay_nacks <name> <input-block> <expect-many|expect-one>
  local name=$1 want=$3 n
  cat > "$WORK/rn.yaml" <<YAML
input:
$2
output: { reject: "always refuses" }
YAML
  "$SF" --config "$WORK/rn.yaml" --smp 1 --memory 512M --overprovisioned \
      >/dev/null 2>"$WORK/rn.err" &
  local pid=$!
  sleep 4
  # SIGTERM, not SIGKILL: the counters are printed in the shutdown summary, and
  # killing outright leaves nothing to read.
  kill -TERM "$pid" 2>/dev/null
  local i
  for i in $(seq 1 40); do kill -0 "$pid" 2>/dev/null || break; sleep 0.5; done
  kill -9 "$pid" 2>/dev/null; wait "$pid" 2>/dev/null
  n=$(grep -oE 'nacks=[0-9]+' "$WORK/rn.err" | tail -1 | cut -d= -f2)
  n=${n:-0}
  # "Many" is anything well past one; the rate is a backoff ramp, not a
  # guaranteed count, so the threshold is deliberately loose.
  if [ "$want" = many ] && [ "$n" -gt 5 ]; then
    note "$name (replayed $n times)"
  elif [ "$want" = one ] && [ "$n" -le 1 ]; then
    note "$name (nacked $n time(s), not replayed)"
  else
    bad "$name" "wanted $want, saw nacks=$n"
  fi
}
replay_nacks "a nacked batch is replayed by default" \
  '  generate: { count: 1, interval: "", mapping: '"'"'root.n = 1'"'"' }' many
replay_nacks "auto_replay_nacks: false stops the replay" \
  '  generate: { count: 1, interval: "", mapping: '"'"'root.n = 1'"'"', auto_replay_nacks: false }' one

# The field is accepted on a REGISTERED connector too, not only the built-ins --
# it is lifted out of the body before the component's own spec decodes it, the
# way scanners are, because it is an engine field no connector declares.
cat > "$WORK/rn2.yaml" <<'YAML'
input:
  kafka:
    addresses: [ h:9092 ]
    topics: [ t ]
    consumer_group: g
    auto_replay_nacks: false
output: { drop: {} }
YAML
if timeout 30 "$LINT" lint "$WORK/rn2.yaml" >/dev/null 2>&1; then
  note "auto_replay_nacks is accepted on a registered connector"
else
  bad "auto_replay_nacks is accepted on a registered connector" "sfconfig lint refused it"
fi
if [ -x "$RC" ]; then
  if timeout 30 "$RC" lint "$WORK/rn2.yaml" >/dev/null 2>&1; then
    note "and the same config lints on the reference"
  else
    bad "and the same config lints on the reference" "the reference refused it"
  fi
fi

# ---- lint reports every section's error, not just the first -------------------
#
# parse_pipeline THROWS on the first problem, which is right for `run` and
# `build` -- neither should act on a half-understood config -- but it meant lint
# could report at most ONE diagnostic per file. A config with a bad field in the
# input, another in a processor and a third in the output printed one line; the
# reference prints all three, so the user fixed one, re-ran, and found the next.
#
# Lint now parses the document several times, each with one section real and the
# others replaced by a stand-in, and merges the results. The three checks below
# are the three ways that can go wrong: too few errors, DUPLICATED errors (a
# resource block is present in every probe), and a clean config paying for it.
cat > "$WORK/li_multi.yaml" <<'YAML'
input:
  generate:
    count: 1
    interval: ""
    mapping: 'root = {}'
    bogus_field_one: 1
pipeline:
  processors:
    - mapping: 'root = this'
      bogus_field_two: 2
output:
  stdout:
    bogus_field_three: 3
YAML
li_n=$(timeout 30 "$LINT" lint "$WORK/li_multi.yaml" 2>&1 | grep -c 'error:')
if [ "$li_n" -eq 3 ]; then
  note "lint reports an error from each section, not only the first"
else
  bad "lint reports an error from each section, not only the first" \
      "expected 3 diagnostics, got $li_n"
fi
if [ -x "$RC" ]; then
  # The reference's diagnostics are `<path>(line,col) message` -- no colon, so
  # counting those is what nearly made this check vacuous at zero.
  rc_n=$(timeout 30 "$RC" lint "$WORK/li_multi.yaml" 2>&1 | grep -c '\.yaml(')
  [ "$rc_n" -eq 3 ] \
    && note "and the reference reports the same number" \
    || bad "and the reference reports the same number" "the reference gave $rc_n"
fi

# A resource block is carried into every probe, so an error in one is seen
# several times and must be merged down to one.
cat > "$WORK/li_res.yaml" <<'YAML'
input: { generate: { count: 1, interval: "", mapping: 'root = {}' } }
pipeline: { processors: [ { dedupe: { cache: c, key: x } } ] }
output: { drop: {} }
cache_resources:
  - label: c
    memory: { bad_field: 1 }
YAML
li_r=$(timeout 30 "$LINT" lint "$WORK/li_res.yaml" 2>&1 | grep -c 'error:')
[ "$li_r" -eq 1 ] \
  && note "an error in a resource block is reported once, not once per probe" \
  || bad "an error in a resource block is reported once, not once per probe" \
         "got $li_r diagnostics"

# And a clean config still costs one parse and exits 0.
cat > "$WORK/li_ok.yaml" <<'YAML'
input: { generate: { count: 1, interval: "", mapping: 'root = {}' } }
output: { drop: {} }
YAML
if timeout 30 "$LINT" lint "$WORK/li_ok.yaml" >/dev/null 2>&1; then
  note "a clean config still lints clean"
else
  bad "a clean config still lints clean" "lint refused it"
fi

exit $fail
