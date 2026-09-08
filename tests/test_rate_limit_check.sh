#!/usr/bin/env bash
# `rate_limit_resources` and the components that name one.
#
# A rate limit is only observable in TIME, so every check here measures elapsed
# milliseconds rather than output. That makes the thresholds the interesting
# part: each one is wide enough that scheduler noise cannot fail it, and narrow
# enough that a limit doing nothing at all cannot pass it. A check that only
# asserted "the messages came out" would go green with the whole feature
# removed, which is exactly the false pass this file exists to avoid.
set -uo pipefail
cd "$(dirname "$0")/.."
SF=${1:-build/swordfish-run}
RC=${2:-../redpanda-connect}
export SWORDFISH_CONFIG="$PWD/build/swordfish-config"
WORK=$(mktemp -d /tmp/sfrl.XXXXXX)
SEA_ARGS="--smp 1 --memory 512M --overprovisioned"

PIDS=()
cleanup() {
  local p
  for p in "${PIDS[@]:-}"; do kill -9 "$p" 2>/dev/null; done
  rm -rf "$WORK"
}
# A signal must END the script rather than resume it -- see test_socket_check.sh.
trap cleanup EXIT
trap 'cleanup; exit 143' INT TERM

fail=0
note() { echo "ok   $1"; }
bad()  { echo "FAIL $1: $2" >&2; fail=1; }

SF_PORT_BAND=21000
# shellcheck source=tests/ports.sh
. "$(dirname "${BASH_SOURCE[0]}")/ports.sh"

# Elapsed milliseconds of one run. `date +%s%N` rather than $SECONDS, which has
# one-second resolution and would round every measurement here to 0 or 1.
elapsed_ms() {  # <command...>
  local start end
  start=$(date +%s%N)
  "$@" >/dev/null 2>&1
  end=$(date +%s%N)
  echo $(( (end - start) / 1000000 ))
}

between() {  # <name> <ms> <lo> <hi>
  if [ "$2" -ge "$3" ] && [ "$2" -le "$4" ]; then note "$1"
  else bad "$1" "took ${2}ms, expected ${3}-${4}ms"; fi
}

# ---- the `rate_limit` processor ----------------------------------------------
#
# 6 messages at 2 per 300ms is three windows: the first two go straight through,
# then two waits of 300ms. Startup is measured too, so the floor allows for the
# run itself and the ceiling for one extra window of slack.
cat > "$WORK/proc.yaml" <<'YAML'
input:
  generate: { count: 6, interval: 0s, mapping: 'root.n = counter()' }
pipeline:
  processors:
    - rate_limit: { resource: slow }
output:
  stdout: {}
rate_limit_resources:
  - label: slow
    local: { count: 2, interval: 300ms }
YAML
# shellcheck disable=SC2086
ms=$(elapsed_ms timeout 30 "$SF" --config "$WORK/proc.yaml" $SEA_ARGS)
between "the rate_limit processor spends two windows on six messages" "$ms" 550 1500

got=$(timeout 30 "$SF" --config "$WORK/proc.yaml" $SEA_ARGS 2>/dev/null | tr '\n' '|')
[ "$got" = '{"n":1}|{"n":2}|{"n":3}|{"n":4}|{"n":5}|{"n":6}|' ] \
  && note "throttling delays messages without dropping or reordering them" \
  || bad "throttling delays messages without dropping or reordering them" "got '$got'"

# The same source and the same six messages with no limit at all: this is what
# proves the time above came from the throttling rather than from `generate`,
# the pipeline machinery or process startup. Without a control, a slow binary
# would pass the check above for the wrong reason.
cat > "$WORK/nolimit.yaml" <<'YAML'
input:
  generate: { count: 6, interval: 0s, mapping: 'root.n = counter()' }
output:
  stdout: {}
YAML
# shellcheck disable=SC2086
ms=$(elapsed_ms timeout 30 "$SF" --config "$WORK/nolimit.yaml" $SEA_ARGS)
[ "$ms" -lt 400 ] && note "the same pipeline without a limit is not slow" \
                  || bad "the same pipeline without a limit is not slow" "took ${ms}ms"

if [ -x "$RC" ]; then
  "$RC" lint "$WORK/proc.yaml" >/dev/null 2>&1 \
    && note "the reference accepts our rate_limit_resources config" \
    || bad "the reference accepts our rate_limit_resources config" "it failed to lint"
  ms=$(elapsed_ms timeout 30 "$RC" run --log.level off "$WORK/proc.yaml")
  between "the reference spends the same two windows on it" "$ms" 550 2500
fi

# ---- sharing ------------------------------------------------------------------
#
# TWO processors naming ONE label must contend for the same permits, which is
# what makes a rate limit a resource rather than a setting. Six messages through
# two processors is twelve accesses: at 2 per 300ms that is five waits, so it
# must take MORE than the single-processor case above rather than the same.
cat > "$WORK/shared.yaml" <<'YAML'
input:
  generate: { count: 6, interval: 0s, mapping: 'root.n = counter()' }
pipeline:
  processors:
    - rate_limit: { resource: slow }
    - rate_limit: { resource: slow }
output:
  stdout: {}
rate_limit_resources:
  - label: slow
    local: { count: 2, interval: 300ms }
YAML
# shellcheck disable=SC2086
ms=$(elapsed_ms timeout 60 "$SF" --config "$WORK/shared.yaml" $SEA_ARGS)
between "two components naming one limit share its permits" "$ms" 1300 3000

# ---- refusals -----------------------------------------------------------------
refuses() {  # <name> <config> <expected substring>
  local out
  # shellcheck disable=SC2086
  out=$(timeout 30 "$SF" --config "$2" $SEA_ARGS 2>&1)
  case "$out" in
    *"$3"*) note "$1" ;;
    *)      bad "$1" "expected '$3', got: $(echo "$out" | tail -2 | tr '\n' ' ')" ;;
  esac
}

sed 's/resource: slow/resource: missing/' "$WORK/proc.yaml" > "$WORK/undeclared.yaml"
refuses "a label with no rate_limit_resources entry is refused by name" \
        "$WORK/undeclared.yaml" "is not declared in rate_limit_resources"

sed 's/    local: { count: 2, interval: 300ms }/    redis: { url: "redis:\/\/x", key: y }/' \
    "$WORK/proc.yaml" > "$WORK/redis.yaml"
refuses "a rate limit that is not \`local\` is refused rather than approximated" \
        "$WORK/redis.yaml" "swordfish implements no other kind yet"

# The budget is DIVIDED across shards, so a count below the shard count cannot
# be shared without either exceeding the limit or starving a shard. Refused by
# name; silently rounding either way is the failure this rule exists to prevent.
sed 's/count: 2,/count: 1,/' "$WORK/proc.yaml" > "$WORK/tiny.yaml"
out=$(timeout 30 "$SF" --config "$WORK/tiny.yaml" --smp 2 --memory 512M --overprovisioned 2>&1)
case "$out" in
  *"cannot be divided across 2 shards"*)
    note "a count smaller than the shard count is refused rather than rounded" ;;
  *) bad "a count smaller than the shard count is refused rather than rounded" \
         "$(echo "$out" | tail -2 | tr '\n' ' ')" ;;
esac

# ---- http_client input --------------------------------------------------------
#
# This is what retired the swordfish-only `poll_period` field: an unthrottled
# poll is a hot loop, and the reference's answer is a rate limit rather than a
# sleep. The peer counts requests, so the assertion is on the REQUEST count
# rather than on time -- a limit that delayed nothing would show far more.
PORT=$(free_port)
python3 - "$PORT" >"$WORK/feed.log" 2>&1 <<'PY' &
import http.server, sys
n = 0
class H(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        global n
        n += 1
        body = ('{"tick":%d}' % n).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
    def log_message(self, *a):
        pass
http.server.HTTPServer(("127.0.0.1", int(sys.argv[1])), H).serve_forever()
PY
PIDS+=("$!")
for i in $(seq 1 60); do
  curl -s -o /dev/null --max-time 1 "http://127.0.0.1:$PORT/feed" && break
  sleep 0.25
done

cat > "$WORK/poll.yaml" <<YAML
input:
  http_client:
    url: http://127.0.0.1:$PORT/feed
    verb: GET
    rate_limit: polls
output:
  stdout: {}
rate_limit_resources:
  - label: polls
    local: { count: 1, interval: 400ms }
YAML
# Two seconds at one poll per 400ms is about five, and the curl probe above
# already spent one tick. A hot loop would produce hundreds, so the ceiling is
# what carries the check.
# shellcheck disable=SC2086
got=$(timeout 30 timeout 2 "$SF" --config "$WORK/poll.yaml" $SEA_ARGS 2>/dev/null | wc -l)
if [ "$got" -ge 2 ] && [ "$got" -le 9 ]; then
  note "http_client input polls at the rate its limit allows"
else
  bad "http_client input polls at the rate its limit allows" "made $got polls in 2s"
fi

if [ -x "$RC" ]; then
  "$RC" lint "$WORK/poll.yaml" >/dev/null 2>&1 \
    && note "the reference accepts a rate-limited http_client input" \
    || bad "the reference accepts a rate-limited http_client input" "it failed to lint"
  ref=$(timeout 30 timeout 2 "$RC" run --log.level off "$WORK/poll.yaml" 2>/dev/null | wc -l)
  if [ "$ref" -ge 2 ] && [ "$ref" -le 9 ]; then
    note "the reference polls the same peer at the same rate"
  else
    bad "the reference polls the same peer at the same rate" "it made $ref polls in 2s"
  fi
fi

# ---- http_server input --------------------------------------------------------
#
# On the server side the limit does NOT delay: a request over it is answered 429
# with a Retry-After, as the reference does. Delaying instead would hold open the
# connections the limit exists to protect.
PORT=$(free_port)
cat > "$WORK/srv.yaml" <<YAML
input:
  http_server:
    address: 127.0.0.1:$PORT
    path: /post
    rate_limit: posts
output:
  drop: {}
rate_limit_resources:
  - label: posts
    local: { count: 2, interval: 60s }
YAML
# shellcheck disable=SC2086
"$SF" --config "$WORK/srv.yaml" $SEA_ARGS >"$WORK/srv.log" 2>&1 &
PIDS+=("$!")
for i in $(seq 1 60); do
  curl -s -o /dev/null --max-time 1 -X POST -d x "http://127.0.0.1:$PORT/post" && break
  sleep 0.25
done
# The probe above already spent one of the two permits, so the second POST is the
# last allowed one and the third must be refused.
codes=""
for i in 1 2; do
  codes="$codes$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 \
                      -X POST -d x "http://127.0.0.1:$PORT/post") "
done
retry=$(curl -s -o /dev/null -D - --max-time 5 -X POST -d x \
             "http://127.0.0.1:$PORT/post" 2>/dev/null | tr -d '\r' \
        | awk 'tolower($1)=="retry-after:"{print $2}')
case "$codes" in
  "200 429 ") note "http_server answers 429 once the limit is spent" ;;
  *)          bad "http_server answers 429 once the limit is spent" "got codes: $codes" ;;
esac
[ -n "$retry" ] && [ "$retry" -gt 0 ] \
  && note "the 429 carries a Retry-After the client can act on" \
  || bad "the 429 carries a Retry-After the client can act on" "header was '$retry'"

# ---- the compiled path --------------------------------------------------------
#
# `swordfish build` emits C++ rather than walking the tree, so a limit that works
# interpreted proves nothing about a built binary. Same config, same timing.
if [ -x build/swordfish-build ]; then
  if timeout 900 build/swordfish-build "$WORK/proc.yaml" -o "$WORK/proc_bin" \
        >"$WORK/build.log" 2>&1; then
    # shellcheck disable=SC2086
    ms=$(elapsed_ms timeout 60 "$WORK/proc_bin" $SEA_ARGS)
    between "a compiled binary throttles exactly as the interpreter does" "$ms" 550 1500
    got=$(timeout 60 "$WORK/proc_bin" $SEA_ARGS 2>/dev/null | tr '\n' '|')
    [ "$got" = '{"n":1}|{"n":2}|{"n":3}|{"n":4}|{"n":5}|{"n":6}|' ] \
      && note "and emits the same messages in the same order" \
      || bad "and emits the same messages in the same order" "got '$got'"
  else
    bad "a compiled binary throttles exactly as the interpreter does" \
        "swordfish build failed: $(tail -3 "$WORK/build.log" | tr '\n' ' ')"
  fi
fi

exit $fail
