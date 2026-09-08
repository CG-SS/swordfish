#!/usr/bin/env bash
# `http_server` input, `http_client` output and `sync_response`.
#
# These need a server running while a client talks to it, which neither
# `swordfish test` nor a single-process run can express -- hence a script, as
# for the inputs and outputs gates.
#
# When redpanda-connect is present the strongest checks here are the two
# CROSS-implementation ones: our client into its server, and its client into our
# server. Those are the only evidence that the wire format matches, as opposed
# to our encoder agreeing with our decoder.
set -uo pipefail
cd "$(dirname "$0")/.."
SF=${1:-build/swordfish-run}
RC=${2:-../redpanda-connect}
export SWORDFISH_CONFIG="$PWD/build/swordfish-config"
WORK=$(mktemp -d /tmp/sfhttp.XXXXXX)
SEA_ARGS="--smp 1 --memory 512M --overprovisioned"

PIDS=()
cleanup() {
  local p
  for p in "${PIDS[@]:-}"; do kill -9 "$p" 2>/dev/null; done
  rm -rf "$WORK"
}
# A signal must END the script, not just tidy up: a bare `trap cleanup TERM`
# runs the handler and then RESUMES, so a `timeout` firing mid-run deleted the
# work directory and every check after it failed with "No such file" rather
# than reporting the one that actually hung.
trap cleanup EXIT
trap 'cleanup; exit 143' INT TERM

fail=0
note() { echo "ok   $1"; }
bad()  { echo "FAIL $1: $2" >&2; fail=1; }

# A port nobody else on this machine is using -- see tests/ports.sh for why the
# obvious "bind 0 and close it" is not good enough. A collision here shows up as
# a mysterious empty response rather than as "address in use".
SF_PORT_BAND=20500
# shellcheck source=tests/ports.sh
. "$(dirname "${BASH_SOURCE[0]}")/ports.sh"

# Starts a pipeline and waits for its endpoint to answer. Returns non-zero if it
# never does, so a test can report "the server never came up" rather than
# reporting whatever an unanswered curl produced.
start_and_wait() {  # <impl: sf|rc> <config> <port> <path>
  local impl=$1 cfg=$2 port=$3 path=$4 i
  if [ "$impl" = sf ]; then
    # shellcheck disable=SC2086
    "$SF" --config "$cfg" $SEA_ARGS >>"$WORK/server.log" 2>&1 &
  else
    "$RC" run --log.level off "$cfg" >>"$WORK/server.log" 2>&1 &
  fi
  local pid=$!
  PIDS+=("$pid")
  for i in $(seq 1 60); do
    if curl -s -o /dev/null --max-time 1 -X POST -d 'ready-probe' \
            "http://127.0.0.1:$port$path"; then
      echo "$pid"; return 0
    fi
    kill -0 "$pid" 2>/dev/null || break
    sleep 0.25
  done
  echo "$pid"; return 1
}

stop() { kill -TERM "$1" 2>/dev/null; wait "$1" 2>/dev/null; }

# ---- the server accepts, routes and refuses -----------------------------------
PORT=$(free_port)
cat > "$WORK/server.yaml" <<YAML
input:
  http_server:
    address: "127.0.0.1:$PORT"
    path: /post
output:
  file: { path: $WORK/received.txt }
YAML
pid=$(start_and_wait sf "$WORK/server.yaml" "$PORT" /post) || bad "http_server starts" "it never answered on port $PORT"
if [ "$fail" = 0 ]; then
  note "http_server starts and accepts a POST"
  curl -s -o /dev/null -X POST -d '{"a":1}' "http://127.0.0.1:$PORT/post"
  curl -s -o /dev/null -X POST -d '{"a":2}' "http://127.0.0.1:$PORT/post"
  code=$(curl -s -o /dev/null -w '%{http_code}' -X GET "http://127.0.0.1:$PORT/post")
  [ "$code" = 405 ] && note "a verb outside allowed_verbs is 405" \
                    || bad "a verb outside allowed_verbs is 405" "got $code"
  code=$(curl -s -o /dev/null -w '%{http_code}' -X POST -d x "http://127.0.0.1:$PORT/elsewhere")
  [ "$code" = 404 ] && note "another path is 404" || bad "another path is 404" "got $code"
  stop "$pid"
  got=$(LC_ALL=C sort < "$WORK/received.txt" | tr '\n' '|')
  [ "$got" = 'ready-probe|{"a":1}|{"a":2}|' ] \
    && note "every accepted body reached the output" \
    || bad "every accepted body reached the output" "got '$got'"
fi

# ---- sync_response, against the reference where possible ----------------------
#
# `unarchive: lines` turns one request into a batch, so the same config exercises
# both the single-part and the multi-part response shape.
sync_config() {  # <port>
  cat <<YAML
input:
  http_server:
    address: "127.0.0.1:$1"
    path: /post
pipeline:
  processors:
    - unarchive: { format: lines }
output:
  sync_response: {}
YAML
}

# Header order is not significant in HTTP and the two servers emit a different
# one, so the header block is sorted before comparing. The BODY is compared
# exactly -- that is the part a caller parses.
norm_response() {
  local all headers body
  all=$(cat)
  # Down to the first blank line is the header block; past it is the body.
  # Plain sed and sort rather than gawk's asort, which is not in every awk.
  headers=$(printf '%s\n' "$all" | sed '/^$/q' | sed '$d' | LC_ALL=C sort)
  body=$(printf '%s\n' "$all" | sed -n '/^$/,$p' | tail -n +2)
  printf '%s\n\n%s' "$headers" "$body" | sed 's/[0-9a-f]\{60\}/BOUNDARY/g'
}

sync_probe() {  # <port> -- prints "<single response>@@<multipart response>"
  local one many
  one=$(curl -s -D - -X POST -d 'hello' "http://127.0.0.1:$1/post" \
        | tr -d '\r' | grep -iv '^date:\|^server:' | norm_response)
  many=$(curl -s -D - -X POST --data-binary $'a\nb\nc' "http://127.0.0.1:$1/post" \
         | tr -d '\r' | grep -iv '^date:\|^server:' | norm_response)
  printf '%s@@%s' "$one" "$many"
}

PORT=$(free_port)
sync_config "$PORT" > "$WORK/sync.yaml"
pid=$(start_and_wait sf "$WORK/sync.yaml" "$PORT" /post) || bad "sync_response starts" "no answer"
sf_sync=$(sync_probe "$PORT")
stop "$pid"

case "$sf_sync" in
  *"Content-Type: application/octet-stream"*"hello"*) note "a one-message response is the body verbatim" ;;
  *) bad "a one-message response is the body verbatim" "got: $sf_sync" ;;
esac
case "$sf_sync" in
  *"multipart/form-data; boundary=BOUNDARY"*"--BOUNDARY--"*) note "a many-message response is multipart" ;;
  *) bad "a many-message response is multipart" "got: $sf_sync" ;;
esac

if [ -x "$RC" ]; then
  PORT=$(free_port)
  sync_config "$PORT" > "$WORK/sync_rc.yaml"
  if ! "$RC" lint "$WORK/sync_rc.yaml" >/dev/null 2>&1; then
    bad "the reference accepts our http_server config" "it failed to lint"
  else
    note "the reference accepts our http_server config"
  fi
  pid=$(start_and_wait rc "$WORK/sync_rc.yaml" "$PORT" /post) || bad "reference sync_response starts" "no answer"
  rc_sync=$(sync_probe "$PORT")
  stop "$pid"
  if [ "$rc_sync" = "$sf_sync" ]; then
    note "sync_response is byte-identical to the reference"
  else
    bad "sync_response is byte-identical to the reference" \
        $'\n  swordfish: '"$sf_sync"$'\n  reference: '"$rc_sync"
  fi
fi

# ---- client and server, ours and theirs ---------------------------------------
sender() {  # <port> <count>
  cat <<YAML
input:
  generate: { count: $2, interval: 0s, mapping: 'root.n = counter()' }
output:
  http_client:
    url: http://127.0.0.1:$1/post
    verb: POST
YAML
}
receiver() {  # <port> <file>
  cat <<YAML
input:
  http_server:
    address: "127.0.0.1:$1"
    path: /post
output:
  file: { path: $2 }
YAML
}

# <name> <receiver impl> <sender impl>
round_trip() {
  local name=$1 recv=$2 send=$3 port out
  port=$(free_port)
  out="$WORK/rt.txt"
  rm -f "$out"
  receiver "$port" "$out" > "$WORK/recv.yaml"
  sender "$port" 3           > "$WORK/send.yaml"
  local pid
  pid=$(start_and_wait "$recv" "$WORK/recv.yaml" "$port" /post) || {
    bad "$name" "the receiver never came up"; return; }
  if [ "$send" = sf ]; then
    # shellcheck disable=SC2086
    timeout 30 "$SF" --config "$WORK/send.yaml" $SEA_ARGS >/dev/null 2>&1
  else
    timeout 30 "$RC" run --log.level off "$WORK/send.yaml" >/dev/null 2>&1
  fi
  sleep 0.5
  stop "$pid"
  local got
  got=$(LC_ALL=C sort < "$out" 2>/dev/null | tr '\n' '|')
  # The probe body is there too: it is how the receiver was known to be up.
  [ "$got" = 'ready-probe|{"n":1}|{"n":2}|{"n":3}|' ] \
    && note "$name" || bad "$name" "got '$got'"
}

round_trip "http_client to http_server, both ours" sf sf
if [ -x "$RC" ]; then
  round_trip "our http_client into the reference's http_server" rc sf
  round_trip "the reference's http_client into our http_server" sf rc
fi

# ---- the http_client INPUT, polling and streaming ------------------------------
#
# The peer is Python rather than the reference: neither implementation has an
# output that serves a stream to a connecting client yet, so there is nothing to
# point ours at. The check that IS cross-implementation is the one below it --
# the reference reads the same peer and must produce the same messages.
PORT=$(free_port)
python3 - "$PORT" >"$WORK/peer.log" 2>&1 <<'PY' &
import http.server, sys
n = [0]
class H(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        n[0] += 1
        body = ('{"tick":%d}' % n[0]).encode()
        self.send_response(200)
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
output:
  stdout: {}
YAML
# Polling never ends, so the run is bounded and only its SHAPE is checked: the
# counter in each body differs between runs, so the digits are normalised away.
# shellcheck disable=SC2086
got=$(timeout 3 "$SF" --config "$WORK/poll.yaml" $SEA_ARGS 2>/dev/null | head -3 \
      | sed 's/[0-9][0-9]*/N/' | tr '\n' '|')
[ "$got" = '{"tick":N}|{"tick":N}|{"tick":N}|' ] \
  && note "http_client input polls and each body is a message" \
  || bad "http_client input polls and each body is a message" "got '$got'"

if [ -x "$RC" ]; then
  "$RC" lint "$WORK/poll.yaml" >/dev/null 2>&1 \
    && note "the reference accepts our http_client input config" \
    || bad "the reference accepts our http_client input config" "it failed to lint"
  ref=$(timeout 3 "$RC" run --log.level off "$WORK/poll.yaml" 2>/dev/null | head -3 \
        | sed 's/[0-9][0-9]*/N/' | tr '\n' '|')
  [ "$ref" = "$got" ] && note "the reference polls the same peer identically" \
                      || bad "the reference polls the same peer identically" "got '$ref'"
fi

# Streaming: one long-lived chunked response, framed line by line.
PORT=$(free_port)
python3 - "$PORT" >"$WORK/stream.log" 2>&1 <<'PY' &
import socket, sys, time
srv = socket.socket()
srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
srv.bind(("127.0.0.1", int(sys.argv[1])))
srv.listen(4)
while True:
    c, _ = srv.accept()
    c.recv(65536)
    c.sendall(b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n"
              b"Transfer-Encoding: chunked\r\n\r\n")
    for i in range(1, 4):
        body = ('{"s":%d}\n' % i).encode()
        c.sendall(b"%x\r\n" % len(body) + body + b"\r\n")
        time.sleep(0.1)
    c.sendall(b"0\r\n\r\n")
    c.close()
PY
PIDS+=("$!")
for i in $(seq 1 60); do
  curl -s -o /dev/null --max-time 1 "http://127.0.0.1:$PORT/feed" && break
  sleep 0.25
done
cat > "$WORK/stream.yaml" <<YAML
input:
  http_client:
    url: http://127.0.0.1:$PORT/feed
    verb: GET
    stream:
      enabled: true
      reconnect: false
output:
  stdout: {}
YAML
# shellcheck disable=SC2086
got=$(timeout 8 "$SF" --config "$WORK/stream.yaml" $SEA_ARGS 2>/dev/null | tr '\n' '|')
[ "$got" = '{"s":1}|{"s":2}|{"s":3}|' ] \
  && note "http_client input streams a chunked response line by line" \
  || bad "http_client input streams a chunked response line by line" "got '$got'"
if [ -x "$RC" ]; then
  ref=$(timeout 8 "$RC" run --log.level off "$WORK/stream.yaml" 2>/dev/null | tr '\n' '|')
  [ "$ref" = "$got" ] && note "the reference streams the same peer identically" \
                      || bad "the reference streams the same peer identically" "got '$ref'"
fi

# ---- the http_server OUTPUT ---------------------------------------------------
#
# Readiness is probed on a path the output does NOT serve, so the probe gets a
# 404 and consumes nothing. Probing the message endpoint would take a batch --
# and when none was ready it would block for the whole `timeout` and take one
# anyway, which makes every expectation below a guess.
wait_http() {  # <port>
  local i code
  for i in $(seq 1 80); do
    code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 1 \
           "http://127.0.0.1:$1/__probe" 2>/dev/null)
    [ -n "$code" ] && [ "$code" != 000 ] && return 0
    sleep 0.25
  done
  return 1
}

srvout_cfg() {  # <port> <count>
  cat <<YAML
input:
  generate: { count: $2, interval: 0s, mapping: 'root.n = counter()' }
output:
  http_server:
    address: "127.0.0.1:$1"
    path: /get
YAML
}

PORT=$(free_port)
srvout_cfg "$PORT" 4 > "$WORK/srvout.yaml"
# shellcheck disable=SC2086
"$SF" --config "$WORK/srvout.yaml" $SEA_ARGS >>"$WORK/server.log" 2>&1 &
PIDS+=("$!"); srvpid=$!
if wait_http "$PORT"; then
  # Every message must arrive, the LAST one included: it is the one a shutdown
  # race used to cut off after it had already been acked.
  got=""
  for i in 1 2 3 4; do got="$got$(curl -s -m 8 "http://127.0.0.1:$PORT/get")|"; done
  stop "$srvpid"
  [ "$got" = '{"n":1}|{"n":2}|{"n":3}|{"n":4}|' ] \
    && note "http_server output serves one batch per request, to the last one" \
    || bad "http_server output serves one batch per request, to the last one" "got '$got'"
else
  bad "http_server output serves one batch per request, to the last one" "no server"
fi

# CONCURRENT requests: the regression test for the rendezvous. With a
# seastar::queue underneath, a second request blocked in pop overwrote the
# first's promise and both failed, so most batches were nacked rather than
# delivered. Sequential requests cannot see it.
PORT=$(free_port)
srvout_cfg "$PORT" 6 > "$WORK/srvout2.yaml"
# shellcheck disable=SC2086
"$SF" --config "$WORK/srvout2.yaml" $SEA_ARGS >>"$WORK/server.log" 2>&1 &
PIDS+=("$!"); srvpid=$!
if wait_http "$PORT"; then
  rm -f "$WORK"/conc.*
  cpids=()
  for i in $(seq 1 10); do
    ( curl -s -m 12 "http://127.0.0.1:$PORT/get" > "$WORK/conc.$i" 2>&1 ) &
    cpids+=("$!")
  done
  # Only the clients: a bare `wait` would also wait on the server process and on
  # any other job this script has left running.
  for cp in "${cpids[@]}"; do wait "$cp" 2>/dev/null; done
  stop "$srvpid"
  # Counted FILE BY FILE. A response body carries no trailing newline, so `cat`
  # of the lot joins several into one line, and `grep -c` then reports two
  # deliveries where there were six -- a measurement bug that reads exactly like
  # the delivery bug this case exists to catch, and did once.
  bodies=0
  for f in "$WORK"/conc.*; do
    [ -s "$f" ] || continue
    case "$(cat "$f")" in '{"n":'*) bodies=$((bodies + 1)) ;; esac
  done
  distinct=$(for f in "$WORK"/conc.*; do [ -s "$f" ] && cat "$f" && echo; done \
             | grep '^{"n":' | sort -u | wc -l)
  if [ "$bodies" = 6 ] && [ "$distinct" = 6 ]; then
    note "ten concurrent requests share six batches, none lost or duplicated"
  else
    bad "ten concurrent requests share six batches, none lost or duplicated" \
        "$bodies delivered, $distinct distinct (expected 6 and 6)"
  fi
else
  bad "ten concurrent requests share six batches, none lost or duplicated" "no server"
fi

# Streaming: one long-lived response, one batch per line.
PORT=$(free_port)
cat > "$WORK/srvstream.yaml" <<YAML
input:
  generate: { count: 3, interval: 100ms, mapping: 'root.n = counter()' }
output:
  http_server:
    address: "127.0.0.1:$PORT"
    stream_path: /get/stream
YAML
# shellcheck disable=SC2086
"$SF" --config "$WORK/srvstream.yaml" $SEA_ARGS >>"$WORK/server.log" 2>&1 &
PIDS+=("$!"); srvpid=$!
if wait_http "$PORT"; then
  got=$(timeout 15 curl -s -N -m 10 "http://127.0.0.1:$PORT/get/stream" | tr '\n' '|')
  stop "$srvpid"
  [ "$got" = '{"n":1}|{"n":2}|{"n":3}|' ] \
    && note "http_server output streams one batch per line" \
    || bad "http_server output streams one batch per line" "got '$got'"
else
  bad "http_server output streams one batch per line" "no server"
fi

# Client and server across implementations, both ways.
srvout_round_trip() {  # <name> <server impl> <client impl>
  local name=$1 srv=$2 cli=$3 port out pid
  port=$(free_port)
  srvout_cfg "$port" 3 > "$WORK/rt_srv.yaml"
  cat > "$WORK/rt_cli.yaml" <<YAML
input:
  http_client: { url: "http://127.0.0.1:$port/get", verb: GET }
output:
  stdout: {}
YAML
  if [ "$srv" = sf ]; then
    # shellcheck disable=SC2086
    "$SF" --config "$WORK/rt_srv.yaml" $SEA_ARGS >>"$WORK/server.log" 2>&1 &
  else
    "$RC" run --log.level off "$WORK/rt_srv.yaml" >>"$WORK/server.log" 2>&1 &
  fi
  pid=$!; PIDS+=("$pid")
  wait_http "$port" || { bad "$name" "the server never came up"; return; }
  if [ "$cli" = sf ]; then
    # shellcheck disable=SC2086
    out=$(timeout 12 "$SF" --config "$WORK/rt_cli.yaml" $SEA_ARGS 2>/dev/null \
          | head -3 | sort | tr '\n' '|')
  else
    out=$(timeout 12 "$RC" run --log.level off "$WORK/rt_cli.yaml" 2>/dev/null \
          | head -3 | sort | tr '\n' '|')
  fi
  stop "$pid"
  [ "$out" = '{"n":1}|{"n":2}|{"n":3}|' ] && note "$name" || bad "$name" "got '$out'"
}

srvout_round_trip "our http_client input against our http_server output" sf sf
if [ -x "$RC" ]; then
  "$RC" lint "$WORK/rt_srv.yaml" >/dev/null 2>&1 \
    && note "the reference accepts our http_server output config" \
    || bad "the reference accepts our http_server output config" "it failed to lint"
  srvout_round_trip "our http_client input against the reference's http_server output" rc sf
  srvout_round_trip "the reference's http_client input against our http_server output" sf rc
fi

# ---- concurrent posts into the http_server INPUT ------------------------------
#
# The same rendezvous regression from the other side: every concurrent request
# is a PRODUCER, and with a seastar::queue underneath the second one to block
# broke the first's promise and was answered 503 for no reason.
PORT=$(free_port)
cat > "$WORK/concin.yaml" <<YAML
input:
  http_server:
    address: "127.0.0.1:$PORT"
    path: /post
pipeline:
  processors:
    - sleep: { duration: 20ms }
output:
  file: { path: $WORK/concin.txt }
YAML
rm -f "$WORK/concin.txt"
# shellcheck disable=SC2086
"$SF" --config "$WORK/concin.yaml" $SEA_ARGS >>"$WORK/server.log" 2>&1 &
PIDS+=("$!"); srvpid=$!
if wait_http "$PORT"; then
  cpids=()
  for i in $(seq 1 12); do
    ( curl -s -o /dev/null -m 20 -X POST -d "body-$i" "http://127.0.0.1:$PORT/post" ) &
    cpids+=("$!")
  done
  for cp in "${cpids[@]}"; do wait "$cp" 2>/dev/null; done
  sleep 0.5
  stop "$srvpid"
  n=$(grep -c '^body-' "$WORK/concin.txt" 2>/dev/null || true)
  d=$(grep '^body-' "$WORK/concin.txt" 2>/dev/null | sort -u | wc -l)
  if [ "${n:-0}" = 12 ] && [ "$d" = 12 ]; then
    note "twelve concurrent posts all reach the pipeline"
  else
    bad "twelve concurrent posts all reach the pipeline" \
        "${n:-0} arrived, $d distinct (expected 12 and 12)"
  fi
else
  bad "twelve concurrent posts all reach the pipeline" "no server"
fi

# ---- websocket, both directions ------------------------------------------------
#
# Ours are CLIENTS, as the reference's are. Its server side lives on
# `http_server`'s `ws_path`, which gives the cross-implementation peer for both
# directions; `ws_path` on OUR http_server is refused by name and is checked
# below.
#
# Two of these use a bare Python server instead, because they assert things no
# peer pipeline can show: that the last message reached the wire, and that a
# message over 512 bytes arrives as one frame.

ws_probe_up() {  # <port> -- waits for the probe server to accept
  local i
  for i in $(seq 1 60); do
    python3 - "$1" <<'PY' && return 0
import socket, sys
s = socket.socket(); s.settimeout(0.25)
try:
    s.connect(("127.0.0.1", int(sys.argv[1]))); s.close(); sys.exit(0)
except OSError:
    sys.exit(1)
PY
    sleep 0.25
  done
  return 1
}

# Every message must arrive, the LAST one included. seastar's socket output
# batches its flushes, so send_data() resolves before the bytes are on the wire
# and a teardown that shuts the socket down discards whatever is pending -- which
# lost the final message of every run while the pipeline reported it acked.
PORT=$(free_port)
rm -f "$WORK/ws_all.txt"
python3 "$PWD/tests/ws_probe_server.py" "$PORT" "$WORK/ws_all.txt" >>"$WORK/server.log" 2>&1 &
PIDS+=("$!"); probe=$!
if ws_probe_up "$PORT"; then
  cat > "$WORK/ws_send.yaml" <<YAML
input:
  generate: { count: 6, interval: 60ms, mapping: 'root.n = counter()' }
output:
  websocket:
    url: "ws://127.0.0.1:$PORT/post/ws"
YAML
  # shellcheck disable=SC2086
  timeout 25 "$SF" --config "$WORK/ws_send.yaml" $SEA_ARGS >/dev/null 2>&1
  sleep 0.7
  got=$(tr '\n' '|' < "$WORK/ws_all.txt" 2>/dev/null)
  [ "$got" = '{"n":1}|{"n":2}|{"n":3}|{"n":4}|{"n":5}|{"n":6}|' ] \
    && note "websocket output delivers every message, the last one included" \
    || bad "websocket output delivers every message, the last one included" "got '$got'"
else
  bad "websocket output delivers every message, the last one included" "no probe server"
fi
kill "$probe" 2>/dev/null

# A message over 512 bytes must be ONE frame. Writing through seastar's
# websocket output_stream would split it into 512-byte pieces, each sent as its
# own complete frame, so the peer would see five messages instead of one.
PORT=$(free_port)
rm -f "$WORK/ws_big.txt"
python3 "$PWD/tests/ws_probe_server.py" "$PORT" "$WORK/ws_big.txt" >>"$WORK/server.log" 2>&1 &
PIDS+=("$!"); probe=$!
if ws_probe_up "$PORT"; then
  cat > "$WORK/ws_big.yaml" <<YAML
input:
  generate: { count: 1, interval: 0s, mapping: 'root.big = "x".repeat(2000)' }
output:
  websocket:
    url: "ws://127.0.0.1:$PORT/post/ws"
YAML
  # shellcheck disable=SC2086
  timeout 25 "$SF" --config "$WORK/ws_big.yaml" $SEA_ARGS >/dev/null 2>&1
  sleep 0.7
  frames=$(wc -l < "$WORK/ws_big.txt" 2>/dev/null || echo 0)
  len=$(awk 'NR==1{print length($0)}' "$WORK/ws_big.txt" 2>/dev/null)
  if [ "${frames:-0}" = 1 ] && [ "${len:-0}" = 2010 ]; then
    note "a message over 512 bytes is one frame, not several"
  else
    bad "a message over 512 bytes is one frame, not several" \
        "${frames:-0} frames, first of length ${len:-0} (expected 1 and 2010)"
  fi
else
  bad "a message over 512 bytes is one frame, not several" "no probe server"
fi
kill "$probe" 2>/dev/null

# Cross-implementation, both directions, against the reference's ws_path.
if [ -x "$RC" ]; then
  PORT=$(free_port)
  cat > "$WORK/ws_rcrecv.yaml" <<YAML
input:
  http_server:
    address: "127.0.0.1:$PORT"
    ws_path: /post/ws
output:
  file: { path: $WORK/ws_rc.txt }
YAML
  cat > "$WORK/ws_sfsend.yaml" <<YAML
input:
  generate: { count: 4, interval: 60ms, mapping: 'root.n = counter()' }
output:
  websocket:
    url: "ws://127.0.0.1:$PORT/post/ws"
YAML
  "$RC" lint "$WORK/ws_sfsend.yaml" >/dev/null 2>&1 \
    && note "the reference accepts our websocket output config" \
    || bad "the reference accepts our websocket output config" "it failed to lint"
  rm -f "$WORK/ws_rc.txt"
  "$RC" run --log.level off "$WORK/ws_rcrecv.yaml" >>"$WORK/server.log" 2>&1 &
  PIDS+=("$!"); rcpid=$!
  sleep 2
  # shellcheck disable=SC2086
  timeout 25 "$SF" --config "$WORK/ws_sfsend.yaml" $SEA_ARGS >/dev/null 2>&1
  sleep 1
  stop "$rcpid"
  got=$(tr '\n' '|' < "$WORK/ws_rc.txt" 2>/dev/null)
  [ "$got" = '{"n":1}|{"n":2}|{"n":3}|{"n":4}|' ] \
    && note "our websocket output into the reference's websocket server" \
    || bad "our websocket output into the reference's websocket server" "got '$got'"

  PORT=$(free_port)
  cat > "$WORK/ws_rcsrv.yaml" <<YAML
input:
  generate: { count: 4, interval: 150ms, mapping: 'root.n = counter()' }
output:
  http_server:
    address: "127.0.0.1:$PORT"
    ws_path: /get/ws
YAML
  cat > "$WORK/ws_sfcli.yaml" <<YAML
input:
  websocket:
    url: "ws://127.0.0.1:$PORT/get/ws"
output:
  stdout: {}
YAML
  "$RC" lint "$WORK/ws_sfcli.yaml" >/dev/null 2>&1 \
    && note "the reference accepts our websocket input config" \
    || bad "the reference accepts our websocket input config" "it failed to lint"
  "$RC" run --log.level off "$WORK/ws_rcsrv.yaml" >>"$WORK/server.log" 2>&1 &
  PIDS+=("$!"); rcpid=$!
  sleep 2
  # shellcheck disable=SC2086
  got=$(timeout 12 "$SF" --config "$WORK/ws_sfcli.yaml" $SEA_ARGS 2>/dev/null \
        | head -4 | tr '\n' '|')
  stop "$rcpid"
  [ "$got" = '{"n":1}|{"n":2}|{"n":3}|{"n":4}|' ] \
    && note "our websocket input from the reference's websocket server" \
    || bad "our websocket input from the reference's websocket server" "got '$got'"
fi

# ---- configuration that must be refused by name -------------------------------
refuses() {  # <name> <config> <expected substring>
  local err
  # shellcheck disable=SC2086
  err=$(timeout 30 "$SF" --config "$2" $SEA_ARGS 2>&1 >/dev/null)
  grep -qF "$3" <<<"$err" && note "$1" \
    || bad "$1" "expected an error containing '$3', got: $(tail -2 <<<"$err")"
}

cat > "$WORK/https.yaml" <<'YAML'
input: { generate: { count: 1, interval: 0s, mapping: 'root = {}' } }
output:
  http_client:
    url: https://example.com/post
YAML
cat > "$WORK/tls.yaml" <<'YAML'
input: { generate: { count: 1, interval: 0s, mapping: 'root = {}' } }
output:
  http_client:
    url: http://127.0.0.1:1/post
    tls:
      enabled: true
YAML
cat > "$WORK/noaddr.yaml" <<'YAML'
input:
  http_server:
    path: /post
output: { drop: {} }
YAML

cat > "$WORK/wss.yaml" <<'YAML'
input: { generate: { count: 1, interval: 0s, mapping: 'root = {}' } }
output:
  websocket:
    url: wss://example.com/ws
YAML
cat > "$WORK/wspath.yaml" <<'YAML'
input:
  http_server:
    address: "127.0.0.1:1"
    ws_path: /post/ws
output: { drop: {} }
YAML

refuses "wss is refused rather than sent in the clear"  "$WORK/wss.yaml"    "wss is not implemented"
# No backtick in the expected text: the message contains one and it would open a
# command substitution inside the double quotes.
refuses "ws_path is refused rather than silently unserved" "$WORK/wspath.yaml" \
        "websocket cannot share a listener"
refuses "https is refused rather than sent in the clear" "$WORK/https.yaml" "https is not implemented"
refuses "an unimplemented tls block is named"            "$WORK/tls.yaml"   "tls"
refuses "http_server without an address is refused"      "$WORK/noaddr.yaml" "address"

# ---- websocket connection fields ---------------------------------------------------
#
# `open_message_type` was accepted and IGNORED: send_message always sent a
# BINARY frame, so `text` produced the wrong frame type -- which some servers
# reject and which no comparison of message bodies would have shown. Read back
# off the wire by a neutral Python server, so the assertion is on the frame
# opcode rather than on our own encoder agreeing with our own decoder.
PORT=$(free_port)
python3 - "$PORT" "$WORK/opcode.txt" >"$WORK/wsopc.log" 2>&1 <<'PY' &
import asyncio, sys, hashlib, base64, struct
async def handle(r, w):
    req = await r.readuntil(b"\r\n\r\n")
    key = ""
    for line in req.decode(errors="replace").split("\r\n"):
        if line.lower().startswith("sec-websocket-key:"):
            key = line.split(":", 1)[1].strip()
    acc = base64.b64encode(hashlib.sha1(
        (key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest()).decode()
    w.write(("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n"
             "Connection: Upgrade\r\nSec-WebSocket-Accept: " + acc + "\r\n\r\n").encode())
    await w.drain()
    hdr = await r.readexactly(2)
    opcode = hdr[0] & 0x0F
    open(sys.argv[2], "w").write(str(opcode))
    w.close()
async def main():
    srv = await asyncio.start_server(handle, "127.0.0.1", int(sys.argv[1]))
    async with srv: await srv.serve_forever()
asyncio.run(main())
PY
PIDS+=("$!")
sleep 1

opcode_for() {  # <open_message_type> -> the opcode the peer saw
  rm -f "$WORK/opcode.txt"
  cat > "$WORK/wsopc.yaml" <<YAML
input:
  websocket:
    url: "ws://127.0.0.1:$PORT/"
    open_message: hello
    open_message_type: $1
    connection: { max_retries: 0 }
output: { drop: {} }
YAML
  # shellcheck disable=SC2086
  timeout 6 "$SF" --config "$WORK/wsopc.yaml" $SEA_ARGS >/dev/null 2>&1
  cat "$WORK/opcode.txt" 2>/dev/null
}
got=$(opcode_for binary)
[ "$got" = "2" ] && note "open_message_type binary sends a binary frame (opcode 2)" \
                 || bad "open_message_type binary sends a binary frame (opcode 2)" "opcode $got"
got=$(opcode_for text)
[ "$got" = "1" ] && note "open_message_type text sends a TEXT frame (opcode 1)" \
                 || bad "open_message_type text sends a TEXT frame (opcode 1)" "opcode $got"

# `connection.max_retries` replaced a swordfish-only `reconnect_period` the
# reference lints as unrecognised. Both directions are asserted.
cat > "$WORK/ws_old.yaml" <<'YAML'
input: { websocket: { url: "ws://127.0.0.1:9/x", reconnect_period: 2s } }
output: { drop: {} }
YAML
out=$(build/sfconfig lint "$WORK/ws_old.yaml" 2>&1)
# spec_of's wording, not only_fields': the websocket config is declared rather
# than hand-parsed, so the message comes from the config framework.
case "$out" in
  *"'reconnect_period' is not recognised"*) note "the swordfish-only reconnect_period is gone" ;;
  *) bad "the swordfish-only reconnect_period is gone" "lint said '$out'" ;;
esac
cat > "$WORK/ws_new.yaml" <<'YAML'
input: { websocket: { url: "ws://127.0.0.1:9/x", connection: { max_retries: 3 } } }
output: { drop: {} }
YAML
if out=$(build/sfconfig lint "$WORK/ws_new.yaml" 2>&1) && [ -z "$out" ]; then
  note "and the reference's connection.max_retries is accepted"
else
  bad "and the reference's connection.max_retries is accepted" "lint said '$out'"
fi
if [ -x "$RC" ]; then
  "$RC" lint "$WORK/ws_new.yaml" >/dev/null 2>&1 \
    && note "the reference accepts the same websocket config" \
    || bad "the reference accepts the same websocket config" "it failed to lint"
fi
# `timeout` bounds the ENQUEUE as well as the wait for a response. It bounded
# only the wait, and the queue is one deep, so once the pipeline stalled every
# request past the first parked in push() for ever: connections accumulated and
# the server stopped accepting, which is the outcome `timeout` exists to prevent.
# The status is 408, as the reference answers -- it said 503, and the two mean
# different things to a caller deciding whether to retry.
TO_PORT=$(free_port)
cat > "$WORK/timeout.yaml" <<YAML
input:
  http_server:
    address: 127.0.0.1:$TO_PORT
    path: /post
    timeout: 1s
pipeline:
  processors: [ { sleep: { duration: 30s } } ]
output: { drop: {} }
YAML
# shellcheck disable=SC2086
timeout 40 "$SF" --config "$WORK/timeout.yaml" $SEA_ARGS >/dev/null 2>&1 &
TOSRV=$!
PIDS+=("$TOSRV")
for _i in $(seq 1 60); do
  curl -s -o /dev/null --max-time 1 "http://127.0.0.1:$TO_PORT/__probe" && break
  sleep 0.2
done
to_seq=$(curl -s -o /dev/null -w '%{http_code}' --max-time 8 -X POST -d a \
              "http://127.0.0.1:$TO_PORT/post" 2>/dev/null)
# Concurrent, because it is the QUEUE filling that used to park a request for
# ever -- a sequential probe never fills a one-deep queue.
: > "$WORK/timeout.codes"
# Wait on THESE three and nothing else. A bare `wait` waits for every background
# job the script has ever started, and several of those -- the websocket probe
# servers -- do not exit until the EXIT trap fires, so it hung the run for as
# long as the outer timeout allowed.
to_pids=()
for _i in 1 2 3; do
  ( curl -s -o /dev/null -w '%{http_code}\n' --max-time 8 -X POST -d z \
         "http://127.0.0.1:$TO_PORT/post" >> "$WORK/timeout.codes" 2>/dev/null ) &
  to_pids+=("$!")
done
wait "${to_pids[@]}"
kill -KILL "$TOSRV" 2>/dev/null; wait "$TOSRV" 2>/dev/null
to_conc=$(sort "$WORK/timeout.codes" | tr '\n' ' ')
if [ "$to_seq" != 408 ]; then
  bad "a stalled pipeline answers 408 rather than holding the connection" \
      "sequential request got $to_seq"
elif [ "$to_conc" != "408 408 408 " ]; then
  bad "a stalled pipeline answers 408 rather than holding the connection" \
      "concurrent requests got '$to_conc'"
else
  note "a stalled pipeline answers 408 rather than holding the connection"
fi

# `path` has three documented forms and swordfish implemented one. A trailing `/`
# "will match against all extensions of that path", and `{param}` segments "are
# added to ingested messages as metadata" -- neither was implemented and neither
# was refused, so a documented path form 404'd every request it was written to
# serve while both linters passed.
path_case() {  # <name> <configured path> <request path> <expected code>
  local name=$1 cfg=$2 reqp=$3 want=$4 port code
  port=$(free_port)
  cat > "$WORK/path.yaml" <<YAML
input: { http_server: { address: 127.0.0.1:$port, path: "$cfg" } }
pipeline: { processors: [ { mapping: 'root.seen = content().string()' } ] }
output: { stdout: {} }
YAML
  # shellcheck disable=SC2086
  timeout 20 "$SF" --config "$WORK/path.yaml" $SEA_ARGS >/dev/null 2>&1 &
  local pid=$!
  PIDS+=("$pid")
  local i
  for i in $(seq 1 60); do
    curl -s -o /dev/null --max-time 1 "http://127.0.0.1:$port/__probe" && break
    sleep 0.2
  done
  code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 -X POST -d hi \
              "http://127.0.0.1:$port$reqp" 2>/dev/null)
  kill -KILL "$pid" 2>/dev/null; wait "$pid" 2>/dev/null
  [ "$code" = "$want" ] && note "$name" || bad "$name" "expected $want, got $code"
}
path_case "an exact path matches"                      "/post"   "/post"          200
path_case "and a query string does not break it"       "/post"   "/post?x=1"      200
path_case "a trailing slash matches everything under"  "/post/"  "/post/anything" 200
path_case "but not a mere prefix of the segment"       "/post/"  "/postX"         404
path_case "a {param} segment matches one segment"      "/p/{id}" "/p/42"          200
path_case "and not two"                                "/p/{id}" "/p/42/extra"    404
path_case "and not an empty one"                       "/p/{id}" "/p/"            404

# A captured parameter becomes metadata under its own name, as it does there.
PARAM_PORT=$(free_port)
cat > "$WORK/param.yaml" <<YAML
input: { http_server: { address: 127.0.0.1:$PARAM_PORT, path: "/p/{id}/{kind}" } }
pipeline:
  processors: [ { mapping: 'root = { "id": meta("id"), "kind": meta("kind") }' } ]
output: { stdout: {} }
YAML
# shellcheck disable=SC2086
timeout 20 "$SF" --config "$WORK/param.yaml" $SEA_ARGS > "$WORK/param.out" 2>/dev/null &
PSRV=$!
PIDS+=("$PSRV")
for _i in $(seq 1 60); do
  curl -s -o /dev/null --max-time 1 "http://127.0.0.1:$PARAM_PORT/__probe" && break
  sleep 0.2
done
curl -s -o /dev/null --max-time 5 -X POST -d hi \
     "http://127.0.0.1:$PARAM_PORT/p/42/widget" 2>/dev/null
sleep 1
kill -KILL "$PSRV" 2>/dev/null; wait "$PSRV" 2>/dev/null
got=$(grep '^{' "$WORK/param.out" | head -1)
[ "$got" = '{"id":"42","kind":"widget"}' ] \
  && note "path parameters become metadata under their own names" \
  || bad "path parameters become metadata under their own names" "got '$got'"

# `allowed_verbs` on the OUTPUT could only ever restrict: both routes were
# registered GET-only, so `allowed_verbs: [POST]` 404'd every request where the
# reference answers 200. Every verb is registered now and verb_allowed() is the
# only filter -- which is also what makes a disallowed verb a 405 rather than the
# router's 404, as it is there.
verb_case() {  # <name> <allowed_verbs yaml> <method> <expected>
  local name=$1 verbs=$2 method=$3 want=$4 port code
  port=$(free_port)
  cat > "$WORK/verb.yaml" <<YAML
input: { generate: { count: 3, interval: 100ms, mapping: 'root = "hello"' } }
output: { http_server: { address: 127.0.0.1:$port, path: /get, allowed_verbs: $verbs } }
YAML
  # shellcheck disable=SC2086
  timeout 20 "$SF" --config "$WORK/verb.yaml" $SEA_ARGS >/dev/null 2>&1 &
  local pid=$!
  PIDS+=("$pid")
  local i
  for i in $(seq 1 60); do
    curl -s -o /dev/null --max-time 1 "http://127.0.0.1:$port/get" && break
    sleep 0.2
  done
  code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 -X "$method" \
              "http://127.0.0.1:$port/get" 2>/dev/null)
  kill -KILL "$pid" 2>/dev/null; wait "$pid" 2>/dev/null
  [ "$code" = "$want" ] && note "$name" || bad "$name" "expected $want, got $code"
}
verb_case "allowed_verbs can ADD a verb, not only restrict" "[POST]" POST 200
verb_case "a disallowed verb is 405, not the router's 404"  "[GET]"  POST 405
verb_case "and an allowed one still answers"                "[GET]"  GET  200

# A stream client that DIES must stop competing for batches. A peer's FIN leaves
# the socket writable, so every write into the dead connection still succeeds and
# the handler's write-error path never fires: before patches/0001 gave seastar's
# httpd a way to report the disconnect, the zombie handler kept taking batches
# from the shared queue and acking them as delivered. Of eight messages with one
# client killed, the survivor saw four.
#
# The assertion is that the surviving client sees an UNBROKEN run to the end. A
# zombie taking every other batch is exactly what that catches.
STREAM_PORT=$(free_port)
cat > "$WORK/zombie.yaml" <<YAML
input:
  generate:
    count: 8
    interval: 1s
    mapping: 'root = "m" + count("zombie").string()'
output:
  http_server:
    address: 127.0.0.1:$STREAM_PORT
    stream_path: /get/stream
YAML
# shellcheck disable=SC2086
timeout 30 "$SF" --config "$WORK/zombie.yaml" $SEA_ARGS >/dev/null 2>&1 &
ZSRV=$!
PIDS+=("$ZSRV")
# A TCP connect, NOT an HTTP GET: probing /get/stream would make the probe itself
# a stream client, and it would eat the first batches before the test began.
for _i in $(seq 1 60); do
  python3 - "$STREAM_PORT" <<'PYEOF' && break
import socket, sys
s = socket.socket(); s.settimeout(0.25)
try:
    s.connect(("127.0.0.1", int(sys.argv[1]))); s.close(); sys.exit(0)
except OSError:
    sys.exit(1)
PYEOF
  sleep 0.2
done
timeout 20 curl -sN "http://127.0.0.1:$STREAM_PORT/get/stream" > "$WORK/zombie.out" 2>&1 &
ZCLI=$!
sleep 1.5
kill -KILL "$ZCLI" 2>/dev/null            # the client dies mid-stream
sleep 0.5
timeout 12 curl -sN "http://127.0.0.1:$STREAM_PORT/get/stream" > "$WORK/live.out" 2>&1 &
LCLI=$!
sleep 9
kill -KILL "$LCLI" 2>/dev/null; wait "$LCLI" 2>/dev/null
kill -KILL "$ZSRV" 2>/dev/null; wait "$ZSRV" 2>/dev/null
live=$(tr '\n' ' ' < "$WORK/live.out")
# Whatever it starts at, the run must be CONSECUTIVE and reach m8.
gap=0
prev=
for tok in $live; do
  n=${tok#m}
  [ -n "$prev" ] && [ "$n" != "$((prev + 1))" ] && gap=1
  prev=$n
done
if [ -z "$live" ]; then
  bad "a disconnected stream client stops taking batches" "the live client received nothing"
elif [ "$gap" = 1 ]; then
  bad "a disconnected stream client stops taking batches" \
      "the live client saw gaps -- a zombie handler is still taking batches: '$live'"
elif [ "$prev" != 8 ]; then
  bad "a disconnected stream client stops taking batches" \
      "the live client did not reach m8: '$live'"
else
  note "a disconnected stream client stops taking batches"
fi

# `headers` INTERPOLATE, on http_client in both directions and on sync_response --
# "this field supports interpolation functions" in the reference. They were sent
# literally, so a per-message correlation id went out as the raw `${! ... }` text
# and the receiver misrouted or rejected it, with no error on either side.
HDR_PORT=$(free_port)
# Written to a FILE and then run: a heredoc-fed `python3 - ... &` races
# wait_listen, because the background job does not start until the shell has read
# the whole heredoc.
cat > "$WORK/hdr_srv.py" <<'PYEOF'
import http.server, sys
port, out = int(sys.argv[1]), sys.argv[2]
class H(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        n = int(self.headers.get('content-length', 0))
        body = self.rfile.read(n)
        with open(out, 'a') as f:
            f.write('%s %s\n' % (self.headers.get('X-Trace', '<absent>'), body.decode()))
        self.send_response(200); self.send_header('content-length', '0'); self.end_headers()
    def log_message(self, *a): pass
http.server.HTTPServer(('127.0.0.1', port), H).serve_forever()
PYEOF
timeout 60 python3 "$WORK/hdr_srv.py" "$HDR_PORT" "$WORK/hdr.seen" >/dev/null 2>&1 &
HDR_SRV=$!
PIDS+=("$HDR_SRV")
# This file has no wait_listen (that lives in the socket check); poll inline.
for _i in $(seq 1 60); do
  curl -s -o /dev/null --max-time 1 -X POST -d x "http://127.0.0.1:$HDR_PORT/" && break
  sleep 0.2
done
cat > "$WORK/hdr.yaml" <<YAML
input: { generate: { count: 2, interval: 0s, mapping: 'root.id = counter()' } }
output:
  http_client:
    url: "http://127.0.0.1:$HDR_PORT/"
    verb: POST
    headers:
      X-Trace: 'id-\${! json("id") }'
YAML
: > "$WORK/hdr.seen"
# shellcheck disable=SC2086
timeout 30 "$SF" --config "$WORK/hdr.yaml" $SEA_ARGS >/dev/null 2>&1
hdr_sf=$(sort "$WORK/hdr.seen" | tr '\n' '|')
if [ -x "$RC" ]; then
  : > "$WORK/hdr.seen"
  timeout 30 "$RC" run --log.level off "$WORK/hdr.yaml" >/dev/null 2>&1
  hdr_rc=$(sort "$WORK/hdr.seen" | tr '\n' '|')
  [ "$hdr_sf" = "$hdr_rc" ] && note "http_client headers interpolate per message" \
                            || bad "http_client headers interpolate per message" \
                                   "swordfish '$hdr_sf' reference '$hdr_rc'"
else
  case "$hdr_sf" in
    *'id-1'*) note "http_client headers interpolate per message" ;;
    *) bad "http_client headers interpolate per message" "got '$hdr_sf'" ;;
  esac
fi
kill -KILL "$HDR_SRV" 2>/dev/null; wait "$HDR_SRV" 2>/dev/null

# A multipart request is a BATCH -- "the multiple parts are consumed as a batch
# of messages, where each body part is a message of the batch". It arrived as a
# single message containing the wire framing instead, so a three-part producer
# got one message of boundaries and part headers.
#
# The same case covers the request metadata, because the two are read from the
# same request: every header (in Go's canonical casing), every cookie, and the
# peer's IP. Only verb, path and user-agent were set, so a mapping reading an
# `Authorization` or `X-Request-Id` header saw nothing.
mp_run() {  # <bin-kind> <port> -> writes $WORK/mp.<kind>
  local kind=$1 port=$2 pid i
  cat > "$WORK/mp.yaml" <<YAML
input:
  http_server:
    address: 127.0.0.1:$port
    path: /post
pipeline:
  processors:
    - mapping: |
        root.body = content().string()
        root.xf = meta("X-Foo")
        root.ck = meta("sess")
        root.ip = meta("http_server_remote_ip")
output: { stdout: {} }
YAML
  if [ "$kind" = sf ]; then
    # shellcheck disable=SC2086
    timeout 30 "$SF" --config "$WORK/mp.yaml" $SEA_ARGS > "$WORK/mp.$kind" 2>/dev/null &
  else
    timeout 30 "$RC" run --log.level off "$WORK/mp.yaml" > "$WORK/mp.$kind" 2>/dev/null &
  fi
  pid=$!
  PIDS+=("$pid")
  for i in $(seq 1 60); do
    curl -s -o /dev/null --max-time 1 "http://127.0.0.1:$port/__probe" && break
    sleep 0.2
  done
  curl -s -o /dev/null --max-time 5 -X POST \
       -H 'X-Foo: bar' -H 'Cookie: sess=abc; other=1' \
       -F 'a=one' -F 'b=two' -F 'c=three' \
       "http://127.0.0.1:$port/post" 2>/dev/null
  sleep 1
  kill -KILL "$pid" 2>/dev/null; wait "$pid" 2>/dev/null
}
MP_PORT=$(free_port)
mp_run sf "$MP_PORT"
mp_sf=$(sort "$WORK/mp.sf" | tr '\n' '|')
if [ -x "$RC" ]; then
  MP_PORT2=$(free_port)
  mp_run rc "$MP_PORT2"
  mp_rc=$(sort "$WORK/mp.rc" | tr '\n' '|')
  [ -n "$mp_sf" ] && [ "$mp_sf" = "$mp_rc" ] \
    && note "a multipart request becomes a batch, with header/cookie/ip metadata" \
    || bad  "a multipart request becomes a batch, with header/cookie/ip metadata" \
            "swordfish '$mp_sf' reference '$mp_rc'"
else
  case "$mp_sf" in
    *'"body":"one"'*'"body":"three"'*)
      case "$mp_sf" in
        *'"ck":"abc"'*'"ip":"127.0.0.1"'*'"xf":"bar"'*)
          note "a multipart request becomes a batch, with header/cookie/ip metadata" ;;
        *) bad "a multipart request becomes a batch, with header/cookie/ip metadata" \
               "metadata missing from '$mp_sf'" ;;
      esac ;;
    *) bad "a multipart request becomes a batch, with header/cookie/ip metadata" \
           "got '$mp_sf'" ;;
  esac
fi

# The two malformed-multipart answers are NOT the same, and the intuitive reading
# gets one of them wrong: a body that never contains the declared boundary is
# answered 200 with zero messages (mime/multipart's NextPart returns io.EOF at
# once), while a Content-Type carrying no boundary parameter at all is a 400.
# Both were measured against the reference.
BAD_PORT=$(free_port)
cat > "$WORK/mpbad.yaml" <<YAML
input: { http_server: { address: 127.0.0.1:$BAD_PORT, path: /post } }
pipeline: { processors: [ { mapping: 'root = content().string()' } ] }
output: { drop: {} }
YAML
# shellcheck disable=SC2086
timeout 20 "$SF" --config "$WORK/mpbad.yaml" $SEA_ARGS >/dev/null 2>&1 &
MPBAD=$!
PIDS+=("$MPBAD")
for _i in $(seq 1 60); do
  curl -s -o /dev/null --max-time 1 "http://127.0.0.1:$BAD_PORT/__probe" && break
  sleep 0.2
done
mpbad_code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 -X POST \
                  -H 'Content-Type: multipart/form-data; boundary=zzz' \
                  -d 'not multipart at all' \
                  "http://127.0.0.1:$BAD_PORT/post" 2>/dev/null)
mpnb_code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 -X POST \
                 -H 'Content-Type: multipart/form-data' -d x \
                 "http://127.0.0.1:$BAD_PORT/post" 2>/dev/null)
kill -KILL "$MPBAD" 2>/dev/null; wait "$MPBAD" 2>/dev/null
[ "$mpbad_code" = 200 ] && note "a multipart body with no delimiter is 200 and zero messages" \
                        || bad "a multipart body with no delimiter is 200 and zero messages" \
                               "expected 200, got $mpbad_code"
[ "$mpnb_code" = 400 ] && note "a multipart content-type with no boundary is refused" \
                       || bad "a multipart content-type with no boundary is refused" \
                              "expected 400, got $mpnb_code"

# Shutting down must not throw away a reply the pipeline already produced.
# http_server::stop() shuts every OPEN connection, so stopping the server first
# discarded an assembled reply: the message was acked as delivered and the caller
# got an empty reply. SIGTERM lands mid-processor here, so the reply exists only
# because close() waits for the handler and lets it flush.
shut_case() {  # <kind> <port>
  local kind=$1 port=$2 pid
  cat > "$WORK/shut.yaml" <<YAML
input: { http_server: { address: 127.0.0.1:$port, path: /post, timeout: 30s } }
pipeline:
  processors:
    - sleep: { duration: 3s }
    - mapping: 'root = "done:" + content().string()'
output: { sync_response: {} }
YAML
  if [ "$kind" = sf ]; then
    # shellcheck disable=SC2086
    "$SF" --config "$WORK/shut.yaml" $SEA_ARGS >/dev/null 2>&1 &
  else
    "$RC" run --log.level off "$WORK/shut.yaml" >/dev/null 2>&1 &
  fi
  pid=$!
  PIDS+=("$pid")
  local i
  for i in $(seq 1 60); do
    curl -s -o /dev/null --max-time 1 "http://127.0.0.1:$port/__probe" && break
    sleep 0.2
  done
  ( sleep 1; kill -TERM "$pid" 2>/dev/null ) &
  curl -s -w '|%{http_code}' --max-time 15 -X POST -d X "http://127.0.0.1:$port/post" 2>/dev/null
  wait "$pid" 2>/dev/null
}
SHUT_PORT=$(free_port)
shut_sf=$(shut_case sf "$SHUT_PORT")
if [ -x "$RC" ]; then
  SHUT_PORT2=$(free_port)
  shut_rc=$(shut_case rc "$SHUT_PORT2")
else
  shut_rc='done:X|200'
fi
[ "$shut_sf" = "done:X|200" ] && [ "$shut_sf" = "$shut_rc" ] \
  && note "a reply assembled before shutdown is still delivered" \
  || bad  "a reply assembled before shutdown is still delivered" \
          "swordfish '$shut_sf' reference '$shut_rc'"

# `sync_response`'s status and headers apply only when the pipeline actually
# produced one. They were applied unconditionally, so routing to `drop` answered
# `201 Created` with the configured `Content-Type: application/json` over a
# zero-length body -- a caller parsed an empty body as the JSON it was promised.
NR_PORT=$(free_port)
cat > "$WORK/nr.yaml" <<YAML
input:
  http_server:
    address: 127.0.0.1:$NR_PORT
    path: /post
    sync_response: { status: 201, headers: { Content-Type: application/json, X-Foo: bar } }
output: { drop: {} }
YAML
# shellcheck disable=SC2086
timeout 20 "$SF" --config "$WORK/nr.yaml" $SEA_ARGS >/dev/null 2>&1 &
NRPID=$!
PIDS+=("$NRPID")
for _i in $(seq 1 60); do
  curl -s -o /dev/null --max-time 1 "http://127.0.0.1:$NR_PORT/__probe" && break
  sleep 0.2
done
nr_out=$(curl -s -i --max-time 5 -X POST -d hello "http://127.0.0.1:$NR_PORT/post" 2>/dev/null \
         | grep -iE '^HTTP/|^X-Foo|^Content-Type' | tr -d '\r' | tr '\n' ' ')
kill -KILL "$NRPID" 2>/dev/null; wait "$NRPID" 2>/dev/null
case "$nr_out" in
  'HTTP/1.1 200 OK '*)
    case "$nr_out" in
      *X-Foo*|*application/json*)
        bad "sync_response status and headers need a response to apply to" \
            "configured headers leaked: '$nr_out'" ;;
      *) note "sync_response status and headers need a response to apply to" ;;
    esac ;;
  *) bad "sync_response status and headers need a response to apply to" "got '$nr_out'" ;;
esac

# Malformed multipart, differentially. The parser is hand-rolled index
# arithmetic, so it gets an adversarial corpus rather than the two happy paths.
# Three of these diverged when the corpus was first run and each divergence was
# a real defect, the sharpest being `mp_empty`: a part whose body is EMPTY is
# encoded `CRLF CRLF --boundary`, and searching for the delimiter from the start
# of the body walks straight past it and swallows the boundary into the body.
mp_corpus() {  # <kind> <port> <bin>
  local kind=$1 port=$2 bin=$3 pid i
  cat > "$WORK/mpc.yaml" <<YAML
input: { http_server: { address: 127.0.0.1:$port, path: /post } }
pipeline: { processors: [ { mapping: 'root.n = content().string()' } ] }
output: { stdout: {} }
YAML
  if [ "$kind" = sf ]; then
    # shellcheck disable=SC2086
    "$bin" --config "$WORK/mpc.yaml" $SEA_ARGS > "$WORK/mpc.$kind.out" 2>/dev/null &
  else
    "$bin" run --log.level off "$WORK/mpc.yaml" > "$WORK/mpc.$kind.out" 2>/dev/null &
  fi
  pid=$!
  PIDS+=("$pid")
  for i in $(seq 1 80); do
    curl -s -o /dev/null --max-time 1 "http://127.0.0.1:$port/__probe" && break
    sleep 0.2
  done
  : > "$WORK/mpc.$kind.codes"
  for i in "${!MPC_NAME[@]}"; do
    printf '%s=%s\n' "${MPC_NAME[$i]}" \
      "$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 -X POST \
             -H 'Content-Type: multipart/form-data; boundary=zzz' \
             --data-binary "@$WORK/mpc.$i" "http://127.0.0.1:$port/post" 2>/dev/null)" \
      >> "$WORK/mpc.$kind.codes"
  done
  sleep 1
  kill -KILL "$pid" 2>/dev/null; wait "$pid" 2>/dev/null
}
MPC_NAME=(trunc_delim  no_hdr_term  no_close  immediate_close  empty_body \
          one_part     two_parts    preamble  boundary_in_body only_crlf \
          unterminated mp_empty)
MPC_BODY=('--zzz'
          '--zzz\r\n'
          '--zzz\r\n\r\n'
          '--zzz--'
          ''
          '--zzz\r\n\r\nA\r\n--zzz--\r\n'
          '--zzz\r\nX: 1\r\n\r\nA\r\n--zzz\r\n\r\nB\r\n--zzz--\r\n'
          'preamble\r\n--zzz\r\n\r\nA\r\n--zzz--\r\nepilogue'
          '--zzz\r\n\r\n--zzz\r\n--zzz--\r\n'
          '\r\n\r\n\r\n'
          '--zzz\r\n\r\nA'
          '--zzz\r\n\r\n\r\n--zzz\r\n\r\nB\r\n--zzz--\r\n')
for _i in "${!MPC_BODY[@]}"; do
  # shellcheck disable=SC2059
  printf -- "${MPC_BODY[$_i]}" > "$WORK/mpc.$_i"
done
MPC_PORT=$(free_port)
mp_corpus sf "$MPC_PORT" "$SF"
mpc_sf="$(cat "$WORK/mpc.sf.codes" | tr '\n' ' ')|$(sort "$WORK/mpc.sf.out" | tr '\n' ' ')"
if [ -x "$RC" ]; then
  MPC_PORT2=$(free_port)
  mp_corpus rc "$MPC_PORT2" "$RC"
  mpc_rc="$(cat "$WORK/mpc.rc.codes" | tr '\n' ' ')|$(sort "$WORK/mpc.rc.out" | tr '\n' ' ')"
  [ "$mpc_sf" = "$mpc_rc" ] \
    && note "malformed multipart bodies answer exactly as the reference does" \
    || bad  "malformed multipart bodies answer exactly as the reference does" \
            "swordfish '$mpc_sf' reference '$mpc_rc'"
else
  case "$mpc_sf" in
    *'trunc_delim=200'*'no_close=400'*'boundary_in_body=400'*'unterminated=400'*)
      note "malformed multipart bodies answer exactly as the reference does" ;;
    *) bad "malformed multipart bodies answer exactly as the reference does" \
           "got '$mpc_sf'" ;;
  esac
fi


exit $fail
