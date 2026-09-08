#!/usr/bin/env bash
# `socket` in and out, and `socket_server` in.
#
# The valuable checks here are the CROSS-implementation ones: our socket output
# into the reference's socket_server, and its socket output into ours. A test
# where both ends are ours would only prove our framing agrees with itself.
set -uo pipefail
cd "$(dirname "$0")/.."
SF=${1:-build/swordfish-run}
RC=${2:-../redpanda-connect}
export SWORDFISH_CONFIG="$PWD/build/swordfish-config"
WORK=$(mktemp -d /tmp/sfsock.XXXXXX)
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

SF_PORT_BAND=20000
# shellcheck source=tests/ports.sh
. "$(dirname "${BASH_SOURCE[0]}")/ports.sh"

# Waits for something to be listening. Without this the sender races the
# receiver's bind and the run reports an empty file rather than a refused
# connection, which reads like a framing bug and is not one.
wait_listen() {  # <port>
  local i
  for i in $(seq 1 80); do
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

start() {  # <impl: sf|rc> <config>
  if [ "$1" = sf ]; then
    # shellcheck disable=SC2086
    "$SF" --config "$2" $SEA_ARGS >>"$WORK/server.log" 2>&1 &
  else
    "$RC" run --log.level off "$2" >>"$WORK/server.log" 2>&1 &
  fi
  PIDS+=("$!")
  echo "$!"
}
stop() { kill -TERM "$1" 2>/dev/null; wait "$1" 2>/dev/null; }

server_cfg() {  # <port> <file>
  cat <<YAML
input:
  socket_server:
    network: tcp
    address: "127.0.0.1:$1"
output:
  file: { path: $2 }
YAML
}
client_cfg() {  # <port> <count>
  cat <<YAML
input:
  generate: { count: $2, interval: 0s, mapping: 'root.n = counter()' }
output:
  socket:
    network: tcp
    address: "127.0.0.1:$1"
YAML
}

# <name> <server impl> <client impl>
round_trip() {
  local name=$1 srv=$2 cli=$3 port out pid got
  port=$(free_port)
  out="$WORK/rt.txt"
  rm -f "$out"
  server_cfg "$port" "$out" > "$WORK/srv.yaml"
  client_cfg "$port" 3      > "$WORK/cli.yaml"
  pid=$(start "$srv" "$WORK/srv.yaml")
  wait_listen "$port" || { bad "$name" "nothing listened on $port"; stop "$pid"; return; }
  if [ "$cli" = sf ]; then
    # shellcheck disable=SC2086
    timeout 30 "$SF" --config "$WORK/cli.yaml" $SEA_ARGS >/dev/null 2>&1
  else
    timeout 30 "$RC" run --log.level off "$WORK/cli.yaml" >/dev/null 2>&1
  fi
  local rc=$?
  sleep 0.6
  stop "$pid"
  [ "$rc" -ne 0 ] && { bad "$name" "the sender exited $rc"; return; }
  got=$(LC_ALL=C sort < "$out" 2>/dev/null | tr '\n' '|')
  [ "$got" = '{"n":1}|{"n":2}|{"n":3}|' ] && note "$name" || bad "$name" "got '$got'"
}

round_trip "socket output into socket_server, both ours" sf sf
if [ -x "$RC" ]; then
  if "$RC" lint "$WORK/srv.yaml" "$WORK/cli.yaml" >/dev/null 2>&1; then
    note "the reference accepts our socket configs"
  else
    bad "the reference accepts our socket configs" "they failed to lint"
  fi
  round_trip "our socket output into the reference's socket_server" rc sf
  round_trip "the reference's socket output into our socket_server" sf rc
fi

# ---- the socket INPUT connects out and reads ---------------------------------
#
# Nothing in either implementation serves lines to a connecting client, so the
# peer here is a few lines of Python. That makes this a self-consistency check
# rather than a cross-implementation one, and it is labelled as such.
PORT=$(free_port)
python3 - "$PORT" >"$WORK/peer.log" 2>&1 <<'PY' &
import socket, sys
srv = socket.socket()
srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
srv.bind(("127.0.0.1", int(sys.argv[1])))
srv.listen(4)
# Serves EVERY connection, not just the first: wait_listen's readiness probe is
# itself a connection, and a peer that served only one would have exited before
# the pipeline under test ever connected.
while True:
    conn, _ = srv.accept()
    conn.sendall(b'alpha\nbeta\ngamma\n')
    conn.close()
PY
PIDS+=("$!")
wait_listen "$PORT" || bad "socket input reads a stream" "the peer never listened"
cat > "$WORK/in.yaml" <<YAML
input:
  socket:
    network: tcp
    address: "127.0.0.1:$PORT"
output:
  file: { path: $WORK/in.txt }
YAML
rm -f "$WORK/in.txt"
# shellcheck disable=SC2086
timeout 30 "$SF" --config "$WORK/in.yaml" $SEA_ARGS >/dev/null 2>&1
got=$(tr '\n' '|' < "$WORK/in.txt" 2>/dev/null)
[ "$got" = 'alpha|beta|gamma|' ] && note "socket input reads a stream, framed by lines" \
                                 || bad "socket input reads a stream, framed by lines" "got '$got'"

# ---- unix sockets ------------------------------------------------------------
SOCKPATH="$WORK/s.sock"
cat > "$WORK/usrv.yaml" <<YAML
input:
  socket_server:
    network: unix
    address: $SOCKPATH
output:
  file: { path: $WORK/unix.txt }
YAML
cat > "$WORK/ucli.yaml" <<YAML
input:
  generate: { count: 2, interval: 0s, mapping: 'root.n = counter()' }
output:
  socket:
    network: unix
    address: $SOCKPATH
YAML
rm -f "$WORK/unix.txt"
pid=$(start sf "$WORK/usrv.yaml")
for i in $(seq 1 80); do [ -S "$SOCKPATH" ] && break; sleep 0.25; done
if [ -S "$SOCKPATH" ]; then
  # shellcheck disable=SC2086
  timeout 30 "$SF" --config "$WORK/ucli.yaml" $SEA_ARGS >/dev/null 2>&1
  sleep 0.6
  stop "$pid"
  got=$(LC_ALL=C sort < "$WORK/unix.txt" 2>/dev/null | tr '\n' '|')
  [ "$got" = '{"n":1}|{"n":2}|' ] && note "unix sockets carry the same framing" \
                                  || bad "unix sockets carry the same framing" "got '$got'"
else
  stop "$pid"
  bad "unix sockets carry the same framing" "the socket file never appeared"
fi

# ---- concurrent connections into socket_server --------------------------------
#
# The regression test for the rendezvous on the socket side. Each connection is
# served by its own fiber, so each is a PRODUCER; with a seastar::queue
# underneath, the second one to block on a full queue overwrote the first's
# promise and that connection was dropped mid-stream. One connection at a time
# cannot see it.
PORT=$(free_port)
cat > "$WORK/concsrv.yaml" <<YAML
input:
  socket_server:
    network: tcp
    address: "127.0.0.1:$PORT"
pipeline:
  processors:
    - sleep: { duration: 10ms }
output:
  file: { path: $WORK/conc.txt }
YAML
rm -f "$WORK/conc.txt"
pid=$(start sf "$WORK/concsrv.yaml")
if wait_listen "$PORT"; then
  # Eight connections, five lines each, all in flight together.
  python3 - "$PORT" <<'PY'
import socket, sys, threading
port = int(sys.argv[1])
def send(c):
    s = socket.create_connection(("127.0.0.1", port), timeout=10)
    for i in range(5):
        s.sendall(("conn%d-line%d\n" % (c, i)).encode())
    s.shutdown(socket.SHUT_WR)
    s.close()
ts = [threading.Thread(target=send, args=(c,)) for c in range(8)]
for t in ts: t.start()
for t in ts: t.join()
PY
  sleep 1.5
  stop "$pid"
  n=$(grep -c '^conn' "$WORK/conc.txt" 2>/dev/null || true)
  d=$(grep '^conn' "$WORK/conc.txt" 2>/dev/null | sort -u | wc -l)
  if [ "${n:-0}" = 40 ] && [ "$d" = 40 ]; then
    note "eight concurrent connections deliver every line"
  else
    bad "eight concurrent connections deliver every line" \
        "${n:-0} arrived, $d distinct (expected 40 and 40)"
  fi
else
  stop "$pid"
  bad "eight concurrent connections deliver every line" "nothing listened on $PORT"
fi

# ---- configuration that must be refused by name -------------------------------
refuses() {  # <name> <config> <expected substring>
  local err
  # shellcheck disable=SC2086
  err=$(timeout 30 "$SF" --config "$2" $SEA_ARGS 2>&1 >/dev/null)
  grep -qF "$3" <<<"$err" && note "$1" \
    || bad "$1" "expected an error containing '$3', got: $(tail -2 <<<"$err")"
}

cat > "$WORK/udp.yaml" <<'YAML'
input:
  socket:
    network: udp
    address: "127.0.0.1:9999"
output: { drop: {} }
YAML
cat > "$WORK/tls.yaml" <<'YAML'
input:
  socket:
    network: tcp
    address: "127.0.0.1:9999"
    tls:
      enabled: true
output: { drop: {} }
YAML
# `csv` used to be the unimplemented example here; it is implemented now, so the
# case moved to one that still is. `avro` needs a schema reader swordfish does
# not have, and is refused as UNIMPLEMENTED rather than as a typo.
# A scanner name that is not a scanner at all. This used to be `avro`, the one
# scanner that was documented but unbuilt; now that all twelve exist, the case
# it was testing -- that a name is refused rather than quietly ignored, leaving
# `lines` in place -- needs a name that is genuinely not a scanner.
cat > "$WORK/scanner.yaml" <<'YAML'
input:
  socket:
    network: tcp
    address: "127.0.0.1:9999"
    scanner:
      nonsense: {}
output: { drop: {} }
YAML

# The same connector now takes the scanners that ARE built, in the reference's
# component shape -- which is what the socket family gained when `scanner`
# stopped being a bare string choice on the config struct.
cat > "$WORK/scanner_ok.yaml" <<'YAML'
input:
  socket:
    network: tcp
    address: "127.0.0.1:9999"
    scanner:
      csv: { parse_header_row: false }
output: { drop: {} }
YAML

refuses "udp is refused rather than framed by guesswork" "$WORK/udp.yaml"     "is not one of"
refuses "an unimplemented tls block is named"            "$WORK/tls.yaml"     "tls"
refuses "an unknown scanner is named, not ignored"       "$WORK/scanner.yaml" \
        "scanner 'nonsense' is not a scanner"
if build/sfconfig lint "$WORK/scanner_ok.yaml" >/dev/null 2>&1; then
  echo "ok   socket takes a configured scanner in the reference's component shape"
else
  echo "FAIL socket takes a configured scanner in the reference's component shape" >&2; fail=1
fi

exit $fail
