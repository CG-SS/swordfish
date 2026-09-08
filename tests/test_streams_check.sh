#!/bin/bash
# `swordfish streams` — many pipelines in one process, with a REST API.
#
# The API's SHAPES are compared against redpanda-connect's on the same configs,
# because a dashboard or a deploy script written against its streams API is
# meant to keep working. What cannot be compared is the numbers in it -- uptimes
# and counters move -- so those are checked structurally and the field names are
# checked against the reference's own response.
#
# Everything else here is about the thing streams mode adds over `run`: that N
# streams run independently, that the API can create and remove one while the
# others keep going, and that a signal drains all of them.
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1
SF=${1:-build/swordfish}
RC=${2:-../redpanda-connect}
SEA="--smp 1 --memory 512M --overprovisioned"

[ -x "$SF" ] || { echo "no swordfish front end at $SF" >&2; exit 1; }

WORK=$(mktemp -d /tmp/sfstreams.XXXXXX)
SF_PORT_BAND=22000
# shellcheck source=tests/ports.sh
. tests/ports.sh

pass=0
fail=0
ok()  { pass=$((pass + 1)); echo "ok   $1"; }
bad() { fail=$((fail + 1)); echo "FAIL $1" >&2; shift; printf '%s\n' "$@" | sed 's/^/       /' >&2; }

# Every server this script starts is recorded here and killed on the way out,
# whether the script ends, fails or is interrupted. A streams-mode process
# serves until it is signalled, so one left behind holds its port for ever.
PIDS=()
cleanup() {
    for p in "${PIDS[@]:-}"; do
        [ -n "$p" ] || continue
        kill -TERM "$p" 2>/dev/null
    done
    for _ in $(seq 1 20); do
        local alive=0
        for p in "${PIDS[@]:-}"; do
            [ -n "$p" ] || continue
            kill -0 "$p" 2>/dev/null && alive=1
        done
        [ "$alive" -eq 0 ] && break
        sleep 0.25
    done
    for p in "${PIDS[@]:-}"; do
        [ -n "$p" ] || continue
        kill -KILL "$p" 2>/dev/null
    done
    rm -rf "$WORK"
}
trap cleanup EXIT
trap 'cleanup; exit 143' INT TERM

mkdir -p "$WORK/cfgs"
cat > "$WORK/cfgs/alpha.yaml" <<'YAML'
input: { generate: { count: 4, interval: 300ms, mapping: 'root.s = "alpha"' } }
output: { drop: {} }
YAML
cat > "$WORK/cfgs/beta.yaml" <<'YAML'
input: { generate: { count: 4, interval: 300ms, mapping: 'root.s = "beta"' } }
output: { drop: {} }
YAML

# Waits for a server to answer, rather than sleeping a guessed interval.
wait_up() {   # url
    for _ in $(seq 1 60); do
        curl -sf -o /dev/null "$1" 2>/dev/null && return 0
        sleep 0.25
    done
    return 1
}

start_sf() {   # port  extra-args...
    local port=$1; shift
    cat > "$WORK/root.yaml" <<YAML
http: { address: 127.0.0.1:$port, enabled: true }
YAML
    # shellcheck disable=SC2086
    timeout -s KILL 120 "$SF" streams -o "$WORK/root.yaml" "$@" $SEA \
        > "$WORK/sf.log" 2>&1 &
    PIDS+=($!)
    SF_PID=$!
    wait_up "http://127.0.0.1:$port/streams"
}

echo "== the API's shape, against the reference =="
PORT=$(free_port)
if ! start_sf "$PORT" "$WORK/cfgs/alpha.yaml" "$WORK/cfgs/beta.yaml"; then
    bad "swordfish streams serves its API" "$(tail -5 "$WORK/sf.log")"
else
    ok "swordfish streams serves its API"

    got=$(curl -s "http://127.0.0.1:$PORT/streams")
    # Both ids present, from the FILENAMES, which is how the reference keys the
    # API and therefore how every URL against it is written.
    if printf '%s' "$got" | grep -q '"alpha"' && printf '%s' "$got" | grep -q '"beta"'; then
        ok "stream ids come from the config filenames"
    else
        bad "stream ids come from the config filenames" "$got"
    fi

    # Field names compared against the reference's own answer rather than
    # against a list written here, so the two cannot drift apart quietly.
    if [ -x "$RC" ]; then
        RPORT=$(free_port)
        cat > "$WORK/refroot.yaml" <<YAML
http: { address: 127.0.0.1:$RPORT, enabled: true }
YAML
        timeout -s KILL 60 "$RC" streams -o "$WORK/refroot.yaml" \
            "$WORK/cfgs/alpha.yaml" "$WORK/cfgs/beta.yaml" > "$WORK/ref.log" 2>&1 &
        PIDS+=($!)
        REF_PID=$!
        if wait_up "http://127.0.0.1:$RPORT/streams"; then
            refgot=$(curl -s "http://127.0.0.1:$RPORT/streams")
            keys() { python3 -c '
import json,sys
d = json.load(sys.stdin)
print(" ".join(sorted(d)), "|", " ".join(sorted(next(iter(d.values())))) if d else "")'; }
            a=$(printf '%s' "$got" | keys)
            b=$(printf '%s' "$refgot" | keys)
            if [ -n "$b" ] && [ "$a" = "$b" ]; then
                ok "GET /streams has the reference's ids and fields ($a)"
            else
                bad "GET /streams has the reference's ids and fields" \
                    "swordfish: $a" "reference: $b"
            fi

            # `config` used to be the documented gap here: swordfish compiled a
            # config into factories and kept no document to echo back. It keeps
            # one now, so this case asserts FULL agreement -- and the gap
            # version of it failed the moment the gap closed, which is what a
            # known-gap assertion is for.
            aone=$(curl -s "http://127.0.0.1:$PORT/streams/alpha" | python3 -c '
import json,sys
print(" ".join(sorted(json.load(sys.stdin))))' 2>/dev/null || true)
            b=$(curl -s "http://127.0.0.1:$RPORT/streams/alpha" | python3 -c '
import json,sys
print(" ".join(sorted(json.load(sys.stdin))))' 2>/dev/null || true)
            if [ -n "$b" ] && [ "$aone" = "$b" ]; then
                ok "GET /streams/{id} has the reference's fields ($aone)"
            else
                bad "GET /streams/{id} has the reference's fields" \
                    "swordfish: $aone" "reference: $b"
            fi

            # And the config itself, not just the field name: it is the config
            # as GIVEN, env resolved, which is what the reference reports too.
            acfg=$(curl -s "http://127.0.0.1:$PORT/streams/alpha" | python3 -c '
import json,sys
print(json.dumps(json.load(sys.stdin).get("config"), sort_keys=True))' 2>/dev/null || true)
            bcfg=$(curl -s "http://127.0.0.1:$RPORT/streams/alpha" | python3 -c '
import json,sys
print(json.dumps(json.load(sys.stdin).get("config"), sort_keys=True))' 2>/dev/null || true)
            if [ -n "$bcfg" ] && [ "$bcfg" != "null" ] && [ "$acfg" = "$bcfg" ]; then
                ok "the reported config matches the reference's"
            else
                bad "the reported config matches the reference's" \
                    "swordfish: $acfg" "reference: $bcfg"
            fi
        else
            bad "the reference's streams API came up" "$(tail -3 "$WORK/ref.log")"
        fi
        kill -TERM "$REF_PID" 2>/dev/null
    fi

    code=$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$PORT/streams/nope")
    [ "$code" = 404 ] && ok "an unknown stream is a 404" \
                      || bad "an unknown stream is a 404" "got $code"

    code=$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$PORT/ready")
    [ "$code" = 200 ] && ok "/ready answers for every stream at once" \
                      || bad "/ready answers for every stream at once" "got $code"

    # Per-stream counters, which is what /streams/{id}/stats exists for.
    st=$(curl -s "http://127.0.0.1:$PORT/streams/alpha/stats")
    if printf '%s' "$st" | grep -q '"received"' && printf '%s' "$st" | grep -q '"acks"'; then
        ok "/streams/{id}/stats reports the stream's own counters"
    else
        bad "/streams/{id}/stats reports the stream's own counters" "$st"
    fi

    kill -TERM "$SF_PID" 2>/dev/null
    for _ in $(seq 1 40); do kill -0 "$SF_PID" 2>/dev/null || break; sleep 0.25; done
    if kill -0 "$SF_PID" 2>/dev/null; then
        bad "SIGTERM drains every stream and exits" "still running"
        kill -KILL "$SF_PID" 2>/dev/null
    else
        ok "SIGTERM drains every stream and exits"
    fi
fi

echo "== creating and removing streams through the API =="
PORT=$(free_port)
if ! start_sf "$PORT"; then
    bad "streams mode starts with no configs at all" "$(tail -5 "$WORK/sf.log")"
else
    ok "streams mode starts with no configs at all"
    [ "$(curl -s "http://127.0.0.1:$PORT/streams")" = "{}" ] \
        && ok "with no streams the listing is empty" \
        || bad "with no streams the listing is empty" "$(curl -s "http://127.0.0.1:$PORT/streams")"

    body='input: { generate: { count: 100, interval: 200ms, mapping: "root.x = 1" } }
output: { drop: {} }'
    code=$(curl -s -o /dev/null -w '%{http_code}' -XPOST --data-binary "$body" \
           "http://127.0.0.1:$PORT/streams/made")
    [ "$code" = 200 ] && ok "POST /streams/{id} creates a stream" \
                      || bad "POST /streams/{id} creates a stream" "got $code"

    curl -s "http://127.0.0.1:$PORT/streams" | grep -q '"made"' \
        && ok "the created stream appears in the listing" \
        || bad "the created stream appears in the listing" "$(curl -s "http://127.0.0.1:$PORT/streams")"

    # It must actually be RUNNING, not merely listed. A stream that was
    # registered and never started would satisfy every check above.
    sleep 0.6
    got=$(curl -s "http://127.0.0.1:$PORT/streams/made/stats")
    n=$(printf '%s' "$got" | python3 -c 'import json,sys; print(json.load(sys.stdin)["input"]["received"])' 2>/dev/null || echo 0)
    [ "${n:-0}" -gt 0 ] && ok "the created stream is running, not just registered ($n messages)" \
                        || bad "the created stream is running, not just registered" "$got"

    code=$(curl -s -o /dev/null -w '%{http_code}' -XPOST --data-binary "$body" \
           "http://127.0.0.1:$PORT/streams/made")
    [ "$code" = 400 ] && ok "POST over an existing id is refused" \
                      || bad "POST over an existing id is refused" "got $code"

    code=$(curl -s -o /dev/null -w '%{http_code}' -XPUT --data-binary "$body" \
           "http://127.0.0.1:$PORT/streams/made")
    [ "$code" = 200 ] && ok "PUT replaces an existing stream" \
                      || bad "PUT replaces an existing stream" "got $code"

    # A config error is the CALLER's mistake and comes back as the parser's own
    # words, not a 500 and not a silent acceptance.
    out=$(curl -s -XPOST --data-binary 'input: { nonsense: {} }
output: { drop: {} }' "http://127.0.0.1:$PORT/streams/bad")
    if printf '%s' "$out" | grep -q "is not implemented by swordfish"; then
        ok "a bad config is refused with the parser's own message"
    else
        bad "a bad config is refused with the parser's own message" "$out"
    fi
    curl -s "http://127.0.0.1:$PORT/streams" | grep -q '"bad"' \
        && bad "a refused config leaves no stream behind" "it was registered anyway" \
        || ok "a refused config leaves no stream behind"

    # A stream config carrying a service-wide block is refused: honouring one
    # would hand whichever stream loaded first the whole process's endpoint.
    out=$(curl -s -XPOST --data-binary 'http: { address: 127.0.0.1:1 }
input: { generate: { count: 1 } }
output: { drop: {} }' "http://127.0.0.1:$PORT/streams/withhttp")
    printf '%s' "$out" | grep -q 'service-wide field' \
        && ok "a stream config cannot carry a service-wide block" \
        || bad "a stream config cannot carry a service-wide block" "$out"

    code=$(curl -s -o /dev/null -w '%{http_code}' -XDELETE "http://127.0.0.1:$PORT/streams/made")
    [ "$code" = 200 ] && ok "DELETE removes a stream" || bad "DELETE removes a stream" "got $code"
    [ "$(curl -s "http://127.0.0.1:$PORT/streams")" = "{}" ] \
        && ok "the removed stream is gone from the listing" \
        || bad "the removed stream is gone from the listing" "$(curl -s "http://127.0.0.1:$PORT/streams")"

    # Unimplemented pieces of the reference's API are NAMED, not 404s: "we have
    # not built this" and "you asked for something that does not exist" send a
    # caller to different places.
    out=$(curl -s -w ' [%{http_code}]' -XPATCH --data-binary 'x' "http://127.0.0.1:$PORT/streams/made")
    printf '%s' "$out" | grep -q 'not implemented by swordfish' \
        && ok "PATCH is named as unimplemented, not 404" \
        || bad "PATCH is named as unimplemented, not 404" "$out"
    out=$(curl -s -XPOST --data-binary 'x' "http://127.0.0.1:$PORT/streams")
    printf '%s' "$out" | grep -q 'not implemented by swordfish' \
        && ok "POST /streams (replace-all) is named as unimplemented" \
        || bad "POST /streams (replace-all) is named as unimplemented" "$out"

    kill -TERM "$SF_PID" 2>/dev/null
fi

echo "== --no-api =="
# With no API there is nothing to serve, so the process runs its streams to
# completion and exits, exactly as `run` does.
cat > "$WORK/finite.yaml" <<'YAML'
input: { generate: { count: 2, interval: 0s, mapping: 'root.z = 1' } }
output: { drop: {} }
YAML
# shellcheck disable=SC2086
if timeout -s KILL 60 "$SF" streams --no-api "$WORK/finite.yaml" $SEA > "$WORK/noapi.log" 2>&1; then
    ok "--no-api runs the streams to completion and exits"
else
    bad "--no-api runs the streams to completion and exits" "$(tail -5 "$WORK/noapi.log")"
fi

echo
echo "streams: $pass ok, $fail failed"
[ "$fail" -eq 0 ]
