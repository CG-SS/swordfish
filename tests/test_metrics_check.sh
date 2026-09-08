#!/bin/bash
# Per-component metrics, against redpanda-connect's.
#
# The claim being tested is a compatibility one: every series swordfish exports
# must be a series the reference also exports, with the SAME identity -- the
# same metric name, the same `path`, the same `label`. A dashboard written
# against Redpanda Connect groups by those, so an extra series is noise and a
# differently-keyed one is a broken panel.
#
# It is deliberately a SUBSET check, not equality: the reference exports latency
# summaries and connection failed/lost counters that swordfish does not measure,
# and emitting them as a constant zero would be worse than leaving them out -- a
# flat line reads as "no errors" rather than "not measured".
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1
SF=${1:-build/swordfish}
RC=${2:-../redpanda-connect}
SEA="--smp 1 --memory 512M --overprovisioned"

[ -x "$SF" ] || { echo "no swordfish front end at $SF" >&2; exit 1; }

WORK=$(mktemp -d /tmp/sfmetrics.XXXXXX)
SF_PORT_BAND=22500
# shellcheck source=tests/ports.sh
. tests/ports.sh

pass=0
fail=0
ok()  { pass=$((pass + 1)); echo "ok   $1"; }
bad() { fail=$((fail + 1)); echo "FAIL $1" >&2; shift; printf '%s\n' "$@" | sed 's/^/       /' >&2; }

PIDS=()
cleanup() {
    for p in "${PIDS[@]:-}"; do [ -n "$p" ] && kill -KILL "$p" 2>/dev/null; done
    rm -rf "$WORK"
}
trap cleanup EXIT
trap 'cleanup; exit 143' INT TERM

# A pipeline with TWO processors, one of them labelled: enough to tell a
# per-component series from an aggregate one, which a single-processor config
# cannot.
mkcfg() {   # port
    cat <<YAML
http: { address: 127.0.0.1:$1, enabled: true }
metrics: { prometheus: {} }
input: { generate: { interval: 20ms, mapping: 'root.n = 1' } }
pipeline:
  processors:
    - mapping: 'root = this'
    - label: second
      mapping: 'root = this'
output: { drop: {} }
YAML
}

# The parser lives in a file rather than inline: it needs both quote characters,
# and threading those through `python3 -c` inside a shell string produced a
# syntax error rather than a result.
cat > "$WORK/idents.py" <<'PYSCRIPT'
import re
import sys

# The identity of every series, without its value: "name|path|label".
for line in open(sys.argv[1], encoding="utf-8"):
    if line.startswith("#") or not line.strip():
        continue
    m = re.match(r"([a-zA-Z_:][a-zA-Z0-9_:]*)\{([^}]*)\}", line)
    if not m:
        continue
    name, labels = m.group(1), m.group(2)
    d = dict(re.findall(r'(\w+)="([^"]*)"', labels))
    # quantile is a summary bucket, not an identity -- drop it so a summary
    # collapses to one entry rather than three.
    d.pop("quantile", None)
    print("%s|%s|%s" % (name, d.get("path", ""), d.get("label", "")))
PYSCRIPT

idents() {   # scraped-file
    python3 "$WORK/idents.py" "$1" | sort -u
}

start() {   # binary port cfgfile  -> waits for /metrics
    mkcfg "$2" > "$3"
    if [ "$1" = "$SF" ]; then
        # shellcheck disable=SC2086
        timeout -s KILL 60 "$1" run "$3" $SEA > "$WORK/$(basename "$3").log" 2>&1 &
    else
        timeout -s KILL 60 "$1" run "$3" > "$WORK/$(basename "$3").log" 2>&1 &
    fi
    PIDS+=($!)
    for _ in $(seq 1 60); do
        curl -sf -o /dev/null "http://127.0.0.1:$2/metrics" 2>/dev/null && return 0
        sleep 0.25
    done
    return 1
}

SPORT=$(free_port)
RPORT=$(free_port)
if ! start "$SF" "$SPORT" "$WORK/sf.yaml"; then
    bad "swordfish serves /metrics" "$(tail -4 "$WORK/sf.yaml.log")"
    echo; echo "metrics: $pass ok, $fail failed"; exit 1
fi
ok "swordfish serves /metrics"

sleep 0.5   # let a few messages through so the counters are non-zero
# Captured ONCE. Every check below reads this same snapshot: scraping twice gave
# the second read a later, larger value, which made the pipeline look as though
# a processor had seen more messages than the input read.
curl -s "http://127.0.0.1:$SPORT/metrics" > "$WORK/sf.metrics" 2>/dev/null
mapfile -t SF_IDENTS < <(idents "$WORK/sf.metrics")

echo "== the bug this replaced =="
# Every series used to carry path="root", so a dashboard grouping by path saw
# the input, both processors and the output as one line.
rooted=$(printf '%s\n' "${SF_IDENTS[@]}" | grep -c '|root|' || true)
sfonly=$(printf '%s\n' "${SF_IDENTS[@]}" | grep -c '^swordfish_' || true)
if [ "$rooted" = "$sfonly" ] && [ "$sfonly" -gt 0 ]; then
    ok "only the swordfish_* extras are reported at path=root ($sfonly of them)"
else
    bad "only the swordfish_* extras are reported at path=root" \
        "$rooted series at root, $sfonly of them swordfish_*" \
        "$(printf '%s\n' "${SF_IDENTS[@]}" | grep '|root|' | head -5)"
fi

for want in 'input_received|root.input|' \
            'output_sent|root.output|' \
            'processor_batch_received|root.pipeline.processors.0|' \
            'processor_batch_received|root.pipeline.processors.1|second'; do
    if printf '%s\n' "${SF_IDENTS[@]}" | grep -qxF "$want"; then
        ok "series present: $want"
    else
        bad "series present: $want" "$(printf '%s\n' "${SF_IDENTS[@]}" | head -10)"
    fi
done

echo "== the counters agree with each other =="
# A pass-through pipeline: everything the input read reaches both processors and
# the output. This is the check that the per-component counters are counting the
# component and not something else.
val() {   # name path -- from the single snapshot above
    grep -F "$1{label=" "$WORK/sf.metrics" | grep -F "path=\"$2\"" \
        | awk '{print $NF}' | head -1
}
IN=$(val input_received root.input)
P0=$(val processor_received root.pipeline.processors.0)
P1=$(val processor_received root.pipeline.processors.1)
if [ -n "$IN" ] && [ "${IN:-0}" -gt 0 ]; then
    ok "the input's counter is moving ($IN received)"
else
    bad "the input's counter is moving" "input_received=$IN"
fi
# The pipeline runs behind the input, so a processor's count can lag by the
# queue depth -- it must never EXCEED the input's, and must not be zero.
if [ -n "$P0" ] && [ "${P0:-0}" -gt 0 ] && [ "${P0:-0}" -le "${IN:-0}" ]; then
    ok "the first processor sees what the input read ($P0 of $IN)"
else
    bad "the first processor sees what the input read" "processor.0=$P0 input=$IN"
fi
if [ -n "$P1" ] && [ "${P1:-0}" -gt 0 ] && [ "${P1:-0}" -le "${P0:-0}" ]; then
    ok "the second processor sees what the first produced ($P1 of $P0)"
else
    bad "the second processor sees what the first produced" "processor.1=$P1 processor.0=$P0"
fi

echo "== every series is one the reference also exports =="
if [ ! -x "$RC" ]; then
    bad "the reference is available" "no redpanda-connect at $RC"
else
    if ! start "$RC" "$RPORT" "$WORK/ref.yaml"; then
        bad "the reference serves /metrics" "$(tail -4 "$WORK/ref.yaml.log")"
    else
        ok "the reference serves /metrics"
        curl -s "http://127.0.0.1:$RPORT/metrics" > "$WORK/rc.metrics" 2>/dev/null
        mapfile -t RC_IDENTS < <(idents "$WORK/rc.metrics")
        if [ "${#RC_IDENTS[@]}" -lt 10 ]; then
            bad "the reference exported a comparable set" \
                "only ${#RC_IDENTS[@]} series; the comparison would prove little"
        else
            missing=""
            for id in "${SF_IDENTS[@]}"; do
                # swordfish_* are ours by design and namespaced to say so.
                case "$id" in swordfish_*) continue ;; esac
                printf '%s\n' "${RC_IDENTS[@]}" | grep -qxF "$id" || missing="$missing$id"$'\n'
            done
            if [ -z "$missing" ]; then
                ok "every non-swordfish_* series matches one of the reference's (${#SF_IDENTS[@]} checked)"
            else
                bad "every non-swordfish_* series matches one of the reference's" \
                    "these have no counterpart:" "$missing"
            fi
            # And the extras are namespaced, so a Benthos dashboard ignores them
            # rather than mistaking them for its own.
            extras=$(printf '%s\n' "${SF_IDENTS[@]}" | grep -c '^swordfish_' || true)
            if [ "$extras" -gt 0 ]; then
                ok "swordfish's own metrics are prefixed swordfish_ ($extras)"
            else
                bad "swordfish's own metrics are prefixed swordfish_" "none found"
            fi
        fi
    fi
fi

echo "== the text is well-formed =="
# HELP and TYPE go once per family. With a series per component the same family
# now appears many times, and a repeated TYPE is a scrape error in Prometheus.
dupe=$(grep '^# TYPE ' "$WORK/sf.metrics" | awk '{print $3}' | sort | uniq -d)
if [ -z "$dupe" ]; then
    ok "each metric family declares its TYPE exactly once"
else
    bad "each metric family declares its TYPE exactly once" "repeated: $dupe"
fi

echo
echo "metrics: $pass ok, $fail failed"
[ "$fail" -eq 0 ]
