#!/bin/bash
# `swordfish echo`, and the YAML writer underneath it.
#
# The writer is what the streams API uses to report a stream's config and what
# `echo` prints, so the property that matters is FAITHFULNESS, not looks:
# parsing what it wrote must give back what it was given, including whether each
# scalar was quoted. A config where `"true"` came back as `true` would change
# what the field means.
#
# That property is checked over every real config in the tree rather than over
# examples chosen by whoever wrote the writer -- which is how the two bugs it
# shipped with were found (`-1` and a docker-compose command line, both plain
# scalars starting with `-`, both wrongly quoted).
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1
SF=${1:-build/swordfish}
CFG=${2:-build/sfconfig}
RC=${3:-../redpanda-connect}

[ -x "$SF" ] || { echo "no swordfish front end at $SF" >&2; exit 1; }

WORK=$(mktemp -d /tmp/sfecho.XXXXXX)
trap 'rm -rf "$WORK"' EXIT
trap 'rm -rf "$WORK"; exit 143' INT TERM

pass=0
fail=0
ok()  { pass=$((pass + 1)); echo "ok   $1"; }
bad() { fail=$((fail + 1)); echo "FAIL $1" >&2; shift; printf '%s\n' "$@" | sed 's/^/       /' >&2; }

echo "== the writer round-trips every config in the tree =="
# Every .yaml and .yml under the reference's config trees and our own fixtures.
# A file needing unset environment variables is skipped by the tool and says so;
# a file that DIFFERS is a writer bug and names the path that changed.
mapfile -t CORPUS < <(find ../connect-main/config ../benthos-main/config tests/fixtures \
                           -name '*.yaml' -o -name '*.yml' 2>/dev/null | sort)
if [ "${#CORPUS[@]}" -lt 50 ]; then
    bad "the corpus is present" "found only ${#CORPUS[@]} files; the check would prove little"
else
    out=$("$CFG" roundtrip "${CORPUS[@]}" 2>&1)
    line=$(printf '%s' "$out" | grep '^roundtrip:' | tail -1)
    if printf '%s' "$out" | grep -q '^DIFFERS'; then
        bad "every config round-trips through the YAML writer" \
            "$(printf '%s' "$out" | grep -A1 '^DIFFERS' | head -8)"
    elif [ -z "$line" ]; then
        bad "every config round-trips through the YAML writer" "no result line: $out"
    else
        ok "every config round-trips through the YAML writer ($line)"
    fi
fi

echo "== the scalars a writer gets wrong =="
# Each of these is a shape that reads back as something ELSE if it is written
# carelessly. The round trip over the corpus would catch most of them; they are
# here by name so a failure says which rule broke.
check_rt() {   # label  yaml
    printf '%s\n' "$2" > "$WORK/rt.yaml"
    if "$CFG" roundtrip "$WORK/rt.yaml" 2>&1 | grep -q '^DIFFERS'; then
        bad "$label" "$("$CFG" roundtrip "$WORK/rt.yaml" 2>&1 | head -2)"
    else
        ok "$1"
    fi
}
check_rt "a quoted number stays a string"        'a: "5"'
check_rt "a quoted bool stays a string"          'a: "true"'
check_rt "a quoted null stays a string"          'a: "null"'
check_rt "a negative number stays plain"         'a: -1'
check_rt "a flag-like scalar stays plain"        'a: --kafka-addr internal://0.0.0.0:9092'
check_rt "a colon inside a word stays plain"     'a: pgdata:/var/lib/postgresql/data'
check_rt "a multi-line mapping survives"         $'a: |\n  root = this\n  root.b = 1'
check_rt "an empty mapping and list survive"     $'a: {}\nb: []'
check_rt "nested sequences survive"              $'a:\n  - - x\n    - y\n  - - z'
check_rt "a string with quotes survives"         "a: \"he said \\\"hi\\\"\""
check_rt "a string with a leading space survives" 'a: "  padded"'
check_rt "an empty string survives"              'a: ""'

echo "== echo itself =="

cat > "$WORK/env.yaml" <<'YAML'
input:
  generate:
    count: 1
    mapping: 'root.n = ${SF_ECHO_NUM:7}'
output: { stdout: {} }
YAML

# The one thing echo is FOR, per the reference's own description: showing a
# config after environment variables have been resolved.
got=$("$SF" echo "$WORK/env.yaml" 2>&1)
if printf '%s' "$got" | grep -q 'root.n = 7'; then
    ok "echo resolves \${VAR} with its default"
else
    bad "echo resolves \${VAR} with its default" "$got"
fi
got=$(SF_ECHO_NUM=42 "$SF" echo "$WORK/env.yaml" 2>&1)
if printf '%s' "$got" | grep -q 'root.n = 42'; then
    ok "echo resolves \${VAR} from the environment"
else
    bad "echo resolves \${VAR} from the environment" "$got"
fi

# echo is idempotent, which is the cheapest evidence that what it prints is a
# fixed point of the writer rather than something that drifts each pass.
"$SF" echo "$WORK/env.yaml" > "$WORK/once.yaml" 2>/dev/null
"$SF" echo "$WORK/once.yaml" > "$WORK/twice.yaml" 2>/dev/null
if [ -s "$WORK/once.yaml" ] && cmp -s "$WORK/once.yaml" "$WORK/twice.yaml"; then
    ok "echo is idempotent"
else
    bad "echo is idempotent" "$(diff "$WORK/once.yaml" "$WORK/twice.yaml" | head -6)"
fi

# And what it prints still runs. A pretty-printer that produced a config the
# parser then refused would be worse than none.
if "$SF" lint "$WORK/once.yaml" > /dev/null 2>&1; then
    ok "what echo prints still lints"
else
    bad "what echo prints still lints" "$("$SF" lint "$WORK/once.yaml" 2>&1 | head -3)"
fi

# A template usage is left AS WRITTEN, which is what the reference does --
# verified against redpanda-connect 4.107.2 rather than assumed.
mkdir -p "$WORK/t"
cat > "$WORK/t/tpl.yaml" <<'YAML'
name: echo_tpl
type: processor
fields:
  - name: field
    type: string
mapping: 'root.mapping = "root = this\nroot.%s = 1".format(this.field)'
YAML
cat > "$WORK/usetpl.yaml" <<'YAML'
input: { generate: { count: 1, interval: 0s, mapping: 'root.n = 1' } }
pipeline:
  processors:
    - echo_tpl: { field: n }
output: { stdout: {} }
YAML
got=$("$SF" echo -t "$WORK/t/tpl.yaml" "$WORK/usetpl.yaml" 2>&1)
if printf '%s' "$got" | grep -q 'echo_tpl'; then
    ok "echo leaves a template usage as written, as the reference does"
else
    bad "echo leaves a template usage as written, as the reference does" "$got"
fi
if [ -x "$RC" ]; then
    ref=$("$RC" echo -t "$WORK/t/tpl.yaml" "$WORK/usetpl.yaml" 2>&1)
    if printf '%s' "$ref" | grep -q 'echo_tpl'; then
        ok "the reference leaves it as written too (the claim above is measured)"
    else
        bad "the reference leaves it as written too (the claim above is measured)" \
            "the reference now expands templates in echo; swordfish should follow"
    fi
fi

# A config echo cannot run is refused rather than pretty-printed.
cat > "$WORK/broken.yaml" <<'YAML'
input: { generate: { count: 1, mappingg: 'root = 1' } }
output: { stdout: {} }
YAML
out=$("$SF" echo "$WORK/broken.yaml" 2>&1); rc=$?
if [ "$rc" -ne 0 ] && printf '%s' "$out" | grep -q "no field 'mappingg'"; then
    ok "echo refuses a config that would not run"
else
    bad "echo refuses a config that would not run" "rc=$rc" "$out"
fi

echo
echo "echo: $pass ok, $fail failed"
[ "$fail" -eq 0 ]
