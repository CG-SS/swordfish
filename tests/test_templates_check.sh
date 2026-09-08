#!/bin/bash
# Config templates, compared against redpanda-connect on the same files.
#
# A template is a component defined by a Bloblang mapping, so the thing to check
# is not that it parses but that the config it PRODUCES behaves identically in
# both implementations -- and that the same file compiles to a binary that
# behaves the same way again, because a template that meant one thing to `run`
# and another to `build` would break the project's central promise.
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1
SF=${1:-build/swordfish}
RC=${2:-../redpanda-connect}
SEA="--smp 1 --memory 512M --overprovisioned"

[ -x "$SF" ] || { echo "no swordfish front end at $SF" >&2; exit 1; }

WORK=$(mktemp -d /tmp/sftmpl.XXXXXX)
trap 'rm -rf "$WORK"' EXIT
trap 'rm -rf "$WORK"; exit 143' INT TERM
mkdir -p "$WORK/t"

pass=0
fail=0
ok()  { pass=$((pass + 1)); echo "ok   $1"; }
bad() { fail=$((fail + 1)); echo "FAIL $1" >&2; shift; printf '%s\n' "$@" | sed 's/^/       /' >&2; }

# Runs a config through both implementations with the given templates and
# compares stdout. Two empty outputs are a FAILURE, not agreement: a template
# that fails to load makes both refuse the config, which would otherwise look
# like a pass.
compare() {   # label  templates  config
    local label=$1 tpl=$2 cfg=$3 a b
    a=$(timeout -s KILL 60 "$SF" run -t "$tpl" "$cfg" $SEA 2>/dev/null | grep '^{' | sort)
    b=$(timeout -s KILL 60 "$RC" run -t "$tpl" "$cfg" 2>/dev/null | grep '^{' | sort)
    if [ -z "$b" ]; then
        bad "$label" "the reference produced nothing; the case proves nothing"
    elif [ "$a" = "$b" ]; then
        ok "$label"
    else
        bad "$label" "swordfish: $a" "reference: $b"
    fi
}

echo "== a template produces what the reference's does =="

cat > "$WORK/t/doubler.yaml" <<'YAML'
name: doubler
type: processor
fields:
  - name: field
    type: string
  - name: times
    type: int
    default: 2
mapping: |
  root.mapping = "root = this\nroot.%s = this.%s * %v".format(this.field, this.field, this.times)
YAML
cat > "$WORK/proc.yaml" <<'YAML'
input: { generate: { count: 2, interval: 0s, mapping: 'root.n = 3' } }
pipeline:
  processors:
    - doubler: { field: n, times: 5 }
output: { stdout: { codec: lines } }
YAML
compare "a processor template" "$WORK/t/doubler.yaml" "$WORK/proc.yaml"

# An input template that expands to a BROKER, which is the docs' own example
# shape: one template usage becoming several components.
cat > "$WORK/t/multi_gen.yaml" <<'YAML'
name: multi_gen
type: input
fields:
  - name: labels
    type: string
    kind: list
  - name: count
    type: int
    default: 1
mapping: |
  root.broker.inputs = this.labels.map_each(l -> {
    "generate": { "count": this.count, "interval": "0s", "mapping": "root.who = \"%s\"".format(l) }
  })
YAML
cat > "$WORK/input.yaml" <<'YAML'
input:
  multi_gen:
    labels: [ alpha, beta ]
    count: 2
output: { stdout: { codec: lines } }
YAML
compare "an input template expanding to a broker" "$WORK/t/multi_gen.yaml" "$WORK/input.yaml"

# A default that the config does not override, which is the field-handling half.
cat > "$WORK/input_default.yaml" <<'YAML'
input:
  multi_gen:
    labels: [ solo ]
output: { stdout: { codec: lines } }
YAML
compare "a field default is applied" "$WORK/t/multi_gen.yaml" "$WORK/input_default.yaml"

echo "== compiled and interpreted agree =="
# The same file through `build`. A template that expanded differently in the two
# backends would be the one failure this project cannot tolerate.
if SWORDFISH_CONFIG="$PWD/build/swordfish-config" \
   timeout -s KILL 900 "$SF" build -t "$WORK/t/doubler.yaml" "$WORK/proc.yaml" \
       -o "$WORK/proc.bin" > "$WORK/build.log" 2>&1; then
    a=$(timeout -s KILL 60 "$SF" run -t "$WORK/t/doubler.yaml" "$WORK/proc.yaml" $SEA 2>/dev/null | grep '^{')
    b=$(timeout -s KILL 60 "$WORK/proc.bin" $SEA 2>/dev/null | grep '^{')
    if [ -n "$a" ] && [ "$a" = "$b" ]; then
        ok "a templated config compiles and behaves identically"
    else
        bad "a templated config compiles and behaves identically" \
            "interpreted: $a" "compiled: $b"
    fi
else
    bad "a templated config compiles and behaves identically" "$(tail -3 "$WORK/build.log")"
fi

echo "== the mistakes a template can make =="

# Without -t the template is not a component, and saying so is the whole point
# of the named-error rule: "not implemented" and "not a thing" are different.
out=$(timeout -s KILL 30 "$SF" lint "$WORK/proc.yaml" 2>&1)
printf '%s' "$out" | grep -q "processor 'doubler' is not implemented" \
    && ok "without -t the template's name is refused by name" \
    || bad "without -t the template's name is refused by name" "$out"

# A field with no default and no value.
cat > "$WORK/missing.yaml" <<'YAML'
input: { generate: { count: 1, interval: 0s, mapping: 'root.n = 1' } }
pipeline:
  processors:
    - doubler: { times: 2 }
output: { stdout: {} }
YAML
out=$(timeout -s KILL 30 "$SF" lint -t "$WORK/t/doubler.yaml" "$WORK/missing.yaml" 2>&1)
printf '%s' "$out" | grep -q 'needs a `field`' \
    && ok "a required field with no default is named" \
    || bad "a required field with no default is named" "$out"

# A key the template does not declare. Accepting it silently would leave the
# user's setting doing nothing at all, which is the failure mode the config
# parser already refuses everywhere else.
cat > "$WORK/typo.yaml" <<'YAML'
input: { generate: { count: 1, interval: 0s, mapping: 'root.n = 1' } }
pipeline:
  processors:
    - doubler: { field: n, timez: 2 }
output: { stdout: {} }
YAML
out=$(timeout -s KILL 30 "$SF" lint -t "$WORK/t/doubler.yaml" "$WORK/typo.yaml" 2>&1)
printf '%s' "$out" | grep -q "has no field 'timez'" \
    && ok "a field the template does not declare is refused" \
    || bad "a field the template does not declare is refused" "$out"

# A list field given a scalar. Without this check the Bloblang mapping fails
# with a type error naming neither the field nor the template.
cat > "$WORK/wrongkind.yaml" <<'YAML'
input:
  multi_gen:
    labels: solo
output: { stdout: {} }
YAML
out=$(timeout -s KILL 30 "$SF" lint -t "$WORK/t/multi_gen.yaml" "$WORK/wrongkind.yaml" 2>&1)
printf '%s' "$out" | grep -q 'is a list' \
    && ok "a list field given a scalar is named" \
    || bad "a list field given a scalar is named" "$out"

# metrics_mapping renames per-component metric paths, and swordfish reports one
# set of counters for the whole stream -- there are no paths to rename.
cat > "$WORK/t/metrics.yaml" <<'YAML'
name: with_metrics
type: processor
mapping: 'root.mapping = "root = this"'
metrics_mapping: 'root = this'
YAML
out=$(timeout -s KILL 30 "$SF" template lint "$WORK/t/metrics.yaml" 2>&1)
printf '%s' "$out" | grep -q 'metrics_mapping` is not implemented' \
    && ok "metrics_mapping is named as unimplemented" \
    || bad "metrics_mapping is named as unimplemented" "$out"
rm -f "$WORK/t/metrics.yaml"

echo "== template lint, and its exit code =="

cat > "$WORK/tested.yaml" <<'YAML'
name: prefixer
type: processor
fields:
  - name: prefix
    type: string
    default: "pre-"
mapping: |
  root.mapping = "root = this\nroot.name = \"%s\" + this.name".format(this.prefix)
tests:
  - name: uses the default prefix
    config: {}
    expected:
      mapping: "root = this\nroot.name = \"pre-\" + this.name"
  - name: honours an explicit prefix
    config: { prefix: "x-" }
    expected:
      mapping: "root = this\nroot.name = \"x-\" + this.name"
YAML
cat > "$WORK/wrongexpect.yaml" <<'YAML'
name: broken
type: processor
mapping: 'root.mapping = "root = this"'
tests:
  - name: the expectation does not match
    config: {}
    expected: { mapping: "root = something else" }
YAML
cat > "$WORK/badoutput.yaml" <<'YAML'
name: produces_nonsense
type: input
mapping: 'root.no_such_input = {}'
tests:
  - name: the produced config must lint
    config: {}
YAML

# Exit codes are compared against the reference's, because that is what a CI
# pipeline keys on. The MESSAGES differ and are meant to.
for f in tested wrongexpect badoutput; do
    timeout -s KILL 30 "$SF" template lint "$WORK/$f.yaml" > /dev/null 2>&1; a=$?
    if [ -x "$RC" ]; then
        timeout -s KILL 30 "$RC" template lint "$WORK/$f.yaml" > /dev/null 2>&1; b=$?
        if [ "$a" = "$b" ]; then
            ok "template lint $f exits $a, as the reference does"
        else
            bad "template lint $f exits as the reference does" \
                "swordfish $a, reference $b"
        fi
    fi
done

# And it must say WHICH test failed, not merely that one did.
out=$(timeout -s KILL 30 "$SF" template lint "$WORK/wrongexpect.yaml" 2>&1)
printf '%s' "$out" | grep -q 'the expectation does not match' \
    && ok "template lint names the failing test" \
    || bad "template lint names the failing test" "$out"

# A template whose mapping produces a usage of itself never finishes. The depth
# limit turns that into an error with a name instead of a hang.
cat > "$WORK/t/cycle.yaml" <<'YAML'
name: cycle
type: processor
mapping: 'root.cycle = {}'
YAML
cat > "$WORK/cyclic.yaml" <<'YAML'
input: { generate: { count: 1, interval: 0s, mapping: 'root.n = 1' } }
pipeline:
  processors:
    - cycle: {}
output: { stdout: {} }
YAML
out=$(timeout -s KILL 60 "$SF" lint -t "$WORK/t/cycle.yaml" "$WORK/cyclic.yaml" 2>&1)
printf '%s' "$out" | grep -q 'still expanding after' \
    && ok "a self-referential template is stopped, not left to hang" \
    || bad "a self-referential template is stopped, not left to hang" "$out"

echo
echo "templates: $pass ok, $fail failed"
[ "$fail" -eq 0 ]
