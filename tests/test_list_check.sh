#!/bin/bash
# `swordfish list`, and the one property that keeps it honest: what it SHOWS
# must be what the parser ACCEPTS.
#
# The built-in components -- stdin, file, generate, stdout, drop, broker,
# sequence, switch -- are not in the component registry; they are constructed
# directly by build_stream and parsed by hand. Nothing described them, so `list`
# omitted the seven most basic components in the project while the reference
# listed all of them. They now come from one table that the parser also takes
# its `only_fields()` names from, and this gate checks the pairing empirically
# rather than trusting it: for every field `list` advertises, a config using it
# must lint; for every field it does not, a config using it must be refused.
#
# That check is the whole point. config_main.cc records what happened the last
# time documentation lived in a separate hardcoded struct -- "`list` and `lint`
# disagreed about what a `kafka` input accepts" -- and a second hand-written
# list would have reintroduced it.
# NOT `pipefail`. Nearly every check here is `<command that exits non-zero> |
# grep -q <phrase>` -- lint EXITS 1 when it refuses a config, which is the case
# being tested -- and under pipefail the pipeline reports lint's failure rather
# than grep's match, so a check that found its phrase was recorded as a failure.
# Three of the five original failures in this gate were that and nothing else.
set -u
cd "$(dirname "$0")/.." || exit 1
SF=${1:-build/swordfish}
RC=${2:-../redpanda-connect}

[ -x "$SF" ] || { echo "no swordfish front end at $SF" >&2; exit 1; }

WORK=$(mktemp -d /tmp/sflist.XXXXXX)
trap 'rm -rf "$WORK"' EXIT
trap 'rm -rf "$WORK"; exit 143' INT TERM

pass=0
fail=0
ok()  { pass=$((pass + 1)); echo "ok   $1"; }
bad() { fail=$((fail + 1)); echo "FAIL $1" >&2; shift; printf '%s\n' "$@" | sed 's/^/       /' >&2; }

"$SF" list > "$WORK/list.txt" 2>/dev/null

echo "== the built-ins are listed at all =="
# The bug this gate was written for. A user asking "what does this build
# implement?" was told about http_client and kafka and not about stdout.
for c in generate file stdin stdout drop broker sequence switch; do
    if grep -qxF "$c" "$WORK/list.txt"; then
        ok "list documents the built-in '$c'"
    else
        bad "list documents the built-in '$c'" "not in the output"
    fi
done

# And the reference lists them too, so this is a gap being closed rather than an
# invention. Measured, not asserted.
if [ -x "$RC" ]; then
    reflist=$("$RC" list 2>/dev/null)
    missing=""
    for c in generate file stdin stdout drop broker; do
        printf '%s' "$reflist" | grep -qE "^ *- $c\$" || missing="$missing $c"
    done
    if [ -z "$missing" ]; then
        ok "the reference lists these components too"
    else
        bad "the reference lists these components too" "not found there:$missing"
    fi
fi

echo "== what list shows is what lint accepts =="
# For each documented field of a built-in, a config that sets it must lint --
# unless list says it is unimplemented, in which case it must be REFUSED. The
# field values are deliberately typeless placeholders; a type error would be
# reported differently from an unknown-field error, and only the latter is
# what this checks.
python3 - "$WORK/list.txt" > "$WORK/fields.txt" <<'PYSCRIPT'
import re
import sys

# "<kind> <field> <unimplemented?>" for the built-ins only.
BUILTINS = {"generate", "file", "stdin", "stdout", "drop", "broker", "sequence", "switch"}
kind = None
for line in open(sys.argv[1], encoding="utf-8"):
    if re.match(r"^[a-z_0-9]+$", line.strip()) and not line.startswith(" "):
        kind = line.strip()
        continue
    m = re.match(r"^  ([a-z_0-9]+)  <", line)
    if m and kind in BUILTINS:
        unimpl = "1" if "(not implemented by swordfish)" in line else "0"
        print(kind, m.group(1), unimpl)
PYSCRIPT

# A component is an input or an output depending on where it is legal; a few are
# both (file, broker), so each is tried in the section the table put it in.
section_of() {   # kind field -> "input" or "output"
    case "$1" in
        generate|stdin|sequence) echo input ;;
        stdout|drop|switch)      echo output ;;
        # file and broker exist on both sides with different fields. The field
        # decides which one this row came from.
        file)   case "$2" in paths|delete_on_finish) echo input ;; path) echo output ;;
                             *) echo input ;; esac ;;
        broker) case "$2" in inputs) echo input ;; outputs|pattern) echo output ;;
                             *) echo input ;; esac ;;
        *) echo input ;;
    esac
}

checked=0
while read -r kind field unimpl; do
    [ -n "$kind" ] || continue
    sect=$(section_of "$kind" "$field")
    # Some built-ins refuse a config for a REQUIRED field before they ever look
    # at the one under test, so the required fields are supplied. Without this,
    # `sequence: { sharded_join: {} }` was refused for having no `inputs` and the
    # case recorded that as "lint accepted the field".
    case "$kind.$sect" in
        sequence.input|broker.input) req=', inputs: [ { generate: { count: 1 } } ]' ;;
        broker.output)               req=', outputs: [ { drop: {} } ]' ;;
        file.input)                  req=', paths: [ /dev/null ]' ;;
        file.output)                 req=', path: /dev/null' ;;
        switch.output)               req=', cases: []' ;;
        *)                           req='' ;;
    esac
    if [ "$sect" = input ]; then
        printf 'input: { %s: { %s: {}%s } }\noutput: { drop: {} }\n' \
               "$kind" "$field" "$req" > "$WORK/f.yaml"
    else
        printf 'input: { generate: { count: 1 } }\noutput: { %s: { %s: {}%s } }\n' \
               "$kind" "$field" "$req" > "$WORK/f.yaml"
    fi
    out=$("$SF" lint "$WORK/f.yaml" 2>&1)
    checked=$((checked + 1))
    if printf '%s' "$out" | grep -q "has no field '$field'"; then
        bad "list shows $kind.$field but lint calls it unknown" "$out"
    elif [ "$unimpl" = 1 ] && ! printf '%s' "$out" | grep -q 'not implemented'; then
        bad "list marks $kind.$field unimplemented but lint accepted it" "${out:-<no error>}"
    fi
done < "$WORK/fields.txt"
if [ "$checked" -lt 20 ]; then
    bad "enough fields were checked" "only $checked; the parse of list's output is probably wrong"
else
    ok "every field list shows is a field lint knows ($checked checked)"
fi

echo "== and a field list does NOT show is refused =="
# The other direction: the table must not be a superset either, or `list` would
# be advertising nothing while the parser quietly accepted more.
for pair in "generate wibble" "stdout nonsense" "broker not_a_field"; do
    set -- $pair
    if [ "$1" = stdout ]; then
        printf 'input: { generate: { count: 1 } }\noutput: { %s: { %s: 1 } }\n' "$1" "$2" > "$WORK/f.yaml"
    else
        printf 'input: { %s: { %s: 1 } }\noutput: { drop: {} }\n' "$1" "$2" > "$WORK/f.yaml"
    fi
    out=$("$SF" lint "$WORK/f.yaml" 2>&1)
    if printf '%s' "$out" | grep -q "has no field '$2'"; then
        ok "an undocumented field on $1 is refused"
    else
        bad "an undocumented field on $1 is refused" "$out"
    fi
done

echo
echo "list: $pass ok, $fail failed"
[ "$fail" -eq 0 ]
