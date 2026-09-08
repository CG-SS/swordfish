#!/bin/bash
# `swordfish create` -- a starting config for named components.
#
# The property that matters is that what it prints is a config swordfish will
# ACCEPT. A scaffolding tool that emits something the parser refuses is worse
# than none, and it is easy to do by accident: the first version emitted both
# `scanner` and the deprecated `codec` on `stdin`, which are mutually exclusive,
# so `swordfish create` produced a config `swordfish run` rejected.
#
# So every component this build implements is scaffolded and linted -- not a
# handful chosen by hand -- and the ones whose fields are all optional are RUN,
# because linting a config is a weaker claim than starting it.
#
# NOT `pipefail`: several checks are `<command that exits non-zero> | grep`, and
# under pipefail the pipeline reports the command's failure rather than the
# match. That cost three false failures in tests/test_list_check.sh.
set -u
cd "$(dirname "$0")/.." || exit 1
SF=${1:-build/swordfish}
RC=${2:-../redpanda-connect}
SEA="--smp 1 --memory 512M --overprovisioned"

[ -x "$SF" ] || { echo "no swordfish front end at $SF" >&2; exit 1; }

WORK=$(mktemp -d /tmp/sfcreate.XXXXXX)
trap 'rm -rf "$WORK"' EXIT
trap 'rm -rf "$WORK"; exit 143' INT TERM

pass=0
fail=0
ok()  { pass=$((pass + 1)); echo "ok   $1"; }
bad() { fail=$((fail + 1)); echo "FAIL $1" >&2; shift; printf '%s\n' "$@" | sed 's/^/       /' >&2; }

echo "== the expression forms the reference documents =="
for expr in "" "generate//stdout" "stdin//stdout" "generate/mapping/stdout" \
            "generate/mapping,split/stdout" "generate//stdout,drop"; do
    if [ -z "$expr" ]; then
        "$SF" create > "$WORK/c.yaml" 2>"$WORK/err"
    else
        "$SF" create "$expr" > "$WORK/c.yaml" 2>"$WORK/err"
    fi
    rc=$?
    label="create '${expr:-<no expression>}'"
    if [ "$rc" -ne 0 ] || [ ! -s "$WORK/c.yaml" ]; then
        bad "$label produces a config" "$(head -3 "$WORK/err")"
        continue
    fi
    out=$("$SF" lint "$WORK/c.yaml" 2>&1)
    if [ -n "$out" ]; then
        bad "$label lints" "$out" "--- the config ---" "$(cat "$WORK/c.yaml")"
    else
        ok "$label lints"
    fi
done

echo "== the shapes an expression can be written in =="
# The reference trims whitespace around a component name, so an expression
# copied from its docs with spaces has to work here. It did not: we reported
# "'generate ' is not an input".
out=$("$SF" create 'generate / noop / stdout' 2>&1)
if printf '%s' "$out" | grep -q '^  generate:'; then
    ok "whitespace around a component name is trimmed"
else
    bad "whitespace around a component name is trimmed" "$out"
fi
if [ -x "$RC" ]; then
    if "$RC" create 'generate / mapping / stdout' >/dev/null 2>&1; then
        ok "the reference trims it too"
    else
        bad "the reference trims it too" "it rejected a spaced expression"
    fi
fi
# A component with no fields is `drop: {}`, not a `{}` hanging on the next line.
# Both parse; only one reads as finished.
"$SF" create 'generate//stdout,drop' > "$WORK/m.yaml" 2>/dev/null
if grep -q -- '- drop: {}' "$WORK/m.yaml"; then
    ok "a component with no fields renders on one line"
else
    bad "a component with no fields renders on one line" "$(cat "$WORK/m.yaml")"
fi

echo "== a scaffold with no required fields RUNS =="
# Linting is the weaker claim. `stdin//stdout` and `generate//stdout` have no
# required fields, so the untouched scaffold has to start and move data --
# which is what caught `scanner` and `codec` being emitted together.
"$SF" create 'stdin//stdout' > "$WORK/io.yaml" 2>/dev/null
# shellcheck disable=SC2086
got=$(printf 'alpha\nbeta\n' | timeout -s KILL 60 "$SF" run "$WORK/io.yaml" $SEA 2>/dev/null)
if [ "$got" = "$(printf 'alpha\nbeta')" ]; then
    ok "the stdin//stdout scaffold runs and passes data through"
else
    bad "the stdin//stdout scaffold runs and passes data through" \
        "got: ${got:-<nothing>}" "$(cat "$WORK/io.yaml")"
fi

"$SF" create 'generate//stdout' > "$WORK/gen.yaml" 2>/dev/null
# shellcheck disable=SC2086
n=$(timeout -s KILL 20 "$SF" run "$WORK/gen.yaml" $SEA 2>/dev/null | grep -c '^{')
if [ "${n:-0}" -gt 0 ]; then
    ok "the generate//stdout scaffold runs ($n messages)"
else
    bad "the generate//stdout scaffold runs" "$(cat "$WORK/gen.yaml")"
fi

echo "== and the scaffold COMPILES, not just runs =="
# The project's central claim is that interpreted and compiled never disagree
# about what a config means, so a scaffold that `swordfish run` accepts and
# `swordfish build` cannot compile would be a real defect -- and `create` is
# where a new user's first config comes from.
if timeout -s KILL 900 "$SF" build "$WORK/io.yaml" -o "$WORK/io_bin" >/dev/null 2>&1 \
   && [ -x "$WORK/io_bin" ]; then
    # shellcheck disable=SC2086
    got=$(printf 'one\ntwo\n' | timeout -s KILL 60 "$WORK/io_bin" $SEA 2>/dev/null)
    if [ "$got" = "$(printf 'one\ntwo')" ]; then
        ok "the stdin//stdout scaffold compiles and the binary passes data through"
    else
        bad "the stdin//stdout scaffold compiles and the binary passes data through" \
            "got: ${got:-<nothing>}"
    fi
else
    bad "the stdin//stdout scaffold compiles" "swordfish build produced no binary"
fi

echo "== every component this build implements scaffolds without a structural error =="
# The whole listing, not a sample -- and the distinction that matters is WHICH
# error a scaffold can legitimately provoke.
#
# `swordfish lint` is deeper than the reference's. Measured: the reference's own
# `create kafka//stdout` writes `addresses: [] # No default (required)`, its lint
# accepts that (rc=0), and its `run` then fails with "must specify at least one
# topic". So the reference's create output lints but does not necessarily run.
# Ours refuses the empty required value at lint time instead, which is the more
# useful moment to be told -- but it means "every scaffold lints clean" is the
# wrong property to assert here.
#
# The right one is that a scaffold is never STRUCTURALLY wrong: no unknown
# field, no type error, no mutually exclusive pair. The only complaint it may
# provoke is "you still have to fill this in", and only in the three shapes
# below. A new error phrase fails this gate on purpose -- someone should look.
FILLIN='required field is missing|requires `|needs at least'
STRUCTURAL_CHECKED=0
VERDICT=""
# Sets VERDICT rather than echoing it: called from $( ), the counter below would
# increment in a subshell and the total would stay 0, leaving the "enough were
# checked" guard permanently tripped.
check_scaffold() {   # label, config file
    local out
    out=$("$SF" lint "$2" 2>&1)
    STRUCTURAL_CHECKED=$((STRUCTURAL_CHECKED + 1))
    if [ -z "$out" ]; then
        VERDICT=clean
    elif printf '%s' "$out" | grep -qE "$FILLIN"; then
        VERDICT=fillin
    else
        printf 'structural %s: %s\n' "$1" "$out" >> "$WORK/structural.txt"
        VERDICT=structural
    fi
}

"$SF" list > "$WORK/list.txt" 2>/dev/null
mapfile -t KINDS < <(grep -E '^[a-z_0-9]+$' "$WORK/list.txt" | sort -u)
: > "$WORK/structural.txt"
if [ "${#KINDS[@]}" -lt 20 ]; then
    bad "the listing was readable" "only ${#KINDS[@]} components; the sweep would prove little"
else
    n_in=0; n_out=0; clean_in=""; clean_out=""
    for k in "${KINDS[@]}"; do
        # A kind may be an input, an output, a processor, or several. Whichever
        # position `create` accepts it in is the one tested; one it accepts in
        # none is skipped, since `list` covers processors and caches too.
        if "$SF" create "$k//stdout" > "$WORK/k.yaml" 2>/dev/null; then
            n_in=$((n_in + 1))
            check_scaffold "input.$k" "$WORK/k.yaml"
            [ "$VERDICT" = clean ] && clean_in="$clean_in $k"
        fi
        if "$SF" create "generate//$k" > "$WORK/k.yaml" 2>/dev/null; then
            n_out=$((n_out + 1))
            check_scaffold "output.$k" "$WORK/k.yaml"
            [ "$VERDICT" = clean ] && clean_out="$clean_out $k"
        fi
    done
    if [ "$STRUCTURAL_CHECKED" -lt 20 ]; then
        bad "enough scaffolds were checked" "only $STRUCTURAL_CHECKED; the sweep is not proving much"
    elif [ -s "$WORK/structural.txt" ]; then
        bad "no scaffold has a structural error ($n_in inputs, $n_out outputs)" "$(cat "$WORK/structural.txt")"
    else
        ok "no scaffold has a structural error ($n_in inputs, $n_out outputs checked)"
    fi
fi

echo "== a component with no required field scaffolds into a config that lints clean =="
# The strong direction, and the one that catches the bug this gate was written
# for: `stdin` has no required fields, so its scaffold must lint with nothing to
# say. It did not -- it emitted `scanner` and the deprecated `codec` together.
# Which components those are is read from `list`, so the two commands are paired
# rather than both trusted: if `list` says a component needs nothing, `create`
# has to produce something complete.
python3 - "$WORK/list.txt" > "$WORK/norequired.txt" <<'PYSCRIPT'
import re
import sys

kinds, cur, req = [], None, False
for line in open(sys.argv[1], encoding="utf-8"):
    if re.match(r"^[a-z_0-9]+$", line.strip()) and not line.startswith(" "):
        if cur and not req:
            kinds.append(cur)
        cur, req = line.strip(), False
    elif "(required)" in line:
        req = True
if cur and not req:
    kinds.append(cur)
for k in sorted(set(kinds)):
    print(k)
PYSCRIPT

notclean=""; n_free=0
while read -r k; do
    [ -n "$k" ] || continue
    for expr in "$k//stdout" "generate//$k"; do
        "$SF" create "$expr" > "$WORK/k.yaml" 2>/dev/null || continue
        n_free=$((n_free + 1))
        out=$("$SF" lint "$WORK/k.yaml" 2>&1)
        # `broker` and `sequence` take no *field* marked required, but the
        # parser still insists on a child; that is a genuine required value and
        # `list` cannot express it. Allowed here, not elsewhere.
        if [ -n "$out" ] && ! printf '%s' "$out" | grep -qE 'needs at least'; then
            notclean="$notclean$expr: $out"$'\n'
        fi
    done
done < "$WORK/norequired.txt"
if [ "$n_free" -lt 6 ]; then
    bad "enough no-required-field components were found" "only $n_free"
elif [ -z "$notclean" ]; then
    ok "every component with no required field lints clean ($n_free scaffolds)"
else
    bad "every component with no required field lints clean" "$notclean"
fi

echo "== what create says is required is what lint asks for =="
# The pairing in the other direction. If lint refuses a scaffold for a missing
# value, the scaffold must have MARKED that field -- otherwise the user is told
# to fill in something the scaffold never showed them.
unmarked=""
for k in kafka file switch; do
    for expr in "$k//stdout" "generate//$k"; do
        "$SF" create "$expr" > "$WORK/k.yaml" 2>/dev/null || continue
        out=$("$SF" lint "$WORK/k.yaml" 2>&1)
        [ -n "$out" ] || continue
        grep -q '# (required)' "$WORK/k.yaml" || unmarked="$unmarked$expr: lint refuses it but nothing is marked required"$'\n'
    done
done
if [ -z "$unmarked" ]; then
    ok "a scaffold lint refuses marks the fields to fill in"
else
    bad "a scaffold lint refuses marks the fields to fill in" "$unmarked"
fi

echo "== the mistakes an expression can make =="
out=$("$SF" create 'no_such_input//stdout' 2>&1)
if printf '%s' "$out" | grep -q "is not an input swordfish implements"; then
    ok "an unknown input is named"
else
    bad "an unknown input is named" "$out"
fi
out=$("$SF" create 'generate//no_such_output' 2>&1)
if printf '%s' "$out" | grep -q "is not an output swordfish implements"; then
    ok "an unknown output is named"
else
    bad "an unknown output is named" "$out"
fi
out=$("$SF" create 'generate/no_such_processor/stdout' 2>&1)
if printf '%s' "$out" | grep -q "is not a processor swordfish implements"; then
    ok "an unknown processor is named"
else
    bad "an unknown processor is named" "$out"
fi
# A fourth section is refused, not dropped. This scaffolded a working pipeline
# and said nothing about the extra text until 2026-09-08; the reference rejects
# it ("more component separators than expected"), and a silent ignore is the one
# thing this project does not do.
out=$("$SF" create 'generate//stdout/extra' 2>&1)
if printf '%s' "$out" | grep -q "more than three sections"; then
    ok "a fourth section is refused rather than dropped"
else
    bad "a fourth section is refused rather than dropped" "$out"
fi
if [ -x "$RC" ]; then
    "$RC" create 'generate//stdout/extra' >/dev/null 2>&1
    if [ $? -ne 0 ]; then
        ok "the reference refuses it too"
    else
        bad "the reference refuses it too" "it accepted a four-section expression"
    fi
fi

# Nothing is printed before the failure: half a config followed by a complaint
# would be worse than a clean error, because it looks like output.
out=$("$SF" create 'generate//no_such_output' 2>/dev/null)
if [ -z "$out" ]; then
    ok "a failed create prints no partial config"
else
    bad "a failed create prints no partial config" "$out"
fi

echo "== against the reference =="
# The reference takes the same expression, so a command line written for it
# works here. What each PRINTS differs and is meant to -- swordfish leaves out
# the engine blocks -- so what is compared is that both accept the expression
# and both produce something their own linter accepts.
if [ -x "$RC" ]; then
    for expr in "generate//stdout" "generate/mapping/stdout"; do
        "$RC" create "$expr" > "$WORK/r.yaml" 2>/dev/null
        rrc=$?
        "$SF" create "$expr" > "$WORK/s.yaml" 2>/dev/null
        src=$?
        if [ "$rrc" = "$src" ] && [ -s "$WORK/r.yaml" ] && [ -s "$WORK/s.yaml" ]; then
            ok "both accept the expression '$expr'"
        else
            bad "both accept the expression '$expr'" "swordfish rc=$src reference rc=$rrc"
        fi
    done
fi

echo
echo "create: $pass ok, $fail failed"
[ "$fail" -eq 0 ]
