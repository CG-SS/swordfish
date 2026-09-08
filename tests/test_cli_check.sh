#!/bin/bash
# The `swordfish` front end.
#
# The point of this gate is not that the commands work -- each has its own gate
# already -- but that dispatching through `swordfish <cmd>` reaches the SAME
# code as the per-command binary. They share an entry point by construction, so
# the check is cheap and the failure it guards against is a future refactor
# quietly giving one of them a different argv.
#
# It also covers the two things the front end alone is responsible for:
# `--version`, which has nowhere else to live, and
# the distinction between a command swordfish has not built and one that does
# not exist.
set -u

here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/.." && pwd)
cd "$root" || exit 1
build=${SWORDFISH_BUILD:-$root/build}
sf=${1:-$build/swordfish}
sea="--smp 1 --memory 512M --overprovisioned"

[ -x "$sf" ] || { echo "no swordfish front end at $sf" >&2; exit 1; }

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
pass=0
fail=0
ok()  { pass=$((pass + 1)); echo "  ok   $1"; }
bad() { fail=$((fail + 1)); echo "  FAIL $1"; shift; printf '%s\n' "$@" | sed 's/^/         /'; }

cat > "$work/p.yaml" <<'YAML'
input: { generate: { count: 2, interval: 0s, mapping: 'root.n = count("cli_gate")' } }
output: { stdout: { codec: lines } }
YAML
cat > "$work/bad.yaml" <<'YAML'
input: { generate: { count: 2, mappingg: 'root = 1' } }
output: { stdout: {} }
YAML

echo "== the front end's own surface =="

# Asserted on SHAPE, not on a pinned string, so a version
# bump does not fail the gate -- but an empty or unstamped build does.
v=$(timeout -s KILL 30 "$sf" --version 2>&1)
if printf '%s' "$v" | grep -qE '^swordfish [0-9]+\.[0-9]+\.[0-9]+.* \(built [0-9]{4}-[0-9]{2}-[0-9]{2}T'; then
    ok "--version reports a version and a build stamp"
else
    bad "--version reports a version and a build stamp" "got: $v"
fi

for flag in --help help -h; do
    out=$(timeout -s KILL 30 "$sf" "$flag" 2>&1); rc=$?
    if [ "$rc" -eq 0 ] && printf '%s' "$out" | grep -q 'usage: swordfish'; then
        ok "$flag prints usage and exits 0"
    else
        bad "$flag prints usage and exits 0" "rc=$rc" "$(printf '%s' "$out" | head -2)"
    fi
done

out=$(timeout -s KILL 30 "$sf" 2>&1); rc=$?
if [ "$rc" -eq 2 ] && printf '%s' "$out" | grep -q 'usage: swordfish'; then
    ok "no command prints usage and exits 2"
else
    bad "no command prints usage and exits 2" "rc=$rc"
fi

# The same distinction the scanners make: "not built yet" and "not a thing" are
# different mistakes and get different messages.
# `dry-run`, because it is the only entry left in swordfish_main.cc's
# `unbuilt_reference` list. This case has now had to move four times -- streams,
# echo, create -- each time because the command it named got built and the case
# silently started exercising a working command instead of asserting anything.
# Whatever stands here must be a command swordfish genuinely has not built; when
# the last one is built this case is DELETED rather than repointed.
#
# Guarded so it cannot go quietly vacuous a fifth time: if `dry-run` ever starts
# working, the case fails and says so.
out=$(timeout -s KILL 30 "$sf" dry-run 2>&1)
if printf '%s' "$out" | grep -q 'has not built yet'; then
    ok "an unbuilt redpanda-connect command is named as unbuilt"
else
    bad "an unbuilt redpanda-connect command is named as unbuilt" \
        "'dry-run' no longer reports itself unbuilt -- if it was built, delete" \
        "this case or point it at another entry of unbuilt_reference[]." \
        "$(printf '%s' "$out" | head -1)"
fi
# The other list, and a different message: a command swordfish plans and the
# reference does not have at all. Confusing the two was a real bug -- `inspect`
# was described as a redpanda-connect command, which it is not.
out=$(timeout -s KILL 30 "$sf" inspect 2>&1)
if printf '%s' "$out" | grep -q 'redpanda-connect has no such command either'; then
    ok "a planned swordfish-only command is distinguished from a reference one"
else
    bad "a planned swordfish-only command is distinguished from a reference one" \
        "$(printf '%s' "$out" | head -1)"
fi
out=$(timeout -s KILL 30 "$sf" wibble 2>&1)
if printf '%s' "$out" | grep -q "is not a command"; then
    ok "an unknown command is named as unknown"
else
    bad "an unknown command is named as unknown" "$(printf '%s' "$out" | head -1)"
fi

echo "== a global flag may come before the command =="
# `redpanda-connect -t templates/*.yaml lint config.yaml` is the form the
# reference's own documentation uses. Only the trailing position worked here, so
# that command line failed with "'-t' is not a command" -- which defeats the
# point of a front end whose claim is that a reference command line runs
# unchanged. Both positions are checked, and the reference is checked too, so
# this stays a measurement rather than an assumption.
cat > "$work/tmpl.yaml" <<'YAML'
name: cli_gate_gen
type: input
fields: []
mapping: |
  root.generate.mapping = "root = {}"
  root.generate.count = 1
YAML
printf 'input:
  cli_gate_gen: {}
output:
  drop: {}
' > "$work/usest.yaml"
for form in before after; do
    if [ "$form" = before ]; then
        out=$(timeout -s KILL 30 "$sf" -t "$work/tmpl.yaml" lint "$work/usest.yaml" 2>&1); rc=$?
    else
        out=$(timeout -s KILL 30 "$sf" lint -t "$work/tmpl.yaml" "$work/usest.yaml" 2>&1); rc=$?
    fi
    if [ "$rc" -eq 0 ] && [ -z "$out" ]; then
        ok "-t $form the command is accepted"
    else
        bad "-t $form the command is accepted" "rc=$rc" "$out"
    fi
done
ref_bin=${REDPANDA_CONNECT:-$root/../redpanda-connect}
if [ -x "$ref_bin" ]; then
    if timeout -s KILL 30 "$ref_bin" -t "$work/tmpl.yaml" lint "$work/usest.yaml" >/dev/null 2>&1; then
        ok "the reference accepts -t before the command too"
    else
        bad "the reference accepts -t before the command too" "it rejected the leading form"
    fi
fi
# A command whose own parser has no `-t` option is TOLD so. Forwarding it
# regardless made `swordfish -t f blobl 'root = 1'` fail with "mapping parse
# failed" -- blobl had read `-t` as the mapping. The reference accepts the flag
# everywhere and ignores it where it means nothing; naming it is the deliberate
# difference, and it only affects command lines that were meaningless anyway.
out=$(timeout -s KILL 30 "$sf" -t "$work/tmpl.yaml" blobl 'root = 1' 2>&1)
if printf '%s' "$out" | grep -q "does not use templates"; then
    ok "-t before a command that cannot use it is named"
else
    bad "-t before a command that cannot use it is named" "$out"
fi
# ...and that command still works without it, which is the half that would
# otherwise go unnoticed.
out=$(printf '{}\n' | timeout -s KILL 30 "$sf" blobl 'root = 1' 2>&1)
if [ "$out" = "1" ]; then
    ok "the same command still works without the flag"
else
    bad "the same command still works without the flag" "$out"
fi

# Repeated. The config below uses the SECOND template, so a loop that collected
# only the first would fail here rather than pass quietly.
cat > "$work/tmpl2.yaml" <<'YAML'
name: cli_gate_gen2
type: input
fields: []
mapping: |
  root.generate.mapping = "root = {}"
  root.generate.count = 1
YAML
printf 'input:\n  cli_gate_gen2: {}\noutput:\n  drop: {}\n' > "$work/usest2.yaml"
out=$(timeout -s KILL 30 "$sf" -t "$work/tmpl.yaml" -t "$work/tmpl2.yaml" \
        lint "$work/usest2.yaml" 2>&1); rc=$?
if [ "$rc" -eq 0 ] && [ -z "$out" ]; then
    ok "a repeated -t loads every file"
else
    bad "a repeated -t loads every file" "rc=$rc" "$out"
fi

# `--version` and `--help` answer without dispatching, so a leading -t would be
# collected and then dropped. Named rather than ignored.
out=$(timeout -s KILL 30 "$sf" -t "$work/tmpl.yaml" --version 2>&1)
if printf '%s' "$out" | grep -q "does not use templates"; then
    ok "-t before --version is named rather than dropped"
else
    bad "-t before --version is named rather than dropped" "$out"
fi

# A flag that takes a file and is given none must say so, not be swallowed and
# then reported as an unknown command.
out=$(timeout -s KILL 30 "$sf" -t 2>&1)
if printf '%s' "$out" | grep -q "needs a template file"; then
    ok "a global flag with no argument is named"
else
    bad "a global flag with no argument is named" "$out"
fi

echo "== dispatch reaches the same code as the old binaries =="

# Each pair must produce byte-identical output. `same` also refuses two empty
# outputs: this suite has been fooled by that before, and every command below
# has something to say.
same() {   # label  cmd-through-frontend...  --  cmd-through-old-binary...
    local label=$1; shift
    local a=() b=() seen=0
    for arg in "$@"; do
        if [ "$arg" = "--" ]; then seen=1; continue; fi
        if [ "$seen" -eq 0 ]; then a+=("$arg"); else b+=("$arg"); fi
    done
    # STDOUT only, and the exit code. stderr carries Seastar's startup log,
    # which is timestamped and differs between two runs of the SAME binary --
    # comparing it made this case fail on the clock rather than on the code.
    timeout -s KILL 120 "${a[@]}" > "$work/a.out" 2>/dev/null; local ra=$?
    timeout -s KILL 120 "${b[@]}" > "$work/b.out" 2>/dev/null; local rb=$?
    if [ "$ra" != "$rb" ]; then
        bad "$label" "exit codes differ: front end $ra, old binary $rb"
        return
    fi
    # The two differ in argv[0], which reaches usage and error text, so compare
    # with the program name normalised away.
    sed -i 's/swordfish-[a-z]*/PROG/g; s/swordfish [a-z]*:/PROG:/g; s/sfconfig/PROG/g' \
        "$work/a.out" "$work/b.out"
    if ! cmp -s "$work/a.out" "$work/b.out"; then
        bad "$label" "$(diff "$work/a.out" "$work/b.out" | head -6)"
    elif [ ! -s "$work/a.out" ]; then
        bad "$label" "both produced NOTHING; the case proves nothing"
    else
        ok "$label"
    fi
}

# shellcheck disable=SC2086
same "run agrees with swordfish-run" \
    "$sf" run --config "$work/p.yaml" $sea -- \
    "$build/swordfish-run" --config "$work/p.yaml" $sea
x=$(timeout -s KILL 60 "$sf" lint "$work/bad.yaml" 2>&1); rx=$?
y=$(timeout -s KILL 60 "$build/sfconfig" lint "$work/bad.yaml" 2>&1); ry=$?
if [ -n "$x" ] && [ "$x" = "$y" ] && [ "$rx" = "$ry" ] && [ "$rx" != 0 ]; then
    ok "lint agrees with sfconfig lint, and both refuse the config"
else
    bad "lint agrees with sfconfig lint, and both refuse the config" \
        "front end (rc=$rx): $x" "sfconfig (rc=$ry): $y"
fi
same "list agrees with sfconfig list" \
    "$sf" list -- "$build/sfconfig" list
printf '{"a":2}\n{"a":3}\n' > "$work/docs.jsonl"
same "blobl agrees with sfslice blobl" \
    "$sf" blobl 'root.b = this.a * 21' -i "$work/docs.jsonl" -- \
    "$build/sfslice" blobl 'root.b = this.a * 21' -i "$work/docs.jsonl"

echo "== the reference's own invocation shapes =="

# `redpanda-connect run config.yaml` is positional, so `swordfish run` must be
# too -- that is the muscle memory this front end exists to keep working.
# shellcheck disable=SC2086
a=$(timeout -s KILL 60 "$sf" run "$work/p.yaml" $sea 2>/dev/null | grep '^{')
# shellcheck disable=SC2086
b=$(timeout -s KILL 60 "$sf" run --config "$work/p.yaml" $sea 2>/dev/null | grep '^{')
if [ -n "$a" ] && [ "$a" = "$b" ]; then
    ok "run takes the config positionally, as the reference does"
else
    bad "run takes the config positionally, as the reference does" \
        "positional: ${a:-<nothing>}" "--config: ${b:-<nothing>}"
fi

if [ -x "${REDPANDA_CONNECT:-$root/../redpanda-connect}" ]; then
    ref=${REDPANDA_CONNECT:-$root/../redpanda-connect}
    x=$(timeout -s KILL 30 "$sf" blobl 'root.b = this.a * 21' -i "$work/docs.jsonl" 2>&1)
    y=$(timeout -s KILL 30 "$ref" blobl 'root.b = this.a * 21' < "$work/docs.jsonl" 2>&1)
    if [ -n "$y" ] && [ "$x" = "$y" ]; then
        ok "blobl matches redpanda-connect's blobl"
    else
        bad "blobl matches redpanda-connect's blobl" "swordfish: $x" "reference: $y"
    fi
fi

echo "== build, end to end through the front end =="
if SWORDFISH_CONFIG="$build/swordfish-config" \
   timeout -s KILL 900 "$sf" build "$work/p.yaml" -o "$work/p.bin" > "$work/build.log" 2>&1; then
    # shellcheck disable=SC2086
    got=$(timeout -s KILL 60 "$work/p.bin" $sea 2>/dev/null | grep -c '^{')
    if [ "$got" = 2 ]; then
        ok "build produces a binary that runs"
    else
        bad "build produces a binary that runs" "the binary emitted $got records, wanted 2"
    fi
else
    bad "build produces a binary that runs" "$(tail -3 "$work/build.log")"
fi

echo
echo "cli: $pass ok, $fail failed"
[ "$fail" -eq 0 ]
