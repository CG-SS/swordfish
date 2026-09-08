#!/bin/bash
# Differential gate for the `avro` scanner.
#
# Every fixture is read by BOTH redpanda-connect and swordfish, in both output
# modes, and the bytes are compared. The scanner's whole job is to produce the
# same JSON the reference produces, so a byte comparison is the only check worth
# running -- "parses without error" would have passed while the float formatting
# was still wrong.
#
# Each good fixture is checked three ways:
#   * default          -- Avro JSON, unions wrapped
#   * raw_json: true   -- standard JSON, unions unwrapped
#   * metadata         -- @avro_schema and @avro_schema_fingerprint, which are
#                         the canonical schema and its CRC-64-AVRO fingerprint
#
# Each bad fixture is checked for a REFUSAL naming the part of the container
# that is wrong. The reference's wording differs, so those cases assert
# swordfish's own message rather than a diff.
set -u

here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/.." && pwd)
# The other gates cd to the source root first, and ctest passes them paths --
# `../redpanda-connect` -- that only resolve from there. Without this the gate
# looked for the reference next to the BUILD directory and refused to start.
cd "$root" || exit 1
build=${SWORDFISH_BUILD:-$root/build}
# Positional, in the same order as the other gates, so ctest drives them all the
# same way: swordfish-run, redpanda-connect, sfconfig.
sf=${1:-$build/swordfish-run}
ref=${2:-$root/../redpanda-connect}
lint=${3:-$build/sfconfig}
fixtures=$here/fixtures/avro
# One shard and a small heap: the fixtures are a few hundred bytes each, and the
# default is a reactor on every core, which dominated the gate's runtime.
sea_args="--smp 1 --memory 512M --overprovisioned"

if [ ! -x "$sf" ]; then echo "no swordfish-run at $sf" >&2; exit 1; fi
if [ ! -x "$ref" ]; then echo "no redpanda-connect at $ref" >&2; exit 1; fi

# Regenerated every run: the fixtures are derived data, and a stale one would
# quietly test something other than what the generator says it tests.
python3 "$here/make_avro_fixtures.py" "$fixtures" > /dev/null || exit 1

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

pass=0
fail=0

report() {   # ok/FAIL label [detail...]
    if [ "$1" = ok ]; then
        pass=$((pass + 1))
        echo "  ok   $2"
    else
        fail=$((fail + 1))
        echo "  FAIL $2"
        shift 2
        printf '%s\n' "$@" | sed 's/^/         /'
    fi
}

# Runs one config through both binaries and diffs stdout. swordfish's log lines
# go to stderr except the shutdown summary, which is dropped by name rather than
# by position so a change in log volume cannot make this vacuous.
compare() {   # label config
    local label=$1 cfg=$2
    timeout -s KILL 120 "$ref" run "$cfg" 2>/dev/null > "$work/ref.out"
    # shellcheck disable=SC2086 -- sea_args is a deliberate word list.
    timeout -s KILL 120 "$sf" --config "$cfg" $sea_args 2>/dev/null | grep -v '^INFO ' > "$work/sf.out"
    if ! cmp -s "$work/ref.out" "$work/sf.out"; then
        report FAIL "$label" "$(diff "$work/ref.out" "$work/sf.out" | head -8)"
    elif [ ! -s "$work/ref.out" ]; then
        # Two empty outputs are not agreement -- they are the shape a vacuous
        # case takes, and this suite has produced one before. Every fixture
        # compared here has records in it.
        report FAIL "$label" "both produced NOTHING; the case proves nothing"
    else
        report ok "$label"
    fi
}

echo "== good fixtures =="
for f in "$fixtures"/*.avro; do
    # `empty` and `maporder` are checked below on their own terms: a zero-byte
    # OCF file is malformed rather than empty, and a multi-key map has no
    # reproducible byte form in the reference to diff against.
    case $(basename "$f") in bad_*|empty.avro|maporder.avro) continue ;; esac
    name=$(basename "$f" .avro)

    cat > "$work/default.yaml" <<YAML
input: { file: { paths: [ $f ], scanner: { avro: {} } } }
output: { stdout: { codec: lines } }
YAML
    compare "$name  default" "$work/default.yaml"

    cat > "$work/raw.yaml" <<YAML
input: { file: { paths: [ $f ], scanner: { avro: { raw_json: true } } } }
output: { stdout: { codec: lines } }
YAML
    compare "$name  raw_json" "$work/raw.yaml"

    cat > "$work/meta.yaml" <<YAML
input: { file: { paths: [ $f ], scanner: { avro: {} } } }
pipeline:
  processors:
    - mapping: |
        root.schema = @avro_schema
        root.fingerprint = @avro_schema_fingerprint
output: { stdout: { codec: lines } }
YAML
    compare "$name  metadata" "$work/meta.yaml"
done

echo "== malformed fixtures =="
# Each entry is a fixture name and a phrase swordfish's error must contain.
# Matching a phrase rather than the whole string keeps the gate from breaking on
# a reworded error, while still failing if the scanner starts reporting the
# WRONG part of the container.
# check_error <fixture> <phrase> [stricter]
#
# `stricter` marks a fixture the reference does NOT refuse, where swordfish is
# deliberately the stricter of the two. Those cases assert the reference's
# actual behaviour rather than pretending it agrees -- otherwise the only way to
# keep the gate green would be to drop the case, and the divergence would stop
# being tested at all.
check_error() {   # fixture want [stricter]
    local name=$1 want=$2 f=$fixtures/$1.avro
    # `http.enabled: false` because the reference binds :4195 by default, and
    # under `ctest -j4` a second reference process fails with "bind: address
    # already in use". That is logged as `level=error`, which the `stricter`
    # branch below read as "the reference refused the file" -- a false
    # divergence reported on 2026-09-08 for `bad_count_bomb`. The gate now
    # removes the cause as well as checking for it.
    cat > "$work/bad.yaml" <<YAML
http: { enabled: false }
input: { file: { paths: [ $f ], scanner: { avro: {} } } }
output: { drop: {} }
YAML
    local out rc
    # shellcheck disable=SC2086
    out=$(timeout -s KILL 120 "$sf" --config "$work/bad.yaml" $sea_args 2>&1)
    rc=$?
    # The exit status is half the assertion. `bad_deep_nesting` used to arrive
    # here as a SIGSEGV (139), which produces no message at all -- so a check
    # that only looked for a phrase would have called it a wording problem.
    if [ "$rc" -ge 128 ]; then
        report FAIL "$name  refused" "the process died with signal $((rc - 128))" \
            "$(printf '%s' "$out" | grep -iE 'segmentation|abort' | head -2)"
    elif printf '%s' "$out" | grep -qF "$want"; then
        report ok "$name  refused"
    else
        report FAIL "$name  refused" "wanted: $want" "$(printf '%s' "$out" | grep -i avro | head -3)"
    fi
    # The reference must reject it too: a fixture both accept is not testing a
    # rejection, and one only swordfish rejects would be a divergence rather
    # than a gate. Its EXIT CODE cannot be the check -- a header-level read
    # failure is retried for ever there, so the process never ends on its own --
    # so the logged error line is, under a short timeout.
    local refout refrc
    refout=$(timeout -s KILL 20 "$ref" run "$work/bad.yaml" 2>&1)
    refrc=$?
    if [ "${3:-}" = stricter ]; then
        # The reference must NOT refuse it -- that is the whole claim. It runs
        # until the timeout kills it (124, or 137 for the KILL), logging no
        # error, which is what makes swordfish's named refusal an improvement
        # rather than an incompatibility.
        # Only an error ABOUT THE FILE counts. A reference that could not start
        # at all has not made a judgement on the fixture, and treating its
        # startup complaint as a refusal is how this case failed spuriously
        # under `ctest -j4`.
        local reffail
        reffail=$(printf '%s' "$refout" | grep 'level=error' | grep -v 'HTTP Server error')
        if [ "$refrc" -ge 124 ] && [ -z "$reffail" ]; then
            report ok "$name  reference does NOT refuse it (swordfish is stricter)"
        else
            report FAIL "$name  reference does NOT refuse it (swordfish is stricter)" \
                "the reference now refuses it too, so this case can join the others" \
                "$(printf '%s' "$reffail" | head -1 | cut -c1-120)"
        fi
    elif printf '%s' "$refout" | grep -q 'level=error'; then
        report ok "$name  reference also refuses"
    else
        report FAIL "$name  reference also refuses" "the reference accepted it"
    fi
}

check_error empty                "the stream is empty"
check_error bad_magic            "not an Avro OCF stream"
check_error bad_truncated_header "ended before the OCF header was complete"
check_error bad_truncated_block  "ended part-way through a data block"
check_error bad_sync             "sync marker does not match"
check_error bad_codec            "is not one of null, deflate, snappy, zstandard"
check_error bad_codec_spec       "is not one of null, deflate, snappy, zstandard"
check_error bad_schema           "does not compile"

# Hostile containers, every one a live defect the 2026-09-07 audit found. These
# assert only swordfish's own message: the reference refuses them all too (the
# check_error helper verifies that), but its wording differs and, for the two
# resource cases, the reference's own answer is no better than ours was.
#
# What each one is guarding against, because a phrase match does not say it:
#   bad_union_index   -- leaked "vector::_M_range_check" as the user-facing error
#   bad_bytes_length  -- leaked a raw avro::Exception
#   bad_array_count   -- allocated the DECLARED count before reading an element
#   bad_count_bomb    -- looped on a declared count until the shard died
#   bad_deep_nesting  -- SEGFAULTED the process
#   bad_short_count   -- accepted the file and silently DROPPED the extra records
# A sound block before a corrupt one: the reference emits the sound block's
# records and then reports the fault, so this is compared as well as refused.
cat > "$work/partial.yaml" <<YAML
input: { file: { paths: [ $fixtures/partial_then_bad.avro ], scanner: { avro: {} } } }
output: { stdout: { codec: lines } }
YAML
compare "partial_then_bad  records before the fault" "$work/partial.yaml"

check_error bad_union_index      "avro: the container could not be decoded"
check_error bad_bytes_length     "avro: the container could not be decoded"
check_error bad_array_count      "avro: the container could not be decoded"
# The reference does not bound this one: it streams the datums a bogus count
# claims and runs for ever. Bounded here instead, with a named error.
check_error bad_count_bomb       "swordfish will decode from one block" stricter
check_error bad_deep_nesting     "Datum nesting is deeper than"
check_error bad_short_count      "the count and the contents disagree"

echo "== map key order =="
# The one place the scanner does not reproduce the reference's bytes, because
# the reference does not reproduce its own: it writes an Avro map in Go's
# randomised iteration order. swordfish sorts instead. Three things are checked,
# and together they say the ONLY difference is the order.
#
#   1. swordfish is deterministic -- the same bytes on every run.
#   2. its keys come out sorted.
#   3. the reference, given enough tries, produces swordfish's exact bytes.
#      That last one is what rules out a difference hiding behind the order: if
#      a value, an escape or a float were wrong too, no iteration order would
#      ever make the reference agree.
cat > "$work/maporder.yaml" <<YAML
input: { file: { paths: [ $fixtures/maporder.avro ], scanner: { avro: {} } } }
output: { stdout: { codec: lines } }
YAML
# shellcheck disable=SC2086
timeout -s KILL 120 "$sf" --config "$work/maporder.yaml" $sea_args 2>/dev/null \
    | grep -v '^INFO ' > "$work/order.0"
stable=yes
for i in 1 2 3 4; do
    # shellcheck disable=SC2086
    timeout -s KILL 120 "$sf" --config "$work/maporder.yaml" $sea_args 2>/dev/null \
        | grep -v '^INFO ' > "$work/order.$i"
    cmp -s "$work/order.0" "$work/order.$i" || stable=no
done
if [ "$stable" = yes ]; then
    report ok "map key order is the same on every run"
else
    report FAIL "map key order is the same on every run" "$(cat "$work/order.0")"
fi

if grep -q '"m":{"apple":2,"mango":3,"zebra":1}' "$work/order.0" &&
   grep -q '"m":{"a":2,"b":1}' "$work/order.0"; then
    report ok "map keys are sorted, not in encounter order"
else
    report FAIL "map keys are sorted, not in encounter order" "$(grep -o '\"m\":{[^}]*}' "$work/order.0")"
fi

# And the only difference is the ORDER. Sorting each map's entries in BOTH
# outputs and requiring them to be byte-identical proves that: if a value, an
# escape or a float were also wrong, no reordering would make them agree.
#
# Deterministic, unlike asking the reference to happen to iterate in sorted
# order -- forty runs of this fixture produced SIX distinct outputs, and the one
# swordfish writes turned up in one of them. A retry loop would have been a coin
# flip dressed up as a check.
timeout -s KILL 120 "$ref" run "$work/maporder.yaml" 2>/dev/null > "$work/order.ref"
norm() { python3 "$here/sort_avro_maps.py" "$1"; }

if [ -s "$work/order.ref" ] && diff -q <(norm "$work/order.ref") <(norm "$work/order.0") > /dev/null; then
    report ok "everything but the map key order is byte-identical"
else
    report FAIL "everything but the map key order is byte-identical" \
        "$(diff <(norm "$work/order.ref") <(norm "$work/order.0") | head -6)"
fi

echo "== compiled mode =="
# `swordfish build` has to produce a binary that reads Avro too, and that is not
# implied by the interpreter working: the generated code links against a
# hand-maintained flag list in swordfish-config, which is where avro-cpp and fmt
# had to be added. A missing entry there fails at link time -- but only for a
# config that actually uses the scanner, which nothing else in the suite does.
if [ -x "$build/swordfish-build" ]; then
    cat > "$work/compiled.yaml" <<YAML
input: { file: { paths: [ $fixtures/unions.avro ], scanner: { avro: { raw_json: true } } } }
output: { stdout: { codec: lines } }
YAML
    if SWORDFISH_CONFIG="$build/swordfish-config" \
       timeout -s KILL 900 "$build/swordfish-build" "$work/compiled.yaml" \
           -o "$work/compiled.bin" > "$work/compiled.log" 2>&1; then
        timeout -s KILL 120 "$ref" run "$work/compiled.yaml" 2>/dev/null > "$work/c.ref"
        # shellcheck disable=SC2086
        timeout -s KILL 120 "$work/compiled.bin" $sea_args 2>/dev/null \
            | grep -v '^INFO ' > "$work/c.sf"
        if [ -s "$work/c.ref" ] && cmp -s "$work/c.ref" "$work/c.sf"; then
            report ok "a compiled binary reads Avro identically"
        else
            report FAIL "a compiled binary reads Avro identically" \
                "$(diff "$work/c.ref" "$work/c.sf" | head -6)"
        fi
    else
        report FAIL "a compiled binary reads Avro identically" "$(tail -5 "$work/compiled.log")"
    fi
else
    report FAIL "a compiled binary reads Avro identically" "swordfish-build was not built"
fi

echo "== config lint =="
# An unknown field under `avro` must be refused rather than ignored: a typo in
# `raw_json` that is silently dropped would change the output format with no
# diagnostic at all.
cat > "$work/lint.yaml" <<YAML
input: { file: { paths: [ x.avro ], scanner: { avro: { raw_jsonn: true } } } }
output: { drop: {} }
YAML
if timeout -s KILL 60 "$lint" lint "$work/lint.yaml" 2>&1 | grep -q 'raw_jsonn'; then
    report ok "unknown field under avro is refused"
else
    report FAIL "unknown field under avro is refused" \
        "$(timeout -s KILL 60 "$lint" lint "$work/lint.yaml" 2>&1 | head -3)"
fi

echo
echo "avro: $pass ok, $fail failed"
[ "$fail" -eq 0 ]
