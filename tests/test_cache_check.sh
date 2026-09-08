#!/usr/bin/env bash
# `cache_resources`: the `memory`, `lru` and `file` caches.
#
# `dedupe` is the only consumer, so a cache is observable only through which
# messages survive deduplication -- which is exactly what makes it testable: a
# cap-1 LRU lets every repeat through, an `init_values` key suppresses its first
# occurrence, and a TTL that has elapsed lets a key back in. Each case is
# compared against redpanda-connect on the same config.
set -uo pipefail
cd "$(dirname "$0")/.."
SF=${1:-build/swordfish-run}
RC=${2:-../redpanda-connect}
LINT=${3:-build/sfconfig}
export SWORDFISH_CONFIG="$PWD/build/swordfish-config"
WORK=$(mktemp -d /tmp/sfcache.XXXXXX)
SEA_ARGS="--smp 1 --memory 512M --overprovisioned"
trap 'rm -rf "$WORK"' EXIT
trap 'rm -rf "$WORK"; exit 143' INT TERM

fail=0
note() { echo "ok   $1"; }
bad()  { echo "FAIL $1: $2" >&2; fail=1; }

# <name> <cache_resources body, indented four spaces> [extra pipeline lines]
cmp_ref() {
  local name=$1 body=$2 sf ref
  cat > "$WORK/c.yaml" <<YAML
input:
  generate: { count: 6, interval: 0s, mapping: 'root.k = counter() % 3' }
pipeline:
  processors:
    - dedupe: { cache: c, key: '\${! json("k") }' }
output: { stdout: {} }
cache_resources:
  - label: c
$body
YAML
  # shellcheck disable=SC2086
  sf=$(timeout 60 "$SF" --config "$WORK/c.yaml" $SEA_ARGS 2>/dev/null | sort | tr '\n' '|')
  if [ -z "$sf" ]; then bad "$name" "swordfish produced nothing"; return; fi
  if [ ! -x "$RC" ]; then note "$name (no reference to compare against)"; return; fi
  ref=$(timeout 60 "$RC" run --log.level off "$WORK/c.yaml" 2>/dev/null | sort | tr '\n' '|')
  if [ -z "$ref" ]; then
    bad "$name" "the reference produced nothing, so it did not run this config"; return
  fi
  [ "$sf" = "$ref" ] && note "$name" || bad "$name" "swordfish '$sf' reference '$ref'"
}

# Swordfish only, with the expected output written out. Used where the REFERENCE
# IS NOT AN ORACLE: it pipelines messages concurrently, so any case whose result
# depends on the order in which keys reach the cache -- every eviction-sensitive
# one -- gives a different answer run to run. Measured: five runs of the cap-1
# case below produced five different outputs there, and five identical ones here
# (swordfish is sequential within a shard). Comparing them would be a coin toss
# dressed up as a gate, so these assert swordfish's own behaviour and the LRU's
# recency rule is pinned in tests/test_runtime.cc instead.
only_sf() {  # <name> <cache body> <expected>
  local sf
  cat > "$WORK/o.yaml" <<YAML
input:
  generate: { count: 6, interval: 0s, mapping: 'root.k = counter() % 3' }
pipeline:
  processors:
    - dedupe: { cache: c, key: '\${! json("k") }' }
output: { stdout: {} }
cache_resources:
  - label: c
$2
YAML
  # shellcheck disable=SC2086
  sf=$(timeout 60 "$SF" --config "$WORK/o.yaml" $SEA_ARGS 2>/dev/null | sort | tr '\n' '|')
  [ "$sf" = "$3" ] && note "$1" || bad "$1" "expected '$3' got '$sf'"
}

# Six messages cycling through three keys: a working cache lets three through.
# Order-independent, so the reference IS an oracle for these -- whichever way its
# messages interleave, a cache big enough to hold every key catches every repeat.
cmp_ref "the memory cache deduplicates" '    memory: {}'
cmp_ref "the lru cache deduplicates"    '    lru: { cap: 1000 }'

# A cap of one evicts the previous key every time, so nothing is ever seen as a
# duplicate and all six survive. This is what proves eviction happens at all --
# with an unbounded map the result would be three.
only_sf "an lru at cap 1 evicts before the repeat arrives" '    lru: { cap: 1 }' \
        '{"k":0}|{"k":0}|{"k":1}|{"k":1}|{"k":2}|{"k":2}|'

# `init_values` is documented as pre-populating the cache, so key "1" is already
# present and its first occurrence is dropped: four messages become two.
cmp_ref "memory init_values suppress their keys from the start" \
        '    memory: { init_values: { "1": x } }'
cmp_ref "lru init_values do the same"  \
        '    lru: { cap: 100, init_values: { "1": x } }'

# Fields that exist in the reference and change nothing here. `shards` stripes
# Benthos' map to cut lock contention between goroutines; a swordfish cache is
# shard-local and never locked. `optimistic` drops that locking at the cost of a
# non-atomic ADD; under Seastar `add` cannot be preempted, so it is already
# atomic. Accepting them is not an approximation -- there is nothing to
# approximate -- but a config using them must still behave.
cmp_ref "memory shards is accepted and changes nothing" \
        '    memory: { shards: 4 }'
# `optimistic` is swordfish-only for a different reason: in the reference it
# genuinely makes ADD non-atomic, so it lets duplicates through under
# concurrency, while under Seastar `add` cannot be preempted and the flag is
# inert. Comparing them would be comparing against a race.
only_sf "lru optimistic is accepted and changes nothing" \
        '    lru: { cap: 1000, optimistic: true }' \
        '{"k":0}|{"k":1}|{"k":2}|'

# ---- the file cache ---------------------------------------------------------------
#
# One file per key. The reference does NOT create the directory and errors on
# every message when it is missing; swordfish creates it, which is the one
# deliberate difference and is asserted below rather than left implicit. With the
# directory present the two agree exactly, files included.
mkdir -p "$WORK/fc_sf" "$WORK/fc_ref"
cat > "$WORK/file_sf.yaml" <<YAML
input:
  generate: { count: 6, interval: 0s, mapping: 'root.k = counter() % 3' }
pipeline:
  processors:
    - dedupe: { cache: c, key: '\${! json("k") }' }
output: { stdout: {} }
cache_resources:
  - label: c
    file: { directory: $WORK/fc_sf }
YAML
sed "s|$WORK/fc_sf|$WORK/fc_ref|" "$WORK/file_sf.yaml" > "$WORK/file_ref.yaml"
# shellcheck disable=SC2086
sf=$(timeout 60 "$SF" --config "$WORK/file_sf.yaml" $SEA_ARGS 2>/dev/null | sort | tr '\n' '|')
if [ -x "$RC" ]; then
  ref=$(timeout 60 "$RC" run --log.level off "$WORK/file_ref.yaml" 2>/dev/null | sort | tr '\n' '|')
  sf_files=$(ls "$WORK/fc_sf" | sort | tr '\n' ' ')
  ref_files=$(ls "$WORK/fc_ref" | sort | tr '\n' ' ')
  if [ -n "$ref" ] && [ "$sf" = "$ref" ] && [ "$sf_files" = "$ref_files" ]; then
    note "the file cache deduplicates and writes the same files as the reference"
  else
    bad "the file cache deduplicates and writes the same files as the reference" \
        "output '$sf' vs '$ref'; files '$sf_files' vs '$ref_files'"
  fi
else
  [ -n "$sf" ] && note "the file cache deduplicates" || bad "the file cache deduplicates" "no output"
fi

# The state SURVIVES a restart, which is the only reason to choose this cache
# over `memory`: a second run over the same keys emits nothing.
# shellcheck disable=SC2086
again=$(timeout 60 "$SF" --config "$WORK/file_sf.yaml" $SEA_ARGS 2>/dev/null | tr '\n' '|')
[ -z "$again" ] && note "and its state survives a restart, unlike memory" \
                || bad "and its state survives a restart, unlike memory" "it emitted '$again'"

# swordfish creates the directory; the reference does not. Asserted in both
# directions so neither half can drift unnoticed.
cat > "$WORK/file_new.yaml" <<YAML
input: { generate: { count: 1, interval: 0s, mapping: 'root.k = 1' } }
pipeline:
  processors:
    - dedupe: { cache: c, key: '\${! json("k") }' }
output: { stdout: {} }
cache_resources:
  - label: c
    file: { directory: $WORK/made/up/path }
YAML
# shellcheck disable=SC2086
made=$(timeout 60 "$SF" --config "$WORK/file_new.yaml" $SEA_ARGS 2>/dev/null | tr '\n' '|')
[ "$made" = '{"k":1}|' ] && [ -d "$WORK/made/up/path" ] \
  && note "swordfish creates a missing file-cache directory" \
  || bad "swordfish creates a missing file-cache directory" "got '$made'"
if [ -x "$RC" ]; then
  refmade=$(timeout 60 "$RC" run --log.level off "$WORK/file_new2.yaml" 2>/dev/null | tr '\n' '|')
  sed "s|$WORK/made/up/path|$WORK/never/made|" "$WORK/file_new.yaml" > "$WORK/file_new2.yaml"
  refmade=$(timeout 60 "$RC" run --log.level off "$WORK/file_new2.yaml" 2>/dev/null | tr '\n' '|')
  [ -z "$refmade" ] && [ ! -d "$WORK/never/made" ] \
    && note "the reference does not, and errors instead -- the one difference here" \
    || bad "the reference does not, and errors instead" "it emitted '$refmade'"
fi

# A key that names a SUBDIRECTORY is fine -- the reference joins it onto the
# directory (`filepath.Join(f.dir, key)`), so it works when that subdirectory
# exists and fails with ENOENT when it does not. Refusing every '/' outright, as
# this used to, dropped every such message under the default `drop_on_err: true`
# where the reference wrote the file and passed it on.
#
# What IS refused by name is a key that walks OUT of the directory. The reference
# follows it; a dedupe key is message data, so following it is a path traversal
# driven by whatever the source sends. A stated divergence, in the safe direction.
#
# The refusal is a PROCESSING error, so it is observed the way a user would see
# one -- attached to the message and read back with `error()`. Checking stderr
# would not do: a processing error is marked on the message rather than logged.
#
# `drop_on_err: false` is explicit BECAUSE the default is true, as it is in the
# reference: a message whose cache call failed is discarded by default, so
# without this the message carrying the error never reaches the mapping below and
# the case could only pass while the default was wrong.
cat > "$WORK/file_key.yaml" <<YAML
input: { generate: { count: 1, interval: 0s, mapping: 'root.k = "../escaped"' } }
pipeline:
  processors:
    - dedupe: { cache: c, key: '\${! json("k") }', drop_on_err: false }
    - mapping: 'root.why = error()'
output: { stdout: {} }
cache_resources:
  - label: c
    file: { directory: $WORK/fc_sf }
YAML
# shellcheck disable=SC2086
out=$(timeout 60 "$SF" --config "$WORK/file_key.yaml" $SEA_ARGS 2>/dev/null)
case "$out" in
  *"walks out of the cache directory"*) note "a file-cache key that escapes the directory is refused by name" ;;
  *) bad "a file-cache key that escapes the directory is refused by name" "got '$(echo "$out" | tr '\n' ' ')'" ;;
esac

# And a subdirectory key that DOES exist works, as it does there.
mkdir -p "$WORK/fc_sub/sub"
cat > "$WORK/file_subkey.yaml" <<YAML
input:
  generate:
    count: 3
    interval: 0s
    mapping: 'root = ["sub/A","sub/B","sub/A"].index(counter() - 1)'
pipeline:
  processors: [ { dedupe: { cache: c, key: '\${! content() }' } } ]
output: { stdout: {} }
cache_resources:
  - label: c
    file: { directory: $WORK/fc_sub }
YAML
# shellcheck disable=SC2086
sub_sf=$(timeout 60 "$SF" --config "$WORK/file_subkey.yaml" $SEA_ARGS 2>/dev/null | sort | tr '\n' '|')
if [ -x "$RC" ]; then
  rm -rf "$WORK/fc_sub"; mkdir -p "$WORK/fc_sub/sub"
  sub_rc=$(timeout 60 "$RC" run --log.level off "$WORK/file_subkey.yaml" 2>/dev/null | sort | tr '\n' '|')
  [ "$sub_sf" = "$sub_rc" ] && note "a file-cache key naming an existing subdirectory works" \
                            || bad "a file-cache key naming an existing subdirectory works" \
                                   "swordfish '$sub_sf' reference '$sub_rc'"
else
  [ -n "$sub_sf" ] && note "a file-cache key naming an existing subdirectory works" \
                   || bad "a file-cache key naming an existing subdirectory works" "produced nothing"
fi

# A `cache_resources` entry is ONE cache: two components naming that label must
# contend for the same entries, which is what makes it a resource. Each got its
# own private instance, so a second dedupe over one label passed everything the
# first had already seen. Two labels is the control -- it must still pass A and B.
shared_case() {  # <name> <label-for-second-dedupe> <expected>
  local name=$1 lbl=$2 want=$3 got
  cat > "$WORK/share.yaml" <<YAML
cache_resources:
  - label: one
    memory: {}
  - label: two
    memory: {}
input:
  generate:
    count: 4
    interval: 0s
    mapping: 'root = ["A","B","A","B"].index(counter() - 1)'
pipeline:
  processors:
    - dedupe: { cache: one, key: '\${! content() }' }
    - dedupe: { cache: $lbl, key: '\${! content() }' }
output: { stdout: {} }
YAML
  # shellcheck disable=SC2086
  got=$(timeout 60 "$SF" --config "$WORK/share.yaml" $SEA_ARGS 2>/dev/null | sort | tr '\n' '|')
  if [ "$got" != "$want" ]; then bad "$name" "expected '$want' got '$got'"; return; fi
  if [ -x "$RC" ]; then
    local ref
    ref=$(timeout 60 "$RC" run --log.level off "$WORK/share.yaml" 2>/dev/null | sort | tr '\n' '|')
    [ "$ref" = "$got" ] || { bad "$name" "reference '$ref' swordfish '$got'"; return; }
  fi
  note "$name"
}
shared_case "two dedupes on one cache label share its entries" one ''
shared_case "and two labels are still two caches"              two 'A|B|'

# The lru cache does NOT renew recency on a duplicate: the reference's dedupe
# path PEEKS (cache_lru.go's unsafeAdd), so a repeated key still falls out of a
# full cache. Renewing kept it alive and DROPPED a message the reference delivers.
#
# The interval is not cosmetic. At `interval: 0s` the reference's pipeline runs
# the five messages concurrently and can reach the repeated `A` before the first
# one's cache Add has completed, so it emits `A A B C D` instead of `A B C D` --
# measured at one run in eight, which is exactly the kind of gate that fails once
# a fortnight and gets explained away. Spacing them makes the reference's answer
# deterministic without changing what the case tests: A, B, C fill the cache, the
# fourth message hits, and D evicts.
cat > "$WORK/lru_recency.yaml" <<YAML
cache_resources:
  - label: c
    lru: { cap: 3 }
input:
  generate:
    count: 5
    interval: 200ms
    mapping: 'root = ["A","B","C","A","D"].index(counter() - 1)'
pipeline:
  processors: [ { dedupe: { cache: c, key: '\${! content() }' } } ]
output: { stdout: {} }
YAML
# shellcheck disable=SC2086
lru_sf=$(timeout 60 "$SF" --config "$WORK/lru_recency.yaml" $SEA_ARGS 2>/dev/null | sort | tr '\n' '|')
if [ "$lru_sf" != 'A|B|C|D|' ]; then
  bad "an lru hit does not renew recency" "expected 'A|B|C|D|' got '$lru_sf'"
elif [ -x "$RC" ]; then
  lru_rc=$(timeout 60 "$RC" run --log.level off "$WORK/lru_recency.yaml" 2>/dev/null | sort | tr '\n' '|')
  [ "$lru_rc" = "$lru_sf" ] && note "an lru hit does not renew recency" \
                            || bad "an lru hit does not renew recency" \
                                   "reference '$lru_rc' swordfish '$lru_sf'"
else
  note "an lru hit does not renew recency"
fi

# ---- refusals -----------------------------------------------------------------------
refuses() {  # <name> <cache body> <expected substring>
  local out
  cat > "$WORK/r.yaml" <<YAML
input: { generate: { count: 1, interval: 0s, mapping: 'root = 1' } }
pipeline: { processors: [ { dedupe: { cache: c, key: 'x' } } ] }
output: { stdout: {} }
cache_resources:
  - label: c
$2
YAML
  out=$(timeout 30 "$LINT" lint "$WORK/r.yaml" 2>&1)
  case "$out" in
    *"$3"*) note "$1" ;;
    *)      bad "$1" "expected '$3', got '$out'" ;;
  esac
}
refuses "an unimplemented cache kind is named as unimplemented" \
        '    redis: { url: x }' "cache 'redis' is not implemented by swordfish"
refuses "a kind that is not a cache at all is named as a typo" \
        '    bogus: {}' "has no field 'bogus'"
refuses "a non-standard lru algorithm is refused, not approximated" \
        '    lru: { algorithm: arc }' "is not implemented by swordfish"
refuses "an lru cap of zero is refused" \
        '    lru: { cap: 0 }' "must be greater than zero"
refuses "a two_queues ratio without two_queues is refused" \
        '    lru: { two_queues_ghost_ratio: 0.5 }' "only applies to"
refuses "a file cache without a directory is refused" \
        '    file: {}' "requires a \`directory\`"
refuses "a typo inside the memory cache is named" \
        '    memory: { nonsense: 1 }' "has no field 'nonsense'"
refuses "init_values given as a list is refused" \
        '    memory: { init_values: [ a, b ] }' "table of key/value pairs"

# ---- the compiled path ----------------------------------------------------------------
if [ -x build/swordfish-build ]; then
  cat > "$WORK/comp.yaml" <<'YAML'
input:
  generate: { count: 6, interval: 0s, mapping: 'root.k = counter() % 3' }
pipeline:
  processors:
    - dedupe: { cache: c, key: '${! json("k") }' }
output: { stdout: {} }
cache_resources:
  - label: c
    lru: { cap: 1, init_values: { "9": x } }
YAML
  # shellcheck disable=SC2086
  interp=$(timeout 60 "$SF" --config "$WORK/comp.yaml" $SEA_ARGS 2>/dev/null | sort | tr '\n' '|')
  if timeout 900 build/swordfish-build "$WORK/comp.yaml" -o "$WORK/comp.bin" \
        >"$WORK/build.log" 2>&1; then
    # shellcheck disable=SC2086
    compiled=$(timeout 60 "$WORK/comp.bin" $SEA_ARGS 2>/dev/null | sort | tr '\n' '|')
    [ -n "$interp" ] && [ "$interp" = "$compiled" ] \
      && note "a compiled binary caches identically, cap and init_values included" \
      || bad "a compiled binary caches identically, cap and init_values included" \
             "interpreted '$interp' compiled '$compiled'"
  else
    bad "a compiled binary caches identically" \
        "swordfish build failed: $(tail -3 "$WORK/build.log" | tr '\n' ' ')"
  fi
fi

# ---- a key the cache cannot store -----------------------------------------------
#
# A `file` cache turns the key straight into a file name, so a key with an
# embedded NUL is one it genuinely cannot store -- and the path reached open() as
# a C string, so `a\0X` and `a\0Y` both named the file `a`: the second got EEXIST
# and was reported as a DUPLICATE, silently dropping a distinct message. The
# reference refuses it too ("invalid argument"), and `drop_on_err` -- which
# defaults to TRUE there and defaulted to false here -- decides what happens to
# the message afterwards.
printf 'a\0X\na\0Y\n' > "$WORK/nul.txt"
nul_case() {  # <name> <dedupe fields prefix>
  local name=$1 extra=$2 sf ref
  cat > "$WORK/nul.yaml" <<YAML
cache_resources:
  - label: c
    file: { directory: $WORK/nulcache }
input:
  file: { paths: [ $WORK/nul.txt ], scanner: { lines: {} } }
pipeline:
  processors: [ { dedupe: { ${extra}cache: c, key: '\${! content() }' } } ]
output: { stdout: {} }
YAML
  rm -rf "$WORK/nulcache"; mkdir -p "$WORK/nulcache"
  # SORTED: the reference pipelines messages concurrently, so its ORDER is not an
  # oracle -- comparing unsorted made this fail on `aX|aY` against `aY|aX`.
  # shellcheck disable=SC2086
  sf=$(timeout 60 "$SF" --config "$WORK/nul.yaml" $SEA_ARGS 2>/dev/null | sort | tr '\n' '|')
  if [ ! -x "$RC" ]; then note "$name (no reference to compare against)"; return; fi
  rm -rf "$WORK/nulcache"; mkdir -p "$WORK/nulcache"
  ref=$(timeout 60 "$RC" run --log.level off "$WORK/nul.yaml" 2>/dev/null | sort | tr '\n' '|')
  [ "$sf" = "$ref" ] && note "$name" \
                     || bad "$name" "swordfish '$sf' reference '$ref'"
}
nul_case "a NUL in a file-cache key drops the message, as it does there" ""
nul_case "drop_on_err false passes it on instead" "drop_on_err: false, "
nul_case "drop_on_err true is the default" "drop_on_err: true, "

# ---- default_ttl: 0 and shards, both against the reference --------------------
#
# Two claims that were wrong together. `default_ttl: 0` was read as "never
# expires" and stored time_point::max(); the reference computes
# `expires = time.Now().Add(defaultTTL)` UNCONDITIONALLY, so zero puts the expiry
# in the past and the next compaction sweeps it -- a never-expiring entry there
# is a zero `expires`, reserved for init_values. And `memory.shards` was accepted
# and inert on the reasoning that it only relieves goroutine lock contention;
# each shard in fact carries its own last-compaction time and a write sweeps only
# its own shard, so the field decides WHEN entries expire.
#
# The messages are 400ms apart against a 100ms compaction interval, because the
# question is whether a sweep happens BETWEEN the duplicate and its original --
# three messages arriving in the same millisecond cannot tell any of these
# configurations apart, and an earlier draft of this gate could not.
#
# The key pairs are not decoration: which shard a key lands in decides the
# outcome, so these also check that the selector is xxhash64 modulo the count,
# the same one `xxhash.ChecksumString64` gives.
dedupe_case() {  # <name> <cache-body> <key-a> <key-b> <expected>
  local name=$1 body=$2 k1=$3 k2=$4 want=$5 got ref
  cat > "$WORK/dd.yaml" <<YAML
input:
  generate:
    count: 3
    interval: 400ms
    mapping: |
      let n = counter()
      root.k = if \$n == 2 { "$k2" } else { "$k1" }
pipeline:
  processors: [ { dedupe: { cache: c, key: '\${! json("k") }' } } ]
output: { stdout: {} }
cache_resources:
  - label: c
$body
YAML
  got=$(timeout 30 "$SF" --config "$WORK/dd.yaml" --smp 1 --memory 512M \
          --overprovisioned 2>/dev/null \
        | sed -n 's/.*"k":"\([^"]*\)".*/\1/p' | tr '\n' ' ')
  got=${got% }
  if [ "$got" != "$want" ]; then
    echo "FAIL $name: expected '$want', got '$got'" >&2
    fail=1
    return
  fi
  if [ -x "$RC" ]; then
    ref=$(timeout 30 "$RC" run --log.level off "$WORK/dd.yaml" 2>/dev/null \
          | sed -n 's/.*"k":"\([^"]*\)".*/\1/p' | tr '\n' ' ')
    ref=${ref% }
    if [ "$ref" != "$want" ]; then
      echo "FAIL $name: the reference gave '$ref', not '$want', so the expectation" \
           "here is wrong and needs revisiting" >&2
      fail=1
      return
    fi
  fi
  note "$name"
}
dedupe_case "default_ttl 0 expires immediately" \
    "    memory: { default_ttl: 0s, compaction_interval: 100ms }" A B "A B A"
dedupe_case "and a real ttl still dedupes" \
    "    memory: { default_ttl: 5m, compaction_interval: 100ms }" A B "A B"
dedupe_case "shards 1 sweeps the one shard" \
    "    memory: { shards: 1, default_ttl: 0s, compaction_interval: 100ms }" A B "A B A"
dedupe_case "shards 2 puts A and B in different shards" \
    "    memory: { shards: 2, default_ttl: 0s, compaction_interval: 100ms }" A B "A B"
dedupe_case "shards 8 likewise" \
    "    memory: { shards: 8, default_ttl: 0s, compaction_interval: 100ms }" A B "A B"
dedupe_case "shards 2, P and Q collide" \
    "    memory: { shards: 2, default_ttl: 0s, compaction_interval: 100ms }" P Q "P Q P"
dedupe_case "shards 2, X and Y do not" \
    "    memory: { shards: 2, default_ttl: 0s, compaction_interval: 100ms }" X Y "X Y"
dedupe_case "shards 2, M and N collide" \
    "    memory: { shards: 2, default_ttl: 0s, compaction_interval: 100ms }" M N "M N M"

exit $fail
