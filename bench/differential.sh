#!/usr/bin/env bash
# The differential gate in miniature: the interpreter and the compiled
# binary must produce byte-identical output for the same input.
set -euo pipefail
SF_LIBS="$(./build/swordfish-config --libs)"

# Bounds and cleanup, matching bench/vs_reference.sh. Nothing here runs
# unbounded: a hung compile or a hung pipeline is indistinguishable from a slow
# one until something puts a limit on it.
STEP_TIMEOUT=${STEP_TIMEOUT:-600}
exec 3>&2
kill_tree() {
  local p=$1 c
  # Children before parent: killing a `timeout` wrapper alone re-parents the
  # process it was bounding, which then keeps running unsupervised.
  for c in $(pgrep -P "$p" 2>/dev/null); do kill_tree "$c"; done
  kill -9 "$p" 2>/dev/null
}
bench_cleanup() {
  local rc=$?
  trap - EXIT INT TERM
  local c
  for c in $(pgrep -P $$ 2>/dev/null); do kill_tree "$c"; done
  true
  exit $rc
}
trap bench_cleanup EXIT INT TERM
bounded() {  # bounded <what> <command...>
  local what=$1; shift
  timeout -k 10 "$STEP_TIMEOUT" "$@"
  local rc=$?
  if [ "$rc" -eq 124 ] || [ "$rc" -eq 137 ]; then
    echo "HUNG: $what exceeded ${STEP_TIMEOUT}s and was killed" >&3
    return 99
  fi
  return $rc
}

cd "$(dirname "$0")/.."

MAPPINGS=(
  'root = this.n * this.n + this.n / 2 - 1'
  'root = this
root.sq = this.n * this.n'
  'root.tag = "n=%d".format(this.n)'
  'root = true || false && false'
  'root = this.missing | 99'
  'root = if this.n > 3 { "big" } else { "small" }'
  'root.a.b.c = this.n'
  'root = this.n % 7'
  'root = this.n % -1'
  'root.c = counter()'
  'root.c = counter(min: 5)'
  'root.c = counter(min: 5, max: 7)'
  'root.c = counter(set: null)'
  'root.x = """[0-9]+""".length()'
  'root.x = [this.n, this.n].map_each(v -> v * 2)'
  'root.x = [this.n, 5].filter(v -> v > 2)'
  'root.x = [[this.n],[2]].map_each(a -> a.map_each(b -> b * 10))'
  'root.x = this.n.string().re_replace_all(pattern: "[0-9]", value: "#")'
  'root = -this.n'
  'root = if this.n > 1000000 { 1 }'
  'root = this.n * this.n + this.n / 2 - 1'
  'root = (this.n + 1) * (this.n - 1)'
  'root = -this.n * 3'
  'root.deep.path = this.n * 2'
  'root.has = this.exists("n")'
  'root.missing = this.exists("nope.deep")'
  'root = "a" + "b"'
  'root = this.n.string().re_replace_all("[0-9]", "#")'
  'root = this.n.string().re_match("^[0-9]+$")'
  'root = this.n.string().re_find_all("[0-9]")'
  'root = [this.n, this.n * 2, "x"]'
  'root = {"k": this.n, "j": this.n.string()}'
)
# Integer inputs take the specialised branch; float and string inputs take the
# generic fallback. Both sides of every type guard must be covered.
INPUTS=('{"n":0}' '{"n":1}' '{"n":7}' '{"n":-3}' '{"n":1000000}'
        '{"n":7.5}' '{"n":-0.25}' '{"n":9223372036854775807}'
        '{"n":-9223372036854775808}')

pass=0; fail=0
for m in "${MAPPINGS[@]}"; do
  bounded "emit" ./build/sfslice emit "$m" > build/diff_gen.cc
  bounded "compile" g++ -std=c++23 -O2 -Iinclude -c build/diff_gen.cc -o build/diff_gen.o
  bounded "link" g++ -std=c++23 -O2 -Iinclude bench/diff_main.cc build/diff_gen.o $SF_LIBS -o build/diff_compiled
  for doc in "${INPUTS[@]}"; do
    a=$(bounded "interp run" ./build/sfslice run "$m" "$doc" 2>&1 || true)
    b=$(bounded "compiled run" ./build/diff_compiled "$doc" 2>&1 || true)
    if [[ "$a" == "$b" ]]; then pass=$((pass+1)); else
      fail=$((fail+1))
      printf 'MISMATCH\n  mapping: %s\n  input:   %s\n  interp:  %s\n  compiled:%s\n' "$m" "$doc" "$a" "$b"
    fi
  done
done
echo "differential: $pass identical, $fail mismatched"
[[ $fail -eq 0 ]]
