#!/usr/bin/env bash
# Applies this directory's patches for one dependency to that dependency's tree,
# idempotently.
#
#   ./patches/apply.sh ../seastar-master
#   ./patches/apply.sh third_party/avro-cpp
#
# Patches live in a subdirectory named for the dependency -- patches/seastar/,
# patches/avro-cpp/ -- and only that subdirectory's patches are applied. Before
# there was a second dependency they sat loose in patches/ and every one was
# applied to whatever tree was named, which would now try to patch Seastar with
# an Avro change and stop on a conflict that means nothing.
#
# Idempotent because it is going to be run by someone who does not remember
# whether they ran it: each patch is tested with `--dry-run -R` first, and one
# that reverses cleanly is already applied and is skipped. A patch that neither
# applies nor reverses is a real conflict -- the tree has moved under it -- and
# stops the script rather than being forced.
set -uo pipefail

here=$(cd "$(dirname "$0")" && pwd)
target=${1:-}
[ -n "$target" ] || { echo "usage: $0 <dependency-source-dir> [dependency-name]" >&2; exit 2; }
[ -d "$target" ] || { echo "$0: no such directory: $target" >&2; exit 2; }

# The dependency's name picks the patch set. Inferred from the directory's own
# name, with a checkout suffix stripped, so the documented invocations above
# need no second argument: `../seastar-master` is seastar.
dep=${2:-}
if [ -z "$dep" ]; then
    dep=$(basename "$(cd "$target" && pwd)")
    dep=${dep%-master}
    dep=${dep%-main}
fi

dir=$here/$dep
if [ ! -d "$dir" ]; then
    echo "$0: no patches for '$dep'. This directory has:" >&2
    for d in "$here"/*/; do [ -d "$d" ] && echo "  $(basename "$d")" >&2; done
    echo "Pass the name as a second argument if the directory name differs." >&2
    exit 2
fi

shopt -s nullglob
patches=("$dir"/*.patch)
[ ${#patches[@]} -gt 0 ] || { echo "no patches for $dep"; exit 0; }

applied=0 skipped=0
for p in "${patches[@]}"; do
    name=$(basename "$p")
    if patch -p1 -d "$target" --dry-run -R <"$p" >/dev/null 2>&1; then
        echo "already applied: $dep/$name"
        skipped=$((skipped + 1))
        continue
    fi
    if ! patch -p1 -d "$target" --dry-run <"$p" >/dev/null 2>&1; then
        echo "CONFLICT: $dep/$name does not apply to $target and is not already applied." >&2
        echo "  The tree has moved under it. Read patches/README.md -- the question" >&2
        echo "  is whether this project still needs the patch, not how to force it." >&2
        exit 1
    fi
    patch -p1 -d "$target" <"$p" >/dev/null || exit 1
    echo "applied: $dep/$name"
    applied=$((applied + 1))
done

echo "$applied applied, $skipped already present."
if [ "$applied" -gt 0 ]; then
    echo "Rebuild the dependency now (Seastar: cd $target/build && ninja;"
    echo "avro-cpp is built by this project's own CMake and needs nothing)."
fi
