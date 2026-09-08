#!/usr/bin/env bash
# Generate, compile and run the interpreter-vs-compiled differential.
#
#   tools/run_differential.sh <path-to-sfdiffgen> <build-dir>
#
# Run from the source root. Exits non-zero on the first divergence, printing
# the mapping, the input and both answers.
set -euo pipefail

gen=${1:-build/sfdiffgen}
builddir=${2:-build}
src=$builddir/differential_generated.cc
bin=$builddir/sfdifftest

"$gen" "$src" tests/fixtures/bloblang.yaml \
    --strict tests/fixtures/differential.yaml

# Same standard and include path the library was built with; the generated code
# is ordinary C++ against the public runtime header, so nothing else is needed.
${CXX:-c++} -std=c++23 -O1 -I include -I "$builddir" \
    "$src" -o "$bin" -L "$builddir" -lswordfish -Wl,-rpath,"$(cd "$builddir" && pwd)" \
    $(pkg-config --libs re2 2>/dev/null || echo -lre2) -lsimdjson -lyaml-cpp

"$bin"
