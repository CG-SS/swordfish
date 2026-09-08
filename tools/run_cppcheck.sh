#!/usr/bin/env bash
# Static analysis with cppcheck.
#
# Suppressions live in .cppcheck-suppressions and each carries a reason, so a
# reviewer can tell a considered exemption from an unexamined one. The two that
# matter: cppcheck 2.21 does not model C++20 coroutines (every co_await function
# looks like it falls off the end), and it cannot follow the pointer-to-member
# references in spec_of tables (every declaratively-used field looks unused).
set -euo pipefail
cd "$(dirname "$0")/.."

# cppcheck is built from source into ../cppcheck-bin rather than installed, so
# looking only on PATH reports it missing on a machine that has it.
CPPCHECK=${CPPCHECK:-}
if [ -z "$CPPCHECK" ]; then
  for c in cppcheck ../cppcheck-bin/bin/cppcheck; do
    if command -v "$c" >/dev/null 2>&1; then CPPCHECK=$c; break; fi
  done
fi
[ -n "$CPPCHECK" ] && command -v "$CPPCHECK" >/dev/null || {
  echo "cppcheck not found on PATH or in ../cppcheck-bin/bin;" >&2
  echo "set CPPCHECK=/path/to/cppcheck" >&2; exit 1; }

# A build dir is REQUIRED alongside -j, not an optimisation: without one
# cppcheck silently disables the unusedFunction check and says so in a line
# nobody reads, so the run reports success having skipped a check entirely.
BUILD_DIR=${CPPCHECK_BUILD_DIR:-build/cppcheck}
mkdir -p "$BUILD_DIR"

"$CPPCHECK" --enable=all --std=c++23 --language=c++ --inline-suppr \
  --suppressions-list=.cppcheck-suppressions \
  --cppcheck-build-dir="$BUILD_DIR" \
  -I include -I src/kafka/generated \
  -I third_party/simdjson/singleheader -I third_party/re2 \
  --suppress=missingIncludeSystem \
  --suppress=checkersReport \
  --suppress=normalCheckLevelMaxBranches \
  --error-exitcode=1 \
  -j "$(nproc)" src/ include/
echo "cppcheck: clean"
