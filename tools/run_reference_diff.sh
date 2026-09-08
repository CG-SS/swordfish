#!/usr/bin/env bash
# The L4 gate: Swordfish against the REAL Redpanda Connect.
#
#   tools/run_reference_diff.sh [path-to-redpanda-connect] [path-to-sfslice]
#
# The other three gates compare Swordfish to itself or to text copied out of the
# reference's documentation. This one runs the same mappings through the actual
# binary and diffs the bytes, which is the only check that catches a place where
# the documentation and the implementation disagree -- and there is at least one
# (`.length()` is documented as a character count and implemented as a byte
# count).
#
# `redpanda-connect blobl` and `sfslice blobl` share an output contract, so a
# whole batch of documents can be compared in one launch each. See src/cli/main.cc.
set -uo pipefail
cd "$(dirname "$0")/.."

RC=${1:-../redpanda-connect}
SF=${2:-build/sfslice}
[ -x "$RC" ] || { echo "no redpanda-connect binary at $RC" >&2; exit 2; }
[ -x "$SF" ] || { echo "build sfslice first ($SF)" >&2; exit 2; }

# Run against a COPY of sfslice and libswordfish.so, not the build tree. A
# rebuild during a run truncated the library twice, and the second time the run
# still finished and reported "404 agree, 1 differ" -- a confident summary over
# corrupt data, which is worse than a crash. The reference binary is not
# snapshotted: nothing here rebuilds it.
source tools/snapshot_bin.sh
trap snapshot_cleanup EXIT INT TERM
# Not $( ): the function exports LD_LIBRARY_PATH, which a subshell would drop.
snapshot_bin "$SF" || exit 2
SF_SNAP=$SNAP_BIN
snapshot_check "$SF_SNAP" || exit 2

python3 tools/reference_diff.py "$RC" "$SF_SNAP" \
    tests/fixtures/bloblang.yaml tests/fixtures/differential.yaml
rc=$?

# The snapshot is re-checked AFTERWARDS as well. If it became unrunnable during
# the comparison, every "difference" from that point on is an artefact, and
# reporting the tally would repeat exactly the mistake this guard exists for.
if ! snapshot_check "$SF_SNAP"; then
    echo "the snapshot stopped working mid-run; the result above is not trustworthy" >&2
    exit 2
fi
exit $rc
