#!/usr/bin/env bash
# Copy a build output, and the first-party shared libraries it loads, somewhere
# a concurrent rebuild cannot touch.
#
#   source tools/snapshot_bin.sh
#   SF=$(snapshot_bin build/sfslice)      # then run "$SF"
#
# WHY THIS EXISTS
#
# A harness that runs `build/sfslice` is reading a file the build system may
# replace underneath it. Rebuilding during an L4 run truncated
# libswordfish.so mid-comparison, and the run did not fail -- it reported
# "404 agree, 1 differ" and read as a genuine regression. Only the per-case
# detail gave it away: `libswordfish.so: file too short`, and a "reference"
# output belonging to an entirely different mapping.
#
# That is the dangerous shape: a confident summary line over corrupt data. The
# snapshot removes the race rather than asking everyone to remember not to
# rebuild.

# Sets SNAP_BIN to the snapshotted executable, and LD_LIBRARY_PATH so the copy
# resolves its libraries from the snapshot rather than back to build/.
#
# It does NOT echo the path, and callers must not wrap it in $( ). Command
# substitution runs the function in a subshell, where the exports die with it --
# the first version did exactly that, so the snapshot was taken and then
# ignored, and the binary went on loading the library it was meant to be
# protected from. The symptom was identical to having no snapshot at all.
snapshot_bin() {
    local src=$1
    [ -x "$src" ] || { echo "snapshot_bin: no executable at $src" >&2; return 2; }

    # One directory per process, cleaned up by the caller's trap. Deliberately
    # NOT reused between runs: a stale snapshot is the same class of problem.
    if [ -z "${SF_SNAPSHOT_DIR:-}" ]; then
        SF_SNAPSHOT_DIR=$(mktemp -d /tmp/sfsnap.XXXXXX) || return 1
        export SF_SNAPSHOT_DIR
    fi

    cp -- "$src" "$SF_SNAPSHOT_DIR/" || return 1

    # First-party libraries are the ones NOT under a system prefix. Deriving the
    # project root from $0 does not survive being sourced -- $0 is then the
    # caller, or `bash` -- and the first version silently matched nothing,
    # copying no libraries at all while reporting success. Excluding the system
    # prefixes needs no knowledge of where the project lives.
    local lib
    while read -r lib; do
        [ -n "$lib" ] && cp -- "$lib" "$SF_SNAPSHOT_DIR/"
    done < <(ldd "$src" 2>/dev/null | awk '$3 ~ /^\// &&
                 $3 !~ /^\/(usr|lib|lib64|opt\/rh)\// {print $3}')

    export LD_LIBRARY_PATH="$SF_SNAPSHOT_DIR${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
    SNAP_BIN="$SF_SNAPSHOT_DIR/$(basename "$src")"

    # The copy must actually resolve to the SNAPSHOT. The binary carries a
    # RUNPATH pointing at build/, and while LD_LIBRARY_PATH takes precedence
    # over RUNPATH, that only helps if the library was really copied -- so this
    # asserts the outcome rather than trusting the mechanism.
    local resolved
    resolved=$(ldd "$SNAP_BIN" 2>/dev/null | awk -v d="$SF_SNAPSHOT_DIR" '$3 ~ "^"d"/" {n++} END {print n+0}')
    local wanted
    wanted=$(ldd "$src" 2>/dev/null | awk '$3 ~ /^\// &&
                 $3 !~ /^\/(usr|lib|lib64|opt\/rh)\// {n++} END {print n+0}')
    # A count of zero is itself suspicious for this project: sfslice and every
    # Seastar binary here link libswordfish.so, so "nothing to snapshot" means
    # the detection broke, which is how the first version passed while
    # protecting nothing.
    if [ "$wanted" -eq 0 ]; then
        echo "snapshot_bin: found no first-party libraries for $src -- detection is broken" >&2
        return 1
    fi
    if [ "$resolved" -ne "$wanted" ]; then
        echo "snapshot_bin: $resolved of $wanted first-party libraries resolve to the snapshot;" >&2
        echo "  the copy would still load from the build tree. Did you call this inside \$( )?" >&2
        return 1
    fi
    return 0
}

# Verifies the snapshot actually runs. A copy taken DURING a link can be
# truncated exactly like the original, so the snapshot has to be checked rather
# than assumed -- otherwise this just moves the corruption somewhere quieter.
snapshot_check() {
    local bin=$1 out rc
    out=$("$bin" --help 2>&1); rc=$?
    # --help may legitimately exit non-zero; a loader failure is the thing to
    # catch, and it names itself.
    case "$out" in
        *"error while loading shared libraries"*|*"file too short"*|*"cannot open shared object"*)
            echo "snapshot of $bin is not runnable: $out" >&2; return 1 ;;
    esac
    return 0
}

snapshot_cleanup() { [ -n "${SF_SNAPSHOT_DIR:-}" ] && rm -rf "$SF_SNAPSHOT_DIR"; }
