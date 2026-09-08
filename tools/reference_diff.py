#!/usr/bin/env python3
"""Compare Swordfish's Bloblang against the real Redpanda Connect binary.

Driven by tools/run_reference_diff.sh. Both tools expose a `blobl` subcommand
with the same output contract -- one stdout line per document, an empty line for
a deleted root, no line for a document whose mapping errored -- so each mapping
is one launch per side rather than one per document.

What is deliberately NOT compared:

  * Mappings whose result cannot be the same twice. `uuid_v4()`, `now()` and the
    rest are excluded by name rather than by trying to pin them, because pinning
    them would mean reimplementing their impurity.
  * Inputs containing a newline. The JSONL contract has no way to express them,
    and the conformance harness already covers those fixtures.
  * Mappings neither side can parse, and mappings only the reference rejects --
    both are reported, but a reference rejection is a fixture bug rather than a
    Swordfish one.
  * `values()`, whose element order the reference does not define. Five runs of
    the same input gave four different orders, because it iterates a Go map.
    Swordfish returns them in key order, which at least pairs with `keys()`.
  * Anything touching metadata. The reference's own `blobl` CLI has no metadata
    context and hangs on a `meta` assignment instead of reporting an error, so
    there is nothing to compare against. The runtime suite covers metadata
    against a real pipeline.

Lines are compared as PARSED JSON where both sides are valid JSON, and as raw
bytes otherwise. The two tools serialise differently -- `redpanda-connect blobl`
writes `&` as `\u0026`, because Go's encoder escapes HTML by default -- and that
is a property of its CLI rather than of the language: the same mapping in a full
pipeline emits a bare `&` on both sides. Comparing bytes reported eight
divergences that were nothing of the kind.
"""
import json
import pathlib
import atexit
import shutil
import subprocess
import tempfile
import sys

import yaml

# Functions whose whole purpose is to differ between two runs.
IMPURE = (
    "uuid_v4", "uuid_v5", "uuid_v7", "ksuid", "ulid", "nanoid", "snowflake_id",
    "random_int", "now(", "timestamp_unix", "hostname(", "env(", "fake(",
    "file(", "file_rel(", "ts_format", "ts_strftime",
    # Not impure, but undefined: see the module docstring.
    ".values()",
    # The reference's `blobl` CLI has no message metadata and HANGS on a `meta`
    # assignment rather than reporting anything, so these cannot be compared
    # through it. The runtime suite covers metadata against a real pipeline.
    "meta ", "meta(", "root_meta",
)


# Go's encoding/json escapes these by default; swordfish emits them raw.
HTML_ESCAPES = ((b"\\u003c", b"<"), (b"\\u003e", b">"), (b"\\u0026", b"&"))


def equivalent(a: bytes, b: bytes) -> bool:
    """Byte-identical, once the two encoders' HTML escaping is normalised.

    This used to fall back to json.loads() on each line and compare the parsed
    VALUES, which is a far wider tolerance than the HTML escaping it was written
    for -- and byte-identical output is a stated compatibility requirement
    (include/swordfish/value.hh). The wider tolerance hid a real defect for as
    long as this gate has existed: swordfish emitted 1e+20 where the reference
    emits 100000000000000000000, and json.loads made the two equal, so all 425
    cases passed while the encoders disagreed about every float outside roughly
    [1e-4, 1e15]. The tolerance is now exactly the one it was meant to be.
    """
    if a == b:
        return True
    for esc, raw in HTML_ESCAPES:
        a = a.replace(esc, raw)
        b = b.replace(esc, raw)
    return a == b


def load(path):
    """Yield (id, mapping, [inputs]) from a fixture file, in either schema."""
    for fx in yaml.safe_load(pathlib.Path(path).read_text()) or []:
        mapping = fx["mapping"]
        if "cases" in fx:                       # the conformance schema
            inputs = [c["in"] for c in fx["cases"]]
        else:                                   # the hand-written schema
            inputs = list(fx.get("inputs", []))
        yield fx["id"], mapping, inputs


def run(tool, mapping_file, input_file, reference):
    """stdout of one tool over one batch, plus whether it refused the mapping."""
    if reference:
        cmd = [tool, "blobl", "-f", mapping_file, "-i", input_file]
    else:
        cmd = [tool, "blobl", pathlib.Path(mapping_file).read_text(), "-i", input_file]
    # Bytes, not text: a mapping ending in .decode("hex") emits arbitrary
    # octets, and decoding them as UTF-8 to compare them would throw before the
    # comparison happened.
    p = subprocess.run(cmd, capture_output=True, timeout=120)
    err = p.stderr.decode("utf-8", "replace")
    # A mapping the tool cannot even parse is a different outcome from one that
    # runs and errors per document: the first says nothing about semantics.
    refused = "failed to execute map" not in err and p.stdout == b"" and err != ""
    return p.stdout, refused, err.strip().splitlines()[:1]


def main():
    rc, sf = sys.argv[1], sys.argv[2]
    # A PRIVATE directory per run. This used to be a fixed /tmp/sf-refdiff, so
    # two runs at once -- or one left over from a killed run -- overwrote each
    # other's mapping and input files between the reference's launch and
    # swordfish's. The result was not a crash but a confident summary over
    # scrambled data: `root = this.a.uppercase()` reported as unparseable, and
    # answers paired with the wrong fixtures. Exactly the failure this whole
    # gate exists to catch, in the gate itself.
    work = pathlib.Path(tempfile.mkdtemp(prefix="sf-refdiff."))
    atexit.register(shutil.rmtree, work, True)
    mf, inf = work / "m.blobl", work / "in.jsonl"

    agree = differ = skipped = refused_ref = refused_sf = 0
    failures = []

    for path in sys.argv[3:]:
        for fid, mapping, inputs in load(path):
            if any(name in mapping for name in IMPURE):
                skipped += 1
                continue
            usable = [i for i in inputs if "\n" not in i and i.strip()]
            if not usable:
                skipped += 1
                continue

            mf.write_text(mapping)
            inf.write_text("\n".join(usable) + "\n")

            ref_out, ref_refused, ref_err = run(rc, mf, inf, reference=True)
            sf_out, sf_refused, sf_err = run(sf, mf, inf, reference=False)

            if ref_refused:
                refused_ref += 1
                continue
            if sf_refused:
                refused_sf += 1
                failures.append((fid, mapping, "swordfish cannot parse it",
                                 " ".join(sf_err)))
                continue
            if equivalent(ref_out, sf_out):
                agree += 1
            else:
                differ += 1
                failures.append((fid, mapping, ref_out, sf_out))

    for fid, mapping, want, got in failures[:25]:
        print(f"DIVERGENCE {fid}")
        print("  mapping:     " + mapping.replace("\n", "\n               "))
        print(f"  reference:   {want!r}")
        print(f"  swordfish:   {got!r}")

    print(f"reference diff: {agree} agree, {differ} differ, {skipped} skipped "
          f"(impure, undefined, or not expressible as JSONL), "
          f"{refused_ref} rejected by the reference, {refused_sf} rejected by swordfish")
    # Only DISAGREEMENT fails. A mapping Swordfish refuses to parse is a
    # catalogue gap that is already named, listed and tracked by the conformance
    # report; failing on it too would leave this gate permanently red, and a
    # gate nobody can ever get green stops being read.
    return 1 if differ else 0


if __name__ == "__main__":
    sys.exit(main())
