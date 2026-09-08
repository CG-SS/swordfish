#!/usr/bin/env python3
"""Extract Bloblang conformance fixtures from the reference implementation.

Two sources, deliberately:

  * ``connect-main/docs/.../{functions,methods}.adoc`` — generated documentation
    carrying runnable examples as ``# In:`` / ``# Out:`` pairs.
  * ``benthos-main/internal/bloblang/query/*.go`` — the ``NewExampleSpec`` calls
    the docs are generated FROM, which carry file/line provenance.

Where the two disagree the Go source wins: the docs have at least one known
inaccuracy (``.length()`` is documented as a character count but implemented as
``len()``, i.e. bytes), so a doc-only expectation is a corpus bug rather than a
Swordfish bug.

Output is a single YAML file of fixtures, deliberately language-neutral so the
same corpus can drive the interpreter, the compiled path, and eventually a
differential run against `rpk connect`.
"""
import json
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]
ADOC = ROOT / "connect-main/docs/modules/guides/pages/bloblang"
GOSRC = ROOT / "benthos-main/internal/bloblang/query"


def yaml_quote(s: str) -> str:
    """Single-quoted YAML scalar; the only escape is a doubled quote."""
    return "'" + s.replace("'", "''") + "'"


def yaml_block(s: str, indent: str) -> str:
    """A literal block scalar.

    Mappings are multi-line and may contain anything, including lines that begin
    with `---` or `...`, which a quoted scalar cannot carry. A block scalar has
    no escaping rules at all, so it is the only safe container.

    The chomping indicator matters: ``|-`` strips every trailing newline, which
    silently truncated the expected output of ``format_yaml``, whose result
    really does end in one. ``|`` keeps exactly one and ``|+`` keeps them all.
    """
    trailing = len(s) - len(s.rstrip("\n"))
    indicator = "|-" if trailing == 0 else ("|" if trailing == 1 else "|+")
    body = "\n".join(indent + line if line else "" for line in s.rstrip("\n").split("\n"))
    if trailing > 1:
        body += "\n" * (trailing - 1)
    return indicator + "\n" + body


def extract_adoc(path: pathlib.Path):
    """Yield fixtures from one .adoc file."""
    lines = path.read_text().splitlines()
    section = "?"
    i = 0
    seen = 0
    while i < len(lines):
        line = lines[i]
        m = re.match(r"^=== `([^`]+)`", line)
        if m:
            section, seen = m.group(1), 0
        if line.strip() == "```coffeescript":
            j = i + 1
            block = []
            while j < len(lines) and lines[j].strip() != "```":
                block.append(lines[j])
                j += 1
            fixture = parse_block(block)
            if fixture:
                mapping, cases = fixture
                yield {
                    "id": f"{path.stem}.{section}.{seen}",
                    "source": f"{path.name}:{i + 1}",
                    "mapping": mapping,
                    "cases": cases,
                }
                seen += 1
            i = j
        i += 1


# A multi-line expected output is rendered by the doc generator as a first
# line after "# Out:" followed by lines carrying a SECOND hash:
#
#     # Out: {
#     #      #          "foo": "bar"
#     #      #      }
#
# Everything up to and including that second hash is decoration; what remains
# is the real output indented by a constant amount, which dedenting removes.
# Losing these lines truncated every format_json fixture to "{".
CONT = re.compile(r"^#\s*#(.*)$")


def dedent_continuations(lines):
    """Strip the common leading indent from continuation lines."""
    bodies = [l for l in lines if l.strip()]
    if not bodies:
        return lines
    pad = min(len(l) - len(l.lstrip(" ")) for l in bodies)
    return [l[pad:] if l.strip() else "" for l in lines]


def parse_block(block):
    """Split a coffeescript block into its mapping and its In/Out pairs."""
    mapping, cases = [], []
    pending_in = None
    cont = None                            # continuation lines of the open Out
    def flush():
        nonlocal cont
        if cont is not None and cases:
            cases[-1]["out"] = "\n".join([cases[-1]["out"]] + dedent_continuations(cont))
        cont = None
    for raw in block:
        m_in = re.match(r"^#\s*In:\s*(.*)$", raw.strip())
        m_out = re.match(r"^#\s*Out:\s*(.*)$", raw.strip())
        m_cont = CONT.match(raw.strip()) if cont is not None else None
        if m_cont:
            cont.append(m_cont.group(1))
            continue
        flush()
        if m_in:
            pending_in = m_in.group(1).strip()
        elif m_out:
            # An Out with no preceding In applies to the previous input, which
            # is how the docs express a second message through one mapping.
            cases.append({"in": pending_in if pending_in is not None else "{}",
                          "out": m_out.group(1).strip()})
            pending_in = None
            cont = []
        elif raw.strip().startswith("# "):
            continue                       # prose comment inside the example
        else:
            mapping.append(raw)
    flush()
    text = "\n".join(mapping).strip()
    # An example with no data pair cannot be asserted on; many are impure
    # (uuid_v4, now) and have no fixed expected output.
    if not text or not cases:
        return None
    return text, cases


GO_SPEC = re.compile(
    r'NewExampleSpec\(\s*(?:"(?:[^"\\]|\\.)*"|`[^`]*`)\s*,\s*((?:`[^`]*`\s*,?\s*)+)\)',
    re.S)


def extract_go(path: pathlib.Path):
    """Yield fixtures from NewExampleSpec calls, which carry line numbers."""
    text = path.read_text()
    for m in GO_SPEC.finditer(text):
        parts = re.findall(r"`([^`]*)`", m.group(1))
        if len(parts) < 3:
            continue                       # mapping only, nothing to assert
        mapping, rest = parts[0].strip(), parts[1:]
        if len(rest) % 2:
            rest = rest[:-1]               # unpaired trailing entry
        # NOT stripped: a Go raw string is the exact expected bytes, and
        # format_yaml's output really does end in a newline. The mapping above
        # is stripped because raw strings there are conventionally written with
        # a leading newline for readability.
        cases = [{"in": rest[k], "out": rest[k + 1]}
                 for k in range(0, len(rest), 2)]
        if not mapping or not cases:
            continue
        line = text.count("\n", 0, m.start()) + 1
        yield {
            "id": f"go.{path.stem}.{line}",
            "source": f"{path.name}:{line}",
            "mapping": mapping,
            "cases": cases,
        }


# Some documented examples cannot be asserted on, because the doc itself elides
# what would be needed to run them: the JWT signing examples carry a PEM block
# whose body is "... signature data ...", and several show the output as a
# description in angle brackets rather than the bytes. Keeping them would mean a
# permanently red gate measuring nothing.
#
# The angle-bracket test is deliberately narrow -- two or more plain words, and
# nothing else, between the brackets. A looser one swallowed the format_xml and
# unescape_html fixtures, whose expected output really is markup, which is the
# opposite of what this filter is for.
#
# `<Message deleted>` fits the shape but is NOT a placeholder: it is how the
# reference writes the real outcome of a mapping returning `deleted()`.
REAL_SENTINELS = {"<Message deleted>"}
PROSE = r"<[A-Za-z]+(?: [A-Za-z]+)+>"
PLACEHOLDER_BARE = re.compile(r"^" + PROSE + r"$")
PLACEHOLDER_QUOTED = re.compile(r'"' + PROSE + r'"')


# Compressed output is not determined by the format. The documented compress()
# examples pin the exact bytes Go's DEFLATE encoder produces, including the gzip
# header's mtime and OS fields; zlib's encoder is conformant and produces
# different bytes for the same input, and so does every other implementation.
# The examples that DECOMPRESS Go-produced data are kept -- those do assert
# interoperability, in the direction that matters -- and tests/test_all.cc
# round-trips all seven algorithms.
def compresses(fx) -> bool:
    return ".compress(" in fx["mapping"]


def is_placeholder(fx) -> bool:
    if compresses(fx):
        return True
    if "-----BEGIN" in fx["mapping"] and "..." in fx["mapping"]:
        return True
    for c in fx["cases"]:
        out = c["out"].strip()
        if out in REAL_SENTINELS:
            continue
        if PLACEHOLDER_BARE.match(out) or PLACEHOLDER_QUOTED.search(out):
            return True
    return False


def main():
    fixtures = []
    for name in ("functions.adoc", "methods.adoc"):
        f = ADOC / name
        if f.exists():
            fixtures += list(extract_adoc(f))
    for f in sorted(GOSRC.glob("*.go")):
        if f.name.endswith("_test.go"):
            continue
        fixtures += list(extract_go(f))

    # De-duplicate: the docs are generated from the Go specs, so most fixtures
    # appear twice. Keep the Go one, which carries provenance.
    # The key ignores surrounding whitespace so the doc-derived copy of a
    # fixture still matches the Go one it was generated from -- the doc format
    # cannot express a trailing newline, and keeping both would leave a fixture
    # asserting output the reference does not produce.
    def dedup_key(fx):
        cases = [{"in": c["in"].strip(), "out": c["out"].strip()} for c in fx["cases"]]
        return (fx["mapping"], json.dumps(cases, sort_keys=True))

    by_key, order = {}, []
    for fx in fixtures:
        key = dedup_key(fx)
        if key in by_key:
            if fx["id"].startswith("go."):
                by_key[key] = fx
            continue
        by_key[key] = fx
        order.append(key)

    out = [by_key[k] for k in order if not is_placeholder(by_key[k])]
    dest = ROOT / "swordfish/tests/fixtures/bloblang.yaml"
    dest.parent.mkdir(parents=True, exist_ok=True)
    with dest.open("w") as fh:
        fh.write("# Generated by tools/extract_conformance.py — do not edit.\n")
        fh.write(f"# {len(out)} fixtures from the Redpanda Connect docs and the Benthos source.\n")
        for fx in out:
            fh.write(f"- id: {yaml_quote(fx['id'])}\n")
            fh.write(f"  source: {yaml_quote(fx['source'])}\n")
            fh.write(f"  mapping: {yaml_block(fx['mapping'], '    ')}\n")
            fh.write("  cases:\n")
            for c in fx["cases"]:
                fh.write(f"    - in: {yaml_block(c['in'], '        ')}\n")
                fh.write(f"      out: {yaml_block(c['out'], '        ')}\n")
    skipped = len(by_key) - len(out)
    print(f"{len(out)} fixtures -> {dest.relative_to(ROOT)}")
    if skipped:
        print(f"  {skipped} skipped: the example elides its key, its output, or "
              f"pins implementation-defined compressed bytes")
        for k in order:
            if is_placeholder(by_key[k]):
                print(f"      {by_key[k]['id']}")
    adoc_n = sum(1 for f in out if not f["id"].startswith("go."))
    print(f"  {len(out) - adoc_n} from Go source, {adoc_n} doc-only")


if __name__ == "__main__":
    sys.exit(main())
