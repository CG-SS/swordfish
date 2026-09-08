#!/usr/bin/env python3
"""Generate C++ codecs from the official Kafka protocol message schemas.

Reads third_party/kafka-protocol/message/*.json and writes
src/kafka/generated/messages.hh and protocol.cc.

Why generate: the protocol's per-version rules -- which fields exist, which are
nullable, and where a version switches to the "flexible" encoding with compact
lengths and trailing tagged fields -- are precisely the parts that are easy to
get subtly wrong by hand, and they are stated exactly in these files. This is the
same argument `spec_of` makes for configuration: one description, every consumer
derived from it.

Run: tools/gen_kafka_protocol.py
"""
import json
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
SCHEMAS = ROOT / "third_party/kafka-protocol/message"
OUT = ROOT / "src/kafka/generated"

# ---------------------------------------------------------------------------
# schema model


def load(path):
    """The schemas are JSON with // comments, which json.loads will not take."""
    text = path.read_text()
    text = re.sub(r"^\s*//.*$", "", text, flags=re.M)
    return json.loads(text)


def parse_range(spec):
    """"0+" / "3-5" / "4" / "none" -> (lo, hi) with hi None for open, or None."""
    if spec is None or spec == "none":
        return None
    if spec.endswith("+"):
        return (int(spec[:-1]), None)
    if "-" in spec:
        lo, hi = spec.split("-", 1)
        return (int(lo), int(hi))
    return (int(spec), int(spec))


def in_range(rng, v):
    if rng is None:
        return False
    lo, hi = rng
    return v >= lo and (hi is None or v <= hi)


def snake(name):
    """PascalCase -> snake_case, keeping acronym runs together (NodeId -> node_id,
    ISR -> isr, ThrottleTimeMs -> throttle_time_ms)."""
    s = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)
    return s.lower()


PRIMITIVES = {
    "bool": ("bool", "false"),
    "int8": ("int8_t", "0"),
    "int16": ("int16_t", "0"),
    "int32": ("int32_t", "0"),
    "int64": ("int64_t", "0"),
    "uint16": ("uint16_t", "0"),
    "uint32": ("uint32_t", "0"),
    "float64": ("double", "0"),
    "string": ("std::string", "{}"),
    "bytes": ("std::string", "{}"),
    "uuid": ("uuid", "{}"),
    "records": ("std::string", "{}"),
}


class Field:
    def __init__(self, raw, owner, structs):
        self.raw = raw
        self.name = raw["name"]
        self.cxx_name = snake(self.name)
        self.versions = parse_range(raw["versions"])
        self.nullable = parse_range(raw.get("nullableVersions"))
        self.tag = raw.get("tag")
        self.tagged_versions = parse_range(raw.get("taggedVersions"))
        self.default = raw.get("default")

        t = raw["type"]
        self.is_array = t.startswith("[]")
        base = t[2:] if self.is_array else t
        self.base = base
        self.is_struct = base not in PRIMITIVES
        self.struct = None
        if self.is_struct:
            self.struct = Struct(base, raw["fields"], owner, structs)

    # A field is optional in C++ when the protocol lets it be null at some
    # version, or when it is a tagged struct (present only if the tag was sent).
    @property
    def optional(self):
        if self.nullable is not None and not self.is_array:
            return True
        return self.tag is not None and self.is_struct and not self.is_array

    def cxx_type(self):
        if self.is_struct:
            inner = self.struct.cxx_name
        else:
            inner = PRIMITIVES[self.base][0]
        if self.is_array:
            return f"std::vector<{inner}>"
        if self.optional:
            return f"std::optional<{inner}>"
        return inner

    def initialiser(self):
        if self.is_array or self.optional or self.is_struct:
            return ""
        cxx, zero = PRIMITIVES[self.base]
        d = self.default
        if d is None:
            return f" = {zero}" if cxx not in ("std::string", "uuid") else ""
        if self.base == "bool":
            return f" = {d}"
        if self.base in ("string", "bytes", "records"):
            return "" if d in ('""', "null") else f' = "{d}"'
        if self.base == "uuid":
            return ""
        if d == "null":
            return ""
        # Numeric: the schemas use decimal and hex, and int64 needs a suffix.
        val = d
        suffix = "LL" if self.base == "int64" else ""
        return f" = {val}{suffix}"


class Struct:
    """A generated C++ struct. Nested schema structs are prefixed with the
    message name when they do not already carry it, because two messages may
    define different structs under the same short name."""

    def __init__(self, name, fields, owner, structs):
        base = name if name.startswith(owner) else owner + name
        self.cxx_name = snake(base)
        self.fields = [Field(f, owner, structs) for f in fields]
        prev = structs.get(self.cxx_name)
        if prev is not None:
            if [f.name for f in prev.fields] != [f.name for f in self.fields]:
                sys.exit(f"struct name collision: {self.cxx_name}")
        else:
            structs[self.cxx_name] = self

    def versions_used(self):
        return self.fields


class Message:
    def __init__(self, path, structs):
        d = load(path)
        self.kind = d["type"]
        # A "data" schema is an embedded blob -- the consumer protocol's
        # subscription and assignment -- so it has no api key and is never
        # framed. It still needs versioned encode/decode.
        self.api_key = d.get("apiKey")
        self.name = d["name"]
        self.cxx_name = snake(self.name)
        self.valid = parse_range(d["validVersions"])
        self.flexible = parse_range(d.get("flexibleVersions"))
        self.fields = [Field(f, self.name, structs) for f in d.get("fields", [])]


# ---------------------------------------------------------------------------
# emission


def collect(fields, out):
    """Depth-first so a nested struct is declared before its user."""
    for f in fields:
        if f.is_struct:
            collect(f.struct.fields, out)
            if f.struct.cxx_name not in out:
                out[f.struct.cxx_name] = f.struct


def decl_struct(st, h):
    h.append(f"struct {st.cxx_name} {{")
    for f in st.fields:
        h.append(f"    {f.cxx_type():<40} {f.cxx_name}{f.initialiser()};")
    h.append(f"    void encode(writer& w, int16_t version, bool flex) const;")
    h.append(f"    void decode(reader& r, int16_t version, bool flex);")
    h.append("};")
    h.append("")


def encode_one(f, expr, c, indent):
    """Encode a single value (not the array wrapper) of field f from `expr`."""
    p = "    " * indent
    if f.is_struct:
        c.append(f"{p}{expr}.encode(w, version, flex);")
    elif f.base in ("string",):
        if f.optional:
            c.append(f"{p}w.nullable_string({expr}, flex);")
        else:
            c.append(f"{p}w.string({expr}, flex);")
    elif f.base in ("bytes", "records"):
        if f.optional:
            c.append(f"{p}w.nullable_bytes({expr}, flex);")
        else:
            c.append(f"{p}w.bytes({expr}, flex);")
    elif f.base == "uuid":
        c.append(f"{p}w.uuid_({expr});")
    elif f.base == "bool":
        c.append(f"{p}w.boolean({expr});")
    elif f.base == "float64":
        c.append(f"{p}w.f64({expr});")
    else:
        c.append(f"{p}w.{f.base.replace('int', 'i').replace('uii', 'ui')}({expr});")


def decode_one(f, target, c, indent):
    p = "    " * indent
    if f.is_struct:
        c.append(f"{p}{target}.decode(r, version, flex);")
    elif f.base == "string":
        c.append(f"{p}{target} = r.{'nullable_string' if f.optional else 'string'}(flex);")
    elif f.base in ("bytes", "records"):
        c.append(f"{p}{target} = r.{'nullable_bytes' if f.optional else 'bytes'}(flex);")
    elif f.base == "uuid":
        c.append(f"{p}{target} = r.uuid_();")
    elif f.base == "bool":
        c.append(f"{p}{target} = r.boolean();")
    elif f.base == "float64":
        c.append(f"{p}{target} = r.f64();")
    else:
        c.append(f"{p}{target} = r.{f.base.replace('int', 'i').replace('uii', 'ui')}();")


def version_guard(rng):
    lo, hi = rng
    if hi is None:
        return f"version >= {lo}"
    if lo == hi:
        return f"version == {lo}"
    return f"version >= {lo} && version <= {hi}"


def emit_field_encode(f, c, indent=1):
    p = "    " * indent
    if f.is_array:
        if f.nullable is not None:
            c.append(f"{p}if ({f.cxx_name}.empty() && {version_guard(f.nullable)}) {{")
            c.append(f"{p}    w.null_array(flex);")
            c.append(f"{p}}} else {{")
            inner = indent + 1
        else:
            inner = indent
        pp = "    " * inner
        c.append(f"{pp}w.array_len({f.cxx_name}.size(), flex);")
        c.append(f"{pp}for (const auto& e_ : {f.cxx_name}) {{")
        encode_one(f, "e_", c, inner + 1)
        c.append(f"{pp}}}")
        if f.nullable is not None:
            c.append(f"{p}}}")
    else:
        encode_one(f, f.cxx_name, c, indent)


def emit_field_decode(f, c, indent=1):
    p = "    " * indent
    if f.is_array:
        c.append(f"{p}{{")
        c.append(f"{p}    const int64_t n_ = r.array_len(flex);")
        c.append(f"{p}    {f.cxx_name}.clear();")
        c.append(f"{p}    if (n_ > 0) {{")
        c.append(f"{p}        {f.cxx_name}.resize(static_cast<size_t>(n_));")
        c.append(f"{p}        for (auto& e_ : {f.cxx_name}) {{")
        decode_one(f, "e_", c, indent + 3)
        c.append(f"{p}        }}")
        c.append(f"{p}    }}")
        c.append(f"{p}}}")
    else:
        decode_one(f, f.cxx_name, c, indent)


def tagged_written_test(f):
    """Kafka omits a tagged field whose value is the default."""
    if f.is_array:
        return f"!{f.cxx_name}.empty()"
    if f.optional:
        return f"{f.cxx_name}.has_value()"
    if f.base in ("string", "bytes", "records"):
        return f"!{f.cxx_name}.empty()"
    if f.base == "uuid":
        return f"!{f.cxx_name}.is_zero()"
    d = f.default
    if d is None:
        d = PRIMITIVES[f.base][1]
    suffix = "LL" if f.base == "int64" else ""
    return f"{f.cxx_name} != {d}{suffix}"


def runs(fields):
    """Consecutive fields sharing a version guard, emitted under ONE `if`.

    Most messages add several fields at the same version, and emitting a guard
    apiece produced long stretches of identical consecutive conditions -- which
    static analysis flags, correctly, as duplicated."""
    out = []
    for f in fields:
        if f.versions is None:
            continue                       # "none": removed from the protocol
        g = version_guard(f.versions)
        if out and out[-1][0] == g:
            out[-1][1].append(f)
        else:
            out.append((g, [f]))
    return out


def emit_body(name, fields, c, flex_expr):
    plain = [f for f in fields if f.tag is None]
    tagged = [f for f in fields if f.tag is not None]

    c.append(f"void {name}::encode(writer& w, int16_t version, bool flex) const {{")
    c.append("    (void)w; (void)version; (void)flex;")
    for guard, group in runs(plain):
        c.append(f"    if ({guard}) {{")
        for f in group:
            emit_field_encode(f, c, 2)
        c.append("    }")
    c.append("    if (flex) {")
    if tagged:
        c.append("        writer t_;")
        c.append("        uint32_t n_ = 0;")
        for f in tagged:
            if f.tagged_versions is None:
                continue
            c.append(f"        if ({version_guard(f.tagged_versions)} && {tagged_written_test(f)}) {{")
            c.append("            writer b_;")
            body = []
            emit_field_encode_into(f, body, 3, "b_")
            c += body
            c.append(f"            t_.uvarint({f.tag});")
            c.append("            t_.uvarint(static_cast<uint32_t>(b_.size()));")
            c.append("            t_.raw(b_.view());")
            c.append("            ++n_;")
            c.append("        }")
        c.append("        w.uvarint(n_);")
        c.append("        w.raw(t_.view());")
    else:
        c.append("        w.empty_tags();")
    c.append("    }")
    c.append("}")
    c.append("")

    c.append(f"void {name}::decode(reader& r, int16_t version, bool flex) {{")
    c.append("    (void)r; (void)version; (void)flex;")
    for guard, group in runs(plain):
        c.append(f"    if ({guard}) {{")
        for f in group:
            emit_field_decode(f, c, 2)
        c.append("    }")
    c.append("    if (flex) {")
    c.append("        const uint32_t nt_ = r.uvarint();")
    c.append("        for (uint32_t i_ = 0; i_ < nt_; ++i_) {")
    c.append("            const uint32_t tag_ = r.uvarint();")
    c.append("            const uint32_t sz_ = r.uvarint();")
    c.append("            const size_t end_ = r.pos() + sz_;")
    c.append("            switch (tag_) {")
    for f in tagged:
        if f.tagged_versions is None:
            continue
        c.append(f"            case {f.tag}:")
        c.append(f"                if ({version_guard(f.tagged_versions)}) {{")
        if f.optional and f.is_struct and not f.is_array:
            c.append(f"                    {f.cxx_name}.emplace();")
            c.append(f"                    {f.cxx_name}->decode(r, version, flex);")
        else:
            emit_field_decode(f, c, 5)
        c.append("                }")
        c.append("                break;")
    c.append("            default: break;    // a tag from a newer broker")
    c.append("            }")
    c.append("            r.seek(end_);      // authoritative, even if we skipped it")
    c.append("        }")
    c.append("    }")
    c.append("}")
    c.append("")


def emit_field_encode_into(f, c, indent, sink):
    """Same as emit_field_encode but writing into a named writer (tagged body)."""
    tmp = []
    if f.optional and f.is_struct and not f.is_array:
        p = "    " * indent
        tmp.append(f"{p}{f.cxx_name}->encode(w, version, flex);")
    else:
        emit_field_encode(f, tmp, indent)
    for line in tmp:
        c.append(line.replace("w.", f"{sink}.", 1) if line.lstrip().startswith("w.")
                 else line.replace("(w,", f"({sink},"))


def main():
    structs = {}
    messages = []
    for path in sorted(SCHEMAS.glob("*.json")):
        messages.append(Message(path, structs))

    h = []
    h.append("// Generated by tools/gen_kafka_protocol.py from the official Kafka")
    h.append("// protocol schemas in third_party/kafka-protocol. Do not edit.")
    h.append("#pragma once")
    h.append('#include "swordfish/kafka/wire.hh"')
    h.append("#include <optional>")
    h.append("#include <string>")
    h.append("#include <vector>")
    h.append("")
    h.append("namespace sf::kafka {")
    h.append("")

    c = []
    c.append("// Generated by tools/gen_kafka_protocol.py. Do not edit.")
    c.append('#include "messages.hh"')
    c.append("")
    c.append("namespace sf::kafka {")
    c.append("")

    emitted = {}
    for m in messages:
        collect(m.fields, emitted)
    for st in emitted.values():
        decl_struct(st, h)
    for st in emitted.values():
        emit_body(st.cxx_name, st.fields, c, None)

    for m in messages:
        flex_from = m.flexible[0] if m.flexible else -1
        h.append(f"struct {m.cxx_name} {{")
        if m.api_key is not None:
            h.append(f"    static constexpr int16_t api_key = {m.api_key};")
        h.append(f"    static constexpr int16_t min_version = {m.valid[0]};")
        h.append(f"    static constexpr int16_t max_version = {m.valid[1] if m.valid[1] is not None else 32767};")
        h.append(f"    static constexpr int16_t flexible_from = {flex_from};")
        h.append("    static constexpr bool flexible(int16_t v) {")
        h.append("        return flexible_from >= 0 && v >= flexible_from;")
        h.append("    }")
        for f in m.fields:
            h.append(f"    {f.cxx_type():<40} {f.cxx_name}{f.initialiser()};")
        h.append("    void encode(writer& w, int16_t version, bool flex) const;")
        h.append("    void decode(reader& r, int16_t version, bool flex);")
        h.append("    void encode(writer& w, int16_t version) const { encode(w, version, flexible(version)); }")
        h.append("    void decode(reader& r, int16_t version) { decode(r, version, flexible(version)); }")
        h.append("};")
        h.append("")
        emit_body(m.cxx_name, m.fields, c, None)

    h.append("} // namespace sf::kafka")
    c.append("} // namespace sf::kafka")

    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "messages.hh").write_text("\n".join(h) + "\n")
    (OUT / "messages.cc").write_text("\n".join(c) + "\n")
    print(f"{len(messages)} messages, {len(emitted)} nested structs -> "
          f"{(OUT / 'messages.hh').relative_to(ROOT)} "
          f"({len(h)} + {len(c)} lines)")


if __name__ == "__main__":
    main()
