// The `avro` scanner: an Avro Object Container File stream in, one message per
// datum out.
//
// The container is framed HERE rather than handed to avro-cpp's DataFileReader,
// because that class PULLS from an InputStream whose next() has no "would
// block" -- returning false means end of stream -- while a scanner is PUSHED
// bytes as they arrive and must give them back only when a message is complete.
// Framing costs a header parser and a block loop; in exchange the scanner holds
// one block rather than the whole file, which is what every other scanner here
// does. Block decompression is sf::codec's, already shared by the `decompress`
// scanner and the Kafka record codec. avro-cpp is used for exactly two things:
// compiling the schema and decoding a block's datums.
//
// The output format is Avro JSON (the reference's default) or standard JSON
// (`raw_json: true`), and it is written HERE rather than with avro-cpp's
// jsonEncoder, which diverges from the reference in two places -- see
// write_datum below.

#include "scanner_avro.hh"

#include "swordfish/codecs.hh"
#include "swordfish/value.hh"

#include <stdexcept>
#include <string>

#ifdef SWORDFISH_HAVE_AVRO

#include <avro/Compiler.hh>
#include <avro/Decoder.hh>
#include <avro/Generic.hh>
#include <avro/GenericDatum.hh>
#include <avro/Node.hh>
#include <avro/NodeImpl.hh>
#include <avro/Stream.hh>
#include <avro/Types.hh>
#include <avro/ValidSchema.hh>

#include <zlib.h>

#include <algorithm>
#include <array>
#include <cmath>
#include <iterator>
#include <cstdint>
#include <vector>

namespace sf {
namespace {

// ---- JSON writing --------------------------------------------------------
//
// Avro JSON's escaping is not encoding/json's, and it is not one rule but two.
// Both were probed against redpanda-connect 4.107.2 rather than guessed,
// because the difference is invisible until a byte comparison fails:
//
//   * a `string` escapes only `"`, `\`, the C0 controls and U+2028/U+2029, and
//     emits everything else as literal UTF-8. Notably `<`, `>` and `&` stay as
//     they are -- Go's encoding/json escapes all three -- and so do U+007F,
//     U+0080, U+00A0 and the unassigned runes.
//   * `bytes` and `fixed` are a string of one code point per BYTE, and escape
//     anything outside printable ASCII. So the byte 0x80 writes as a six-
//     character escape, where the same rune inside a `string` field is
//     literal UTF-8.

void append_u_escape(std::string& out, unsigned c) {
    static const char hex[] = "0123456789abcdef";
    out += "\\u";
    out += hex[(c >> 12) & 0xf];
    out += hex[(c >> 8) & 0xf];
    out += hex[(c >> 4) & 0xf];
    out += hex[c & 0xf];
}

// The five short escapes plus the two that JSON always requires. Returns false
// when the byte needs no special treatment from this half of the rule.
bool append_common_escape(std::string& out, unsigned char c) {
    switch (c) {
    case '"':  out += "\\\""; return true;
    case '\\': out += "\\\\"; return true;
    case '\b': out += "\\b";  return true;
    case '\f': out += "\\f";  return true;
    case '\n': out += "\\n";  return true;
    case '\r': out += "\\r";  return true;
    case '\t': out += "\\t";  return true;
    default:   return false;
    }
}

void write_string(std::string& out, std::string_view s) {
    out += '"';
    for (size_t i = 0; i < s.size(); ++i) {
        const auto c = static_cast<unsigned char>(s[i]);
        if (append_common_escape(out, c)) continue;
        if (c < 0x20) { append_u_escape(out, c); continue; }
        // U+2028 and U+2029 are the only runes above ASCII the reference
        // escapes: they end a line in JavaScript but not in JSON, so a document
        // containing them cannot be pasted into a script. Their UTF-8 encodings
        // are E2 80 A8 and E2 80 A9.
        if (c == 0xE2 && i + 2 < s.size() &&
            static_cast<unsigned char>(s[i + 1]) == 0x80) {
            const auto third = static_cast<unsigned char>(s[i + 2]);
            if (third == 0xA8 || third == 0xA9) {
                out += (third == 0xA8) ? "\\u2028" : "\\u2029";
                i += 2;
                continue;
            }
        }
        out += static_cast<char>(c);
    }
    out += '"';
}

void write_bytes(std::string& out, std::string_view s) {
    out += '"';
    for (const char ch : s) {
        const auto c = static_cast<unsigned char>(ch);
        if (append_common_escape(out, c)) continue;
        if (c < 0x20 || c >= 0x7f) { append_u_escape(out, c); continue; }
        out += static_cast<char>(c);
    }
    out += '"';
}

// Avro can carry a non-finite float; JSON has no syntax for one, so every
// writer invents something. The reference writes NaN as `null` and infinity as
// the literal `1e999`, a number so large that a JSON decoder reading it back
// gets Inf -- which is how goavro round-trips it.
//
// This does NOT belong in append_float_g: that function is faithful to
// strconv.FormatFloat(v,'g',-1,bits), and Go's own answer there is "+Inf",
// which is not JSON at all. The JSON spelling is this format's choice.
//
// Writing `null` for infinity, as the first version did, lost the sign and
// collapsed +Inf, -Inf and NaN into one value.
void write_number(std::string& out, double v, int bits) {
    if (std::isnan(v))   { out += "null"; return; }
    if (std::isinf(v))   { out += (v < 0) ? "-1e999" : "1e999"; return; }
    append_float_g(out, v, bits);
}

std::string_view bytes_view(const std::vector<uint8_t>& b) {
    return {reinterpret_cast<const char*>(b.data()), b.size()};
}

// A schema node with any forward reference followed. Recursive schemas are
// represented as AVRO_SYMBOLIC placeholders, and every switch below would fall
// through to "unsupported" on one.
avro::NodePtr resolved(const avro::NodePtr& n) {
    return n->type() == avro::AVRO_SYMBOLIC ? avro::resolveSymbol(n) : n;
}

// The key a union branch is wrapped in: the user's fullname for a named type,
// the type's own name for everything else. Confirmed against the reference,
// which writes {"a.b.Inner":{...}} rather than {"Inner":{...}}.
std::string union_branch_name(const avro::NodePtr& n) {
    switch (n->type()) {
    case avro::AVRO_RECORD:
    case avro::AVRO_ENUM:
    case avro::AVRO_FIXED:
        return n->name().fullname();
    default:
        return avro::toString(n->type());
    }
}

// `node` must already be resolved(). `raw` is the scanner's `raw_json`: it
// unwraps unions and changes nothing else.
//
// Written by hand rather than with avro::jsonEncoder(), which agrees with the
// reference on every shape here -- records, maps, arrays, enums, fixed, union
// branch naming, and ignoring logical types -- but disagrees on two things that
// matter for byte-identical output:
//
//   * floats. jsonEncoder writes 17 significant digits, so 0.1 becomes
//     0.10000000000000001; the reference writes the shortest round-tripping
//     form in strconv's 'g' layout, which is what append_float_g does.
//   * non-ASCII strings. jsonEncoder escapes them to \uXXXX; the reference
//     emits literal UTF-8, so an accented word stays an accented word.
void write_datum(std::string& out, const avro::GenericDatum& d,
                 const avro::NodePtr& node, bool raw) {
    if (node->type() == avro::AVRO_UNION) {
        const avro::NodePtr branch = resolved(node->leafAt(d.unionBranch()));
        // A null branch is a bare JSON null in BOTH modes -- the spec makes
        // null the one union value that is never wrapped.
        if (branch->type() == avro::AVRO_NULL) { out += "null"; return; }
        if (raw) { write_datum(out, d, branch, raw); return; }
        out += '{';
        write_string(out, union_branch_name(branch));
        out += ':';
        write_datum(out, d, branch, raw);
        out += '}';
        return;
    }

    switch (node->type()) {
    case avro::AVRO_NULL:   out += "null"; return;
    case avro::AVRO_BOOL:   out += d.value<bool>() ? "true" : "false"; return;
    case avro::AVRO_INT:    out += std::to_string(d.value<int32_t>()); return;
    case avro::AVRO_LONG:   out += std::to_string(d.value<int64_t>()); return;
    case avro::AVRO_FLOAT:  write_number(out, d.value<float>(), 32); return;
    case avro::AVRO_DOUBLE: write_number(out, d.value<double>(), 64); return;
    case avro::AVRO_STRING: write_string(out, d.value<std::string>()); return;
    case avro::AVRO_BYTES:  write_bytes(out, bytes_view(d.value<std::vector<uint8_t>>())); return;
    case avro::AVRO_FIXED:  write_bytes(out, bytes_view(d.value<avro::GenericFixed>().value())); return;
    case avro::AVRO_ENUM:   write_string(out, d.value<avro::GenericEnum>().symbol()); return;

    case avro::AVRO_ARRAY: {
        const auto& items = d.value<avro::GenericArray>().value();
        const avro::NodePtr item_type = resolved(node->leafAt(0));
        out += '[';
        for (size_t i = 0; i < items.size(); ++i) {
            if (i) out += ',';
            write_datum(out, items[i], item_type, raw);
        }
        out += ']';
        return;
    }
    case avro::AVRO_MAP: {
        // A map node has two leaves: the key type, which Avro fixes as string,
        // and the value type.
        //
        // Keys are emitted SORTED, and this is the one place the scanner does
        // not reproduce the reference's bytes -- because the reference does not
        // reproduce its own. It decodes a map into a Go map and writes it in
        // iteration order, which Go deliberately randomises: the same file read
        // thirty times here gave {"k":3,"z":-1} twenty-five times and
        // {"z":-1,"k":3} five times. There is no order to match, so the choice
        // is which deterministic one to pick, and sorted is the one that keeps
        // swordfish self-consistent: sf::value holds an object's entries sorted,
        // so a message that passes through any Bloblang mapping comes out sorted
        // anyway, and encounter order would make the scanner's raw bytes
        // disagree with the same data one processor later. Go's encoding/json
        // sorts map keys too, so a reference pipeline that touches the message
        // ends up here as well.
        const auto& entries = d.value<avro::GenericMap>().value();
        const avro::NodePtr value_type = resolved(node->leafAt(1));
        std::vector<size_t> order(entries.size());
        for (size_t i = 0; i < order.size(); ++i) order[i] = i;
        std::sort(order.begin(), order.end(), [&](size_t a, size_t b) {
            return entries[a].first < entries[b].first;
        });
        out += '{';
        for (size_t i = 0; i < order.size(); ++i) {
            if (i) out += ',';
            write_string(out, entries[order[i]].first);
            out += ':';
            write_datum(out, entries[order[i]].second, value_type, raw);
        }
        out += '}';
        return;
    }
    case avro::AVRO_RECORD: {
        // Field order is the SCHEMA's, not sorted: the reference writes the
        // record in declaration order, and a message's bytes are compared
        // byte-for-byte.
        const auto& rec = d.value<avro::GenericRecord>();
        out += '{';
        for (size_t i = 0; i < rec.fieldCount(); ++i) {
            if (i) out += ',';
            write_string(out, node->nameAt(i));
            out += ':';
            write_datum(out, rec.fieldAt(i), resolved(node->leafAt(i)), raw);
        }
        out += '}';
        return;
    }
    default:
        throw std::runtime_error("avro: cannot encode a schema of type " +
                                 avro::toString(node->type()));
    }
}

// ---- the schema's canonical form and its fingerprint ----------------------

bool is_primitive_name(std::string_view t) {
    return t == "null" || t == "boolean" || t == "int" || t == "long" ||
           t == "float" || t == "double" || t == "bytes" || t == "string";
}

// The Avro spec's Parsing Canonical Form: names resolved to fullnames, keys in
// the order name/type/fields/symbols/items/values/size, and every other
// attribute -- doc, aliases, defaults, logicalType, precision -- stripped.
//
// Built from ValidSchema::toJson(false), which has already resolved namespaces
// and emits one explicitly on every named type, so no inheritance rules are
// needed here. What toJson does NOT do is order or strip, which is the whole of
// the transform below. This exact string is what the reference exposes as
// @avro_schema and what its fingerprint is computed over; both were checked
// byte-for-byte against redpanda-connect 4.107.2.
void write_canonical(std::string& out, const value& v) {
    if (v.type() == vtype::string) {           // a bare type name or a reference
        write_string(out, v.as_string());
        return;
    }
    if (v.type() == vtype::array) {            // a union
        out += '[';
        const auto& items = v.arr();
        for (size_t i = 0; i < items.size(); ++i) {
            if (i) out += ',';
            write_canonical(out, items[i]);
        }
        out += ']';
        return;
    }
    if (v.type() != vtype::object)
        throw std::runtime_error("avro: the schema is not valid JSON for a schema");

    const value* t = v.find("type");
    if (t == nullptr || t->type() != vtype::string)
        throw std::runtime_error("avro: a schema object has no `type`");
    const std::string kind = t->as_string();

    // [PRIMITIVES]: {"type":"int","logicalType":"date"} canonicalises to "int".
    if (is_primitive_name(kind)) { write_string(out, kind); return; }

    const auto fullname = [&] {
        const value* n = v.find("name");
        const std::string name = (n != nullptr && n->type() == vtype::string)
            ? n->as_string() : std::string();
        const value* ns = v.find("namespace");
        if (ns != nullptr && ns->type() == vtype::string && !ns->as_string().empty())
            return ns->as_string() + "." + name;
        return name;
    };
    const auto open_named = [&](const char* after_type) {
        out += "{\"name\":";
        write_string(out, fullname());
        out += ",\"type\":";
        write_string(out, kind);
        out += ',';
        out += after_type;
    };

    if (kind == "record") {
        open_named("\"fields\":[");
        const value* fields = v.find("fields");
        if (fields != nullptr && fields->type() == vtype::array) {
            const auto& fs = fields->arr();
            for (size_t i = 0; i < fs.size(); ++i) {
                if (i) out += ',';
                const value* fname = fs[i].find("name");
                const value* ftype = fs[i].find("type");
                if (fname == nullptr || ftype == nullptr)
                    throw std::runtime_error("avro: a record field has no `name` or `type`");
                out += "{\"name\":";
                write_string(out, fname->as_string());
                out += ",\"type\":";
                write_canonical(out, *ftype);
                out += '}';
            }
        }
        out += "]}";
    } else if (kind == "enum") {
        open_named("\"symbols\":[");
        const value* syms = v.find("symbols");
        if (syms != nullptr && syms->type() == vtype::array) {
            const auto& ss = syms->arr();
            for (size_t i = 0; i < ss.size(); ++i) {
                if (i) out += ',';
                write_string(out, ss[i].as_string());
            }
        }
        out += "]}";
    } else if (kind == "fixed") {
        open_named("\"size\":");
        const value* size = v.find("size");
        out += std::to_string(size != nullptr ? size->as_i64() : 0);
        out += '}';
    } else if (kind == "array") {
        out += "{\"type\":\"array\",\"items\":";
        const value* items = v.find("items");
        if (items == nullptr) throw std::runtime_error("avro: an array schema has no `items`");
        write_canonical(out, *items);
        out += '}';
    } else if (kind == "map") {
        out += "{\"type\":\"map\",\"values\":";
        const value* values = v.find("values");
        if (values == nullptr) throw std::runtime_error("avro: a map schema has no `values`");
        write_canonical(out, *values);
        out += '}';
    } else {
        throw std::runtime_error("avro: unknown schema type '" + kind + "'");
    }
}

// CRC-64-AVRO, the 64-bit Rabin fingerprint the Avro spec defines over the
// parsing canonical form.
//
// UNSIGNED, which is not a detail: the reference writes 14616926384559147490
// for one of the fixtures here, a value that does not fit in an int64 at all.
// The first schema this was checked against happened to fingerprint below 2^63,
// where the two readings agree, so the sign only showed up once the fixture set
// grew. It reaches a message as a raw_number for the same reason -- sf::value's
// integer is signed, and a `bytes`-style round trip through a double would lose
// the low digits.
uint64_t crc64_avro(std::string_view s) {
    constexpr uint64_t empty = 0xc15d213aa4d7a795ULL;
    static const std::array<uint64_t, 256> table = [] {
        std::array<uint64_t, 256> t{};
        for (uint64_t i = 0; i < 256; ++i) {
            uint64_t fp = i;
            for (int j = 0; j < 8; ++j)
                fp = (fp >> 1) ^ (empty & ~((fp & 1) - 1));
            t[i] = fp;
        }
        return t;
    }();
    uint64_t fp = empty;
    for (const char ch : s)
        fp = (fp >> 8) ^ table[(fp ^ static_cast<unsigned char>(ch)) & 0xff];
    return fp;
}

// ---- the container -------------------------------------------------------

// A cursor that says "need more bytes" instead of throwing, so a chunk that
// stops in the middle of a header simply leaves the parse where it was and the
// next chunk retries it from the start. Malformed input still throws: running
// out of bytes and being wrong are different answers.
struct cursor {
    std::string_view s;
    size_t pos = 0;
    bool   short_read = false;

    bool take(size_t n, std::string_view& out) {
        if (short_read) return false;
        if (s.size() - pos < n) { short_read = true; return false; }
        out = s.substr(pos, n);
        pos += n;
        return true;
    }
    // Avro's `long`: a zigzag-encoded base-128 varint.
    bool varint(int64_t& out) {
        if (short_read) return false;
        uint64_t u = 0;
        int shift = 0;
        size_t i = pos;
        for (;;) {
            if (i >= s.size()) { short_read = true; return false; }
            if (shift >= 64)
                throw std::runtime_error("avro: a varint in the container is malformed");
            const auto c = static_cast<unsigned char>(s[i++]);
            u |= static_cast<uint64_t>(c & 0x7f) << shift;
            if ((c & 0x80) == 0) break;
            shift += 7;
        }
        pos = i;
        // Zigzag, written without negating a signed value.
        out = static_cast<int64_t>((u >> 1) ^ (~(u & 1) + 1));
        return true;
    }
    bool blob(std::string_view& out) {          // a `long` length, then the bytes
        int64_t n = 0;
        if (!varint(n)) return false;
        if (n < 0) throw std::runtime_error("avro: a negative length in the container");
        return take(static_cast<size_t>(n), out);
    }
};

// OCF's snappy codec is the snappy BLOCK format with a four-byte big-endian
// CRC32 of the UNCOMPRESSED data appended. Decompressing without checking that
// trailer would accept a corrupt block, and feeding the trailer to the snappy
// decoder would reject a sound one.
std::string snappy_block(std::string_view in) {
    if (in.size() < 4)
        throw std::runtime_error("avro: a snappy block is too short to hold its checksum");
    std::string plain = codec::snappy_decompress(in.substr(0, in.size() - 4));
    const auto* t = reinterpret_cast<const unsigned char*>(in.data()) + in.size() - 4;
    const uint32_t want = (static_cast<uint32_t>(t[0]) << 24) |
                          (static_cast<uint32_t>(t[1]) << 16) |
                          (static_cast<uint32_t>(t[2]) << 8) |
                          static_cast<uint32_t>(t[3]);
    const auto got = static_cast<uint32_t>(::crc32(
        ::crc32(0L, nullptr, 0),
        reinterpret_cast<const unsigned char*>(plain.data()),
        static_cast<unsigned>(plain.size())));
    if (got != want)
        throw std::runtime_error("avro: a snappy block failed its CRC32 check");
    return plain;
}

class avro_scanner final : public scanner {
public:
    explicit avro_scanner(bool raw_json) : _raw(raw_json) {}

    std::vector<message> feed(std::string_view chunk) override {
        _buf.append(chunk);
        return drain();
    }

    std::vector<message> finish() override {
        std::vector<message> out = drain();
        // drain() may have handed back a block and kept its error; the stream is
        // over, so there is no later call for it to surface on.
        if (!_pending_error.empty()) {
            const std::string msg = std::move(_pending_error);
            _pending_error.clear();
            throw std::runtime_error(msg);
        }
        // A stream with no header at all is an error even when it is EMPTY, and
        // that is not obvious: `lines` and `to_the_end` both read a zero-byte
        // file as zero messages. An OCF file cannot be zero bytes -- it always
        // carries a header -- and the reference agrees, reporting "ocf: reading
        // magic: EOF" rather than finishing quietly. Saying WHICH part of the
        // container is missing is the difference between a usable error and
        // "invalid".
        if (!_have_header)
            throw std::runtime_error(_buf.empty()
                ? "avro: the stream is empty; an Avro OCF file always begins "
                  "with a header"
                : "avro: the stream ended before the OCF header was complete");
        if (!pending().empty())
            throw std::runtime_error("avro: the stream ended part-way through a data block");
        return out;
    }

    std::string name() const override { return "avro"; }

private:
    std::string_view pending() const {
        return std::string_view(_buf).substr(_start);
    }
    // The consumed prefix is dropped lazily. Erasing on every block would make
    // a large file quadratic, since each erase moves everything after it.
    void consume(size_t n) {
        _start += n;
        if (_start > 65536 && _start * 2 >= _buf.size()) {
            _buf.erase(0, _start);
            _start = 0;
        }
    }

    // Everything avro-cpp can throw passes through here, and it throws
    // std::out_of_range and avro::Exception with messages written for a library
    // author: an out-of-range union index arrived as
    // "vector::_M_range_check: __n (which is 9) >= this->size() (which is 2)",
    // which names neither Avro nor the file. Our own errors already say
    // "avro: ..." and are left alone; anything else is labelled here rather
    // than at each of the dozen call sites that could raise it.
    std::vector<message> drain() {
        // An error raised on a PREVIOUS call, held back so the messages decoded
        // before it could be delivered first. See below.
        if (!_pending_error.empty()) {
            const std::string msg = std::move(_pending_error);
            _pending_error.clear();
            throw std::runtime_error(msg);
        }
        std::vector<message> out;
        try {
            if (!_have_header && !read_header()) return out;
            while (read_block(out)) { }
            return out;
        } catch (const std::exception& e) {
            const std::string msg = named(e);
            // A block that decoded before the bad one is still delivered, then
            // the error follows on the next call. The reference does this --
            // given a sound block followed by a corrupt one it emits the sound
            // block's three records and THEN reports the fault -- and throwing
            // straight out of here discarded them, because `out` dies with the
            // exception. It is also what the README already claims for `tar`:
            // a truncated archive still yields its complete members.
            if (out.empty()) throw std::runtime_error(msg);
            _pending_error = msg;
            return out;
        }
    }

    // avro-cpp throws std::out_of_range and avro::Exception with messages
    // written for a library author: an out-of-range union index arrived as
    // "vector::_M_range_check: __n (which is 9) >= this->size() (which is 2)",
    // which names neither Avro nor the file. Our own errors already say
    // "avro: ..." and are left alone; anything else is labelled here rather
    // than at each of the dozen call sites that could raise it.
    static std::string named(const std::exception& e) {
        const std::string msg = e.what();
        if (msg.rfind("avro: ", 0) == 0) return msg;
        return "avro: the container could not be decoded: " + msg;
    }

    // Returns false when more bytes are needed. Nothing is stored until the
    // whole header has been read, so a retry starts clean.
    bool read_header() {
        cursor c{pending()};
        std::string_view magic;
        if (!c.take(4, magic)) return false;
        if (magic != std::string_view("Obj\x01", 4))
            throw std::runtime_error(
                "avro: this is not an Avro OCF stream (its first four bytes are "
                "not the OCF magic)");

        // The metadata is a map<bytes>, which Avro writes as a series of blocks
        // ending with a zero count. A negative count carries a byte size after
        // it, for a reader that wants to skip the block rather than read it.
        std::string schema_json;
        std::string codec_name;
        for (;;) {
            int64_t n = 0;
            if (!c.varint(n)) return false;
            if (n == 0) break;
            if (n < 0) {
                int64_t byte_size = 0;
                if (!c.varint(byte_size)) return false;
                n = -n;
            }
            for (int64_t i = 0; i < n; ++i) {
                std::string_view key;
                std::string_view val;
                if (!c.blob(key) || !c.blob(val)) return false;
                if (key == "avro.schema")     schema_json.assign(val);
                else if (key == "avro.codec") codec_name.assign(val);
            }
        }
        std::string_view sync;
        if (!c.take(16, sync)) return false;

        if (schema_json.empty())
            throw std::runtime_error("avro: the OCF header carries no `avro.schema`");
        // An absent or empty codec means no compression, which is what a writer
        // that never sets the key produces.
        if (codec_name.empty()) codec_name = "null";
        // The four the reference reads. `bzip2` and `xz` are in the Avro spec
        // and in neither implementation, so they land here by name rather than
        // being read as if they were uncompressed -- and the spelling matters:
        // the spec's name is `zstandard`, and a file written with `zstd` in the
        // header is not a legal container however obvious the intent.
        if (codec_name != "null" && codec_name != "deflate" &&
            codec_name != "snappy" && codec_name != "zstandard")
            throw std::runtime_error(
                "avro: the OCF codec '" + codec_name +
                "' is not one of null, deflate, snappy, zstandard");

        try {
            _schema = avro::compileJsonSchemaFromString(schema_json);
        } catch (const std::exception& e) {
            throw std::runtime_error(
                std::string("avro: the OCF header's schema does not compile: ") + e.what());
        }
        _root = resolved(_schema.root());
        // The COMPILED schema's JSON, not the header's. They differ wherever a
        // named type inherits its namespace from an enclosing one: the header
        // writes `{"type":"enum","name":"Colour"}` and the canonical form needs
        // the fullname `sf.test.Colour`, which only the compiled schema knows.
        // Canonicalising the raw header text silently produced a short name and
        // therefore the wrong fingerprint.
        std::string canonical;
        write_canonical(canonical, parse_json(_schema.toJson(false)));
        _fingerprint_meta = value::raw_number(std::to_string(crc64_avro(canonical)));
        _canonical_meta = value(std::move(canonical));
        _codec = std::move(codec_name);
        _sync.assign(sync);
        consume(c.pos);
        _have_header = true;
        return true;
    }

    // Returns false when more bytes are needed for the next whole block.
    bool read_block(std::vector<message>& out) {
        cursor c{pending()};
        int64_t count = 0;
        int64_t size = 0;
        if (!c.varint(count) || !c.varint(size)) return false;
        if (count < 0 || size < 0)
            throw std::runtime_error("avro: a data block declares a negative length");
        std::string_view data;
        std::string_view sync;
        if (!c.take(static_cast<size_t>(size), data)) return false;
        if (!c.take(16, sync)) return false;
        if (sync != _sync)
            throw std::runtime_error(
                "avro: a data block's sync marker does not match the header's; "
                "the stream is corrupt, or two OCF files were concatenated");

        // `data` points into _buf, which consume() may move, so the block is
        // fully decoded BEFORE the cursor's progress is committed.
        std::string plain;
        std::string_view body = data;
        if (_codec == "deflate")     { plain = codec::flate_decompress(data); body = plain; }
        else if (_codec == "snappy") { plain = snappy_block(data);            body = plain; }
        else if (_codec == "zstandard") { plain = codec::zstd_decompress(data); body = plain; }
        // Decoded into a vector of its own and appended only once the block has
        // been ACCEPTED. Appending as it went let a block that was about to be
        // rejected still contribute its first record: on a file whose second
        // block declares one datum and holds three, swordfish emitted four
        // records where the reference emits the three from the sound block.
        std::vector<message> block;
        decode_block(body, static_cast<size_t>(count), block);
        out.insert(out.end(), std::make_move_iterator(block.begin()),
                   std::make_move_iterator(block.end()));
        consume(c.pos);
        return true;
    }

    void decode_block(std::string_view body, size_t count, std::vector<message>& out) {
        // A datum count is a claim about the block, not a measurement of it, and
        // a block of EMPTY records consumes no bytes per datum -- so a
        // 116-byte file can claim 10^15 of them and cost the attacker nothing.
        // The reference streams those and runs for ever; neither that nor
        // exhausting the shard is an answer, so the claim is bounded here and
        // the boundary is a named error. The cap is far above any real block:
        // avro-cpp's own writer syncs at 64 KB, and the largest count seen in
        // the wild is thousands.
        constexpr size_t max_datums_per_block = 10u * 1000u * 1000u;
        if (count > max_datums_per_block)
            throw std::runtime_error(
                "avro: a data block claims " + std::to_string(count) +
                " datums, above the " + std::to_string(max_datums_per_block) +
                " swordfish will decode from one block");

        auto in = avro::memoryInputStream(
            reinterpret_cast<const uint8_t*>(body.data()), body.size());
        const avro::DecoderPtr dec = avro::binaryDecoder();
        dec->init(*in);
        avro::GenericDatum datum(_schema);
        for (size_t i = 0; i < count; ++i) {
            avro::GenericReader::read(*dec, datum);
            std::string json;
            write_datum(json, datum, _root, _raw);
            message m(std::move(json));
            // The two metadata values are built ONCE, when the header is read,
            // and copied per message. sf::value refcounts its payload, so this
            // is a pointer bump rather than a copy of the canonical schema --
            // which for a real schema is kilobytes, on every single datum.
            m.meta().set("avro_schema", _canonical_meta);
            m.meta().set("avro_schema_fingerprint", _fingerprint_meta);
            out.push_back(std::move(m));
        }

        // The count must ACCOUNT for the block, not merely fit inside it. A
        // block that says one datum and holds three decoded one and dropped two
        // -- silently, with a zero exit status, which is the one outcome this
        // project will not produce. The reference makes the same check and says
        // "ocf: N trailing bytes in block".
        //
        // drain() first: the decoder reads ahead, so bytes it buffered but never
        // used have to go back to the stream before what is left can be counted.
        dec->drain();
        size_t trailing = 0;
        const uint8_t* p = nullptr;
        size_t n = 0;
        while (in->next(&p, &n)) trailing += n;
        if (trailing != 0)
            throw std::runtime_error(
                "avro: a data block declares " + std::to_string(count) +
                (count == 1 ? " datum but has " : " datums but has ") +
                std::to_string(trailing) +
                (trailing == 1 ? " byte left over; " : " bytes left over; ") +
                "the count and the contents disagree");
    }

    const bool  _raw;
    std::string _buf;
    size_t      _start = 0;
    bool        _have_header = false;
    std::string _codec;
    std::string _sync;
    value       _canonical_meta;
    value       _fingerprint_meta;
    std::string _pending_error;
    avro::ValidSchema _schema;
    avro::NodePtr     _root;
};

} // namespace

bool avro_scanner_available() { return true; }

scanner_ptr make_avro_scanner(bool raw_json) {
    return std::make_unique<avro_scanner>(raw_json);
}

} // namespace sf

#else // !SWORDFISH_HAVE_AVRO

namespace sf {

bool avro_scanner_available() { return false; }

scanner_ptr make_avro_scanner(bool) {
    // A hard error rather than a fallback: scanning an OCF file as lines would
    // produce garbage messages instead of a diagnosis.
    //
    // The wording is load-bearing. parse_pipeline.cc probes make_scanner() at
    // LINT time and turns the failure into a config error only when the message
    // says "is not implemented" or "is not a scanner" -- anything else it treats
    // as a complaint about the scanner's options rather than its name. Phrased
    // as "this build has no Avro support", the config linted clean and then
    // failed on the first byte of the file, which is exactly the outcome the
    // named-error rule exists to prevent.
    throw std::runtime_error(
        "scanner 'avro' is not implemented by this build of swordfish: it was "
        "configured without avro-cpp, which needs fmt installed "
        "(./install_packages.sh)");
}

} // namespace sf

#endif
