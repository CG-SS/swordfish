// MessagePack, for parse_msgpack() and format_msgpack().
//
// Hand-written because the encoding choices are observable and a general
// library would not make the same ones: the reference encodes with
// vmihailenco/msgpack, which picks the SHORTEST representation for every
// integer and string, so `{"foo":"bar"}` is nine bytes (81 a3 66 6f 6f a3 62 61
// 72) and not a fixed-width encoding of the same values.
#include "swordfish/methods.hh"
#include "swordfish/value.hh"

#include "methods_util.hh"

#include <cstdint>
#include <algorithm>
#include <cstring>
#include <string>
#include <vector>

namespace sf::m {

namespace {

void put(std::string& out, uint8_t b) { out += static_cast<char>(b); }

void put_be(std::string& out, uint64_t v, int bytes) {
    for (int i = bytes - 1; i >= 0; --i)
        out += static_cast<char>((v >> (8 * i)) & 0xFF);
}

void encode(const value& v, std::string& out);

void encode_uint(uint64_t u, std::string& out) {
    if (u < 0x80)        { put(out, static_cast<uint8_t>(u)); return; }
    if (u <= 0xFF)       { put(out, 0xCC); put_be(out, u, 1); return; }
    if (u <= 0xFFFF)     { put(out, 0xCD); put_be(out, u, 2); return; }
    if (u <= 0xFFFFFFFF) { put(out, 0xCE); put_be(out, u, 4); return; }
    put(out, 0xCF); put_be(out, u, 8);
}

void encode_int(int64_t i, std::string& out) {
    if (i >= 0) { encode_uint(static_cast<uint64_t>(i), out); return; }
    if (i >= -32)        { put(out, static_cast<uint8_t>(0xE0 | (i + 32))); return; }
    if (i >= -128)       { put(out, 0xD0); put_be(out, static_cast<uint64_t>(i), 1); return; }
    if (i >= -32768)     { put(out, 0xD1); put_be(out, static_cast<uint64_t>(i), 2); return; }
    if (i >= -2147483648LL) { put(out, 0xD2); put_be(out, static_cast<uint64_t>(i), 4); return; }
    put(out, 0xD3); put_be(out, static_cast<uint64_t>(i), 8);
}

void encode_str(std::string_view s, std::string& out) {
    const size_t n = s.size();
    if (n < 32)          put(out, static_cast<uint8_t>(0xA0 | n));
    else if (n <= 0xFF)  { put(out, 0xD9); put_be(out, n, 1); }
    else if (n <= 0xFFFF){ put(out, 0xDA); put_be(out, n, 2); }
    else                 { put(out, 0xDB); put_be(out, n, 4); }
    out.append(s);
}

void encode_bin(std::string_view s, std::string& out) {
    const size_t n = s.size();
    if (n <= 0xFF)        { put(out, 0xC4); put_be(out, n, 1); }
    else if (n <= 0xFFFF) { put(out, 0xC5); put_be(out, n, 2); }
    else                  { put(out, 0xC6); put_be(out, n, 4); }
    out.append(s);
}

void encode(const value& v, std::string& out) {
    switch (v.type()) {
    case vtype::null: case vtype::deleted: case vtype::nothing:
        put(out, 0xC0);
        return;
    case vtype::boolean:
        put(out, v.as_bool() ? 0xC3 : 0xC2);
        return;
    case vtype::i64:
        encode_int(v.as_i64(), out);
        return;
    case vtype::raw_number:
        // A number too wide for a double has no msgpack representation that
        // keeps it exactly, so it travels as its text rather than silently
        // losing digits.
        encode_str(v.to_display_string(), out);
        return;
    case vtype::f32: {
        put(out, 0xCA);
        const float f = static_cast<float>(v.as_f64());
        uint32_t bits;
        std::memcpy(&bits, &f, 4);
        put_be(out, bits, 4);
        return;
    }
    case vtype::f64: {
        put(out, 0xCB);
        const double d = v.as_f64();
        uint64_t bits;
        std::memcpy(&bits, &d, 8);
        put_be(out, bits, 8);
        return;
    }
    case vtype::timestamp:
        encode_str(v.to_display_string(), out);
        return;
    case vtype::string:
        encode_str(v.as_string(), out);
        return;
    case vtype::bytes:
        encode_bin(v.as_string(), out);
        return;
    case vtype::array: {
        const size_t n = v.arr().size();
        if (n < 16)           put(out, static_cast<uint8_t>(0x90 | n));
        else if (n <= 0xFFFF) { put(out, 0xDC); put_be(out, n, 2); }
        else                  { put(out, 0xDD); put_be(out, n, 4); }
        for (const auto& el : v.arr()) encode(el, out);
        return;
    }
    case vtype::object: {
        const size_t n = v.obj().size();
        if (n < 16)           put(out, static_cast<uint8_t>(0x80 | n));
        else if (n <= 0xFFFF) { put(out, 0xDE); put_be(out, n, 2); }
        else                  { put(out, 0xDF); put_be(out, n, 4); }
        for (const auto& [k, el] : v.obj()) { encode_str(k, out); encode(el, out); }
        return;
    }
    }
}

struct reader {
    std::string_view s;
    size_t i = 0;
    // Nesting is bounded: msgpack arrives as bytes, and three bytes can ask for
    // an array inside an array inside an array without end. Unbounded recursion
    // over attacker-supplied bytes is a stack overflow, not a decode error.
    int depth = 0;
    static constexpr int max_depth = 1024;

    uint8_t byte() {
        if (i >= s.size()) throw eval_error("msgpack: truncated input");
        return static_cast<uint8_t>(s[i++]);
    }
    uint64_t be(int n) {
        uint64_t v = 0;
        for (int k = 0; k < n; ++k) v = (v << 8) | byte();
        return v;
    }
    std::string_view take(size_t n) {
        if (i + n > s.size()) throw eval_error("msgpack: truncated input");
        const std::string_view out = s.substr(i, n);
        i += n;
        return out;
    }

    value read() {
        if (++depth > max_depth) throw eval_error("msgpack: nested too deeply");
        struct pop { int& d; ~pop() { --d; } } pop_{depth};
        const uint8_t b = byte();
        if (b <= 0x7F) return value(static_cast<int64_t>(b));
        if (b >= 0xE0) return value(static_cast<int64_t>(static_cast<int8_t>(b)));
        if ((b & 0xF0) == 0x80) return read_map(b & 0x0F);
        if ((b & 0xF0) == 0x90) return read_array(b & 0x0F);
        if ((b & 0xE0) == 0xA0) return value(std::string(take(b & 0x1F)));
        switch (b) {
        case 0xC0: return value();
        case 0xC2: return value(false);
        case 0xC3: return value(true);
        case 0xC4: return value::bytes(std::string(take(static_cast<size_t>(be(1)))));
        case 0xC5: return value::bytes(std::string(take(static_cast<size_t>(be(2)))));
        case 0xC6: return value::bytes(std::string(take(static_cast<size_t>(be(4)))));
        case 0xCA: { const uint32_t bits = static_cast<uint32_t>(be(4));
                     float f; std::memcpy(&f, &bits, 4);
                     return value::float32(f); }
        case 0xCB: { const uint64_t bits = be(8);
                     double d; std::memcpy(&d, &bits, 8);
                     return value(d); }
        case 0xCC: return value(static_cast<int64_t>(be(1)));
        case 0xCD: return value(static_cast<int64_t>(be(2)));
        case 0xCE: return value(static_cast<int64_t>(be(4)));
        case 0xCF: { const uint64_t u = be(8);
                     // Degrades uint64 -> int64 -> double, as the JSON reader does.
                     if (u <= static_cast<uint64_t>(INT64_MAX))
                         return value(static_cast<int64_t>(u));
                     return value(static_cast<double>(u)); }
        case 0xD0: return value(static_cast<int64_t>(static_cast<int8_t>(be(1))));
        case 0xD1: return value(static_cast<int64_t>(static_cast<int16_t>(be(2))));
        case 0xD2: return value(static_cast<int64_t>(static_cast<int32_t>(be(4))));
        case 0xD3: return value(static_cast<int64_t>(be(8)));
        case 0xD9: return value(std::string(take(static_cast<size_t>(be(1)))));
        case 0xDA: return value(std::string(take(static_cast<size_t>(be(2)))));
        case 0xDB: return value(std::string(take(static_cast<size_t>(be(4)))));
        case 0xDC: return read_array(static_cast<size_t>(be(2)));
        case 0xDD: return read_array(static_cast<size_t>(be(4)));
        case 0xDE: return read_map(static_cast<size_t>(be(2)));
        case 0xDF: return read_map(static_cast<size_t>(be(4)));
        default:
            // The extension family (0xC7-0xC9, 0xD4-0xD8) carries a type code
            // whose meaning is application-defined; decoding one as anything in
            // particular would be a guess.
            throw eval_error("msgpack: unsupported type byte 0x" +
                             std::string(1, "0123456789abcdef"[b >> 4]) +
                             std::string(1, "0123456789abcdef"[b & 15]));
        }
    }

    value read_array(size_t n) {
        // The length is declared in the input, so it is capped before it
        // reaches reserve(): 0xDD says "four billion elements" in five bytes.
        if (n > s.size()) throw eval_error("msgpack: declared array length exceeds the input");
        std::vector<value> items;
        items.reserve(std::min<size_t>(n, 1024));
        for (size_t k = 0; k < n; ++k) items.push_back(read());
        return value::array(std::move(items));
    }

    value read_map(size_t n) {
        if (n > s.size()) throw eval_error("msgpack: declared map length exceeds the input");
        value o = value::object();
        for (size_t k = 0; k < n; ++k) {
            const value key = read();
            if (!key.is_stringy())
                throw eval_error("msgpack: only string keys can be represented");
            o.set(key.as_string(), read());
        }
        return o;
    }
};

} // namespace

value format_msgpack(const value& v) {
    std::string out;
    encode(v, out);
    return value::bytes(std::move(out));
}

value parse_msgpack(const value& v) {
    reader r{want_string(v), 0};
    value out = r.read();
    if (r.i != r.s.size()) throw eval_error("msgpack: trailing bytes after the document");
    return out;
}

} // namespace sf::m
