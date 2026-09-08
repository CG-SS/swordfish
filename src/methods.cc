// Bloblang method implementations.
//
// Each is written once and reached from both execution modes: the interpreter
// calls it through the registry, generated code calls it directly. Semantics
// follow benthos-main/internal/bloblang/query/methods*.go — including the
// details that differ from the obvious C++ reading, such as `.length()`
// counting BYTES on a string despite the documentation saying characters.
#include "swordfish/methods.hh"
#include "swordfish/value.hh"

#include "encoding_util.hh"
#include "methods_util.hh"

#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstring>
#include <numeric>
#include <string>

namespace sf::m {


// ---- coercion ---------------------------------------------------------------

value to_bool(const value& v) {
    switch (v.type()) {
    case vtype::boolean: return v;
    case vtype::i64:     return value(v.as_i64() != 0);
    case vtype::f32:
    case vtype::f64:
    case vtype::raw_number: return value(v.as_f64() != 0);
    case vtype::string: {
        const std::string& s = v.as_string();
        if (s == "true"  || s == "1") return value(true);
        if (s == "false" || s == "0") return value(false);
        throw eval_error("failed to parse string as bool: " + s);
    }
    default: wrong("bool", v);
    }
}

// Names match value.Type in internal/value/type_helpers.go.
value type_of(const value& v) { return value(std::string(v.type_name())); }

value not_null(const value& v) {
    if (v.is_null()) throw eval_error("value is null");
    return v;
}

value not_empty(const value& v) {
    switch (v.type()) {
    case vtype::string:
    case vtype::bytes:  if (v.as_string().empty()) throw eval_error("string is empty"); break;
    case vtype::array:  if (v.arr().empty())       throw eval_error("array is empty");  break;
    case vtype::object: if (v.obj().empty())       throw eval_error("object is empty"); break;
    case vtype::null:   throw eval_error("value is null");
    default: break;
    }
    return v;
}

// ---- strings ----------------------------------------------------------------

value capitalize(const value& v) {
    std::string s = want_string(v);
    bool start = true;
    for (auto& c : s) {
        const auto uc = static_cast<unsigned char>(c);
        c = start ? static_cast<char>(std::toupper(uc)) : static_cast<char>(std::tolower(uc));
        start = std::isspace(uc) != 0;
    }
    return same_stringy(v, std::move(s));
}

value trim(const value& v, const value& cutset) {
    const std::string& s = want_string(v);
    const std::string set = cutset.type() == vtype::string ? cutset.as_string() : " \t\n\r\f\v";
    const size_t b = s.find_first_not_of(set);
    if (b == std::string::npos) return same_stringy(v, std::string{});
    return same_stringy(v, s.substr(b, s.find_last_not_of(set) - b + 1));
}

value trim_prefix(const value& v, const value& prefix) {
    const std::string& s = want_string(v);
    const std::string& p = want_string(prefix);
    return same_stringy(v, s.rfind(p, 0) == 0 ? s.substr(p.size()) : s);
}

value trim_suffix(const value& v, const value& suffix) {
    const std::string& s = want_string(v);
    const std::string& p = want_string(suffix);
    const bool has = s.size() >= p.size() && s.compare(s.size() - p.size(), p.size(), p) == 0;
    return same_stringy(v, has ? s.substr(0, s.size() - p.size()) : s);
}

value has_prefix(const value& v, const value& p) {
    return value(want_string(v).rfind(want_string(p), 0) == 0);
}

value has_suffix(const value& v, const value& p) {
    const std::string& s = want_string(v);
    const std::string& q = want_string(p);
    return value(s.size() >= q.size() && s.compare(s.size() - q.size(), q.size(), q) == 0);
}

value split(const value& v, const value& delim, const value& empty_as_null) {
    const std::string& s = want_string(v);
    const std::string& d = want_string(delim);
    // With empty_as_null, an empty substring becomes null rather than "" --
    // "foo,,qux" splits to ["foo",null,"qux"]. The element is dropped from
    // neither form; only its spelling changes.
    const bool nullify = empty_as_null.type() == vtype::boolean && empty_as_null.as_bool();
    // Each piece keeps the target's stringiness: splitting bytes yields bytes.
    auto piece = [&](std::string part) {
        return (nullify && part.empty()) ? value() : same_stringy(v, std::move(part));
    };
    std::vector<value> out;
    if (d.empty()) {                       // an empty delimiter splits per byte
        for (char c : s) out.push_back(piece(std::string(1, c)));
        return value::array(std::move(out));
    }
    size_t start = 0;
    for (;;) {
        const size_t k = s.find(d, start);
        if (k == std::string::npos) { out.push_back(piece(s.substr(start))); break; }
        out.push_back(piece(s.substr(start, k - start)));
        start = k + d.size();
    }
    return value::array(std::move(out));
}

value join(const value& v, const value& delim) {
    if (!is_seq(v)) wrong("array", v);
    const std::string d = delim.type() == vtype::string ? delim.as_string() : std::string{};
    std::string out;
    bool first = true;
    for (const auto& el : v.arr()) {
        if (!first) out += d;
        first = false;
        if (el.type() != vtype::string)
            throw eval_error(std::string("cannot join a ") + el.type_name() + " element");
        out += el.as_string();
    }
    return value(std::move(out));
}

value replace_all(const value& v, const value& from, const value& to) {
    std::string s = want_string(v);
    const std::string& a = want_string(from);
    const std::string& b = want_string(to);
    // An empty needle matches at every rune boundary, before the first and
    // after the last: strings.ReplaceAll("aaa", "", "-") is "-a-a-a-", not
    // "aaa". Returning the input unchanged was the plausible-looking answer.
    if (a.empty()) {
        std::string out = b;
        for (size_t i = 0; i < s.size();) {
            const size_t n = enc::utf8_rune_len(s, i);
            out.append(s, i, n);
            out += b;
            i += n;
        }
        return same_stringy(v, std::move(out));
    }
    std::string out;
    size_t start = 0;
    for (;;) {
        const size_t k = s.find(a, start);
        if (k == std::string::npos) { out += s.substr(start); break; }
        out += s.substr(start, k - start);
        out += b;
        start = k + a.size();
    }
    return same_stringy(v, std::move(out));
}

value reverse(const value& v) {
    // By RUNE. Reversing the bytes of "日本語" produces a sequence that is not
    // valid UTF-8 at all -- it corrupts the string rather than reversing it --
    // and the reference reverses runes.
    const std::string& s = want_string(v);
    std::string out;
    out.reserve(s.size());
    for (size_t i = s.size(); i > 0;) {
        size_t start = i - 1;
        // Step back over continuation bytes to the lead byte of this rune.
        while (start > 0 && (static_cast<unsigned char>(s[start]) & 0xC0) == 0x80) --start;
        const size_t n = enc::utf8_rune_len(s, start);
        // A lead byte whose sequence does not reach `i` is malformed; copy the
        // single byte rather than guessing.
        out.append(s, start, std::min(n, i - start));
        i = start;
    }
    return same_stringy(v, std::move(out));
}

value quote(const value& v) {
    // JSON string quoting, which is what Go's strconv.Quote produces for the
    // cases Bloblang cares about.
    value s(want_string(v));
    return value(s.to_json());
}

value index_of(const value& v, const value& needle) {
    const std::string& s = want_string(v);
    const size_t k = s.find(want_string(needle));
    return value(k == std::string::npos ? int64_t{-1} : static_cast<int64_t>(k));
}

value repeat(const value& v, const value& count) {
    const std::string& s = want_string(v);
    const int64_t n = count.is_number() ? count.as_i64() : 0;
    if (n <= 0) return same_stringy(v, std::string{});
    std::string out;
    out.reserve(s.size() * static_cast<size_t>(n));
    for (int64_t i = 0; i < n; ++i) out += s;
    return same_stringy(v, std::move(out));
}

value slice(const value& v, const value& low, const value& high) {
    const int64_t lo_in = low.is_number() ? low.as_i64() : 0;
    // Bytes count as a string target here: testing for the string type alone
    // sent a bytes value into the array branch below, where it failed.
    if (v.is_stringy()) {
        const std::string& s = v.as_string();
        const int64_t len = static_cast<int64_t>(s.size());
        int64_t lo = norm_index(lo_in, s.size());
        int64_t hi = high.is_number() ? norm_index(high.as_i64(), s.size()) : len;
        lo = std::clamp<int64_t>(lo, 0, len);
        hi = std::clamp<int64_t>(hi, lo, len);
        return same_stringy(v, s.substr(static_cast<size_t>(lo), static_cast<size_t>(hi - lo)));
    }
    if (is_seq(v)) {
        const auto& a = v.arr();
        const int64_t len = static_cast<int64_t>(a.size());
        int64_t lo = norm_index(lo_in, a.size());
        int64_t hi = high.is_number() ? norm_index(high.as_i64(), a.size()) : len;
        lo = std::clamp<int64_t>(lo, 0, len);
        hi = std::clamp<int64_t>(hi, lo, len);
        return value::array(std::vector<value>(a.begin() + lo, a.begin() + hi));
    }
    wrong("string or array", v);
}

value contains(const value& v, const value& needle) {
    if (v.is_stringy())
        return value(v.as_string().find(want_string(needle)) != std::string::npos);
    if (is_seq(v)) {
        for (const auto& el : v.arr()) if (el == needle) return value(true);
        return value(false);
    }
    if (v.type() == vtype::object) {
        for (const auto& [k, el] : v.obj()) { (void)k; if (el == needle) return value(true); }
        return value(false);
    }
    wrong("string, array or object", v);
}

// ---- encoding ---------------------------------------------------------------

namespace {
// ZeroMQ's base-85, in its own alphabet order. Unlike ascii85 it has no
// zero-run shorthand and requires a length that is a multiple of four.
const char* Z85 =
    "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#";

std::string b64_decode_or_throw(std::string_view in, const char* alpha) {
    // The comment that used to sit here said "the reference tolerates
    // whitespace inside base64", and that is true of NEWLINES and nothing else.
    // Passing strict=false and then discarding the return value meant this
    // could never fail: `"YW Jj ZA=="` decoded happily where the reference says
    // `illegal base64 data at input byte 2`, `"YWJ*jZA=="` likewise, and a
    // truncated `"QUJDR"` gave three bytes where the reference says
    // `unexpected EOF`. Every other decoder in this file -- ascii85, z85, hex --
    // rejects bad input; base64 was the only one that did not.
    size_t chars = 0;
    for (size_t i = 0; i < in.size(); ++i) {
        const char c = in[i];
        if (c == '\r' || c == '\n') continue;   // the one tolerance Go allows
        if (c == '=') break;                     // padding, and the tail with it
        if (!c || !std::strchr(alpha, c))
            throw eval_error("illegal base64 data at input byte " + std::to_string(i));
        ++chars;
    }
    // A final group of ONE character carries six bits, which cannot complete a
    // byte. Groups of two and three are legal, padded or not.
    if (chars % 4 == 1) throw eval_error("unexpected EOF");
    std::string out;
    enc::b64_decode(in, out, alpha, /*strict=*/false);
    return out;
}

std::string hex_decode(std::string_view in) {
    if (in.size() % 2) throw eval_error("hex string has an odd length");
    std::string out;
    out.reserve(in.size() / 2);
    for (size_t i = 0; i < in.size(); i += 2) {
        const int hi = enc::hex_nibble(in[i]), lo = enc::hex_nibble(in[i + 1]);
        if (hi < 0 || lo < 0) throw eval_error("invalid hex digit");
        out += static_cast<char>(hi * 16 + lo);
    }
    return out;
}
} // namespace

// Adobe's ascii85 as Go's encoding/ascii85 implements it: no <~ ~> wrapper,
// "z" as the shorthand for four zero bytes, and a final partial group padded
// with zeros and emitted one character longer than the bytes it carries.
std::string a85_encode(std::string_view in) {
    std::string out;
    out.reserve(in.size() * 5 / 4 + 4);
    size_t i = 0;
    for (; i < in.size(); i += 4) {
        const size_t n = std::min<size_t>(4, in.size() - i);
        uint32_t v = 0;
        for (size_t j = 0; j < 4; ++j)
            v = (v << 8) | (j < n ? static_cast<unsigned char>(in[i + j]) : 0u);
        if (v == 0 && n == 4) { out += 'z'; continue; }
        char g[5];
        for (int j = 4; j >= 0; --j) { g[j] = static_cast<char>('!' + v % 85); v /= 85; }
        out.append(g, n + 1);
    }
    return out;
}

std::string a85_decode(std::string_view in) {
    std::string out;
    uint32_t v = 0;
    int n = 0;
    auto flush = [&](int count) {
        // A partial group is padded with the highest digit before decoding,
        // then truncated -- the inverse of the encoder's zero padding.
        for (int j = count; j < 5; ++j) v = v * 85 + 84;
        for (int j = 3; j >= 4 - (count - 1); --j)
            out += static_cast<char>((v >> (8 * j)) & 0xFF);
    };
    for (char c : in) {
        if (std::isspace(static_cast<unsigned char>(c))) continue;
        if (c == 'z' && n == 0) { out.append(4, '\0'); continue; }
        if (c < '!' || c > 'u') throw eval_error("invalid ascii85 data");
        v = v * 85 + static_cast<uint32_t>(c - '!');
        if (++n == 5) {
            for (int j = 3; j >= 0; --j) out += static_cast<char>((v >> (8 * j)) & 0xFF);
            v = 0;
            n = 0;
        }
    }
    if (n == 1) throw eval_error("invalid ascii85 data");
    if (n > 1) flush(n);
    return out;
}

std::string z85_encode(std::string_view in) {
    if (in.size() % 4 != 0) throw eval_error("z85 input length must be a multiple of 4");
    std::string out;
    out.reserve(in.size() / 4 * 5);
    for (size_t i = 0; i < in.size(); i += 4) {
        uint32_t v = 0;
        for (size_t j = 0; j < 4; ++j) v = (v << 8) | static_cast<unsigned char>(in[i + j]);
        char g[5];
        for (int j = 4; j >= 0; --j) { g[j] = Z85[v % 85]; v /= 85; }
        out.append(g, 5);
    }
    return out;
}

std::string z85_decode(std::string_view in) {
    if (in.size() % 5 != 0) throw eval_error("z85 input length must be a multiple of 5");
    std::string out;
    out.reserve(in.size() / 5 * 4);
    for (size_t i = 0; i < in.size(); i += 5) {
        uint32_t v = 0;
        for (size_t j = 0; j < 5; ++j) {
            const char* p = std::strchr(Z85, in[i + j]);
            if (!p || !in[i + j]) throw eval_error("invalid z85 data");
            v = v * 85 + static_cast<uint32_t>(p - Z85);
        }
        for (int j = 3; j >= 0; --j) out += static_cast<char>((v >> (8 * j)) & 0xFF);
    }
    return out;
}

value encode(const value& v, const value& scheme) {
    const std::string& s = want_string(v);
    const std::string& k = want_string(scheme);
    if (k == "base64")       return value(enc::b64_encode(s));
    if (k == "base64url")    return value(enc::b64_encode(s, enc::B64_URL));
    if (k == "base64rawurl") return value(enc::b64_encode(s, enc::B64_URL, false));
    if (k == "hex")          return value(enc::hex_encode(s));
    if (k == "ascii85")      return value(a85_encode(s));
    if (k == "z85")          return value(z85_encode(s));
    throw eval_error("unrecognized encoding type: " + k);
}

value decode(const value& v, const value& scheme) {
    const std::string& s = want_string(v);
    const std::string& k = want_string(scheme);
    // Every decoder yields bytes; encoders yield strings.
    if (k == "base64")       return value::bytes(b64_decode_or_throw(s, enc::B64_STD));
    if (k == "base64url" || k == "base64rawurl")
                             return value::bytes(b64_decode_or_throw(s, enc::B64_URL));
    if (k == "hex")          return value::bytes(hex_decode(s));
    if (k == "ascii85")      return value::bytes(a85_decode(s));
    if (k == "z85")          return value::bytes(z85_decode(s));
    throw eval_error("unrecognized decoding type: " + k);
}

// ---- numbers ----------------------------------------------------------------

value abs_(const value& v) {
    if (v.type() == vtype::i64) {
        const int64_t x = v.as_i64();
        // Negating INT64_MIN is undefined behaviour, and its true absolute
        // value is not representable as an int64 either. The reference
        // saturates to INT64_MAX -- off by one, but that is the answer a config
        // ported from it expects, and parity beats being right here. Returning
        // the exact value as a double instead was the earlier choice.
        if (x == INT64_MIN) return value(INT64_MAX);
        return value(x < 0 ? -x : x);
    }
    if (v.is_float()) return value(std::fabs(v.as_f64()));
    wrong("number", v);
}

value ceil_(const value& v) {
    if (v.type() == vtype::i64) return v;
    return value(std::ceil(v.as_f64()));
}

value round_(const value& v) {
    if (v.type() == vtype::i64) return v;
    return value(std::round(v.as_f64()));
}

namespace {
value extremum(const value& v, bool want_max) {
    if (!is_seq(v)) wrong("array", v);
    const auto& a = v.arr();
    if (a.empty()) throw eval_error("cannot take min/max of an empty array");
    double best = a[0].as_f64();
    for (const auto& el : a) {
        const double d = el.as_f64();
        if (want_max ? d > best : d < best) best = d;
    }
    return value(best);
}
} // namespace

value min_(const value& v) { return extremum(v, false); }
value max_(const value& v) { return extremum(v, true); }

value sum(const value& v) {
    if (!is_seq(v)) wrong("array", v);
    // Stays integral while every element is, matching the numeric tower.
    bool all_int = true;
    for (const auto& el : v.arr()) if (el.type() != vtype::i64) all_int = false;
    if (all_int) {
        int64_t total = 0;
        for (const auto& el : v.arr()) total = num::fast_add(total, el.as_i64());
        return value(total);
    }
    double total = 0;
    for (const auto& el : v.arr()) total += el.as_f64();
    return value(total);
}

// ---- structured -------------------------------------------------------------

value keys(const value& v) {
    if (v.type() != vtype::object) wrong("object", v);
    std::vector<value> out;
    out.reserve(v.obj().size());
    for (const auto& [k, el] : v.obj()) { (void)el; out.push_back(value(k)); }
    return value::array(std::move(out));
}

value values(const value& v) {
    if (v.type() != vtype::object) wrong("object", v);
    std::vector<value> out;
    out.reserve(v.obj().size());
    for (const auto& [k, el] : v.obj()) { (void)k; out.push_back(el); }
    return value::array(std::move(out));
}

value index(const value& v, const value& i) {
    // A byte array indexes to a NUMBER, not to a one-character string:
    // `"foobar".bytes().index(0)` is 102.
    if (v.type() == vtype::bytes) {
        const std::string& b = v.as_string();
        const int64_t k = norm_index(i.is_number() ? i.as_i64() : 0, b.size());
        if (k < 0 || static_cast<size_t>(k) >= b.size())
            throw eval_error("array index out of bounds");
        return value(static_cast<int64_t>(static_cast<unsigned char>(b[static_cast<size_t>(k)])));
    }
    if (!is_seq(v)) wrong("array", v);
    const auto& a = v.arr();
    const int64_t k = norm_index(i.is_number() ? i.as_i64() : 0, a.size());
    if (k < 0 || static_cast<size_t>(k) >= a.size())
        throw eval_error("array index out of bounds");
    return a[static_cast<size_t>(k)];
}

value get_path(const value& v, const value& path) {
    // Null for a path that is not there, matching plain field access and the
    // reference. Erroring instead made `this.get("a.z.c")` fail where the
    // reference returns null.
    const std::string& p = want_string(path);
    const value* cur = &v;
    size_t start = 0;
    value held;
    while (start <= p.size()) {
        const size_t dot = p.find('.', start);
        const std::string key = p.substr(start, dot == std::string::npos
                                                ? std::string::npos : dot - start);
        if (cur->type() != vtype::object) return value();
        const value* next = cur->find(key);
        if (!next) return value();
        held = *next;
        cur = &held;
        if (dot == std::string::npos) break;
        start = dot + 1;
    }
    return held;
}

value append(const value& v, const std::vector<value>& items) {
    if (!is_seq(v)) wrong("array", v);
    std::vector<value> out = v.arr();
    out.insert(out.end(), items.begin(), items.end());
    return value::array(std::move(out));
}

value merge(const value& v, const value& with) {
    if (v.type() == vtype::object && with.type() == vtype::object) {
        // "conflicting keys create arrays containing both values" -- merge is
        // NOT an overwrite; `assign` is the overwriting variant.
        value out = v;
        for (const auto& [k, el] : with.obj()) {
            const value* existing = out.find(k);
            if (!existing) { out.set(k, el); continue; }
            std::vector<value> both;
            if (existing->type() == vtype::array)
                both = existing->arr();
            else
                both.push_back(*existing);
            if (el.type() == vtype::array)
                both.insert(both.end(), el.arr().begin(), el.arr().end());
            else
                both.push_back(el);
            out.set(k, value::array(std::move(both)));
        }
        return out;
    }
    if (is_seq(v) && is_seq(with)) {
        std::vector<value> out = v.arr();
        out.insert(out.end(), with.arr().begin(), with.arr().end());
        return value::array(std::move(out));
    }
    throw eval_error(std::string("cannot merge ") + v.type_name() + " with " + with.type_name());
}

value unique(const value& v) {
    if (!is_seq(v)) wrong("array", v);
    std::vector<value> out;
    for (const auto& el : v.arr()) {
        bool seen = false;
        for (const auto& o : out) if (o == el) { seen = true; break; }
        if (!seen) out.push_back(el);
    }
    return value::array(std::move(out));
}

value sort_(const value& v) {
    if (!is_seq(v)) wrong("array", v);
    std::vector<value> out = v.arr();
    std::stable_sort(out.begin(), out.end(), [](const value& a, const value& b) {
        if (a.is_number() && b.is_number()) return a.as_f64() < b.as_f64();
        if (a.type() == vtype::string && b.type() == vtype::string)
            return a.as_string() < b.as_string();
        throw eval_error("cannot sort a mixed-type array");
    });
    return value::array(std::move(out));
}

value flatten(const value& v) {
    if (!is_seq(v)) wrong("array", v);
    std::vector<value> out;
    for (const auto& el : v.arr()) {
        if (el.type() == vtype::array)
            for (const auto& inner : el.arr()) out.push_back(inner);
        else out.push_back(el);
    }
    return value::array(std::move(out));
}

// ---- parsing ----------------------------------------------------------------

value parse_json_m(const value& v, const value& use_number) {
    return parse_json(want_string(v),
                      use_number.type() == vtype::boolean && use_number.as_bool());
}

value format_json(const value& v, const value& indent, const value& no_indent,
                  const value& escape_html) {
    // Defaults from methods_strings.go: four spaces of indent, indentation on,
    // and HTML escaping ON -- which differs from ordinary message encoding,
    // where encodeJSON sets SetEscapeHTML(false).
    const std::string ind = indent.type() == vtype::string ? indent.as_string()
                                                           : std::string(4, ' ');
    const bool compact = no_indent.type() == vtype::boolean && no_indent.as_bool();
    const bool escape  = escape_html.type() != vtype::boolean || escape_html.as_bool();
    // Bytes, not a string: the reference's format_json yields a byte array, so
    // a mapping that puts it in a field gets base64 rather than the JSON text.
    return value::bytes(v.to_json_pretty(compact ? std::string_view{} : std::string_view(ind),
                                         escape));
}

} // namespace sf::m

namespace sf {
// Shared with value.cc, which base64-encodes a bytes value when serialising it:
// Go's encoding/json does the same to a []byte, so the two must not drift.
std::string b64_encode_std(std::string_view in) { return enc::b64_encode(in); }
}
