// Bloblang numeric methods: trigonometry, logarithms, bit operations and the
// fixed-width integer conversions.
//
// The conversions are the subtle ones. `.int8()` is not a cast: a value that
// does not fit, or that carries a fractional part, is an ERROR rather than a
// truncation, because the point of the method is to hand a downstream component
// (a SQL driver, say) a value it can store. Silently wrapping 300 to 44 would
// defeat that.
#include "swordfish/methods.hh"
#include "swordfish/value.hh"

#include "methods_util.hh"

#include <cctype>
#include <cerrno>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <string>

namespace sf::m {

namespace {

double want_number(const value& v) {
    if (!v.is_number()) wrong("number", v);
    return v.as_f64();
}

// Go's strconv.ParseInt with base 0: a 0x/0o/0b prefix selects the base, a
// leading 0 means octal, and underscores are permitted as separators. The
// documented `"0xDEADBEEF".int64()` example depends on the prefix handling.
int64_t parse_go_int(const std::string& s, bool& ok) {
    ok = false;
    std::string t;
    t.reserve(s.size());
    for (char c : s) if (c != '_') t += c;
    if (t.empty()) return 0;
    // strtoll's base 0 understands 0x and a leading 0 for octal, but not Go's
    // 0b and 0o, so those are translated to an explicit base.
    int base = 0;
    size_t off = 0;
    bool neg = false;
    if (t[0] == '+' || t[0] == '-') { neg = t[0] == '-'; off = 1; }
    if (t.size() > off + 1 && t[off] == '0') {
        const char c = static_cast<char>(std::tolower(static_cast<unsigned char>(t[off + 1])));
        if (c == 'b') { base = 2;  t = (neg ? "-" : "") + t.substr(off + 2); }
        else if (c == 'o') { base = 8; t = (neg ? "-" : "") + t.substr(off + 2); }
    }
    if (t.empty()) return 0;
    errno = 0;
    char* end = nullptr;
    const long long r = std::strtoll(t.c_str(), &end, base);
    if (errno == ERANGE || end == t.c_str() || *end != '\0') return 0;
    ok = true;
    return static_cast<int64_t>(r);
}

uint64_t parse_go_uint(const std::string& s, bool& ok) {
    ok = false;
    std::string t;
    t.reserve(s.size());
    for (char c : s) if (c != '_') t += c;
    // strtoull accepts a leading '-' and wraps; Go's ParseUint rejects it.
    if (t.empty() || t[0] == '-') return 0;
    int base = 0;
    size_t off = t[0] == '+' ? 1 : 0;
    if (t.size() > off + 1 && t[off] == '0') {
        const char c = static_cast<char>(std::tolower(static_cast<unsigned char>(t[off + 1])));
        if (c == 'b') { base = 2;  t = t.substr(off + 2); }
        else if (c == 'o') { base = 8; t = t.substr(off + 2); }
    }
    if (t.empty()) return 0;
    errno = 0;
    char* end = nullptr;
    const unsigned long long r = std::strtoull(t.c_str(), &end, base);
    if (errno == ERANGE || end == t.c_str() || *end != '\0') return 0;
    ok = true;
    return static_cast<uint64_t>(r);
}

// The shared body of int8..int64. `bits` is the width; the range check is
// performed at that width, so `.int8()` on 300 errors rather than wrapping.
value to_signed(const value& v, int bits) {
    int64_t n = 0;
    switch (v.type()) {
    case vtype::i64:
        n = v.as_i64();
        break;
    case vtype::f32:
    case vtype::f64:
    // A raw number goes the same way: it is a number, and the width check
    // below is what the method is for.
    case vtype::raw_number: {
        const double d = v.as_f64();
        if (std::isnan(d) || std::isinf(d) || d != std::trunc(d))
            throw eval_error("cannot convert a fractional number to an integer, "
                             "use round() first");
        if (d < -9.2233720368547758e18 || d >= 9.2233720368547758e18)
            throw eval_error("number is out of range for an integer");
        n = static_cast<int64_t>(d);
        break;
    }
    case vtype::string:
    case vtype::bytes: {
        bool ok = false;
        n = parse_go_int(v.as_string(), ok);
        if (!ok) throw eval_error("failed to parse string as an integer: " + v.as_string());
        break;
    }
    default:
        wrong("number", v);
    }
    if (bits < 64) {
        const int64_t lo = -(int64_t{1} << (bits - 1));
        const int64_t hi =  (int64_t{1} << (bits - 1)) - 1;
        if (n < lo || n > hi)
            throw eval_error("value " + std::to_string(n) + " is out of range for an int" +
                             std::to_string(bits));
    }
    return value(n);
}

value to_unsigned(const value& v, int bits) {
    uint64_t n = 0;
    switch (v.type()) {
    case vtype::i64: {
        const int64_t s = v.as_i64();
        if (s < 0) throw eval_error("value " + std::to_string(s) +
                                    " is out of range for an unsigned integer");
        n = static_cast<uint64_t>(s);
        break;
    }
    case vtype::f32:
    case vtype::f64:
    case vtype::raw_number: {
        const double d = v.as_f64();
        if (std::isnan(d) || std::isinf(d) || d != std::trunc(d))
            throw eval_error("cannot convert a fractional number to an integer, "
                             "use round() first");
        if (d < 0 || d >= 1.8446744073709552e19)
            throw eval_error("number is out of range for an unsigned integer");
        n = static_cast<uint64_t>(d);
        break;
    }
    case vtype::string:
    case vtype::bytes: {
        bool ok = false;
        n = parse_go_uint(v.as_string(), ok);
        if (!ok) throw eval_error("failed to parse string as an unsigned integer: " +
                                  v.as_string());
        break;
    }
    default:
        wrong("number", v);
    }
    if (bits < 64) {
        const uint64_t hi = (uint64_t{1} << bits) - 1;
        if (n > hi)
            throw eval_error("value " + std::to_string(n) + " is out of range for a uint" +
                             std::to_string(bits));
    }
    // uint64 values above INT64_MAX cannot be represented; Go keeps them as a
    // distinct uint64 type, which this value model does not have, so an
    // out-of-range value is an error rather than a negative number.
    if (n > static_cast<uint64_t>(std::numeric_limits<int64_t>::max()))
        throw eval_error("unsigned value exceeds the representable integer range");
    return value(static_cast<int64_t>(n));
}

// The bit operations are defined on integers; a float that is exactly integral
// is accepted, anything else is not.
int64_t want_integer(const value& v) {
    if (v.type() == vtype::i64) return v.as_i64();
    if (v.is_float()) {
        const double d = v.as_f64();
        if (d == std::trunc(d) && std::isfinite(d)) return static_cast<int64_t>(d);
        throw eval_error("expected an integer, got a fractional number");
    }
    wrong("integer", v);
}

} // namespace

// ---- trigonometry and logarithms ---------------------------------------------

value cos_(const value& v) { return value(std::cos(want_number(v))); }
value sin_(const value& v) { return value(std::sin(want_number(v))); }
value tan_(const value& v) { return value(std::tan(want_number(v))); }
value log_(const value& v) { return value(std::log(want_number(v))); }
value log10_(const value& v) { return value(std::log10(want_number(v))); }

value pow_(const value& v, const value& exponent) {
    return value(std::pow(want_number(v), want_number(exponent)));
}

// ---- bit operations ------------------------------------------------------------

value bitwise_and(const value& v, const value& other) {
    return value(want_integer(v) & want_integer(other));
}
value bitwise_or(const value& v, const value& other) {
    return value(want_integer(v) | want_integer(other));
}
value bitwise_xor(const value& v, const value& other) {
    return value(want_integer(v) ^ want_integer(other));
}

// ---- fixed-width conversions ----------------------------------------------------

value int8_(const value& v)  { return to_signed(v, 8); }
value int16_(const value& v) { return to_signed(v, 16); }
value int32_(const value& v) { return to_signed(v, 32); }
value int64_(const value& v) { return to_signed(v, 64); }

value uint8_(const value& v)  { return to_unsigned(v, 8); }
value uint16_(const value& v) { return to_unsigned(v, 16); }
value uint32_(const value& v) { return to_unsigned(v, 32); }
value uint64_(const value& v) { return to_unsigned(v, 64); }

value float64_(const value& v) {
    if (v.is_number()) return value(v.as_f64());
    if (v.is_stringy()) {
        const std::string& s = v.as_string();
        errno = 0;
        char* end = nullptr;
        const double d = std::strtod(s.c_str(), &end);
        if (end == s.c_str() || *end != '\0' || errno == ERANGE)
            throw eval_error("failed to parse string as a float: " + s);
        return value(d);
    }
    wrong("number", v);
}

value float32_(const value& v) {
    const value wide = float64_(v);
    const double d = wide.as_f64();
    if (std::isfinite(d) &&
        (d > std::numeric_limits<float>::max() || d < std::numeric_limits<float>::lowest()))
        throw eval_error("number is out of range for a 32-bit float");
    return value::float32(static_cast<float>(d));
}

} // namespace sf::m
