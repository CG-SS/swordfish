#include "swordfish/value.hh"

#include "case_tables.hh"
#include "encoding_util.hh"

#include <algorithm>
#include <cmath>
#include <cstdio>
#include <cinttypes>
#include <cctype>
#include <cstdlib>
#include <cstring>
#include <charconv>

namespace sf {

// ---- E1: payload pooling ---------------------------------------------------
//
// The ablation in bench/ showed the cost of a patch is malloc/free churn, not
// the copy-on-write clone itself: a single `root.sq = ...` costs ~190ns, which
// is roughly four allocator round trips (obj_data, its entry vector, and their
// destruction). Messages are naturally scoped, so a per-shard freelist that
// RETAINS each vector's capacity removes that churn entirely.
//
// A stand-in for what a Seastar per-shard arena would do properly.
namespace {

constexpr size_t pool_cap = 512;

// Payloads are cleared when they are RETURNED to the pool, not when they are
// taken out of it. That ordering matters: a pooled object that still held its
// entries would destroy nested values during pool teardown, and each of those
// destructions calls back into free_*() -- pushing onto the very free list being
// iterated. Clearing on free means pooled payloads are empty, so teardown cannot
// re-enter. `draining` is a second belt for any free that arrives mid-teardown.
template <class T>
struct payload_pool {
    std::vector<T*> free_list;
    bool draining = false;
    ~payload_pool() {
        draining = true;
        for (auto* p : free_list) delete p;
        free_list.clear();
    }
};

thread_local payload_pool<obj_data> g_obj;
thread_local payload_pool<arr_data> g_arr;
thread_local payload_pool<str_data> g_str;

// Recycled payloads are already empty but keep their capacity, so reuse usually
// needs no allocation at all -- which is the entire point of the pool.
obj_data* alloc_obj() {
    auto& fl = g_obj.free_list;
    if (!fl.empty()) { auto* d = fl.back(); fl.pop_back(); d->rc = 1; return d; }
    return new obj_data;
}
arr_data* alloc_arr() {
    auto& fl = g_arr.free_list;
    if (!fl.empty()) { auto* d = fl.back(); fl.pop_back(); d->rc = 1; return d; }
    return new arr_data;
}
str_data* alloc_str() {
    auto& fl = g_str.free_list;
    if (!fl.empty()) { auto* d = fl.back(); fl.pop_back(); d->rc = 1; return d; }
    return new str_data;
}

void free_obj(obj_data* d) {
    d->e.clear();                       // releases nested values here, not at teardown
    auto& fl = g_obj.free_list;
    if (!g_obj.draining && fl.size() < pool_cap) fl.push_back(d); else delete d;
}
void free_arr(arr_data* d) {
    d->v.clear();
    auto& fl = g_arr.free_list;
    if (!g_arr.draining && fl.size() < pool_cap) fl.push_back(d); else delete d;
}
void free_str(str_data* d) {
    d->s.clear();
    auto& fl = g_str.free_list;
    if (!g_str.draining && fl.size() < pool_cap) fl.push_back(d); else delete d;
}

} // namespace

rc_base* value::make_str(std::string s) { auto* d = alloc_str(); d->s = std::move(s); return d; }
rc_base* value::make_arr(std::vector<value> v) { auto* d = alloc_arr(); d->v = std::move(v); return d; }
rc_base* value::make_obj() { return alloc_obj(); }

// ---- lifetime --------------------------------------------------------------

void value::release() noexcept {
    switch (t_) {
    case vtype::string:
    case vtype::bytes:
    case vtype::raw_number:
        if (p_ && --p_->rc == 0) free_str(static_cast<str_data*>(p_));
        break;
    case vtype::array:  if (p_ && --p_->rc == 0) free_arr(static_cast<arr_data*>(p_)); break;
    case vtype::object: if (p_ && --p_->rc == 0) free_obj(static_cast<obj_data*>(p_)); break;
    default: break;
    }
    p_ = nullptr;
}

void value::copy_payload(const value& o) noexcept {
    switch (t_) {
    case vtype::boolean: b_ = o.b_; break;
    case vtype::i64:     i_ = o.i_; break;
    case vtype::timestamp: i_ = o.i_; tz_ = o.tz_; break;
    case vtype::f32:
    case vtype::f64:     f_ = o.f_; break;
    case vtype::string: case vtype::bytes: case vtype::raw_number:
    case vtype::array: case vtype::object:
        p_ = o.p_; if (p_) ++p_->rc; break;
    default: p_ = nullptr; break;
    }
}

void value::move_payload(value& o) noexcept {
    switch (t_) {
    case vtype::boolean: b_ = o.b_; break;
    case vtype::i64:     i_ = o.i_; break;
    case vtype::timestamp: i_ = o.i_; tz_ = o.tz_; break;
    case vtype::f32:
    case vtype::f64:     f_ = o.f_; break;
    case vtype::string: case vtype::bytes: case vtype::raw_number:
    case vtype::array: case vtype::object:
        p_ = o.p_; o.p_ = nullptr; o.t_ = vtype::null; break;
    default: p_ = nullptr; break;
    }
}

// Copy-on-write: only clone when the payload is actually shared.
void value::unshare() {
    if (!p_ || p_->rc == 1) return;
    switch (t_) {
    case vtype::string:
    case vtype::bytes:
    case vtype::raw_number: { auto* d = alloc_str(); d->s = static_cast<str_data*>(p_)->s; release(); p_ = d; break; }
    case vtype::array:  { auto* d = alloc_arr(); d->v = static_cast<arr_data*>(p_)->v; release(); p_ = d; break; }
    case vtype::object: { auto* d = alloc_obj(); d->e = static_cast<obj_data*>(p_)->e; release(); p_ = d; break; }
    default: break;
    }
}

const char* value::type_name() const noexcept {
    switch (t_) {
    case vtype::null: return "null";       case vtype::boolean: return "bool";
    case vtype::i64:  return "number";     case vtype::f64:     return "number";
    case vtype::f32:  return "number";
    case vtype::timestamp: return "timestamp";
    case vtype::raw_number: return "number";
    case vtype::string: return "string";   case vtype::bytes:   return "bytes";
    case vtype::array:  return "array";
    case vtype::object: return "object";   case vtype::deleted: return "delete";
    case vtype::nothing: return "nothing";
    }
    return "unknown";
}

// ---- accessors -------------------------------------------------------------

[[noreturn]] static void wrong_type(const char* want, const value& got) {
    throw type_error(want, got.type_name());
}

bool value::as_bool() const { if (t_ != vtype::boolean) wrong_type("bool", *this); return b_; }

int64_t value::as_i64() const {
    if (t_ == vtype::i64) return i_;
    if (t_ == vtype::f64 || t_ == vtype::f32) return static_cast<int64_t>(f_);
    if (t_ == vtype::raw_number)
        return static_cast<int64_t>(std::strtoll(static_cast<str_data*>(p_)->s.c_str(),
                                                 nullptr, 10));
    wrong_type("number", *this);
}

double value::as_f64() const {
    if (t_ == vtype::f64 || t_ == vtype::f32) return f_;
    if (t_ == vtype::i64) return static_cast<double>(i_);
    if (t_ == vtype::raw_number)
        return std::strtod(static_cast<str_data*>(p_)->s.c_str(), nullptr);
    wrong_type("number", *this);
}

const std::string& value::as_string() const {
    if (!is_stringy()) wrong_type("string", *this);
    return static_cast<str_data*>(p_)->s;
}

const std::vector<value>& value::arr() const {
    if (t_ != vtype::array) wrong_type("array", *this);
    return static_cast<arr_data*>(p_)->v;
}

const std::vector<std::pair<std::string, value>>& value::obj() const {
    if (t_ != vtype::object) wrong_type("object", *this);
    return static_cast<obj_data*>(p_)->e;
}

// The array half of the mutating accessor pair a `cpp:` block programs against.
// It used to carry a cppcheck unusedFunction suppression; `xml_add` uses it now
// -- appending in place rather than copying the whole array out and back -- so
// the suppression went stale and cppcheck said so.
std::vector<value>& value::arr_mut() {
    if (t_ != vtype::array) wrong_type("array", *this);
    unshare();
    return static_cast<arr_data*>(p_)->v;
}

std::vector<std::pair<std::string, value>>& value::obj_mut() {
    if (t_ != vtype::object) wrong_type("object", *this);
    unshare();
    return static_cast<obj_data*>(p_)->e;
}

// Entries are kept sorted, so lookup is a binary search and serialisation is
// already in Go's key order.
static auto lower(std::vector<std::pair<std::string, value>>& e, std::string_view k) {
    return std::lower_bound(e.begin(), e.end(), k,
        [](const auto& p, std::string_view key) { return p.first < key; });
}
static auto lower(const std::vector<std::pair<std::string, value>>& e, std::string_view k) {
    return std::lower_bound(e.begin(), e.end(), k,
        [](const auto& p, std::string_view key) { return p.first < key; });
}

const value* value::find(std::string_view key) const noexcept {
    if (t_ != vtype::object) return nullptr;
    const auto& e = static_cast<obj_data*>(p_)->e;
    auto it = lower(e, key);
    return (it != e.end() && it->first == key) ? &it->second : nullptr;
}

const value& value::get(std::string_view key) const {
    if (t_ != vtype::object) wrong_type("object", *this);
    const value* v = find(key);
    if (!v) throw eval_error("field not found: " + std::string(key));
    return *v;
}

void value::set(std::string_view key, value v) {
    if (t_ != vtype::object) { release(); t_ = vtype::object; p_ = make_obj(); }
    auto& e = obj_mut();
    auto it = lower(e, key);
    if (it != e.end() && it->first == key) {
        if (v.is_deleted()) e.erase(it); else it->second = std::move(v);
    } else if (!v.is_deleted()) {
        e.insert(it, {std::string(key), std::move(v)});
    }
}

void value::remove(std::string_view key) {
    if (t_ != vtype::object) return;
    auto& e = obj_mut();
    auto it = lower(e, key);
    if (it != e.end() && it->first == key) e.erase(it);
}

bool value::operator==(const value& o) const noexcept {
    // Deliberately reaches the members directly rather than through as_f64(),
    // as_string(), arr() or obj(): those throw on a type mismatch, and this
    // function is noexcept, so a future edit that dropped one of the type checks
    // would terminate the process rather than return false.
    if (is_number() && o.is_number()) {
        if (t_ == vtype::i64 && o.t_ == vtype::i64) return i_ == o.i_;
        // A raw number holds text, so it goes the long way round through
        // as_f64(); the fast members below are only valid for the unboxed
        // arms, and reading f_ from a pointer payload would be nonsense.
        auto num = [](const value& v) noexcept -> double {
            switch (v.t_) {
            case vtype::i64: return static_cast<double>(v.i_);
            case vtype::raw_number:
                return std::strtod(static_cast<str_data*>(v.p_)->s.c_str(), nullptr);
            default: return v.f_;
            }
        };
        if (t_ == vtype::raw_number || o.t_ == vtype::raw_number) return num(*this) == num(o);
        const double a = t_ == vtype::i64 ? static_cast<double>(i_) : f_;
        const double b = o.t_ == vtype::i64 ? static_cast<double>(o.i_) : o.f_;
        return a == b;
    }
    // A []byte compares equal to the string with the same contents, because
    // RestrictForComparison turns the former into the latter first.
    if (is_stringy() && o.is_stringy())
        return static_cast<str_data*>(p_)->s == static_cast<str_data*>(o.p_)->s;
    // Two timestamps are equal when they name the same instant, whatever zone
    // each is rendered in -- the offset is a display property.
    if (t_ == vtype::timestamp && o.t_ == vtype::timestamp) return i_ == o.i_;
    if (t_ != o.t_) return false;
    switch (t_) {
    case vtype::null: case vtype::deleted: case vtype::nothing: return true;
    case vtype::boolean: return b_ == o.b_;
    case vtype::string: case vtype::bytes:
        return static_cast<str_data*>(p_)->s == static_cast<str_data*>(o.p_)->s;
    case vtype::array: {
        const auto& a = static_cast<arr_data*>(p_)->v;
        const auto& b = static_cast<arr_data*>(o.p_)->v;
        if (a.size() != b.size()) return false;
        for (size_t i = 0; i < a.size(); ++i) if (!(a[i] == b[i])) return false;
        return true;
    }
    case vtype::object: {
        const auto& a = static_cast<obj_data*>(p_)->e;
        const auto& b = static_cast<obj_data*>(o.p_)->e;
        if (a.size() != b.size()) return false;
        // Entries are kept sorted, so a positional comparison is a set
        // comparison.
        for (size_t i = 0; i < a.size(); ++i)
            if (a[i].first != b[i].first || !(a[i].second == b[i].second)) return false;
        return true;
    }
    default: return false;
    }
}

// ---- JSON output -----------------------------------------------------------

// Go's encoding/json float encoder, reimplemented. NOT
// strconv.FormatFloat(v, 'g', -1, 64), which is what the comment here used to
// claim and what the code did: a %.{15..17}g round-trip loop. encoding/json's
// floatEncoder deliberately does something else, and says so --
//
//     Convert as if by ES6 number to string conversion. [...]
//     Like fmt %g, but the exponent cutoffs are different
//     and exponents themselves are not padded to two digits.
//
// -- so it formats with 'f' unless |v| < 1e-6 or |v| >= 1e21, and then strips
// the leading zero from a two-digit negative exponent. %g's own cutoffs are
// nothing like that, and five of six probe values came out differently:
//
//     value                    swordfish             reference
//     1e20                     1e+20                 100000000000000000000
//     1e-6                     1e-06                 0.000001
//     1e-7                     1e-07                 1e-7
//     1e-9                     1e-09                 1e-9
//     123456789012345678.0     1.2345678901234568e+17  123456789012345680
//     1/3                      0.3333333333333333    0.3333333333333333
//
// One in six agreeing is what kept this quiet: a value only has to land outside
// roughly [1e-4, 1e15] for the two to disagree, and value.hh already states that
// byte-identical JSON is a compatibility requirement.
//
// NaN and infinity stay `null` here. Go REFUSES to marshal them at all, which is
// not a useful thing for a stream processor to do to one message in a batch.

namespace {

// The shortest decimal digit string that round-trips back to the same value --
// what Go's AppendFloat(..., -1, bits) computes. C has no such thing, so it is
// found the way it has to be found: try one more significant digit until the
// round trip holds.
struct shortest_decimal {
    char digits[32] = {};
    int  ndigits = 0;
    int  exp10 = 0;        // value == d1.d2d3... x 10^exp10
    bool negative = false;
};

template <class T>
shortest_decimal shortest_of(T v, int max_digits) {
    shortest_decimal r;
    r.negative = std::signbit(v);
    char buf[64] = {};
    for (int p = 1; p <= max_digits; ++p) {
        std::snprintf(buf, sizeof buf, "%.*e", p - 1, static_cast<double>(v));
        const bool ok = std::is_same_v<T, float>
                            ? (std::strtof(buf, nullptr) == static_cast<float>(v))
                            : (std::strtod(buf, nullptr) == static_cast<double>(v));
        if (ok) break;
    }
    const char* c = buf;
    if (*c == '-' || *c == '+') ++c;
    while (*c && *c != 'e' && *c != 'E') {
        if (*c != '.' && r.ndigits < 31) r.digits[r.ndigits++] = *c;
        ++c;
    }
    r.exp10 = *c ? std::atoi(c + 1) : 0;
    return r;
}

// Positional, no exponent: Go's 'f'.
void append_positional(std::string& out, const shortest_decimal& d) {
    if (d.negative) out += '-';
    if (d.exp10 >= 0) {
        const int int_digits = d.exp10 + 1;
        if (d.ndigits <= int_digits) {
            out.append(d.digits, static_cast<size_t>(d.ndigits));
            out.append(static_cast<size_t>(int_digits - d.ndigits), '0');
        } else {
            out.append(d.digits, static_cast<size_t>(int_digits));
            out += '.';
            out.append(d.digits + int_digits, static_cast<size_t>(d.ndigits - int_digits));
        }
    } else {
        out += "0.";
        out.append(static_cast<size_t>(-d.exp10 - 1), '0');
        out.append(d.digits, static_cast<size_t>(d.ndigits));
    }
}

// Go's 'e'. `strip_exp_zero` is encoding/json's cleanup that turns e-09 into
// e-9; strconv's own 'e' and 'g' formats keep both digits, so append_float_g
// below passes false. Under encoding/json a positive exponent only reaches here
// when it is at least 21, so it always has two digits anyway.
void append_exponential(std::string& out, const shortest_decimal& d,
                        bool strip_exp_zero = true) {
    if (d.negative) out += '-';
    out += d.digits[0];
    if (d.ndigits > 1) {
        out += '.';
        out.append(d.digits + 1, static_cast<size_t>(d.ndigits - 1));
    }
    out += 'e';
    int e = d.exp10;
    if (e < 0) { out += '-'; e = -e; } else { out += '+'; }
    char eb[16];   // an exponent is at most 3 digits, but %d's worst case is 11
    // Two digits minimum, EXCEPT a negative single-digit exponent -- which is
    // exactly what encoding/json's e-09 -> e-9 fixup produces.
    std::snprintf(eb, sizeof eb,
                  (strip_exp_zero && d.exp10 < 0 && e < 10) ? "%d" : "%02d", e);
    out += eb;
}

template <class T>
void write_float(std::string& out, T v, int max_digits, T lo, T hi) {
    if (std::isnan(v) || std::isinf(v)) { out += "null"; return; }
    const auto d = shortest_of(v, max_digits);
    const T abs = std::fabs(v);
    // `abs != 0` first, so zero is always positional: Go prints 0 and -0, never
    // 0e+00.
    if (abs != 0 && (abs < lo || abs >= hi)) append_exponential(out, d);
    else                                     append_positional(out, d);
}

} // namespace

// Go's strconv.FormatFloat(v, 'g', -1, bits), which is NOT what encoding/json
// writes and so cannot reuse write_float above. Two differences, both of which
// showed up as byte diffs against the reference's `avro` scanner:
//
//   * the cutoff. 'g' takes the exponent form when the decimal exponent is below
//     -4 or at least 6 -- ftoa.go pins eprec to 6 for the shortest form --
//     rather than encoding/json's |v| < 1e-6 or >= 1e21. So 1e6 is "1e+06" here
//     and "1000000" there.
//   * the exponent's width. 'g' keeps two digits; encoding/json strips the
//     leading zero from a negative one. So -1.5e-7 is "-1.5e-07" here and
//     "-1.5e-7" there.
//
// It shares the digit generation rather than repeating it, because that half is
// identical and is the half with the subtle round-tripping loop in it.
void append_float_g(std::string& out, double v, int bits) {
    if (std::isnan(v) || std::isinf(v)) { out += "null"; return; }
    const shortest_decimal d = bits == 32
        ? shortest_of<float>(static_cast<float>(v), 9)
        : shortest_of<double>(v, 17);
    // Zero is always positional: Go prints 0 and -0, never 0e+00.
    if (v != 0 && (d.exp10 < -4 || d.exp10 >= 6)) append_exponential(out, d, false);
    else                                          append_positional(out, d);
}

static void write_f64(std::string& out, double d) {
    write_float<double>(out, d, 17, 1e-6, 1e21);
}

// The cutoffs are compared at FLOAT width here, as encoding/json does: "Must use
// float32 comparisons for underlying float32 value to get precise cutoffs
// right."
static void write_f32(std::string& out, float f) {
    write_float<float>(out, f, 9, 1e-6f, 1e21f);
}

static void write_json_string(std::string& out, std::string_view s) {
    out += '"';
    for (unsigned char c : s) {
        switch (c) {
        case '"':  out += "\\\""; break;
        case '\\': out += "\\\\"; break;
        case '\n': out += "\\n";  break;
        case '\r': out += "\\r";  break;
        case '\t': out += "\\t";  break;
        default:
            if (c < 0x20) { char b[8]; std::snprintf(b, sizeof b, "\\u%04x", c); out += b; }
            else out += static_cast<char>(c);
        }
    }
    out += '"';
}

void value::to_json(std::string& out) const {
    switch (t_) {
    case vtype::null: case vtype::deleted: case vtype::nothing: out += "null"; break;
    case vtype::boolean: out += b_ ? "true" : "false"; break;
    case vtype::i64: {
        char b[24];
        const int n = std::snprintf(b, sizeof b, "%" PRId64, i_);
        if (n > 0) out.append(b, static_cast<size_t>(n));
        break;
    }
    case vtype::f64: write_f64(out, f_); break;
    case vtype::f32: write_f32(out, static_cast<float>(f_)); break;
    // Go's encoding/json marshals a time.Time as a quoted RFC 3339 string.
    case vtype::timestamp: write_json_string(out, ts_rfc3339_nano(i_, tz_)); break;
    // Verbatim and UNQUOTED: that is the whole point of the type, and what
    // Go's json.Number marshals to.
    case vtype::raw_number: out += static_cast<str_data*>(p_)->s; break;
    case vtype::string: write_json_string(out, as_string()); break;
    // Go's encoding/json marshals a []byte as a base64 string.
    case vtype::bytes:  write_json_string(out, b64_encode_std(as_string())); break;
    case vtype::array: {
        out += '[';
        bool first = true;
        for (const auto& v : arr()) {
            if (!first) out += ',';
            first = false;
            v.to_json(out);
        }
        out += ']';
        break;
    }
    case vtype::object: {
        out += '{';
        bool first = true;
        for (const auto& [k, v] : obj()) {   // already sorted
            if (!first) out += ',';
            first = false;
            write_json_string(out, k);
            out += ':';
            v.to_json(out);
        }
        out += '}';
        break;
    }
    }
}

std::string value::to_json() const { std::string s; s.reserve(64); to_json(s); return s; }

namespace {
void write_pretty(const value& v, std::string& out, std::string_view indent,
                  bool escape_html, size_t depth) {
    auto nl = [&](size_t d) {
        if (indent.empty()) return;
        out += '\n';
        for (size_t i = 0; i < d; ++i) out += indent;
    };
    auto scalar_or_escape = [&](const value& x) {
        if (!escape_html || x.type() != vtype::string) { x.to_json(out); return; }
        // Go's encoder escapes <, > and & as \u003c, \u003e, \u0026 when
        // SetEscapeHTML is on, which format_json enables by default.
        std::string raw;
        x.to_json(raw);
        for (size_t i = 0; i < raw.size(); ++i) {
            switch (raw[i]) {
            case '<': out += "\\u003c"; break;
            case '>': out += "\\u003e"; break;
            case '&': out += "\\u0026"; break;
            default:  out += raw[i];
            }
        }
    };

    if (v.type() == vtype::array) {
        if (v.arr().empty()) { out += "[]"; return; }
        out += '[';
        bool first = true;
        for (const auto& el : v.arr()) {
            if (!first) out += ',';
            first = false;
            nl(depth + 1);
            write_pretty(el, out, indent, escape_html, depth + 1);
        }
        nl(depth);
        out += ']';
        return;
    }
    if (v.type() == vtype::object) {
        if (v.obj().empty()) { out += "{}"; return; }
        out += '{';
        bool first = true;
        for (const auto& [k, el] : v.obj()) {
            if (!first) out += ',';
            first = false;
            nl(depth + 1);
            scalar_or_escape(value(k));
            out += ':';
            if (!indent.empty()) out += ' ';
            write_pretty(el, out, indent, escape_html, depth + 1);
        }
        nl(depth);
        out += '}';
        return;
    }
    scalar_or_escape(v);
}
} // namespace

std::string value::to_json_pretty(std::string_view indent, bool escape_html) const {
    std::string out;
    out.reserve(128);
    write_pretty(*this, out, indent, escape_html, 0);
    return out;
}

std::string value::to_display_string() const {
    if (is_stringy()) return as_string();
    // A timestamp stringifies to the RFC 3339 form WITHOUT quotes, which is
    // what IToString does with a time.Time.
    if (t_ == vtype::timestamp) return ts_rfc3339_nano(i_, tz_);
    if (t_ == vtype::raw_number) return static_cast<str_data*>(p_)->s;
    return to_json();
}

// ---- arithmetic ------------------------------------------------------------

namespace num {

// Both integral -> integral result; otherwise degrade to double.
// Mirrors numberDegradationFunc in query/arithmetic.go.
//
// The integral case computes through uint64_t: Go's int64 arithmetic wraps on
// overflow, while signed overflow is undefined behaviour in C++. Casting through
// unsigned gives Go's semantics and keeps UBSan quiet.
#define SF_DEGRADE(NAME, OP)                                                        \
    value NAME(const value& l, const value& r) {                                    \
        if (l.type() == vtype::i64 && r.type() == vtype::i64)                       \
            return value(static_cast<int64_t>(                                      \
                static_cast<uint64_t>(l.as_i64()) OP                                \
                static_cast<uint64_t>(r.as_i64())));                                \
        if (!l.is_number() || !r.is_number())                                       \
            throw eval_error(std::string("cannot ") + #NAME + " " + l.type_name()   \
                             + " and " + r.type_name());                            \
        return value(l.as_f64() OP r.as_f64());                                     \
    }

SF_DEGRADE(numeric_add, +)
SF_DEGRADE(sub, -)
SF_DEGRADE(mul, *)
#undef SF_DEGRADE

// `+` is the one operator that is not purely numeric. Per sumOp in
// query/arithmetic.go it dispatches on the LEFT operand only:
//   left number -> numeric add, right must also be a number
//   left string -> concatenation, right coerced by IGetString, which accepts
//                  string, bytes and timestamp but NOT numbers
// So "a" + 1 and 1 + "a" are both type errors, asymmetrically arrived at.
value add(const value& l, const value& r) {
    if (l.is_number()) return numeric_add(l, r);
    if (l.is_stringy()) {
        if (!r.is_stringy())
            throw eval_error(std::string("cannot add string and ") + r.type_name());
        return value(l.as_string() + r.as_string());
    }
    throw eval_error(std::string("cannot add ") + l.type_name() + " and " + r.type_name());
}

// Division is special: always float, and zero divisor is an error rather than Inf.
value div(const value& l, const value& r) {
    if (!l.is_number() || !r.is_number())
        throw eval_error(std::string("cannot divide ") + l.type_name() + " and " + r.type_name());
    double d = r.as_f64();
    if (d == 0) throw eval_error("attempted to divide by zero");
    return value(l.as_f64() / d);
}

// cppcheck-suppress unusedFunction
//   Emitted by the Bloblang code generator into generated translation units.
double fast_div(double a, double b) {
    if (b == 0) throw eval_error("attempted to divide by zero");
    return a / b;
}

int64_t fast_mod(int64_t a, int64_t b) {
    if (b == 0) throw eval_error("attempted to modulo by zero");
    // INT64_MIN % -1 is undefined in C++ and raises SIGFPE on x86 -- it killed
    // the process on real input. Go defines it: the quotient overflows back to
    // x, leaving a remainder of 0.
    if (b == -1) return 0;
    return a % b;
}

value mod(const value& l, const value& r) {
    if (!l.is_number() || !r.is_number())
        throw eval_error(std::string("cannot modulo ") + l.type_name() + " and " + r.type_name());
    return value(fast_mod(l.as_i64(), r.as_i64()));   // shares the guards above
}

} // namespace num

// ---- comparison / logic ----------------------------------------------------

bool truthy(const value& v) {
    if (v.type() == vtype::boolean) return v.as_bool();
    throw type_error("bool", v.type_name());
}

static int compare(const value& l, const value& r) {
    if (l.is_number() && r.is_number()) {
        double a = l.as_f64(), b = r.as_f64();
        return a < b ? -1 : (a > b ? 1 : 0);
    }
    if (l.is_stringy() && r.is_stringy()) {
        int c = l.as_string().compare(r.as_string());
        return c < 0 ? -1 : (c > 0 ? 1 : 0);
    }
    throw eval_error(std::string("cannot compare ") + l.type_name() + " and " + r.type_name());
}

value cmp_eq (const value& l, const value& r) { return value(l == r); }
value cmp_neq(const value& l, const value& r) { return value(!(l == r)); }
value cmp_lt (const value& l, const value& r) { return value(compare(l, r) <  0); }
value cmp_lte(const value& l, const value& r) { return value(compare(l, r) <= 0); }
value cmp_gt (const value& l, const value& r) { return value(compare(l, r) >  0); }
value cmp_gte(const value& l, const value& r) { return value(compare(l, r) >= 0); }


// cppcheck-suppress unusedFunction
//   Emitted by the Bloblang code generator into generated translation units.
value coalesce(const value& l, const value& r) {
    if (l.is_null() || l.is_nothing()) return r;
    return l;
}

// ---- methods ---------------------------------------------------------------

namespace m {

value to_string(const value& v) { return value(v.to_display_string()); }

// IToBytes: already-bytes is unchanged, everything else is marshalled the way
// .string() would render it.
value to_bytes(const value& v) {
    if (v.type() == vtype::bytes) return v;
    return value::bytes(v.to_display_string());
}

value length(const value& v) {
    switch (v.type()) {
    case vtype::string:
    case vtype::bytes:  return value(static_cast<int64_t>(v.as_string().size()));
    case vtype::array:  return value(static_cast<int64_t>(v.arr().size()));
    case vtype::object: return value(static_cast<int64_t>(v.obj().size()));
    default: throw eval_error(std::string("cannot take length of ") + v.type_name());
    }
}

namespace {

// Unicode SIMPLE case mapping, rune by rune, from the generated table in
// case_tables.cc. The obvious implementation -- std::toupper over the bytes --
// leaves every non-ASCII character alone, so "h\u00e9llo".uppercase() came out
// as "H\u00e9LLO" where the reference gives "H\u00c9LLO". std::towupper is not
// the fix either: in the "C" locale it also only knows ASCII. Found by the L4
// gate against the real binary.
uint32_t map_case(uint32_t cp, const ucase::case_entry* t, size_t n) {
    size_t lo = 0, hi = n;
    while (lo < hi) {
        const size_t mid = lo + (hi - lo) / 2;
        if (t[mid].from < cp) lo = mid + 1; else hi = mid;
    }
    return (lo < n && t[lo].from == cp) ? static_cast<uint32_t>(cp + t[lo].delta) : cp;
}

value map_case_string(const value& v, const ucase::case_entry* t, size_t n) {
    if (!v.is_stringy()) throw type_error("string", v.type_name());
    const std::string& s = v.as_string();
    std::string out;
    out.reserve(s.size());
    for (size_t i = 0; i < s.size();) {
        const auto b = static_cast<unsigned char>(s[i]);
        if (b < 0x80) {                      // the overwhelmingly common path
            out += static_cast<char>(map_case(b, t, n));
            ++i;
            continue;
        }
        if (enc::utf8_rune_len(s, i) == 1) { // a malformed byte, copied as-is
            out += s[i++];
            continue;
        }
        enc::append_utf8(out, map_case(enc::next_rune(s, i), t, n));
    }
    // A bytes target yields bytes, as it does for every other string method.
    return v.type() == vtype::bytes ? value::bytes(std::move(out)) : value(std::move(out));
}

} // namespace

value uppercase(const value& v) {
    return map_case_string(v, ucase::UPPER, ucase::UPPER_len);
}

value lowercase(const value& v) {
    return map_case_string(v, ucase::LOWER, ucase::LOWER_len);
}

value number(const value& v) {
    // IToNumber is IToFloat64: the result is a float even when the input was an
    // integer, so `.number()` is a widening conversion rather than a no-op.
    if (v.type() == vtype::i64) return value(static_cast<double>(v.as_i64()));
    if (v.is_float()) return v;
    // A raw number widens like any other: `.number()` is documented as a
    // conversion to float64, and keeping the literal text here would make the
    // result compare unequal to the same value written as a double.
    if (v.type() == vtype::raw_number) return value(v.as_f64());
    if (v.is_stringy()) {
        const std::string& s = v.as_string();
        char* end = nullptr;
        double d = std::strtod(s.c_str(), &end);
        if (end == s.c_str() || *end) throw eval_error("failed to parse string as number: " + s);
        return value(d);
    }
    throw eval_error(std::string("cannot convert ") + v.type_name() + " to number");
}

// Bloblang's `exists` takes a dot-separated path and reports whether it
// resolves. A missing intermediate is false rather than an error.
value exists(const value& v, const value& path) {
    const std::string& p = path.as_string();
    const value* cur = &v;
    size_t start = 0;
    while (start <= p.size()) {
        size_t dot = p.find('.', start);
        const std::string key = p.substr(start, dot == std::string::npos ? std::string::npos
                                                                         : dot - start);
        if (cur->type() != vtype::object) return value(false);
        cur = cur->find(key);
        if (!cur) return value(false);
        if (dot == std::string::npos) break;
        start = dot + 1;
    }
    return value(true);
}

value floor_(const value& v) {
    if (v.type() == vtype::i64) return v;
    return value(std::floor(v.as_f64()));
}

namespace {
// Shared iteration for map_each/filter. An object element is presented to the
// callback as a {key, value} pair, matching Bloblang.
value each(const value& v, const std::function<value(const value&)>& fn,
           bool filtering, const char* what) {
    if (v.type() == vtype::array) {
        std::vector<value> out;
        for (const auto& el : v.arr()) {
            value r = fn(el);
            if (filtering) { if (truthy(r)) out.push_back(el); }
            else if (!r.is_deleted()) out.push_back(std::move(r));
        }
        return value::array(std::move(out));
    }
    if (v.type() == vtype::object) {
        value out = value::object();
        for (const auto& [key, val] : v.obj()) {
            value pair = value::object();
            pair.set("key", value(key));
            pair.set("value", val);
            value r = fn(pair);
            if (filtering) { if (truthy(r)) out.set(key, val); }
            else if (!r.is_deleted()) out.set(key, std::move(r));
        }
        return out;
    }
    throw eval_error(std::string("cannot ") + what + " over " + v.type_name());
}
} // namespace

value map_each(const value& v, const std::function<value(const value&)>& fn) {
    return each(v, fn, false, "map_each");
}
value filter(const value& v, const std::function<value(const value&)>& fn) {
    return each(v, fn, true, "filter");
}

// Go-style verb subset sufficient for the slice: %v %s %d %f %t %%
value format(const value& fmt, const std::vector<value>& args) {
    const std::string& f = fmt.as_string();
    std::string out;
    out.reserve(f.size() + 16 * args.size());
    size_t ai = 0;
    for (size_t i = 0; i < f.size(); ++i) {
        if (f[i] != '%') { out += f[i]; continue; }
        if (i + 1 >= f.size()) { out += '%'; break; }
        char verb = f[++i];
        if (verb == '%') { out += '%'; continue; }
        if (ai >= args.size()) throw eval_error("not enough arguments for format string");
        const value& a = args[ai++];
        char buf[64];
        switch (verb) {
        case 'v': out += a.to_display_string(); break;
        case 's': out += a.to_display_string(); break;
        // to_chars rather than snprintf: snprintf re-parses its format string on
        // every call, which measured as the single largest cost in a mapping
        // doing one .format() per message.
        case 'd': { auto [e, ec] = std::to_chars(buf, buf + sizeof buf, a.as_i64());
                    out.append(buf, e); break; }
        case 'f': std::snprintf(buf, sizeof buf, "%f", a.as_f64()); out += buf; break;
        case 't': out += (a.type() == vtype::boolean && a.as_bool()) ? "true" : "false"; break;
        default:  throw eval_error(std::string("unsupported format verb %") + verb);
        }
    }
    return value(std::move(out));
}

} // namespace m
} // namespace sf
