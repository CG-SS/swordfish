// The value model: Bloblang's data type.
//
// Mirrors benthos-main/internal/value/type_helpers.go closely enough that the
// arithmetic and serialisation semantics match Go — number degradation, sorted
// object keys, and `deleted`/`nothing` as first-class values. Both execution
// modes operate on this type, and both call the arithmetic helpers below, so the
// two cannot disagree about semantics.

#pragma once

#include <cstdint>
#include <cstddef>
#include <string>
#include <vector>
#include <utility>
#include <stdexcept>
#include <functional>
#include <string_view>

namespace sf {

// Bloblang evaluation failure. Thrown, caught once at the mapping boundary.
// Exceptions rather than an expected type: a mapping failure is exceptional and
// every call site would otherwise have to thread the error through by hand.
class eval_error : public std::runtime_error {
public:
    explicit eval_error(std::string msg) : std::runtime_error(std::move(msg)) {}
};

// A value was the wrong type for what was asked of it.
//
// Distinct from eval_error so the expression that PRODUCED the value can
// recognise it and say where the value came from -- "expected object value, got
// null" is a poor diagnostic over messy JSON; "…from field `this.user.id`"
// tells you which field to go and look at. Only the code holding the AST knows
// that, and only the throw site knows the type, so the two halves are joined by
// rethrowing (see attach_source in runtime.hh).
//
// Matching redpanda-connect 4.107.2, which words these as
// `expected <want> value, got <type> from <source>`.
class type_error : public eval_error {
public:
    type_error(std::string want, std::string got)
        : eval_error("expected " + want + " value, got " + got),
          want_(std::move(want)), got_(std::move(got)) {}
    const std::string& want() const noexcept { return want_; }
    const std::string& got()  const noexcept { return got_;  }
private:
    std::string want_, got_;
};

enum class vtype : uint8_t {
    null, boolean, i64, f64,
    // A 32-bit float. Behaves as a number everywhere -- `.type()` says
    // "number", arithmetic widens it to double -- and differs only in JSON
    // output, which Go writes with 32-bit shortest-round-trip precision:
    // 6.674282313423543e-11 as a float32 marshals as 6.674283e-11. The
    // `float32()` method exists so a downstream component (a SQL driver, say)
    // receives the width it needs, so the tag has to survive to the edge.
    f32,
    // A moment in time: nanoseconds since the Unix epoch, plus the UTC offset
    // it should be rendered in. Go's value model has this type too, and
    // representing it as an RFC 3339 string instead would make `.type()` report
    // "string" and would let string methods apply to it.
    //
    // The offset is carried rather than a named zone, because a `value` has
    // exactly four spare bytes and a `const time_zone*` needs eight. Everything
    // the reference documents works from the offset; the one difference is that
    // a Go layout asking for a zone ABBREVIATION (`MST`, or strftime `%Z`)
    // renders "UTC" at offset zero and Go's numeric fallback otherwise, which
    // is what Go itself prints for a zone with no name.
    timestamp,
    // A number kept as the literal text that spelled it, which is what Go's
    // json.Number is. `parse_json(use_number: true)` produces these so a value
    // too large or too precise for a double survives a round trip: it compares
    // and computes as a number, and re-serialises as the digits it arrived as
    // rather than as the nearest double.
    raw_number,
    string,
    // Go's []byte. Shares the string payload and behaves like a string in every
    // operation -- RestrictForComparison degrades it to one for comparison and
    // arithmetic, and IGetString accepts it. It differs in exactly two places:
    // `.type()` reports "bytes", and JSON encoding base64-encodes it, because
    // that is what Go's encoding/json does with a byte slice.
    bytes,
    array, object,
    deleted,   // value.Delete  — removes the assignment target
    nothing,   // value.Nothing — assignment skipped entirely
};

class value;

// Standard base64, shared by encode("base64") and by JSON encoding of a bytes
// value -- Go's encoding/json base64s a []byte, so the two must agree.
// Defined in methods.cc, next to the other codecs.
std::string b64_encode_std(std::string_view in);

// Go's strconv.FormatFloat(v, 'g', -1, bits) -- the shortest digit string that
// round-trips, laid out the way %g lays it out. `bits` is 32 or 64 and selects
// how many digits have to round-trip.
//
// This is deliberately NOT how a value serialises itself to JSON: encoding/json
// uses different cutoffs and strips a leading zero from a negative exponent, so
// 1e6 marshals as 1000000 there and formats as 1e+06 here. The `avro` scanner
// needs the strconv form because that is what the reference's Avro-JSON writer
// emits. Defined in value.cc, beside the digit generation both forms share.
void append_float_g(std::string& out, double v, int bits);

// RFC 3339 with a nanosecond fraction, trailing zeros trimmed -- Go's
// time.RFC3339Nano, which is how a timestamp is rendered in JSON and by
// `.string()`. Defined in methods_time.cc next to the rest of the calendar
// arithmetic; declared here because value.cc serialises with it.
std::string ts_rfc3339_nano(int64_t unix_nanos, int32_t offset_sec);

// ---- intrusive refcounting -------------------------------------------------
// 8-byte payload pointer keeps `value` at 16 bytes.
struct rc_base { mutable uint32_t rc = 1; };

struct str_data : rc_base { std::string s; };
struct arr_data : rc_base { std::vector<value> v; };
// Objects keep entries sorted by key: Go's encoding/json marshals maps with
// sorted keys, and byte-identical output is a compatibility requirement.
struct obj_data : rc_base { std::vector<std::pair<std::string, value>> e; };

class value {
public:
    value() noexcept : t_(vtype::null), p_(nullptr) {}
    explicit value(std::nullptr_t) noexcept : value() {}
    explicit value(bool b) noexcept : t_(vtype::boolean), b_(b) {}
    explicit value(int64_t i) noexcept : t_(vtype::i64), i_(i) {}
    explicit value(int i) noexcept : t_(vtype::i64), i_(i) {}
    explicit value(double d) noexcept : t_(vtype::f64), f_(d) {}
    static value float32(float f) noexcept { value r; r.t_ = vtype::f32; r.f_ = f; return r; }
    // `offset_sec` is the zone's offset east of UTC, as time.Time carries it.
    static value ts(int64_t unix_nanos, int32_t offset_sec = 0) noexcept {
        value r;
        r.t_ = vtype::timestamp;
        r.tz_ = offset_sec;
        r.i_ = unix_nanos;
        return r;
    }
    explicit value(std::string s) : t_(vtype::string) { p_ = make_str(std::move(s)); }
    explicit value(std::string_view s) : value(std::string(s)) {}
    explicit value(const char* s) : value(std::string(s)) {}

    static value raw_number(std::string digits) {
        value r;
        r.t_ = vtype::raw_number;
        r.p_ = make_str(std::move(digits));
        return r;
    }
    static value bytes(std::string s) { value r; r.t_ = vtype::bytes; r.p_ = make_str(std::move(s)); return r; }
    static value array(std::vector<value> v = {}) { value r; r.t_ = vtype::array; r.p_ = make_arr(std::move(v)); return r; }
    static value object() { value r; r.t_ = vtype::object; r.p_ = make_obj(); return r; }
    static value deleted() noexcept { value r; r.t_ = vtype::deleted; return r; }
    static value nothing() noexcept { value r; r.t_ = vtype::nothing; return r; }

    value(const value& o) noexcept : t_(o.t_) { copy_payload(o); }
    value(value&& o) noexcept : t_(o.t_) { move_payload(o); }
    value& operator=(const value& o) noexcept { if (this != &o) { release(); t_ = o.t_; copy_payload(o); } return *this; }
    value& operator=(value&& o) noexcept { if (this != &o) { release(); t_ = o.t_; move_payload(o); } return *this; }
    ~value() { release(); }

    vtype type() const noexcept { return t_; }
    bool is_null()    const noexcept { return t_ == vtype::null; }
    bool is_deleted() const noexcept { return t_ == vtype::deleted; }
    bool is_nothing() const noexcept { return t_ == vtype::nothing; }
    bool is_number()  const noexcept {
        return t_ == vtype::i64 || t_ == vtype::f64 || t_ == vtype::f32 ||
               t_ == vtype::raw_number;
    }
    bool is_float()   const noexcept { return t_ == vtype::f64 || t_ == vtype::f32; }
    bool is_ts()      const noexcept { return t_ == vtype::timestamp; }
    // Only meaningful for a timestamp.
    int64_t ts_nanos()  const noexcept { return i_; }
    int32_t ts_offset() const noexcept { return tz_; }
    // String or bytes. Nearly every string operation accepts both; only type()
    // and JSON encoding tell them apart.
    bool is_stringy() const noexcept { return t_ == vtype::string || t_ == vtype::bytes; }

    const char* type_name() const noexcept;

    bool        as_bool()   const;
    int64_t     as_i64()    const;
    double      as_f64()    const;   // coerces i64 -> f64
    const std::string& as_string() const;

    const std::vector<value>& arr() const;
    const std::vector<std::pair<std::string, value>>& obj() const;

    // Mutating accessors: clone first when the payload is shared (copy-on-write).
    std::vector<value>& arr_mut();
    std::vector<std::pair<std::string, value>>& obj_mut();

    // Object field access. get() throws on a missing key or a non-object,
    // matching Bloblang's "field not found" error.
    const value& get(std::string_view key) const;
    const value* find(std::string_view key) const noexcept;
    void set(std::string_view key, value v);
    void remove(std::string_view key);

    std::string to_json() const;
    void to_json(std::string& out) const;

    // Pretty-printed form used by format_json(). `indent` is one level's worth
    // of whitespace; empty means compact. `escape_html` turns < > & into \uXXXX,
    // which format_json enables by default even though message encoding does
    // not (encodeJSON sets SetEscapeHTML(false)).
    std::string to_json_pretty(std::string_view indent, bool escape_html) const;

    // Bloblang's `.string()` coercion, which is not the same as JSON encoding:
    // strings pass through unquoted.
    std::string to_display_string() const;

    bool operator==(const value& o) const noexcept;

private:
    // Out of line so payload allocation goes through the pool in value.cc.
    static rc_base* make_str(std::string s);
    static rc_base* make_arr(std::vector<value> v);
    static rc_base* make_obj();

    void release() noexcept;
    void copy_payload(const value& o) noexcept;
    void move_payload(value& o) noexcept;
    void unshare();

    vtype t_;
    // Sits in what would otherwise be padding before the 8-byte union, so a
    // timestamp costs nothing: the zone offset in seconds east of UTC.
    int32_t tz_ = 0;
    union {
        bool     b_;
        int64_t  i_;
        double   f_;
        rc_base* p_;
    };
};

static_assert(sizeof(value) <= 16, "value must stay 16 bytes");

// ---- arithmetic ------------------------------------------------------------
// The single implementation of Bloblang arithmetic. Both the interpreter and
// generated code call these, so the two backends cannot disagree.
// Semantics from benthos-main/internal/bloblang/query/arithmetic.go:
//   * + - degrade i64 -> f64 and stay integral when both sides are integral
//   /     always produces a float; division by zero is an error, not Inf
namespace num {
// ---- unboxed fast paths ----------------------------------------------------
// Used by generated code under a type guard (tier 4). Semantics must match the
// boxed versions EXACTLY: integer arithmetic wraps as Go's does, and division or
// modulo by zero is an error rather than Inf or UB.
inline int64_t fast_add(int64_t a, int64_t b) noexcept {
    return static_cast<int64_t>(static_cast<uint64_t>(a) + static_cast<uint64_t>(b));
}
inline int64_t fast_sub(int64_t a, int64_t b) noexcept {
    return static_cast<int64_t>(static_cast<uint64_t>(a) - static_cast<uint64_t>(b));
}
inline int64_t fast_mul(int64_t a, int64_t b) noexcept {
    return static_cast<int64_t>(static_cast<uint64_t>(a) * static_cast<uint64_t>(b));
}
double  fast_div(double a, double b);
int64_t fast_mod(int64_t a, int64_t b);

// `+` concatenates when the left operand is a string; see value.cc.
value add(const value& l, const value& r);
value numeric_add(const value& l, const value& r);
value sub(const value& l, const value& r);
value mul(const value& l, const value& r);
value div(const value& l, const value& r);
value mod(const value& l, const value& r);
}

// ---- comparison / logic ----------------------------------------------------
bool  truthy(const value& v);
value cmp_eq (const value& l, const value& r);
value cmp_neq(const value& l, const value& r);
value cmp_lt (const value& l, const value& r);
value cmp_lte(const value& l, const value& r);
value cmp_gt (const value& l, const value& r);
value cmp_gte(const value& l, const value& r);
// No bool_and/bool_or here on purpose. `&&` and `||` must short-circuit, and a
// function taking both operands cannot -- they are evaluated before the call.
// Both backends render them inline instead: the interpreter returns early
// (interp.cc), the emitter produces real C++ `&&`/`||` (emit.cc). Functions
// with these names existed, unused, and would have quietly broken that.
// `|` — Bloblang's coalesce, which binds as tightly as `*`.
value coalesce(const value& l, const value& r);

// `use_number` keeps every number as the text that spelled it (Go's
// json.Number), so a value too large or too precise for a double survives a
// round trip. Defined in json.cc.
// Whether a literal entry is omitted rather than written. `nothing` and
// `deleted` both drop out of an object or array literal -- that is how
// `{"a": if false { 1 }}` yields `{}` rather than `{"a":null}`. An explicit
// null is NOT omitted, so this cannot simply test for absence of a value.
//
// Shared by the interpreter and the emitter deliberately: the rule has three
// cases and two of them were wrong in one backend and right in the other, which
// is precisely the drift a single definition prevents.
inline bool literal_omits(const value& v) noexcept {
    return v.is_nothing() || v.is_deleted();
}

value parse_json(std::string_view text, bool use_number = false);

} // namespace sf
