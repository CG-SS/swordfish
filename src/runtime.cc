#include "swordfish/runtime.hh"

#include <algorithm>
#include <cstdlib>
#include <unistd.h>

namespace sf {

value index_of(const value& v, const value& idx) {
    const auto& a = v.arr();
    int64_t i = idx.as_i64();
    if (i < 0) i += static_cast<int64_t>(a.size());
    if (i < 0 || static_cast<size_t>(i) >= a.size()) throw eval_error("array index out of bounds");
    return a[static_cast<size_t>(i)];
}

value assign_path(value root, const std::vector<std::string>& path, size_t i, value v) {
    if (i == path.size()) return v;
    value child = (root.type() == vtype::object && root.find(path[i]))
        ? *root.find(path[i])
        : value::object();
    root.set(path[i], assign_path(std::move(child), path, i + 1, std::move(v)));
    return root;
}

namespace fn {
value counter(exec_ctx& ctx, const std::string& site,
              int64_t min_v, int64_t max_v, const value& set) {
    // The four behaviours the reference documents for `set`, plus "absent",
    // which is distinct from all of them:
    //   absent (nothing) -> increment
    //   a deletion       -> reset to min
    //   null             -> read without incrementing
    //   an integer       -> set to that value
    auto it = ctx.counters.find(site);
    const bool first = it == ctx.counters.end();
    if (first) it = ctx.counters.emplace(site, min_v).first;

    if (set.is_deleted()) {
        it->second = min_v;
    } else if (set.is_null()) {
        // read only
    } else if (set.is_number()) {
        it->second = set.as_i64();
    } else if (!first) {
        // absent, or any other value: increment, wrapping at max.
        if (++it->second > max_v) it->second = min_v;
    }
    return value(it->second);
}

// ---- message functions ------------------------------------------------------

value content(const exec_ctx& ctx) {
    // Bytes, not a string: `content().type()` is documented as "bytes", and the
    // difference is observable in JSON encoding too.
    return value::bytes(ctx.message_of("content").as_bytes());
}

value json_of(const exec_ctx& ctx, const value& path) {
    // Always the ROOT document, which is what distinguishes json() from `this`:
    // inside a lambda or a nested context `this` is the element, json() is not.
    const value& root = ctx.message_of("json").as_structured();
    const std::string p = path.type() == vtype::string ? path.as_string() : std::string();
    if (p.empty()) return root;
    const value* cur = &root;
    size_t start = 0;
    while (start <= p.size()) {
        const size_t dot = p.find('.', start);
        const std::string key = p.substr(start, dot == std::string::npos
                                                ? std::string::npos : dot - start);
        // Null for a path that is not there, as plain field access and `.get()`
        // both do. Erroring instead made `json("nope.deeper")` fail where the
        // reference returns null.
        if (cur->type() != vtype::object) return value();
        cur = cur->find(key);
        if (!cur) return value();
        if (dot == std::string::npos) break;
        start = dot + 1;
    }
    return *cur;
}

value error_of(const exec_ctx& ctx) {
    const message& m = ctx.message_of("error");
    return m.has_error() ? value(m.error()) : value();
}

value errored(const exec_ctx& ctx) {
    return value(ctx.message_of("errored").has_error());
}

value batch_index(const exec_ctx& ctx) { return value(ctx.batch_index); }
value batch_size(const exec_ctx& ctx)  { return value(ctx.batch_size); }

// ---- pure functions ---------------------------------------------------------

value deleted() { return value::deleted(); }
value nothing() { return value::nothing(); }

value throw_(const value& why) {
    throw eval_error(why.type() == vtype::string ? why.as_string() : why.to_json());
}

value range(const value& start, const value& stop, const value& step) {
    const int64_t a = start.is_number() ? start.as_i64() : 0;
    const int64_t b = stop.is_number()  ? stop.as_i64()  : 0;
    const int64_t s = step.is_number()  ? step.as_i64()  : 1;
    // rangeFunction: the length is (stop-start)/step with Go's TRUNCATING
    // integer division, so range(0, 250, 100) has two elements, not three. An
    // empty range is an error rather than an empty array.
    // |step|, computed unsigned so negating INT64_MIN is not undefined. It is
    // also the divisor below, so the zero check is written against it directly.
    const uint64_t mag = (s > 0) ? static_cast<uint64_t>(s) : 0u - static_cast<uint64_t>(s);
    if (mag == 0) throw eval_error("range step must be greater than or less than 0");
    if (s < 0 && b > a)
        throw eval_error("with negative step arg stop must be <= start");
    if (s > 0 && a >= b)
        throw eval_error("with positive step arg start must be < stop");
    // The guards above make the difference run the same way as the step, so it
    // is computed unsigned: `stop - start` overflows int64 at the extremes.
    const uint64_t span = (s > 0) ? static_cast<uint64_t>(b) - static_cast<uint64_t>(a)
                                  : static_cast<uint64_t>(a) - static_cast<uint64_t>(b);
    const uint64_t n = span / mag;
    std::vector<value> out;
    out.reserve(static_cast<size_t>(std::min<uint64_t>(n, 1u << 20)));
    // Computed rather than accumulated, matching Go's `start + step*i`.
    for (uint64_t i = 0; i < n; ++i)
        out.push_back(value(num::fast_add(a, num::fast_mul(s, static_cast<int64_t>(i)))));
    return value::array(std::move(out));
}

value hostname() {
    char buf[256] = {};
    if (::gethostname(buf, sizeof(buf) - 1) != 0) throw eval_error("failed to get hostname");
    return value(std::string(buf));
}

value env(const value& name, const value& no_cache) {
    (void)no_cache;                       // we never cache, so it changes nothing
    const char* v = ::getenv(name.as_string().c_str());
    return v ? value(std::string(v)) : value();
}
}

} // namespace sf
