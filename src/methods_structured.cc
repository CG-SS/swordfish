// Bloblang object and array methods.
//
// The lambda-taking ones (all, any, fold, sort_by, find_by, find_all_by,
// map_each_key) keep the same shape map_each and filter established: the
// iteration lives here and the per-element callback is supplied by the caller,
// so the interpreter's AST walk and the emitted C++ lambda run through one
// implementation and cannot diverge.
#include "swordfish/methods.hh"
#include "swordfish/value.hh"

#include "methods_util.hh"

#include <algorithm>
#include <functional>
#include <cstdint>
#include <string>
#include <vector>

namespace sf::m {

namespace {

// A dot path, as gabs.DotPathToSlice produces. No escaping: Bloblang's field
// paths do not have any, and a key containing a dot is simply unreachable.
std::vector<std::string> dot_path(std::string_view p) {
    std::vector<std::string> out;
    size_t start = 0;
    for (;;) {
        const size_t dot = p.find('.', start);
        if (dot == std::string_view::npos) { out.emplace_back(p.substr(start)); break; }
        out.emplace_back(p.substr(start, dot - start));
        start = dot + 1;
    }
    return out;
}

const value* lookup_path(const value& root, const std::vector<std::string>& path) {
    const value* cur = &root;
    for (const auto& seg : path) {
        if (cur->type() != vtype::object) return nullptr;
        cur = cur->find(seg);
        if (!cur) return nullptr;
    }
    return cur;
}

// Removes one dot path, pruning nothing: an object left empty by the removal
// stays present, which is what the documented `without` output shows.
value remove_path(value v, const std::vector<std::string>& path, size_t i) {
    if (v.type() != vtype::object) return v;
    if (i + 1 == path.size()) { v.remove(path[i]); return v; }
    const value* child = v.find(path[i]);
    if (!child) return v;
    v.set(path[i], remove_path(*child, path, i + 1));
    return v;
}

// Copies just one dot path out of `src` into `dst`. A path whose prefix is
// missing contributes nothing, so `with` silently ignores unknown fields.
void keep_path(const value& src, value& dst, const std::vector<std::string>& path, size_t i) {
    if (src.type() != vtype::object) return;
    const value* child = src.find(path[i]);
    if (!child) return;
    if (i + 1 == path.size()) { dst.set(path[i], *child); return; }
    const value* existing = dst.find(path[i]);
    value sub = existing ? *existing : value::object();
    keep_path(*child, sub, path, i + 1);
    dst.set(path[i], std::move(sub));
}

void collapse_into(const value& v, const std::string& prefix, bool include_empty,
                   value& out) {
    if (v.type() == vtype::object) {
        if (v.obj().empty()) {
            if (include_empty && !prefix.empty()) out.set(prefix, v);
            return;
        }
        for (const auto& [k, el] : v.obj())
            collapse_into(el, prefix.empty() ? k : prefix + "." + k, include_empty, out);
        return;
    }
    if (v.type() == vtype::array) {
        if (v.arr().empty()) {
            if (include_empty && !prefix.empty()) out.set(prefix, v);
            return;
        }
        for (size_t i = 0; i < v.arr().size(); ++i) {
            const std::string k = std::to_string(i);
            collapse_into(v.arr()[i], prefix.empty() ? k : prefix + "." + k,
                          include_empty, out);
        }
        return;
    }
    if (!prefix.empty()) out.set(prefix, v);
}

// The recursive half of `assign`: objects merge key by key, and anything else
// on the right simply replaces what is on the left.
value assign_into(const value& dst, const value& src) {
    if (dst.type() != vtype::object || src.type() != vtype::object) return src;
    value out = dst;
    for (const auto& [k, el] : src.obj()) {
        const value* existing = out.find(k);
        out.set(k, existing ? assign_into(*existing, el) : el);
    }
    return out;
}

// The `.merge()` collision rule, reused by squash: two values under one key
// become an array holding both, flattening a side that is already an array.
value merge_values(const value& a, const value& b) {
    std::vector<value> both;
    if (a.type() == vtype::array) both = a.arr(); else both.push_back(a);
    if (b.type() == vtype::array) both.insert(both.end(), b.arr().begin(), b.arr().end());
    else both.push_back(b);
    return value::array(std::move(both));
}

void diff_into(const value& from, const value& to, std::vector<std::string>& path,
               std::vector<value>& out) {
    auto record = [&](const char* type, const value& f, const value& t) {
        value entry = value::object();
        entry.set("From", f);
        std::vector<value> p;
        p.reserve(path.size());
        for (const auto& seg : path) p.push_back(value(seg));
        entry.set("Path", value::array(std::move(p)));
        entry.set("To", t);
        entry.set("Type", value(std::string(type)));
        out.push_back(std::move(entry));
    };

    if (from.type() == vtype::object && to.type() == vtype::object) {
        // Walk the left side first, then the keys only the right side has, so
        // the changelog order is deterministic and matches the reference's.
        for (const auto& [k, el] : from.obj()) {
            path.push_back(k);
            const value* other = to.find(k);
            if (other) diff_into(el, *other, path, out);
            else       record("delete", el, value());
            path.pop_back();
        }
        for (const auto& [k, el] : to.obj()) {
            if (from.find(k)) continue;
            path.push_back(k);
            record("create", value(), el);
            path.pop_back();
        }
        return;
    }
    if (from.type() == vtype::array && to.type() == vtype::array) {
        const auto& a = from.arr();
        const auto& b = to.arr();
        for (size_t i = 0; i < std::max(a.size(), b.size()); ++i) {
            path.push_back(std::to_string(i));
            if (i >= a.size())      record("create", value(), b[i]);
            else if (i >= b.size()) record("delete", a[i], value());
            else                    diff_into(a[i], b[i], path, out);
            path.pop_back();
        }
        return;
    }
    if (!(from == to)) record("update", from, to);
}

value set_path(value root, const std::vector<std::string>& path, size_t i, const value& v) {
    if (i == path.size()) return v;
    value child = (root.type() == vtype::object && root.find(path[i]))
                      ? *root.find(path[i]) : value::object();
    if (root.type() != vtype::object) root = value::object();
    root.set(path[i], set_path(std::move(child), path, i + 1, v));
    return root;
}

} // namespace

// ---- coercion ----------------------------------------------------------------

// `.not()` is a method rather than an operator because Bloblang has no prefix
// `!`. It insists on a boolean: a truthiness coercion here would make
// `this.missing.not()` quietly true.
value not_(const value& v) {
    if (v.type() != vtype::boolean) wrong("bool", v);
    return value(!v.as_bool());
}

value array_of(const value& v) {
    if (v.type() == vtype::array) return v;
    return value::array({v});
}

// ---- merging -----------------------------------------------------------------

value assign(const value& v, const value& with) {
    if (v.type() == vtype::array) {
        std::vector<value> out = v.arr();
        if (with.type() == vtype::array)
            out.insert(out.end(), with.arr().begin(), with.arr().end());
        else
            out.push_back(with);
        return value::array(std::move(out));
    }
    if (v.type() != vtype::object) wrong("object or array", v);
    return assign_into(v, with);
}

value squash(const value& v) {
    const auto& a = want_array(v);
    value out = value::object();
    for (const auto& el : a) {
        if (el.type() != vtype::object) wrong("object", el);
        for (const auto& [k, inner] : el.obj()) {
            const value* existing = out.find(k);
            out.set(k, existing ? merge_values(*existing, inner) : inner);
        }
    }
    return out;
}

// ---- shaping -------------------------------------------------------------------

value collapse(const value& v, const value& include_empty) {
    const bool keep = include_empty.type() == vtype::boolean && include_empty.as_bool();
    value out = value::object();
    collapse_into(v, {}, keep, out);
    return out;
}

value enumerated(const value& v) {
    const auto& a = want_array(v);
    std::vector<value> out;
    out.reserve(a.size());
    for (size_t i = 0; i < a.size(); ++i) {
        value e = value::object();
        e.set("index", value(static_cast<int64_t>(i)));
        e.set("value", a[i]);
        out.push_back(std::move(e));
    }
    return value::array(std::move(out));
}

value key_values(const value& v) {
    const auto& o = want_object(v);
    std::vector<value> out;
    out.reserve(o.size());
    for (const auto& [k, el] : o) {
        value e = value::object();
        e.set("key", value(k));
        e.set("value", el);
        out.push_back(std::move(e));
    }
    return value::array(std::move(out));
}

value explode(const value& v, const value& path_v) {
    if (v.type() != vtype::object) wrong("object", v);
    const auto path = dot_path(want_string(path_v));
    const value* target = lookup_path(v, path);
    if (!target) throw eval_error("target field not found: " + want_string(path_v));

    // Everything except the exploded field is repeated into each output
    // document, so the surrounding structure survives the expansion.
    if (target->type() == vtype::array) {
        std::vector<value> out;
        out.reserve(target->arr().size());
        for (const auto& el : target->arr())
            out.push_back(set_path(v, path, 0, el));
        return value::array(std::move(out));
    }
    if (target->type() == vtype::object) {
        value out = value::object();
        for (const auto& [k, el] : target->obj())
            out.set(k, set_path(v, path, 0, el));
        return out;
    }
    throw eval_error("expected an array or object at path " + want_string(path_v) +
                     ", got " + target->type_name());
}

value with(const value& v, const std::vector<value>& paths) {
    if (v.type() != vtype::object) wrong("object", v);
    value out = value::object();
    for (const auto& p : paths) keep_path(v, out, dot_path(want_string(p)), 0);
    return out;
}

value without(const value& v, const std::vector<value>& paths) {
    if (v.type() != vtype::object) wrong("object", v);
    value out = v;
    for (const auto& p : paths) out = remove_path(std::move(out), dot_path(want_string(p)), 0);
    return out;
}

value concat(const value& v, const std::vector<value>& others) {
    std::vector<value> out = want_array(v);
    for (const auto& o : others) {
        if (o.type() == vtype::array) out.insert(out.end(), o.arr().begin(), o.arr().end());
        else out.push_back(o);
    }
    return value::array(std::move(out));
}

value zip(const value& v, const std::vector<value>& others) {
    const auto& first = want_array(v);
    std::vector<const std::vector<value>*> cols{&first};
    for (const auto& o : others) cols.push_back(&want_array(o));
    for (const auto* c : cols)
        if (c->size() != first.size())
            throw eval_error("zip requires every array to be the same length");
    std::vector<value> out;
    out.reserve(first.size());
    for (size_t i = 0; i < first.size(); ++i) {
        std::vector<value> row;
        row.reserve(cols.size());
        for (const auto* c : cols) row.push_back((*c)[i]);
        out.push_back(value::array(std::move(row)));
    }
    return value::array(std::move(out));
}

// ---- searching -------------------------------------------------------------------

value find(const value& v, const value& needle) {
    const auto& a = want_array(v);
    for (size_t i = 0; i < a.size(); ++i)
        if (a[i] == needle) return value(static_cast<int64_t>(i));
    return value(int64_t{-1});
}

value find_all(const value& v, const value& needle) {
    const auto& a = want_array(v);
    std::vector<value> out;
    for (size_t i = 0; i < a.size(); ++i)
        if (a[i] == needle) out.push_back(value(static_cast<int64_t>(i)));
    return value::array(std::move(out));
}

// ---- diff and patch ----------------------------------------------------------------

value diff(const value& v, const value& other) {
    std::vector<std::string> path;
    std::vector<value> out;
    diff_into(v, other, path, out);
    return value::array(std::move(out));
}

value patch(const value& v, const value& changelog) {
    const auto& entries = want_array(changelog);
    value out = v;
    for (const auto& e : entries) {
        if (e.type() != vtype::object) wrong("object", e);
        const value* type = e.find("Type");
        const value* path = e.find("Path");
        const value* to   = e.find("To");
        if (!type || !path || path->type() != vtype::array)
            throw eval_error("changelog entry needs a Type and a Path");
        std::vector<std::string> segs;
        segs.reserve(path->arr().size());
        for (const auto& seg : path->arr()) segs.push_back(want_string(seg));
        if (segs.empty()) throw eval_error("changelog entry has an empty Path");
        const std::string& t = want_string(*type);
        if (t == "delete") out = remove_path(std::move(out), segs, 0);
        else if (t == "create" || t == "update")
            out = set_path(std::move(out), segs, 0, to ? *to : value());
        else throw eval_error("unrecognised changelog operation: " + t);
    }
    return out;
}

// ---- lambda-taking ------------------------------------------------------------------

value all(const value& v, const lambda& fn) {
    const auto& a = want_array(v);
    for (const auto& el : a) {
        const value r = fn(el);
        if (r.type() != vtype::boolean)
            throw eval_error(std::string("all: expected a boolean result, got ") + r.type_name());
        if (!r.as_bool()) return value(false);
    }
    // An empty array is documented as false, which is not the usual
    // vacuous-truth reading and has to be written out explicitly.
    return value(!a.empty());
}

value any(const value& v, const lambda& fn) {
    const auto& a = want_array(v);
    for (const auto& el : a) {
        const value r = fn(el);
        if (r.type() != vtype::boolean)
            throw eval_error(std::string("any: expected a boolean result, got ") + r.type_name());
        if (r.as_bool()) return value(true);
    }
    return value(false);
}

value find_by(const value& v, const lambda& fn) {
    const auto& a = want_array(v);
    for (size_t i = 0; i < a.size(); ++i) {
        const value r = fn(a[i]);
        if (r.type() == vtype::boolean && r.as_bool()) return value(static_cast<int64_t>(i));
    }
    return value(int64_t{-1});
}

value find_all_by(const value& v, const lambda& fn) {
    const auto& a = want_array(v);
    std::vector<value> out;
    for (size_t i = 0; i < a.size(); ++i) {
        const value r = fn(a[i]);
        if (r.type() == vtype::boolean && r.as_bool())
            out.push_back(value(static_cast<int64_t>(i)));
    }
    return value::array(std::move(out));
}

value sort_by(const value& v, const lambda& fn) {
    const auto& a = want_array(v);
    // The keys are computed once each rather than inside the comparator: a
    // comparison-time callback would run the query O(n log n) times, and the
    // query may be arbitrarily expensive.
    std::vector<std::pair<value, size_t>> keyed;
    keyed.reserve(a.size());
    for (size_t i = 0; i < a.size(); ++i) keyed.emplace_back(fn(a[i]), i);
    std::stable_sort(keyed.begin(), keyed.end(),
                     [](const auto& x, const auto& y) {
        const value& p = x.first;
        const value& q = y.first;
        if (p.is_number() && q.is_number()) return p.as_f64() < q.as_f64();
        if (p.is_stringy() && q.is_stringy()) return p.as_string() < q.as_string();
        throw eval_error("sort_by requires every key to be a string or a number");
    });
    std::vector<value> out;
    out.reserve(a.size());
    for (const auto& [k, i] : keyed) { (void)k; out.push_back(a[i]); }
    return value::array(std::move(out));
}

// A stable merge sort rather than std::stable_sort, because the comparator is
// user-supplied: a mapping that does not describe a strict weak ordering is a
// config bug, and std::stable_sort's reaction to one is undefined behaviour
// rather than a wrong answer. Merging never compares an element with itself and
// never runs off the end, whatever the predicate says.
namespace {
void merge_sort(std::vector<value>& a, const std::function<bool(const value&, const value&)>& less) {
    if (a.size() < 2) return;
    std::vector<value> buf(a.size());
    for (size_t width = 1; width < a.size(); width *= 2) {
        for (size_t lo = 0; lo < a.size(); lo += 2 * width) {
            const size_t mid = std::min(lo + width, a.size());
            const size_t hi  = std::min(lo + 2 * width, a.size());
            size_t i = lo, j = mid, k = lo;
            while (i < mid && j < hi) buf[k++] = less(a[j], a[i]) ? a[j++] : a[i++];
            while (i < mid) buf[k++] = a[i++];
            while (j < hi)  buf[k++] = a[j++];
        }
        a.swap(buf);
    }
}
} // namespace

value sort_with(const value& v, const lambda& fn) {
    std::vector<value> out = want_array(v);
    merge_sort(out, [&](const value& l, const value& r) {
        value pair = value::object();
        pair.set("left", l);
        pair.set("right", r);
        const value res = fn(pair);
        if (res.type() != vtype::boolean)
            throw eval_error("sort: the comparison must yield a boolean");
        return res.as_bool();
    });
    return value::array(std::move(out));
}

value unique_by(const value& v, const lambda& fn) {
    const auto& a = want_array(v);
    std::vector<value> keys;
    std::vector<value> out;
    for (const auto& el : a) {
        const value k = fn(el);
        bool seen = false;
        for (const auto& prev : keys) if (prev == k) { seen = true; break; }
        if (seen) continue;
        keys.push_back(k);
        out.push_back(el);
    }
    return value::array(std::move(out));
}

value map_each_key(const value& v, const lambda& fn) {
    const auto& o = want_object(v);
    value out = value::object();
    for (const auto& [k, el] : o) {
        const value nk = fn(value(k));
        // `nothing` -- an `if` with no else, say -- leaves the key alone, which
        // is what makes `map_each_key(k -> if ... { ... })` a conditional
        // rename rather than a wipe. Bytes are NOT accepted: the reference
        // switches on `string` specifically.
        if (nk.is_nothing()) { out.set(k, el); continue; }
        if (nk.type() != vtype::string)
            throw eval_error(std::string("unexpected result from key mapping: expected "
                                         "string, got ") + nk.type_name());
        out.set(nk.as_string(), el);
    }
    return out;
}

value fold(const value& v, const value& initial, const lambda& fn) {
    const auto& a = want_array(v);
    value tally = initial;
    for (const auto& el : a) {
        value pair = value::object();
        pair.set("tally", tally);
        pair.set("value", el);
        tally = fn(pair);
    }
    return tally;
}

} // namespace sf::m
