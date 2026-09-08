// The runtime surface shared by the interpreter and by generated code.
// Generated .cc files include exactly this header and nothing else — it is the
// single header a generated program compiles against.
#pragma once

#include "swordfish/methods.hh"
#include "swordfish/value.hh"
#include "swordfish/message.hh"

#include <map>
#include <random>
#include <string>
#include <vector>

namespace sf {

struct exec_ctx {
    const value*                    this_v = nullptr;
    // Metadata splits into a WRITE target and a READ source, because Bloblang
    // does. `meta x = ...` builds the metadata of the message being produced,
    // while `meta("x")` reads the metadata of the message that came IN --
    // so within one mapping a write is not visible to a later read:
    //
    //     meta foo = "updated"
    //     root.a = meta("foo")        # the INPUT's foo, not "updated"
    //     root.b = root_meta("foo")   # "updated"
    //
    // Verified against redpanda-connect 4.107.2. Swordfish used to point both
    // at the output and so returned "updated" for both.
    //
    // Keeping a separate read source costs nothing: message::shallow_copy()
    // already gives the output its own metadata (a plain member, unlike the
    // refcounted payload), so the input's copy is an untouched snapshot that
    // exists whether or not anyone reads it.
    //
    // Both are null when there is no message (a bare expression, a `generate`
    // mapping); using meta then errors rather than being silently dropped.
    // What `root` starts as. Null means `nothing`, which is what `mapping` and
    // `bloblang` do: an unassigned root leaves the message content alone.
    // `mutation` differs -- it starts root at the INPUT document, so a mapping
    // that sets one field keeps the rest. Treating the three as aliases dropped
    // every field a mutation did not mention, which is silent data loss and is
    // what the Benthos test corpus caught.
    const value*                    root_init = nullptr;
    metadata*                       meta = nullptr;      // written by `meta x = ...`
    const metadata*                 meta_in = nullptr;   // read by meta() / metadata()
    std::map<std::string, value>    vars;
    std::map<std::string, int64_t>  counters;
    // random_int() resolves its seed once per call site and then keeps the
    // generator, which is what "only resolved once during the lifetime of the
    // mapping" means.
    std::map<std::string, std::mt19937_64> rngs;
    // The message being mapped, for content(), json(), error() and errored().
    // Null where there is none (a bare expression, a `generate` mapping); those
    // functions then error rather than quietly returning an empty result.
    const message*                  msg = nullptr;
    // Every message of the current batch, for `from_all()`, which evaluates its
    // target once per message. Null outside a batch context, where from_all is
    // an error rather than a silent single-element array.
    const std::vector<message>*     all = nullptr;
    // Position within the batch, for batch_index() and batch_size(). A message
    // outside a batch is a batch of one.
    int64_t                         batch_index = 0;
    int64_t                         batch_size  = 1;
    // How deep `.apply()` currently is. A map that applies itself is legal to
    // write and recurses without bound; the interpreter and generated code both
    // count, so the failure is an error with a name rather than a stack
    // overflow. Benthos has no such limit and does crash on one.
    int                             apply_depth = 0;
    static constexpr int            max_apply_depth = 64;

    const message& message_of(const char* what) const {
        if (!msg) throw eval_error(std::string(what) + "() requires a message");
        return *msg;
    }

    // `this` resolves lazily: a mapping that never references it must work on a
    // message that is not valid JSON. Benthos behaves the same way, which is why
    // `root.error = "unparseable"` inside a `catch` is a working idiom.
    const value& self() const {
        if (!this_v) throw eval_error("message could not be parsed as structured data");
        return *this_v;
    }

    void meta_set(std::string_view key, value v) {
        if (!meta) throw eval_error("no metadata is available in this context");
        if (v.is_deleted()) meta->remove(key); else meta->set(key, std::move(v));
    }
    value meta_get(std::string_view key) const {
        if (!meta_in) throw eval_error("no metadata is available in this context");
        const value* v = meta_in->find(key);
        return v ? *v : value();
    }
    // meta() with no argument is every entry as an object, matching the
    // reference. The interpreter used to throw here and the emitter used to
    // look up the empty key and return null -- a divergence between the two
    // backends that no fixture exercised.
    value meta_all() const {
        if (!meta_in) throw eval_error("no metadata is available in this context");
        value out = value::object();
        for (const auto& [k, v] : meta_in->entries()) out.set(k, v);
        return out;
    }

    const value& var(const std::string& n) const {
        auto it = vars.find(n);
        if (it == vars.end()) throw eval_error("variable not found: " + n);
        return it->second;
    }
};

// Rethrows a type_error naming where the offending value came from, and what it
// was. `source` is rendered by the code holding the AST (describe_source in the
// blobl layer); `v` is the value itself.
//
// Only a type_error is decorated, and only once: an error already carrying a
// source passes straight through, so a chain like `this.a.foo().bar()` reports
// the innermost failure rather than accumulating a suffix per link.
[[noreturn]] inline void attach_source(const eval_error& e, std::string_view source,
                                       const value& v, bool source_has_value = false) {
    const auto* te = dynamic_cast<const type_error*>(&e);
    if (!te || source.empty()) throw eval_error(e.what());

    std::string msg = std::string(e.what()) + " from " + std::string(source);
    // A string literal names its own value -- `string literal ("x")` -- so
    // appending it again produced `("x") ("x")`.
    if (source_has_value) throw eval_error(std::move(msg));
    // The value itself, but only when printing it helps. A null has nothing to
    // show, and an array or object would bury the message in its own contents.
    // Matches the reference, which prints `(5)`, `(true)`, `("hi")` and nothing
    // for null, array and object.
    switch (v.type()) {
    case vtype::boolean: msg += v.as_bool() ? " (true)" : " (false)"; break;
    case vtype::i64: case vtype::f64: case vtype::f32: case vtype::raw_number:
        msg += " (" + v.to_json() + ")"; break;
    case vtype::string:
        msg += " (" + v.to_json() + ")"; break;
    // bytes are NOT shown, alongside null, array and object: the reference
    // prints `got bytes from function content` with no value, and base64 of a
    // payload would bury the message it is attached to.
    default: break;                      // null, bytes, array, object, timestamp
    }
    throw eval_error(std::move(msg));
}

// Nulls every per-call pointer in an exec_ctx on the way out.
//
// These all point at things that die when a processor's `process()` returns --
// the batch (a by-value parameter), a message inside it, a value parsed on the
// stack. A member exec_ctx outlives the call, so leaving them set leaves it
// holding dangling pointers until the next call happens to overwrite them.
// Nothing reads them in between today; this makes that a property of the type
// rather than of the current call order.
//
// RAII because the alternative is assignments at the end of the function, and
// this session has already produced several bugs where an exception skipped
// exactly that kind of trailing cleanup.
struct ctx_scope {
    exec_ctx& c;
    ~ctx_scope() {
        c.all = nullptr;
        c.msg = nullptr;
        c.this_v = nullptr;
        c.meta = nullptr;
        c.meta_in = nullptr;
        c.root_init = nullptr;
    }
};

// Binds a lambda's parameter for the duration of a scope and puts the variable
// back EXACTLY as it was -- including back to unbound when it was never set.
//
// The emitted code used to do this inline and got both halves wrong. It wrote
// `auto& sl_ = ctx.vars[p]; auto saved_ = sl_; sl_ = arg; ... ; sl_ = saved_;`,
// so `operator[]` inserted a default-constructed null when `p` was unbound and
// then wrote that null back, leaving the name bound to null afterwards:
// `[1,2].map_each(zz -> zz)` followed by `$zz.catch("unset")` gave `null`
// compiled where the interpreter and the reference both gave `"unset"`. And the
// restore was a straight-line statement, so a body that threw skipped it
// entirely and left the CALLER's variable clobbered with the last element:
// with `let zz = "orig"`, a throwing `map_each(zz -> ...)` under a `.catch()`
// left `$zz` as `1` compiled against `"orig"` on the other two.
//
// A destructor gets both right, and cannot be skipped.
class scoped_var {
public:
    scoped_var(exec_ctx& c, std::string name, const value& v)
        : _c(c), _name(std::move(name)) {
        const auto it = _c.vars.find(_name);
        _had = it != _c.vars.end();
        if (_had) _saved = it->second;
        _c.vars[_name] = v;
    }
    ~scoped_var() {
        // Runs during unwinding when the body threw, where an escaping
        // exception would call std::terminate.
        try {
            if (_had) _c.vars[_name] = std::move(_saved);
            else      _c.vars.erase(_name);
        } catch (...) {}
    }
    scoped_var(const scoped_var&) = delete;
    scoped_var& operator=(const scoped_var&) = delete;

private:
    exec_ctx&   _c;
    std::string _name;
    value       _saved;
    bool        _had = false;
};

// `from_all()`: evaluate the target once per message of the batch and collect
// the results. The per-message rebinding lives here so the interpreter and the
// emitted code share it rather than each reimplementing which fields move.
//
// The context is rebound IN PLACE and restored, not copied: `vars`, `counters`
// and `rngs` must stay shared, and a counter that reset per message would be a
// subtle wrong answer rather than an error.
//
// `this` is NOT rebound, and that is the whole subtlety of these two methods.
// The reference's own documentation says so -- "functions that support this
// behavior are `content`, `json` and `meta`" -- and its implementation is
// literally `ctx.Index = f.index` and nothing else
// (benthos internal/bloblang/query/methods.go, fromMethod::Exec and
// fromAllMethod). So `this.id.from_all()` yields the CURRENT message's id
// repeated once per message, not each message's id; `json("id").from_all()`
// yields each message's.
//
// This used to rebind this_v as well, which made the interpreter answer
// [10,20,30] where the reference and the compiled backend both answer
// [10,10,10] -- the two backends disagreeing about what a config means, with the
// interpreter on the wrong side. The compiled path was never affected because
// its emitted closure reads the prologue's `self_` capture rather than
// ctx.this_v.
template <class F>
value eval_from_all(exec_ctx& ctx, F&& f) {
    if (!ctx.all) throw eval_error("from_all requires a batch context");
    struct restore {
        exec_ctx& c;
        const message* msg = nullptr;
        const metadata* meta_in = nullptr; int64_t idx = 0;
        ~restore() { c.msg = msg; c.meta_in = meta_in; c.batch_index = idx; }
    } guard{ctx, ctx.msg, ctx.meta_in, ctx.batch_index};

    std::vector<value> out;
    out.reserve(ctx.all->size());
    for (size_t i = 0; i < ctx.all->size(); ++i) {
        const message& m = (*ctx.all)[i];
        ctx.msg = &m;
        ctx.meta_in = &m.meta();
        ctx.batch_index = static_cast<int64_t>(i);
        out.push_back(f(ctx));
    }
    return value::array(std::move(out));
}

// `from(i)`: evaluate the target against message `i` of the batch instead of
// the current one. Shares the rebinding with eval_from_all so the two cannot
// disagree about which fields move.
template <class F>
value eval_from(exec_ctx& ctx, int64_t idx, F&& f) {
    if (!ctx.all) throw eval_error("from requires a batch context");
    if (idx < 0 || static_cast<size_t>(idx) >= ctx.all->size())
        throw eval_error("from(" + std::to_string(idx) + ") is outside a batch of " +
                         std::to_string(ctx.all->size()));
    // `this` is deliberately not rebound -- see eval_from_all above.
    struct restore {
        exec_ctx& c;
        const message* msg = nullptr;
        const metadata* meta_in = nullptr; int64_t i = 0;
        ~restore() { c.msg = msg; c.meta_in = meta_in; c.batch_index = i; }
    } guard{ctx, ctx.msg, ctx.meta_in, ctx.batch_index};

    const message& m = (*ctx.all)[static_cast<size_t>(idx)];
    ctx.msg = &m;
    ctx.meta_in = &m.meta();
    ctx.batch_index = idx;
    return f(ctx);
}

// Bloblang functions. Impure ones take the context so their state is explicit
// and neither backend can accidentally fold them (02-bloblang.md § 6.3).
namespace fn {
// An optional integer bound. Named-argument resolution fills unsupplied
// parameters with null, so `counter(set: null)` arrives with null min and max --
// both backends must treat that as "use the default" rather than coercing.
inline int64_t opt_int(const value& v, int64_t dflt) {
    return v.is_number() ? v.as_i64() : dflt;
}

// An incrementing sequence per call site, resetting to `min` once `max` is
// passed. `set` steers it: a non-negative integer sets the counter, null reads
// it without incrementing, and a deletion resets it to min.
//
// `site` identifies the call site so two counters in one mapping stay
// independent, which is what "each counter instance maintains its own state"
// means in the reference documentation.
value counter(exec_ctx& ctx, const std::string& site,
              int64_t min_v, int64_t max_v, const value& set);

// Message functions. They read the message rather than `this`, which is the
// point: `json("foo")` reaches the ROOT document even from inside a lambda or a
// nested mapping context, where `this` does not.
value content(const exec_ctx& ctx);
value json_of(const exec_ctx& ctx, const value& path);
value error_of(const exec_ctx& ctx);
value errored(const exec_ctx& ctx);
value batch_index(const exec_ctx& ctx);
value batch_size(const exec_ctx& ctx);

// Pure functions: no context, so both backends can call them identically and
// constant folding stays available.
value deleted();
value nothing();
value throw_(const value& why);
value range(const value& start, const value& stop, const value& step);
value hostname();
value env(const value& name, const value& no_cache);
value pi();
value uuid_v4();
value uuid_v7(const value& at);
value ulid(const value& encoding, const value& random_source);
value ksuid();
value nanoid(const value& length, const value& alphabet);
value snowflake_id(const value& node_id);
value read_file(const value& path, const value& no_cache);
value zero_bytes(const value& length);

// Context-taking, so both backends name them explicitly.
value random_int(exec_ctx& ctx, const std::string& site, const value& seed,
                 int64_t min_v, int64_t max_v);
value count(exec_ctx& ctx, const value& name);
value root_meta(const exec_ctx& ctx, const value& key);
value error_source_name(const exec_ctx& ctx);
value error_source_label(const exec_ctx& ctx);
value error_source_path(const exec_ctx& ctx);

// ---- time (methods_time.cc) --------------------------------------------------
value now();
value timestamp_unix();
value timestamp_unix_milli();
value timestamp_unix_micro();
value timestamp_unix_nano();
}

// Operations shared by both backends. Anything with observable semantics lives
// here rather than being reimplemented in the emitter, so the two backends
// cannot drift (the § 4.3 contract, applied in miniature).
// A missing field yields NULL rather than failing. This is not what it looks
// like from the documentation -- every documented example reads a field that
// exists -- but it is what the reference does, and its own error text gives it
// away: `this.missing.uppercase()` fails with "expected string value, got null
// from field `this.missing`", so the field access succeeded and produced null.
// The same is true of a field read from a non-object.
//
// Throwing here instead made `root.x = this.missing` an error where the
// reference produces {"x":null}. Found by the L4 gate against the real binary;
// the conformance corpus and the interpreter/compiled differential both missed
// it, because neither has anything to compare against on this point.
inline const value& get_field(const value& v, std::string_view key) noexcept {
    static const value absent;
    // A numeric segment indexes an ARRAY: `this.items.0` is the first element,
    // and `this.0.keys()` reads through the first element of an array root.
    // On an object the same segment is an ordinary key, so `{"0":"zero"}.0`
    // still reads the key -- the target's type decides, not the spelling.
    if (v.type() == vtype::array) {
        if (key.empty() || key.find_first_not_of("0123456789") != std::string_view::npos)
            return absent;
        const auto& a = v.arr();
        unsigned long long idx = 0;
        for (char c : key) {
            idx = idx * 10 + static_cast<unsigned>(c - '0');
            if (idx >= a.size()) return absent;      // also caps runaway digits
        }
        return a[static_cast<size_t>(idx)];
    }
    if (v.type() != vtype::object) return absent;
    const value* f = v.find(key);
    return f ? *f : absent;
}

value index_of(const value& v, const value& idx);

// root.a.b = x — creates intermediate objects as needed.
value assign_path(value root, const std::vector<std::string>& path, size_t i, value v);

// `|` and `.or()`: the right-hand side is evaluated only if the left is absent,
// and a failure on the left counts as absent. Both sides are passed as thunks so
// generated code short-circuits exactly like the interpreter does.
template <class L, class R>
value coalesce_lazy(L&& l, R&& r) {
    try {
        value v = l();
        if (!v.is_null() && !v.is_nothing()) return v;
    } catch (const eval_error&) { /* fall through */ }
    return r();
}

// `.catch(fallback)`: the fallback runs only when the target fails, and it is
// handed the error MESSAGE as its value -- which is what makes
// `catch(err -> {"error":err})` work.
template <class T, class D>
value catch_or(T&& t, D&& d) {
    std::string err;
    try { return t(); } catch (const eval_error& e) { err = e.what(); }
    // Outside the handler: a fallback that throws must not do so while an
    // exception is being handled, and it may itself be a mapping that fails.
    return d(value(std::move(err)));
}

// `.number(default)` and `.bool(default)`: the fallback covers a failure to
// evaluate the TARGET as well as a failure to coerce it, so the target has to
// be a thunk too -- numberCoerceMethod wraps target.Exec, not just IToNumber.
template <class T, class D>
value coerce_or(T&& t, D&& d) {
    try { return t(); } catch (const eval_error&) { return d(); }
}

// `this` in generated code. The pointer is loaded once in the function
// prologue, but the null check happens at each USE: a mapping may reference
// `this` inside a `.catch(...)` on a message that is not structured, and the
// interpreter resolves `this` lazily for exactly that reason. Binding
// `ctx.self()` eagerly instead made `root = this.catch(deleted())` throw in
// compiled mode while the interpreter returned a deletion -- caught by the
// differential gate.
inline const value& self_of(const value* p) {
    if (!p) throw eval_error("message could not be parsed as structured data");
    return *p;
}

// A computed object key. Only a string will do: an object with a numeric key
// has no representation, and coercing one silently would hide the mistake.
inline const std::string& object_key(const value& v) {
    if (!v.is_stringy())
        throw eval_error(std::string("an object key must be a string, got ") + v.type_name());
    return v.as_string();
}

// A match arm's condition must be a boolean; anything else means the arm does
// not match, which is what `matched, _ := caseVal.(bool)` does in Go.
inline bool truthy_bool(const value& v) {
    return v.type() == vtype::boolean && v.as_bool();
}

// `.apply("name")`: the map runs with the target as `this` and with a FRESH
// variable scope, because the reference resets ctx.Vars before executing it.
// Counters deliberately survive -- counter() is stateful by design, and the
// documented `map increment { root = counter() }` example depends on it.
template <class F>
value apply_map(exec_ctx& ctx, const value& target, F&& f) {
    if (ctx.apply_depth >= exec_ctx::max_apply_depth)
        throw eval_error("apply: maps are nested too deeply (a map applying itself?)");
    const value* saved_this = ctx.this_v;
    std::map<std::string, value> saved_vars;
    saved_vars.swap(ctx.vars);
    ctx.this_v = &target;
    ++ctx.apply_depth;
    value r;
    try { r = f(ctx); }
    catch (...) {
        --ctx.apply_depth;
        ctx.this_v = saved_this;
        ctx.vars.swap(saved_vars);
        throw;
    }
    --ctx.apply_depth;
    ctx.this_v = saved_this;
    ctx.vars.swap(saved_vars);
    return r;
}

// A generated mapping is just this.
using mapping_fn = value (*)(exec_ctx&);

} // namespace sf
