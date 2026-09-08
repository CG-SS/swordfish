// Backend A — the tree-walking interpreter.
//
// This is the semantic oracle: `swordfish run`, `swordfish test`, constant
// folding at build time, and the differential harness all go through it.
#include "swordfish/blobl/interp.hh"
#include "swordfish/methods.hh"
#include "swordfish/regex.hh"
#include "swordfish/blobl/method_registry.hh"

namespace sf::blobl {

// Regex methods take their pattern as the first argument: the interpreter looks
// it up in a cache, generated code hoists it to a file-scope static. Kept as one
// list so the two backends cannot disagree about which methods those are.
bool is_regex_method(std::string_view name) {
    return name == "re_match" || name == "re_replace_all" || name == "re_replace" ||
           name == "re_find_all" || name == "re_find_all_submatch" ||
           name == "re_find_object" || name == "re_find_all_object";
}

value interp::eval(const node& n, exec_ctx& ctx) const {
    return std::visit([&](const auto& k) -> value {
        using T = std::decay_t<decltype(k)>;

        if constexpr (std::is_same_v<T, lit_node>)   { return k.v; }
        else if constexpr (std::is_same_v<T, this_node>) { return ctx.self(); }
        else if constexpr (std::is_same_v<T, var_node>)  { return ctx.var(k.name); }
        else if constexpr (std::is_same_v<T, field_node>) {
            return get_field(eval(*k.target, ctx), k.key);
        }
        else if constexpr (std::is_same_v<T, index_node>) {
            value t = eval(*k.target, ctx);
            int64_t i = eval(*k.index, ctx).as_i64();
            const auto& a = t.arr();
            if (i < 0) i += static_cast<int64_t>(a.size());
            if (i < 0 || static_cast<size_t>(i) >= a.size())
                throw eval_error("array index out of bounds");
            return a[static_cast<size_t>(i)];
        }
        else if constexpr (std::is_same_v<T, not_node>) {
            return value(!truthy(eval(*k.operand, ctx)));
        }
        else if constexpr (std::is_same_v<T, neg_node>) {
            return num::sub(value(int64_t{0}), eval(*k.operand, ctx));
        }
        else if constexpr (std::is_same_v<T, binary_node>) {
            // Short-circuit before evaluating the RHS, matching Bloblang.
            if (k.op == binop::and_) {
                if (!truthy(eval(*k.lhs, ctx))) return value(false);
                return value(truthy(eval(*k.rhs, ctx)));
            }
            if (k.op == binop::or_) {
                if (truthy(eval(*k.lhs, ctx))) return value(true);
                return value(truthy(eval(*k.rhs, ctx)));
            }
            if (k.op == binop::coalesce) {
                try {
                    value l = eval(*k.lhs, ctx);
                    if (!l.is_null() && !l.is_nothing()) return l;
                } catch (const eval_error&) { /* fall through to RHS */ }
                return eval(*k.rhs, ctx);
            }
            value l = eval(*k.lhs, ctx), r = eval(*k.rhs, ctx);
            switch (k.op) {
            case binop::mul: return num::mul(l, r);
            case binop::div: return num::div(l, r);
            case binop::mod: return num::mod(l, r);
            case binop::add: return num::add(l, r);
            case binop::sub: return num::sub(l, r);
            case binop::eq:  return cmp_eq(l, r);
            case binop::neq: return cmp_neq(l, r);
            case binop::lt:  return cmp_lt(l, r);
            case binop::lte: return cmp_lte(l, r);
            case binop::gt:  return cmp_gt(l, r);
            case binop::gte: return cmp_gte(l, r);
            default: throw eval_error("unreachable binary op");
            }
        }
        else if constexpr (std::is_same_v<T, if_node>) {
            if (truthy(eval(*k.cond, ctx))) return eval(*k.then_, ctx);
            if (k.else_) return eval(*k.else_, ctx);
            return value::nothing();
        }
        else if constexpr (std::is_same_v<T, match_node>) {
            // The context becomes `this` for every case AND for the value that
            // wins, which is what makes `match this.foo { this == 1 => ... }`
            // read the way it looks.
            value ctx_val;
            const value* saved = ctx.this_v;
            if (k.context) {
                ctx_val = eval(*k.context, ctx);
                ctx.this_v = &ctx_val;
            }
            value result = value::nothing();       // no case matched
            try {
                for (const auto& [cond, val] : k.cases) {
                    if (cond) {
                        const value c = eval(*cond, ctx);
                        if (c.type() != vtype::boolean || !c.as_bool()) continue;
                    }
                    result = eval(*val, ctx);
                    break;
                }
            } catch (...) { ctx.this_v = saved; throw; }
            ctx.this_v = saved;
            return result;
        }
        else if constexpr (std::is_same_v<T, array_node>) {
            std::vector<value> items;
            items.reserve(k.items.size());
            for (const auto& it : k.items) {
                value v = eval(*it, ctx);
                if (literal_omits(v)) continue;      // see value.hh
                items.push_back(std::move(v));
            }
            return value::array(std::move(items));
        }
        else if constexpr (std::is_same_v<T, object_node>) {
            value o = value::object();
            for (const auto& e : k.entries) {
                const value key = eval(*e.key, ctx);
                if (!key.is_stringy())
                    throw eval_error(std::string("an object key must be a string, got ") +
                                     key.type_name());
                value v = eval(*e.value, ctx);
                if (literal_omits(v)) continue;
                o.set(key.as_string(), std::move(v));
            }
            return o;
        }
        else if constexpr (std::is_same_v<T, call_node>) {
            if (k.name == "metadata" || k.name == "meta") {
                // No argument means every entry, as an object.
                if (k.args.empty() || !k.args[0]) return ctx.meta_all();
                const value key = eval(*k.args[0], ctx);
                if (key.is_stringy() && key.as_string().empty()) return ctx.meta_all();
                return ctx.meta_get(key.as_string());
            }
            if (k.name == "counter") {
                // Each call site gets its own counter; the span makes the key.
                const std::string site = std::to_string(n.span.line) + ":" +
                                         std::to_string(n.span.col);
                const int64_t lo = (k.args.size() > 0 && k.args[0])
                    ? fn::opt_int(eval(*k.args[0], ctx), 1) : 1;
                const int64_t hi = (k.args.size() > 1 && k.args[1])
                    ? fn::opt_int(eval(*k.args[1], ctx), INT64_MAX) : INT64_MAX;
                // `nothing` marks an absent `set`, which is distinct from a
                // deletion (reset) and from null (read without incrementing).
                value set = value::nothing();
                if (k.args.size() > 2 && k.args[2]) set = eval(*k.args[2], ctx);
                return fn::counter(ctx, site, lo, hi, set);
            }
            // Message functions: they read the message, not `this`.
            if (k.name == "content")     return fn::content(ctx);
            if (k.name == "json")
                return fn::json_of(ctx, k.args.empty() || !k.args[0] ? value()
                                                                     : eval(*k.args[0], ctx));
            if (k.name == "error")       return fn::error_of(ctx);
            if (k.name == "errored")     return fn::errored(ctx);
            if (k.name == "batch_index") return fn::batch_index(ctx);
            if (k.name == "batch_size")  return fn::batch_size(ctx);
            if (k.name == "count")
                return fn::count(ctx, k.args.empty() || !k.args[0] ? value()
                                                                   : eval(*k.args[0], ctx));
            if (k.name == "root_meta")
                return fn::root_meta(ctx, k.args.empty() || !k.args[0] ? value()
                                                                       : eval(*k.args[0], ctx));
            if (k.name == "error_source_name")  return fn::error_source_name(ctx);
            if (k.name == "error_source_label") return fn::error_source_label(ctx);
            if (k.name == "error_source_path")  return fn::error_source_path(ctx);
            if (k.name == "random_int") {
                // Keyed by call site, as counter() is: two random_int calls in
                // one mapping are independent sequences.
                const std::string site = std::to_string(n.span.line) + ":" +
                                         std::to_string(n.span.col);
                const value seed = (!k.args.empty() && k.args[0]) ? eval(*k.args[0], ctx)
                                                                  : value();
                const int64_t lo = (k.args.size() > 1 && k.args[1])
                    ? fn::opt_int(eval(*k.args[1], ctx), 0) : 0;
                const int64_t hi = (k.args.size() > 2 && k.args[2])
                    ? fn::opt_int(eval(*k.args[2], ctx), INT64_MAX - 1) : INT64_MAX - 1;
                return fn::random_int(ctx, site, seed, lo, hi);
            }

            // Everything else comes from the registry, same as the methods.
            if (const function_def* def = find_function(k.name); def && def->invoke) {
                std::vector<value> args;
                args.reserve(k.args.size());
                for (const auto& a : k.args) args.push_back(a ? eval(*a, ctx) : value());
                return def->invoke(args);
            }
            throw eval_error("unknown function: " + k.name);
        }
        else if constexpr (std::is_same_v<T, lambda_node>) {
            // A bare lambda is not a value; it only means something as the
            // argument of a method that applies it.
            throw eval_error("a lambda can only be used as a method argument");
        }
        else if constexpr (std::is_same_v<T, method_node>) {
            // Coercion with a fallback runs BEFORE the target is evaluated: the
            // default covers a target that fails to resolve as well as one that
            // fails to coerce.
            if (k.name == "number" || k.name == "bool") {
                const node* dflt = k.args.empty() ? nullptr : k.args[0].get();
                auto coerce = [&](const value& x) {
                    return k.name == "number" ? m::number(x) : m::to_bool(x);
                };
                if (!dflt) return coerce(eval(*k.target, ctx));
                try { return coerce(eval(*k.target, ctx)); }
                catch (const eval_error&) { return coerce(eval(*dflt, ctx)); }
            }

            // `from_all` re-evaluates its TARGET once per message, so like
            // `or` and `catch` it must run before the target is touched.
            if (k.name == "from_all") {
                return eval_from_all(ctx, [&](exec_ctx& c) { return eval(*k.target, c); });
            }

            // `from(i)` rebinds before the target runs, like from_all.
            if (k.name == "from") {
                if (k.args.empty() || !k.args[0])
                    throw eval_error("from requires a message index");
                const int64_t idx = eval(*k.args[0], ctx).as_i64();
                return eval_from(ctx, idx, [&](exec_ctx& c) { return eval(*k.target, c); });
            }

            // `or` is lazy on BOTH sides, exactly like coalesce_lazy on the
            // compiled path: a target that fails to RESOLVE falls back rather
            // than propagating. Evaluating the target first made
            // `this.or(content())` -- the standard idiom for a stream carrying
            // both JSON and plain text -- an error in the interpreter while the
            // compiled path handled it correctly. The two backends disagreed
            // and no fixture covered it.
            if (k.name == "or") {
                try {
                    value v = eval(*k.target, ctx);
                    if (!v.is_null() && !v.is_nothing()) return v;
                } catch (const eval_error&) { /* fall through to the fallback */ }
                return (k.args.empty() || !k.args[0]) ? value() : eval(*k.args[0], ctx);
            }

            // `catch` also runs before the argument list: the fallback must not
            // be evaluated unless the target actually fails, and it sees the
            // error message as its value.
            if (k.name == "catch") {
                if (k.args.empty() || !k.args[0])
                    throw eval_error("catch requires a fallback argument");
                std::string err;
                try { return eval(*k.target, ctx); }
                catch (const eval_error& e) { err = e.what(); }
                const value ev(std::move(err));
                const auto* lam = std::get_if<lambda_node>(&k.args[0]->kind);
                if (lam) {
                    const std::string& p = lam->param;
                    const bool had = ctx.vars.count(p) > 0;
                    value saved = had ? ctx.vars[p] : value();
                    ctx.vars[p] = ev;
                    value r;
                    try { r = eval(*lam->body, ctx); }
                    catch (...) {
                        if (had) ctx.vars[p] = std::move(saved); else ctx.vars.erase(p);
                        throw;
                    }
                    if (had) ctx.vars[p] = std::move(saved); else ctx.vars.erase(p);
                    return r;
                }
                const value* saved = ctx.this_v;
                ctx.this_v = &ev;
                value r;
                try { r = eval(*k.args[0], ctx); }
                catch (...) { ctx.this_v = saved; throw; }
                ctx.this_v = saved;
                return r;
            }

            value t = eval(*k.target, ctx);

            // Everything below runs inside a try so a TYPE error can say where
            // the offending value came from. The description is derived from
            // the target's AST, which only this layer has; the type is known
            // only at the throw site. attach_source joins the two.
            try {

            // `.apply("name")` runs a declared map with the target as `this`
            // and a FRESH variable scope -- the reference resets ctx.Vars, so a
            // `let` inside a map cannot see or clobber the caller's. Counters
            // are deliberately not reset: counter() is stateful by design.
            if (k.name == "apply") {
                if (k.args.empty() || !k.args[0]) throw eval_error("apply requires a map name");
                const value name_v = eval(*k.args[0], ctx);
                if (!name_v.is_stringy()) throw eval_error("apply requires a map name");
                const mapping* target_map = scope_ ? scope_->find_map(name_v.as_string()) : nullptr;
                if (!target_map)
                    throw eval_error("map " + name_v.as_string() + " was not found");
                // Through the same helper generated code uses, so the scope
                // reset and the recursion guard cannot differ between backends.
                const interp sub(*target_map, *this);
                return apply_map(ctx, t, [&](exec_ctx& c) { return sub.run_ctx(c); });
            }

            // Lambda-taking methods are handled before the argument list is
            // evaluated, because the query must be applied per element rather
            // than reduced to a value once.
            const lambda_method* lm = find_lambda_method(k.name);
            // An optional query that was not supplied means the plain,
            // table-driven form: `sort()` and `unique()` are the same methods
            // with and without one.
            if (lm && lm->query_optional &&
                (k.args.size() <= lm->leading_args || !k.args[lm->leading_args]))
                lm = nullptr;
            if (lm) {
                const size_t qi = lm->leading_args;
                if (k.args.size() <= qi || !k.args[qi])
                    throw eval_error(k.name + " requires a query argument");
                // fold's initial tally is an ordinary argument and is evaluated
                // BEFORE the element binding exists.
                const value initial = qi > 0 && k.args[0] ? eval(*k.args[0], ctx) : value();
                const node& query = *k.args[qi];
                const auto* lam = std::get_if<lambda_node>(&query.kind);

                // Two argument forms, both in the reference docs: a named lambda
                // (`map_each(num -> num.abs())`) binds the element to a variable,
                // and a bare query (`map_each(this.string())`) rebinds `this` to
                // the element instead.
                value result;
                if (lam) {
                    // The parameter shadows any variable of the same name, and the
                    // previous binding is restored afterwards.
                    const std::string& p = lam->param;
                    const bool had = ctx.vars.count(p) > 0;
                    value saved = had ? ctx.vars[p] : value();
                    auto restore = [&] {
                        if (had) ctx.vars[p] = std::move(saved); else ctx.vars.erase(p);
                    };
                    auto apply = [&](const value& element) {
                        ctx.vars[p] = element;
                        return eval(*lam->body, ctx);
                    };
                    try { result = call_lambda_method(k.name, t, initial, apply); }
                    catch (...) { restore(); throw; }
                    restore();
                } else {
                    const value* saved = ctx.this_v;
                    auto apply = [&](const value& element) {
                        ctx.this_v = &element;
                        return eval(query, ctx);
                    };
                    try { result = call_lambda_method(k.name, t, initial, apply); }
                    catch (...) { ctx.this_v = saved; throw; }
                    ctx.this_v = saved;
                }
                return result;
            }

            std::vector<value> args;
            args.reserve(k.args.size());
            for (const auto& a : k.args) args.push_back(eval(*a, ctx));

            // Table-driven methods first: one lookup instead of a dispatch
            // chain that has to be edited for every method added.
            if (const method_def* def = find_method(k.name); def && def->invoke)
                return def->invoke(t, args);


            // Regex methods take their pattern as the first argument. The
            // interpreter looks it up in a cache; generated code hoists it.
            if (is_regex_method(k.name)) {
                if (args.empty()) throw eval_error(k.name + " requires a pattern argument");
                const re& r = cached_re(args[0].as_string());
                if (k.name == "re_match")            return m::re_match(t, r);
                if (k.name == "re_find_all")         return m::re_find_all(t, r);
                if (k.name == "re_find_all_submatch")return m::re_find_all_submatch(t, r);
                if (k.name == "re_find_object")      return m::re_find_object(t, r);
                if (k.name == "re_find_all_object")  return m::re_find_all_object(t, r);
                // re_replace is the same operation as re_replace_all under the
                // older name; the reference keeps both.
                if (args.size() < 2) throw eval_error(k.name + " requires a replacement");
                return m::re_replace_all(t, r, args[1]);
            }
            throw eval_error("unknown method: " + k.name);
            } catch (const eval_error& e) {
                const auto sd = describe_source(*k.target);
                attach_source(e, sd.text, t, sd.includes_value);
            }
        }
        else { throw eval_error("unhandled AST node"); }
    }, n.kind);
}

value interp::run(const value& input, exec_ctx& ctx) const {
    ctx.this_v = &input;
    return run_ctx(ctx);
}

value interp::run_ctx(exec_ctx& ctx) const {
    // `nothing`, not null: mapPart starts newValue at value.Nothing, so a
    // mapping whose statements all skip leaves the original content in place
    // rather than replacing it with null.
    value root = ctx.root_init ? *ctx.root_init : value::nothing();
    bool root_set = false;

    for (const auto& st : m_.statements) {
        // Benthos reports a failing statement as
        // `failed assignment (line N): <message>`, and a user's test may assert
        // on that text -- `throw("...")` makes the inner message theirs, so the
        // wrapper is the only part we control. The line is the statement's,
        // 1-based, within the mapping source.
        value v;
        try {
            v = eval(*st.expr, ctx);
        } catch (const eval_error& e) {
            throw eval_error("failed assignment (line " +
                             std::to_string(st.expr->span.line) + "): " + e.what());
        }
        if (v.is_nothing()) continue;                 // assignment skipped

        switch (st.target.kind) {
        case target_kind::var:
            if (v.is_deleted()) ctx.vars.erase(st.target.name);
            else                ctx.vars[st.target.name] = std::move(v);
            break;
        case target_kind::meta:
            ctx.meta_set(st.target.name, std::move(v));
            break;
        case target_kind::root:
            root = std::move(v);
            root_set = true;
            break;
        case target_kind::root_path:
            // Only when there is nothing to build on. This exists so
            // `root.a = 1` starts an object rather than assigning into
            // `nothing` -- but for a mutation root already holds the input
            // document, and replacing it here discarded exactly the fields the
            // mutation was supposed to keep.
            if (!root_set) {
                if (root.is_nothing()) root = value::object();
                root_set = true;
            }
            root = assign_path(std::move(root), st.target.path, 0, std::move(v));
            break;
        }
    }
    return root;
}

} // namespace sf::blobl
