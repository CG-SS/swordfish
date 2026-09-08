// The set of known functions and methods, and the pass that validates a mapping
// against it.
//
// Benthos resolves names when BUILDING a mapping (query.MethodSet::Init raises
// "unrecognised method 'x'"), not while evaluating it. That distinction matters
// far more than it looks: an eval-time error can be swallowed by `|` or
// `.catch()`, so an unimplemented method silently becomes a plausible wrong
// answer instead of a loud failure. Validating at parse time makes an
// unimplemented method a CONFIG error with a line number — which is what
// this project requires while coverage is incomplete.
#include "swordfish/blobl/names.hh"

#include "swordfish/blobl/method_registry.hh"
#include "swordfish/blobl/signatures.hh"

#include <set>

namespace sf::blobl {

namespace {

// Kept in one place so the interpreter, the emitter and this validator cannot
// disagree about what exists. Parameter names come from the reference
// documentation; they are what makes named-argument calls resolvable.
// Arity is checked here, at build time, for the same reason names are.
void check_arity(const signature& sig, size_t given, const char* what,
                 source_span where, std::vector<name_error>& out) {
    if (sig.variadic) return;
    const size_t req = sig.required_count();
    if (given < req || given > sig.params.size())
        out.push_back({where, std::string(what) + " '" + std::string(sig.name) + "' expects " +
                              (req == sig.params.size() ? std::to_string(req)
                                                        : std::to_string(req) + " to " +
                                                          std::to_string(sig.params.size())) +
                              " argument(s), got " + std::to_string(given)});
}

// Rewrites a named-argument call into positional form, so every backend sees a
// plain positional list and neither needs to know about named arguments.
// Missing optional parameters become explicit nulls, which is what the callee's
// default handling expects.
void resolve_named_args(const signature& sig, std::vector<node_ptr>& args,
                        std::vector<std::string>& names, const char* what,
                        source_span where, std::vector<name_error>& out) {
    bool any_named = false;
    for (const auto& n : names) if (!n.empty()) any_named = true;
    if (!any_named) { names.clear(); return; }

    if (sig.variadic) {
        out.push_back({where, std::string(what) + " '" + std::string(sig.name) +
                              "' takes variadic arguments, which cannot be named"});
        return;
    }
    for (size_t i = 0; i < names.size(); ++i)
        if (names[i].empty()) {
            out.push_back({where, "cannot mix positional and named arguments in a call to " +
                                  std::string(what) + " '" + std::string(sig.name) + "'"});
            return;
        }

    std::vector<node_ptr> slots(sig.params.size());
    for (size_t i = 0; i < names.size(); ++i) {
        const size_t idx = sig.index_of(names[i]);
        if (idx == static_cast<size_t>(-1)) {
            out.push_back({where, std::string(what) + " '" + std::string(sig.name) +
                                  "' has no parameter named '" + names[i] + "'"});
            return;
        }
        if (slots[idx]) {
            out.push_back({where, "argument '" + names[i] + "' given twice"});
            return;
        }
        slots[idx] = std::move(args[i]);
    }
    for (size_t i = 0; i < sig.params.size(); ++i)
        if (!slots[i] && sig.params[i].required) {
            out.push_back({where, std::string(what) + " '" + std::string(sig.name) +
                                  "' requires argument '" + std::string(sig.params[i].name) + "'"});
            return;
        }
    // Trailing unset optionals are dropped rather than padded with nulls, so
    // arity checks downstream still see a sensible count.
    while (!slots.empty() && !slots.back()) slots.pop_back();
    for (auto& sl : slots) if (!sl) sl = mk<lit_node>(where, value());
    args = std::move(slots);
    names.clear();
}

void walk(node& n, std::vector<name_error>& out);
// The maps in scope, so `.apply("x")` can be checked at build time like every
// other name. Null while validating a bare query, which has no map definitions.
thread_local const mapping* g_scope = nullptr;

void walk_all(std::vector<node_ptr>& v, std::vector<name_error>& out) {
    for (auto& n : v) if (n) walk(*n, out);
}

void walk(node& n, std::vector<name_error>& out) {
    const source_span span = n.span;
    std::visit([&](auto& k) {
        using T = std::decay_t<decltype(k)>;
        if constexpr (std::is_same_v<T, method_node>) {
            const signature* sig = find_method_signature(k.name);
            if (!sig) {
                out.push_back({span, "unrecognised method '" + k.name + "'"});
            } else {
                resolve_named_args(*sig, k.args, k.arg_names, "method", span, out);
                check_arity(*sig, k.args.size(), "method", span, out);
            }
            // `.apply("name")` names a map, and an unknown one has to be a
            // build error for the same reason an unknown method is: `|` and
            // `.catch()` would otherwise turn it into a plausible wrong answer.
            if (k.name == "apply") {
                if (k.args.size() != 1 || !k.args[0]) {
                    out.push_back({span, "apply requires a map name"});
                } else if (const auto* lit = std::get_if<lit_node>(&k.args[0]->kind);
                           lit && lit->v.is_stringy()) {
                    if (!g_scope)
                        out.push_back({span, "apply is not available here: this mapping "
                                             "declares no maps"});
                    else if (!g_scope->find_map(lit->v.as_string()))
                        out.push_back({span, "map '" + lit->v.as_string() + "' was not found"});
                }
                // A computed name cannot be checked here; it is checked at run
                // time instead.
            }
            walk(*k.target, out);
            walk_all(k.args, out);
        } else if constexpr (std::is_same_v<T, call_node>) {
            const signature* sig = find_function_signature(k.name);
            if (!sig) {
                out.push_back({span, "unrecognised function '" + k.name + "'"});
            } else {
                resolve_named_args(*sig, k.args, k.arg_names, "function", span, out);
                check_arity(*sig, k.args.size(), "function", span, out);
            }
            walk_all(k.args, out);
        } else if constexpr (std::is_same_v<T, lambda_node>) {
            walk(*k.body, out);
        } else if constexpr (std::is_same_v<T, field_node>) {
            walk(*k.target, out);
        } else if constexpr (std::is_same_v<T, index_node>) {
            walk(*k.target, out); walk(*k.index, out);
        } else if constexpr (std::is_same_v<T, binary_node>) {
            walk(*k.lhs, out); walk(*k.rhs, out);
        } else if constexpr (std::is_same_v<T, neg_node>) {
            walk(*k.operand, out);
        } else if constexpr (std::is_same_v<T, if_node>) {
            walk(*k.cond, out); walk(*k.then_, out);
            if (k.else_) walk(*k.else_, out);
        } else if constexpr (std::is_same_v<T, array_node>) {
            walk_all(k.items, out);
        } else if constexpr (std::is_same_v<T, object_node>) {
            for (auto& e : k.entries) {
                // A key whose type is known at PARSE time is checked here. The
                // reference refuses `{5: ...}` outright -- "object keys must be
                // strings, received: int64" -- and shuts down; this accepted the
                // config and failed the mapping at run time instead, so the
                // failure arrived in production rather than at lint. A computed
                // key can only be checked when it is evaluated, and
                // object_key() does that.
                if (e.key)
                    if (const auto* lit = std::get_if<lit_node>(&e.key->kind))
                        if (!lit->v.is_stringy())
                            throw eval_error(std::string("object keys must be strings, "
                                                         "received: ") + lit->v.type_name());
                if (e.key)   walk(*e.key, out);
                if (e.value) walk(*e.value, out);
            }
        } else if constexpr (std::is_same_v<T, match_node>) {
            if (k.context) walk(*k.context, out);
            for (auto& [cond, val] : k.cases) {
                if (cond) walk(*cond, out);
                if (val)  walk(*val, out);
            }
        }
    }, n.kind);
}

} // namespace

// Signatures come from the one registry the backends also use, so validation
// can never disagree with what is actually callable.
const signature* find_method_signature(std::string_view name) {
    const method_def* d = find_method(name);
    return d ? &d->sig : nullptr;
}
const signature* find_function_signature(std::string_view name) {
    const function_def* d = find_function(name);
    return d ? &d->sig : nullptr;
}


std::vector<name_error> validate_names(mapping& m) {
    std::vector<name_error> out;
    const mapping* saved = g_scope;
    g_scope = &m;
    for (auto& st : m.statements) if (st.expr) walk(*st.expr, out);
    // A map body sees the same set of maps the outer mapping does, so it can
    // apply a sibling -- and itself, which the reference also permits.
    for (auto& nm : m.maps)
        for (auto& st : nm.body->statements) if (st.expr) walk(*st.expr, out);
    g_scope = saved;
    return out;
}

} // namespace sf::blobl
