// Backend B — the C++ emitter.
//
// Emits calls into the SAME runtime helpers the interpreter uses (sf::num::*,
// sf::m::*, sf::cmp_*). The win is that dispatch happens here, at build time,
// instead of per message: the optimiser sees direct calls it can inline.
//
// The one rule that must never be broken: emit from the AST with FULL
// parenthesisation. Bloblang's precedence does not match C++'s — `&&`/`||`
// share a level, all six comparisons share a level, and `|` binds like `*`.
#include "swordfish/blobl/emit.hh"
#include "swordfish/blobl/interp.hh"
#include "swordfish/methods.hh"
#include "swordfish/blobl/method_registry.hh"
#include "swordfish/config/spec.hh"
#include "swordfish/regex.hh"

#include <cctype>
#include <cmath>
#include <set>
#include <optional>
#include <limits>
#include <sstream>
#include <cstdio>

namespace sf::blobl {

namespace {

using sf::cfg::cxx_string_literal;

std::string literal(const value& v) {
    switch (v.type()) {
    case vtype::null:    return "sf::value()";
    case vtype::boolean: return std::string("sf::value(") + (v.as_bool() ? "true" : "false") + ")";
    case vtype::i64:     return "sf::value(int64_t{" + std::to_string(v.as_i64()) + "})";
    case vtype::f32: {
        // 9 significant digits round-trip any float exactly, and std::to_string
        // would not: it formats with %f, so 6.674283e-11 becomes 0.000000.
        std::ostringstream os;
        os.precision(9);
        os << static_cast<float>(v.as_f64());
        std::string lit = os.str();
        if (lit.find_first_of(".eE") == std::string::npos) lit += ".0";
        return "sf::value::float32(" + lit + "f)";
    }
    case vtype::f64: {
        const double d = v.as_f64();
        if (std::isnan(d))  return "sf::value(std::numeric_limits<double>::quiet_NaN())";
        if (std::isinf(d))  return std::string("sf::value(") + (d < 0 ? "-" : "") +
                                   "std::numeric_limits<double>::infinity())";
        std::ostringstream os;
        os.precision(17);
        os << d;
        std::string lit = os.str();
        // Without a decimal point or exponent, `2.0` prints as "2" and
        // sf::value("2") selects the INTEGER constructor -- silently changing a
        // float literal into an int. Force the literal to stay a double.
        if (lit.find_first_of(".eE") == std::string::npos) lit += ".0";
        return "sf::value(" + lit + ")";
    }
    case vtype::string:  return "sf::value(std::string(" + cxx_string_literal(v.as_string()) + "))";
    default:
        // Unreachable: the parser only builds null, boolean, integer, float and
        // string literals, and constant folding produces nothing else. It
        // THROWS rather than emitting a null, because emitting one would turn
        // a compiler bug into a wrong answer at run time instead of a build
        // failure.
        throw eval_error(std::string("cannot emit a literal of type ") + v.type_name());
    }
}


// ---- tier 4: guard-and-specialise -----------------------------------------
//
// Bloblang has no schema, so `this.n` is statically untyped and a type INFERENCE
// pass cannot narrow it. The technique a tracing JIT uses works instead: emit one
// runtime type check, an unboxed fast path under it, and the generic boxed path
// as a fallback. The branch is perfectly predicted and measured free.
//
// Correctness rests on the fast path reproducing sf::num::* exactly -- integer
// wrap-around, and division/modulo by zero raising rather than trapping. The
// differential gate feeds both integer and float inputs so both branches run.
enum class ftype { i64, f64 };

struct fast_expr {
    std::string code;
    ftype       type = ftype::i64;
};

class fast_path {
public:
    // Returns nullopt when the expression is not purely arithmetic over
    // `this.<field>` reads and numeric literals.
    std::optional<fast_expr> build(const node& n) {
        return visit(n);
    }
    // this.<field> reads discovered, in first-seen order.
    const std::vector<std::string>& leaves() const { return _leaves; }

private:
    std::vector<std::string> _leaves;

    std::string leaf_var(const std::string& key) {
        for (size_t i = 0; i < _leaves.size(); ++i)
            if (_leaves[i] == key) return "f" + std::to_string(i);
        _leaves.push_back(key);
        return "f" + std::to_string(_leaves.size() - 1);
    }

    static std::string as(const fast_expr& e, ftype want) {
        if (e.type == want) return e.code;
        return "static_cast<double>(" + e.code + ")";   // only i64 -> f64 widens
    }

    std::optional<fast_expr> visit(const node& n) {
        return std::visit([&](const auto& k) -> std::optional<fast_expr> {
            using T = std::decay_t<decltype(k)>;
            if constexpr (std::is_same_v<T, lit_node>) {
                if (k.v.type() == vtype::i64)
                    return fast_expr{"int64_t(" + std::to_string(k.v.as_i64()) + ")", ftype::i64};
                if (k.v.type() == vtype::f64) {
                    std::ostringstream os; os.precision(17); os << k.v.as_f64();
                    std::string lit = os.str();
                    if (lit.find_first_of(".eE") == std::string::npos) lit += ".0";
                    return fast_expr{lit, ftype::f64};
                }
                return std::nullopt;
            } else if constexpr (std::is_same_v<T, field_node>) {
                // Only a direct `this.<key>` read qualifies; anything deeper
                // would need its own guard.
                if (!std::holds_alternative<this_node>(k.target->kind)) return std::nullopt;
                return fast_expr{leaf_var(k.key), ftype::i64};
            } else if constexpr (std::is_same_v<T, not_node>) {
                // Not an arithmetic node: the unboxed path handles numbers, and
                // `!` yields a bool. Declining sends it to the generic emitter.
                return std::nullopt;
            } else if constexpr (std::is_same_v<T, neg_node>) {
                auto o = visit(*k.operand);
                if (!o) return std::nullopt;
                // Plain `-x` is undefined at INT64_MIN. The generic path goes
                // through num::sub, which wraps via uint64 as Go does; the fast
                // path must use the same helper rather than a bare negation.
                if (o->type == ftype::i64)
                    return fast_expr{"sf::num::fast_sub(0, " + o->code + ")", ftype::i64};
                return fast_expr{"(-(" + o->code + "))", ftype::f64};
            } else if constexpr (std::is_same_v<T, binary_node>) {
                auto l = visit(*k.lhs); if (!l) return std::nullopt;
                auto r = visit(*k.rhs); if (!r) return std::nullopt;
                const bool both_int = l->type == ftype::i64 && r->type == ftype::i64;
                switch (k.op) {
                case binop::add: case binop::sub: case binop::mul: {
                    const char* fn = k.op == binop::add ? "add"
                                   : k.op == binop::sub ? "sub" : "mul";
                    const char* op = k.op == binop::add ? "+" : k.op == binop::sub ? "-" : "*";
                    if (both_int)
                        return fast_expr{std::string("sf::num::fast_") + fn + "(" +
                                         l->code + ", " + r->code + ")", ftype::i64};
                    return fast_expr{"(" + as(*l, ftype::f64) + " " + op + " " +
                                     as(*r, ftype::f64) + ")", ftype::f64};
                }
                case binop::div:   // always float, and zero is an error
                    return fast_expr{"sf::num::fast_div(" + as(*l, ftype::f64) + ", " +
                                     as(*r, ftype::f64) + ")", ftype::f64};
                case binop::mod:
                    if (!both_int) return std::nullopt;
                    return fast_expr{"sf::num::fast_mod(" + l->code + ", " + r->code + ")",
                                     ftype::i64};
                default: return std::nullopt;   // comparisons/logic stay boxed
                }
            } else {
                return std::nullopt;
            }
        }, n.kind);
    }
};

class emitter {
public:
    explicit emitter(const emit_options& o) : opt_(o) {}

    std::string run(const mapping& m) {
        scope_ = &m;
        // Bodies are rendered first so that hoisted constants -- compiled
        // regexes, and later parsed formats and lookup tables -- are known by the
        // time the file header is written. Tier 1 of 02-bloblang.md § 6.2.
        //
        // Each `map` becomes its own function, emitted before the main one so
        // that a forward declaration is unnecessary and mutual recursion between
        // two maps still compiles (the declarations below cover that case).
        std::ostringstream maps;
        for (const auto& nm : m.maps)
            maps << "static sf::value " << map_fn(nm.name) << "(sf::exec_ctx& ctx);\n";
        if (!m.maps.empty()) maps << "\n";
        for (const auto& nm : m.maps) {
            maps << "static sf::value " << map_fn(nm.name) << "(sf::exec_ctx& ctx) {\n";
            std::ostringstream mb;
            const bool saved_uses_this = uses_this_;
            uses_this_ = false;
            for (const auto& st : nm.body->statements) emit_statement(mb, st);
            if (uses_this_)
                maps << "    const sf::value* self_ = ctx.this_v;\n    (void)self_;\n";
            uses_this_ = saved_uses_this;
            maps << "    sf::value root = ctx.root_init ? *ctx.root_init : sf::value::nothing();\n"
                 << mb.str() << "\n    return root;\n}\n\n";
        }

        std::ostringstream body;
        for (const auto& st : m.statements) {
            if (opt_.line_comments && !st.source_text.empty())
                body << "\n    // " << one_line(st.source_text) << "\n";
            emit_statement(body, st);
        }

        std::ostringstream os;
        os << "// Generated by swordfish. Do not edit.\n"
              "#include <swordfish/runtime.hh>\n"
              "#include <swordfish/regex.hh>\n\n"
              "namespace sf::gen {\n\n";
        for (const auto& h : hoisted_)
            os << "static const sf::re " << h.second << "{" << h.first << "};\n";
        if (!hoisted_.empty()) os << "\n";
        os << maps.str();
        os << "sf::value " << opt_.function_name << "(sf::exec_ctx& ctx) {\n";
        // `this` binds only when the mapping actually uses it. ctx.self() throws
        // on an unstructured message, so binding it unconditionally would make a
        // mapping like `root.error = "bad"` fail on input the interpreter
        // handles fine -- the pipeline differential gate caught exactly that.
        if (uses_this_)
            os << "    const sf::value* self_ = ctx.this_v;\n"
                  "    (void)self_;\n";
        // `nothing`, not null: mapPart starts newValue at value.Nothing, so a
        // mapping whose statements all skip leaves the content unchanged.
        os << "    sf::value root = ctx.root_init ? *ctx.root_init : sf::value::nothing();\n"
           << body.str()
           << "\n    return root;\n}\n\n} // namespace sf::gen\n";
        return os.str();
    }

private:
    const emit_options& opt_;
    // pattern literal -> generated identifier, in first-seen order
    std::vector<std::pair<std::string, std::string>> hoisted_;
    bool uses_this_ = false;
    // Innermost element binding for the bare-query form of map_each/filter.
    // Empty means `this` is the message root.
    std::vector<std::string> this_stack_;
    unsigned                 lambda_seq_ = 0;

    // The maps declared by the mapping being emitted, so `.apply("x")` can be
    // resolved to a generated function name.
    const mapping* scope_ = nullptr;

    std::string map_fn(const std::string& name) const {
        std::string out = opt_.function_name + "_map_";
        // Bloblang map names are identifiers already, but a defensive scrub
        // keeps a future looser grammar from emitting invalid C++.
        for (char c : name)
            out += (std::isalnum(static_cast<unsigned char>(c)) || c == '_') ? c : '_';
        return out;
    }

    std::string hoist_pattern(const std::string& cxx_literal) {
        for (const auto& h : hoisted_) if (h.first == cxx_literal) return h.second;
        std::string name = opt_.function_name + "_re_" + std::to_string(hoisted_.size());
        hoisted_.emplace_back(cxx_literal, name);
        return name;
    }

    static std::string one_line(std::string s) {
        for (auto& c : s) if (c == '\n') c = ' ';
        return s;
    }

    void emit_statement(std::ostringstream& os, const statement& st) {
        const std::string generic = expr(*st.expr);

        // Compute the value, taking an unboxed path when the expression is pure
        // arithmetic over `this.<field>` reads.
        // Each statement runs inside a try so a failure carries the same
        // `failed assignment (line N):` wrapper the interpreter produces. Zero
        // cost when nothing throws, and without it the two backends report
        // different text for the same failing mapping -- which a user's unit
        // test can assert on.
        os << "    try {\n";
        fast_path fp;
        auto fast = fp.build(*st.expr);
        // `root = <expr>` is by far the most common statement, and routing it
        // through a default-constructed v_ costs a move-assign on the hot path.
        // Assign straight into root in that case.
        const bool direct = st.target.kind == target_kind::root;
        if (fast && !fp.leaves().empty()) {
            if (!direct) os << "        sf::value v_;\n";
            for (size_t i = 0; i < fp.leaves().size(); ++i)
                os << "        const sf::value& g" << i << " = sf::get_field(sf::self_of(self_), "
                   << cxx_string_literal(fp.leaves()[i]) << ");\n";
            os << "        if (";
            for (size_t i = 0; i < fp.leaves().size(); ++i)
                os << (i ? " && " : "") << "g" << i << ".type() == sf::vtype::i64";
            os << ") [[likely]] {\n";
            for (size_t i = 0; i < fp.leaves().size(); ++i)
                os << "            const int64_t f" << i << " = g" << i << ".as_i64();\n";
            // The fast path is arithmetic, which never yields `nothing`, so
            // assigning straight into root is safe there. The generic branch
            // still needs the guard.
            os << "            " << (direct ? "root" : "v_")
               << " = sf::value(" << fast->code << ");\n"
                  "        } else {\n";
            if (direct) {
                os << "            sf::value g_ = " << generic << ";\n"
                      "            if (!g_.is_nothing()) root = std::move(g_);\n";
            } else {
                os << "            v_ = " << generic << ";\n";
            }
            os << "        }\n";
            if (!direct) emit_assignment(os, st);
        } else {
            if (direct) {
                // `root = <expr>` where the expression yields `nothing` must
                // SKIP the assignment, as the interpreter does.
                os << "        sf::value v_ = " << generic << ";\n"
                      "        if (!v_.is_nothing()) root = std::move(v_);\n";
            } else {
                os << "        sf::value v_ = " << generic << ";\n";
                emit_assignment(os, st);
            }
        }
        os << "    } catch (const sf::eval_error& e_) {\n"
              "        throw sf::eval_error(\"failed assignment (line "
           << st.expr->span.line << "): \" + std::string(e_.what()));\n"
              "    }\n";
    }

    // Assignment of the already-computed `v_`, shared by both paths.
    static void emit_assignment(std::ostringstream& os, const statement& st) {
        switch (st.target.kind) {
        case target_kind::root:
            os << "        root = std::move(v_);\n";
            break;
        case target_kind::root_path: {
            std::string path = "std::vector<std::string>{";
            for (size_t i = 0; i < st.target.path.size(); ++i) {
                if (i) path += ", ";
                path += cxx_string_literal(st.target.path[i]);
            }
            path += "}";
            os << "        if (!v_.is_nothing()) {\n"
                  "            static const std::vector<std::string> p_ = " << path << ";\n"
                  "            root = sf::assign_path(std::move(root), p_, 0, std::move(v_));\n"
                  "        }\n";
            break;
        }
        case target_kind::var:
            os << "        if (v_.is_deleted()) ctx.vars.erase("
               << cxx_string_literal(st.target.name) << ");\n"
                  "        else if (!v_.is_nothing()) ctx.vars["
               << cxx_string_literal(st.target.name) << "] = std::move(v_);\n";
            break;
        case target_kind::meta:
            os << "        if (!v_.is_nothing()) ctx.meta_set("
               << cxx_string_literal(st.target.name) << ", std::move(v_));\n";
            break;
        }
    }

    // A lambda-taking method: emit a C++ lambda over the same runtime helper
    // the interpreter calls, so the two backends cannot diverge on iteration
    // order or on what an empty input means.
    std::string emit_lambda_method(const lambda_method& lm, const method_node& k,
                                   const std::string& t) {
        const size_t qi = lm.leading_args;
        // `sort` and `unique` reach their query forms under different runtime
        // names, because the plain forms already own the obvious ones.
        const std::string fname = k.name == "sort"   ? "sort_with"
                                : k.name == "unique" ? "unique_by" : k.name;
        // Any leading arguments (fold's initial tally) are ordinary expressions
        // and are emitted before the callback.
        std::string lead;
        for (size_t i = 0; i < qi; ++i)
            lead += ", " + (k.args[i] ? expr(*k.args[i]) : std::string("sf::value()"));
        const auto* lam = std::get_if<lambda_node>(&k.args[qi]->kind);
        if (lam) {
            const std::string var = "lv_" + lam->param;
            // sf::scoped_var, not an inline save-and-restore: the variable has
            // to go back to UNBOUND when it was never set, and it has to be
            // restored when the body throws. See its comment in runtime.hh.
            return "sf::m::" + fname + "(" + t + lead + ", [&](const sf::value& " + var +
                   ") { sf::scoped_var sv_(ctx, " + cxx_string_literal(lam->param) +
                   ", " + var + "); return " + expr(*lam->body) + "; })";
        }
        // Bare-query form: `this` inside the argument is the element. Pushing
        // the element's name onto this_stack_ redirects every this_node below
        // it, so no C++ name is ever shadowed.
        const std::string var = "el_" + std::to_string(++lambda_seq_);
        this_stack_.push_back(var);
        const std::string body = expr(*k.args[qi]);
        this_stack_.pop_back();
        return "sf::m::" + fname + "(" + t + lead + ", [&](const sf::value& " +
               var + ") { return " + body + "; })";
    }

    // Every binary result is wrapped in parentheses by construction, because we
    // emit function calls rather than infix operators. That sidesteps the
    // precedence mismatch entirely -- the safest possible answer to it.
    // The method-call emission itself. `t` is the C++ expression for the
    // target, which the caller may have bound to a temp so a failure can
    // report the value without evaluating the target twice.
    std::string emit_method(const method_node& k, const std::string& t) {
                // Table-driven: the registry names the runtime function, so
                // adding a method needs no change here. Only the specials below
                // — lambdas, hoisted regexes, lazy arguments — are hand-written.
                // Lambda-taking methods are tested first: `sort` and `unique`
                // have both a plain and a query form, and the plain one is the
                // table-driven entry below.
                if (const lambda_method* lm = find_lambda_method(k.name);
                    lm && k.args.size() > lm->leading_args && k.args[lm->leading_args])
                    return emit_lambda_method(*lm, k, t);
                if (const method_def* def = find_method(k.name); def && !def->cxx.empty()) {
                    std::string s = "sf::m::" + std::string(def->cxx) + "(" + t;
                    if (def->sig.variadic) {
                        s += ", std::vector<sf::value>{";
                        for (size_t i = 0; i < k.args.size(); ++i) {
                            if (i) s += ", ";
                            s += expr(*k.args[i]);
                        }
                        s += "}";
                    } else {
                        // The runtime function's arity, not the declared
                        // parameter count -- they differ where a documented
                        // optional argument is not implemented yet.
                        for (size_t i = 0; i < def->cxx_arity; ++i)
                            s += ", " + ((i < k.args.size() && k.args[i])
                                         ? expr(*k.args[i]) : std::string("sf::value()"));
                    }
                    return s + ")";
                }

                if (is_regex_method(k.name)) {
                    // A literal pattern is compiled once at namespace scope; a
                    // computed one falls back to the interpreter's cache.
                    std::string rx;
                    if (!k.args.empty()) {
                        if (auto* lit = std::get_if<lit_node>(&k.args[0]->kind);
                            lit && lit->v.type() == vtype::string)
                            rx = hoist_pattern(cxx_string_literal(lit->v.as_string()));
                        else
                            rx = "sf::cached_re((" + expr(*k.args[0]) + ").as_string())";
                    }
                    if (k.name == "re_match")             return "sf::m::re_match(" + t + ", " + rx + ")";
                    if (k.name == "re_find_all")          return "sf::m::re_find_all(" + t + ", " + rx + ")";
                    if (k.name == "re_find_all_submatch") return "sf::m::re_find_all_submatch(" + t + ", " + rx + ")";
                    if (k.name == "re_find_object")       return "sf::m::re_find_object(" + t + ", " + rx + ")";
                    if (k.name == "re_find_all_object")   return "sf::m::re_find_all_object(" + t + ", " + rx + ")";
                    return "sf::m::re_replace_all(" + t + ", " + rx + ", " +
                           (k.args.size() > 1 ? expr(*k.args[1]) : "sf::value(std::string())") + ")";
                }
                if (k.name == "apply") {
                    // A literal map name resolves here; a computed one cannot,
                    // and the reference does not allow it either.
                    const auto* lit = k.args.empty() || !k.args[0]
                        ? nullptr : std::get_if<lit_node>(&k.args[0]->kind);
                    if (!lit || !lit->v.is_stringy())
                        throw eval_error("apply requires a literal map name in compiled mode");
                    if (!scope_ || !scope_->find_map(lit->v.as_string()))
                        throw eval_error("map " +
                                         (lit->v.is_stringy() ? lit->v.as_string()
                                                              : std::string("?")) +
                                         " was not found");
                    return "sf::apply_map(ctx, " + t + ", &" +
                           map_fn(lit->v.as_string()) + ")";
                }
                // `catch`: the fallback is a thunk so it runs only on failure,
                // and it sees the error message as its value.
                if (k.name == "catch") {
                    if (k.args.empty() || !k.args[0]) return t;
                    const auto* lam = std::get_if<lambda_node>(&k.args[0]->kind);
                    if (lam) {
                        const std::string var = "lv_" + lam->param;
                        return "sf::catch_or([&]{ return " + t + "; }, [&](const sf::value& " +
                               var + ") { sf::scoped_var sv_(ctx, " +
                               cxx_string_literal(lam->param) + ", " + var + "); return " +
                               expr(*lam->body) + "; })";
                    }
                    const std::string var = "el_" + std::to_string(++lambda_seq_);
                    this_stack_.push_back(var);
                    const std::string body = expr(*k.args[0]);
                    this_stack_.pop_back();
                    return "sf::catch_or([&]{ return " + t + "; }, [&](const sf::value& " +
                           var + ") { return " + body + "; })";
                }
                if (k.name == "number" || k.name == "bool") {
                    const std::string fn = k.name == "number" ? "sf::m::number"
                                                              : "sf::m::to_bool";
                    if (k.args.empty() || !k.args[0])
                        return fn + "(" + t + ")";
                    return "sf::coerce_or([&]{ return " + fn + "(" + t + "); }, [&]{ return "
                         + fn + "(" + expr(*k.args[0]) + "); })";
                }
                if (k.name == "from") {
                    if (k.args.empty() || !k.args[0])
                        throw eval_error("from requires a message index");
                    return "sf::eval_from(ctx, (" + expr(*k.args[0]) +
                           ").as_i64(), [&](sf::exec_ctx& ctx) { return " +
                           expr(*k.target) + "; })";
                }
                if (k.name == "from_all") {
                    // The lambda parameter is deliberately named `ctx`: it
                    // shadows the enclosing one, so the target expression --
                    // emitted verbatim and full of `ctx.` references -- binds
                    // to the rebound context with no rewriting at all.
                    return "sf::eval_from_all(ctx, [&](sf::exec_ctx& ctx) { return " +
                           expr(*k.target) + "; })";
                }
                if (k.name == "or") {
                    return "sf::coalesce_lazy([&]{ return " + t + "; }, [&]{ return "
                         + (k.args.empty() ? std::string("sf::value()") : expr(*k.args[0])) + "; })";
                }
                throw eval_error("cannot emit unknown method " + k.name);
    }

    std::string expr(const node& n) {
        return std::visit([&](const auto& k) -> std::string {
            using T = std::decay_t<decltype(k)>;

            if constexpr (std::is_same_v<T, lit_node>)  { return literal(k.v); }
            else if constexpr (std::is_same_v<T, this_node>) {
                if (!this_stack_.empty()) return this_stack_.back();
                uses_this_ = true;
                return "sf::self_of(self_)";
            }
            else if constexpr (std::is_same_v<T, var_node>)  {
                return "ctx.var(" + cxx_string_literal(k.name) + ")";
            }
            else if constexpr (std::is_same_v<T, field_node>) {
                return "sf::get_field(" + expr(*k.target) + ", " + cxx_string_literal(k.key) + ")";
            }
            else if constexpr (std::is_same_v<T, index_node>) {
                return "sf::index_of(" + expr(*k.target) + ", " + expr(*k.index) + ")";
            }
            else if constexpr (std::is_same_v<T, not_node>) {
                return "sf::value(!sf::truthy(" + expr(*k.operand) + "))";
            }
            else if constexpr (std::is_same_v<T, neg_node>) {
                return "sf::num::sub(sf::value(int64_t{0}), " + expr(*k.operand) + ")";
            }
            else if constexpr (std::is_same_v<T, binary_node>) {
                const std::string l = expr(*k.lhs), r = expr(*k.rhs);
                // Both operands used to be passed straight as call arguments,
                // and C++ leaves the order of those INDETERMINATELY SEQUENCED --
                // GCC evaluates them right to left. So when both sides fail,
                // the compiled binary reported the RIGHT one's error where the
                // interpreter and the reference both report the left's:
                // `(throw("LEFT") + throw("RIGHT")).catch(e -> e)` gave "RIGHT"
                // compiled and "LEFT" on the other two. Unspecified, not merely
                // different: it is not stable across compilers or -O levels.
                //
                // Naming the left operand first makes the sequencing explicit.
                // The short-circuit forms below already emit real lambdas for
                // the same reason; these cases had simply not been given the
                // same treatment.
                auto lr = [&](std::string_view fn) {
                    return "([&]{ sf::value l_ = " + l + "; sf::value r_ = " + r +
                           "; return " + std::string(fn) + "(l_, r_); }())";
                };
                switch (k.op) {
                case binop::mul: return lr("sf::num::mul");
                case binop::div: return lr("sf::num::div");
                case binop::mod: return lr("sf::num::mod");
                case binop::add: return lr("sf::num::add");
                case binop::sub: return lr("sf::num::sub");
                case binop::eq:  return lr("sf::cmp_eq");
                case binop::neq: return lr("sf::cmp_neq");
                case binop::lt:  return lr("sf::cmp_lt");
                case binop::lte: return lr("sf::cmp_lte");
                case binop::gt:  return lr("sf::cmp_gt");
                case binop::gte: return lr("sf::cmp_gte");
                // Short-circuit forms become real C++ lambdas so the RHS is not
                // evaluated eagerly -- matching the interpreter exactly.
                case binop::and_:
                    return "sf::value(sf::truthy(" + l + ") && sf::truthy(" + r + "))";
                case binop::or_:
                    return "sf::value(sf::truthy(" + l + ") || sf::truthy(" + r + "))";
                case binop::coalesce:
                    return "sf::coalesce_lazy([&]{ return " + l + "; }, [&]{ return " + r + "; })";
                }
                return "sf::value()";
            }
            else if constexpr (std::is_same_v<T, if_node>) {
                return "([&]() -> sf::value { if (sf::truthy(" + expr(*k.cond) + ")) return "
                     + expr(*k.then_) + "; return "
                     + (k.else_ ? expr(*k.else_) : std::string("sf::value::nothing()")) + "; }())";
            }
            else if constexpr (std::is_same_v<T, array_node>) {
                // Built element by element rather than with an initialiser
                // list, because an item evaluating to `nothing` or `deleted`
                // is omitted (sf::literal_omits) and a braced list cannot
                // express that.
                std::string s = "([&]{ std::vector<sf::value> a_; a_.reserve(" +
                                std::to_string(k.items.size()) + "); ";
                for (const auto& it : k.items)
                    s += "{ sf::value v_ = " + expr(*it) +
                         "; if (!sf::literal_omits(v_)) a_.push_back(std::move(v_)); } ";
                return s + "return sf::value::array(std::move(a_)); }())";
            }
            else if constexpr (std::is_same_v<T, match_node>) {
                // Emitted as an immediately-invoked lambda so the arms stay
                // lazy: only the winning one runs, as in the interpreter.
                const std::string var = "mc_" + std::to_string(++lambda_seq_);
                std::string ctx_init = "sf::value " + var + " = ";
                if (k.context) ctx_init += expr(*k.context) + ";";
                else           ctx_init += "sf::value();";
                if (k.context) this_stack_.push_back(var);
                std::string arms;
                for (const auto& [cond, val] : k.cases) {
                    if (cond)
                        arms += "if (sf::truthy_bool(" + expr(*cond) + ")) return " +
                                expr(*val) + "; ";
                    else
                        arms += "return " + expr(*val) + "; ";
                }
                if (k.context) this_stack_.pop_back();
                std::string s = "([&]{ ";
                if (k.context) s += ctx_init + " ";
                s += arms;
                // No arm matched: `nothing`, which skips the assignment.
                return s + "return sf::value::nothing(); }())";
            }
            else if constexpr (std::is_same_v<T, object_node>) {
                std::string s = "([&]{ sf::value o_ = sf::value::object(); ";
                for (const auto& e : k.entries) {
                    // A written-out key is a string literal in the AST and is
                    // emitted as one, so the usual case costs no evaluation.
                    const auto* lit = std::get_if<lit_node>(&e.key->kind);
                    if (lit && lit->v.type() == vtype::string) {
                        s += "{ sf::value v_ = " + expr(*e.value) +
                             "; if (!sf::literal_omits(v_)) o_.set(" +
                             cxx_string_literal(lit->v.as_string()) +
                             ", std::move(v_)); } ";
                        continue;
                    }
                    // A COMPUTED key is evaluated FIRST, and validated, before
                    // the value. The key used to sit inside the o_.set() call:
                    // it ran after the value, and not at all when the value was
                    // omitted. Two divergences from that, both measured --
                    // `{throw("KEYSIDE"): throw("VALSIDE")}` reported VALSIDE
                    // compiled where the interpreter and the reference both
                    // report the key side, and `{5: deleted()}` produced `{}`
                    // and a successful run where the interpreter fails the
                    // mapping and the reference refuses the config outright.
                    // The second is accept-and-approximate: an object with an
                    // illegal key ran to completion and reported success.
                    s += "{ const std::string k_ = sf::object_key(" + expr(*e.key) +
                         "); sf::value v_ = " + expr(*e.value) +
                         "; if (!sf::literal_omits(v_)) o_.set(k_, std::move(v_)); } ";
                }
                return s + "return o_; }())";
            }
            else if constexpr (std::is_same_v<T, call_node>) {
                if (k.name == "metadata" || k.name == "meta") {
                    // Must match the interpreter exactly, including the no-key
                    // form: this pair diverged silently until a fixture used it.
                    if (k.args.empty() || !k.args[0]) return "ctx.meta_all()";
                    return "[&]{ const sf::value k_ = " + expr(*k.args[0]) +
                           "; return (k_.is_stringy() && k_.as_string().empty())"
                           " ? ctx.meta_all() : ctx.meta_get(k_.as_string()); }()";
                }
                if (k.name == "counter") {
                    // min/max/set, matching the interpreter. The call site is
                    // keyed by source position so two counters in one mapping
                    // stay independent.
                    const std::string site = std::to_string(n.span.line) + ":" +
                                             std::to_string(n.span.col);
                    // Same guard as the interpreter: an unsupplied named
                    // argument arrives as null, not as a number.
                    const std::string lo = k.args.size() > 0 && k.args[0]
                        ? "sf::fn::opt_int(" + expr(*k.args[0]) + ", 1)" : "int64_t{1}";
                    const std::string hi = k.args.size() > 1 && k.args[1]
                        ? "sf::fn::opt_int(" + expr(*k.args[1]) + ", INT64_MAX)" : "INT64_MAX";
                    const std::string set = (k.args.size() > 2 && k.args[2])
                        ? expr(*k.args[2]) : "sf::value::nothing()";
                    return "sf::fn::counter(ctx, " + cxx_string_literal(site) + ", " +
                           lo + ", " + hi + ", " + set + ")";
                }
                if (k.name == "count")
                    return "sf::fn::count(ctx, " +
                           (k.args.empty() || !k.args[0] ? std::string("sf::value()")
                                                         : expr(*k.args[0])) + ")";
                if (k.name == "root_meta")
                    return "sf::fn::root_meta(ctx, " +
                           (k.args.empty() || !k.args[0] ? std::string("sf::value()")
                                                         : expr(*k.args[0])) + ")";
                if (k.name == "error_source_name")  return "sf::fn::error_source_name(ctx)";
                if (k.name == "error_source_label") return "sf::fn::error_source_label(ctx)";
                if (k.name == "error_source_path")  return "sf::fn::error_source_path(ctx)";
                if (k.name == "random_int") {
                    const std::string site = std::to_string(n.span.line) + ":" +
                                             std::to_string(n.span.col);
                    const std::string seed = (!k.args.empty() && k.args[0])
                        ? expr(*k.args[0]) : "sf::value()";
                    const std::string lo = (k.args.size() > 1 && k.args[1])
                        ? "sf::fn::opt_int(" + expr(*k.args[1]) + ", 0)" : "int64_t{0}";
                    const std::string hi = (k.args.size() > 2 && k.args[2])
                        ? "sf::fn::opt_int(" + expr(*k.args[2]) + ", INT64_MAX - 1)"
                        : "int64_t{INT64_MAX - 1}";
                    return "sf::fn::random_int(ctx, " + cxx_string_literal(site) + ", " +
                           seed + ", " + lo + ", " + hi + ")";
                }
                if (k.name == "content")     return "sf::fn::content(ctx)";
                if (k.name == "json")
                    return "sf::fn::json_of(ctx, " +
                           (k.args.empty() || !k.args[0] ? std::string("sf::value()")
                                                         : expr(*k.args[0])) + ")";
                if (k.name == "error")       return "sf::fn::error_of(ctx)";
                if (k.name == "errored")     return "sf::fn::errored(ctx)";
                if (k.name == "batch_index") return "sf::fn::batch_index(ctx)";
                if (k.name == "batch_size")  return "sf::fn::batch_size(ctx)";

                // Table-driven, exactly as for methods: the registry names the
                // runtime function and how many arguments it takes.
                if (const function_def* def = find_function(k.name); def && !def->cxx.empty()) {
                    std::string s = "sf::fn::" + std::string(def->cxx) + "(";
                    for (size_t i = 0; i < def->cxx_arity; ++i)
                        s += (i ? ", " : "") + ((i < k.args.size() && k.args[i])
                                                ? expr(*k.args[i]) : std::string("sf::value()"));
                    return s + ")";
                }
                // Unreachable: names.cc rejects an unknown function at parse
                // time. Failing the build beats emitting something that runs.
                throw eval_error("cannot emit unknown function " + k.name);
            }
            else if constexpr (std::is_same_v<T, method_node>) {
                // A type error names where its value came from, exactly as the
                // interpreter does (attach_source in runtime.hh). The target is
                // bound to a temp so it is evaluated ONCE and the handler can
                // report the offending value; without the temp the target
                // expression would run a second time, which for an impure one
                // -- counter(), now() -- would give a different answer than the
                // failure was about.
                // NOT for the lazy methods. They take their target as a
                // closure so a failure can be handled -- `catch`, `or`, the
                // coercions with a default -- or re-evaluate it against another
                // message (`from`, `from_all`). Binding it to a temp here
                // evaluated it eagerly, outside the very handler meant to catch
                // it, so `this.d.uint8().catch(0)` propagated instead of
                // yielding 0. The existing fixtures caught this immediately.
                static const std::set<std::string_view> lazy_target = {
                    "catch", "or", "from", "from_all", "number", "bool"};
                const auto sd = lazy_target.count(k.name) ? source_desc{}
                                                          : describe_source(*k.target);
                if (sd.text.empty()) return emit_method(k, expr(*k.target));
                return "([&]{ const sf::value t_ = " + expr(*k.target) +
                       "; try { return " + emit_method(k, "t_") +
                       "; } catch (const sf::eval_error& e_) { sf::attach_source(e_, " +
                       cxx_string_literal(sd.text) + ", t_, " +
                       (sd.includes_value ? "true" : "false") + "); } }())";
            }
            else {
                // Every node kind the parser builds has a case above. Reaching
                // here means one does not, and the only kind that currently can
                // is a `lambda_node` handed to a method that does not take a
                // lambda -- names.cc validates a method's NAME and ARITY but
                // never its argument types, so `"  hi  ".trim(zz -> zz)` gets
                // this far.
                //
                // It used to emit `sf::value()`, a null literal, and the
                // compiled mapping then SUCCEEDED with a fabricated answer while
                // the interpreter and the reference both failed the mapping:
                // measured, `root.a = "  hi  ".trim(zz -> zz)` gave `{"a":"hi"}`
                // compiled against an untouched message on the other two. That
                // is the accept-and-approximate rule the project forbids, and it
                // broke the stronger invariant that the two backends never
                // disagree about what a config means.
                //
                // So it throws, as literal() above already does for the same
                // reason: a gap in the emitter must be a build failure, not a
                // wrong answer at run time.
                throw eval_error(
                    "cannot compile this expression: " +
                    std::string(std::holds_alternative<lambda_node>(n.kind)
                                    ? "a lambda was passed to a method that does not take one"
                                    : "unsupported expression kind") +
                    ". The interpreter rejects it too; this is not a case where "
                    "the two backends may differ.");
            }
        }, n.kind);
    }
};

} // namespace

std::string emit_cpp(const mapping& m, const emit_options& opt) {
    return emitter(opt).run(m);
}

// A `cpp:` block takes the same shape a translated mapping does. The only
// difference is that the body is the user's text rather than something we
// generated, so it is wrapped in #line directives: a compile error must name
// the line in the user's YAML, not a line in a file they never wrote.
std::string emit_cpp(const cpp_block& b, const emit_options& opt) {
    std::ostringstream os;
    os << "// Generated by swordfish. Do not edit.\n"
          "#include <swordfish/runtime.hh>\n"
          "#include <swordfish/regex.hh>\n";
    for (const auto& inc : b.includes) {
        // Accept both <foo> and "foo"; bare names get angle brackets.
        const bool bracketed = !inc.empty() && (inc.front() == '<' || inc.front() == '"');
        os << "#include " << (bracketed ? inc : "<" + inc + ">") << "\n";
    }
    // Unlike a translated mapping, a cpp: body always binds `self`: we cannot
    // know whether the user's code references it. A cpp: block therefore
    // requires structured input, which is documented behaviour rather than a
    // divergence -- it has no interpreted counterpart to differ from.
    os << "\nnamespace sf::gen {\n\n"
       << "sf::value " << opt.function_name << "(sf::exec_ctx& ctx) {\n"
          "    const sf::value& self = ctx.self();\n"
          "    (void)self;\n"
          "    sf::value root = ctx.root_init ? *ctx.root_init : sf::value::nothing();\n";   // as above

    if (b.source_line > 0 && !b.source_file.empty())
        os << "#line " << b.source_line << " \"" << b.source_file << "\"\n";
    os << b.body;
    if (!b.body.empty() && b.body.back() != '\n') os << "\n";

    // No trailing #line reset: `#line 0` is not valid, and the only code after
    // the body is `return root;` plus closing braces, which cannot fail on its
    // own. Anything that does error there is a consequence of the user's body.
    os << "    return root;\n}\n\n} // namespace sf::gen\n";
    return os.str();
}

} // namespace sf::blobl
