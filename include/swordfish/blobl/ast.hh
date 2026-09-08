// Bloblang AST. One tree, two backends: interpreter (src/blobl/interp.cc) and
// C++ emitter (src/blobl/emit.cc).
#pragma once

#include "swordfish/value.hh"

#include <memory>
#include <variant>
#include <optional>
#include <string>
#include <vector>

namespace sf::blobl {

struct source_span { uint32_t line = 0, col = 0; };

// Binary operators, grouped by the precedence pass that folds them.
// Order matters: see parser.cc, which mirrors NewArithmeticExpression's four
// left-associative passes in benthos-main/internal/bloblang/query/arithmetic.go.
enum class binop {
    // pass 1 (tightest)
    mul, div, mod, coalesce,
    // pass 2
    add, sub,
    // pass 3 — all six share one level, unlike C++
    eq, neq, lt, lte, gt, gte,
    // pass 4 — && and || share one level, unlike C++
    and_, or_,
};

int  precedence_pass(binop op);   // 1..4

struct node;
using node_ptr = std::unique_ptr<node>;

struct lit_node    { value v; };
struct this_node   {};
struct var_node    { std::string name; };
struct field_node  { node_ptr target; std::string key; };
struct index_node  { node_ptr target; node_ptr index; };
// Arguments may be given positionally or by name -- `range(start: 0, stop: 5)`
// is the same call as `range(0, 5)`. Names are resolved to positions during
// validation, so by the time a backend sees the node, `args` is positional.
struct call_node   { std::string name; std::vector<node_ptr> args;
                     std::vector<std::string> arg_names; };
struct method_node { node_ptr target; std::string name; std::vector<node_ptr> args;
                     std::vector<std::string> arg_names; };
// `x -> expr`, passed as a method argument. The parameter is referenced by a
// BARE identifier inside the body, so the parser tracks which names are in
// scope to distinguish it from an unknown identifier.
struct lambda_node { std::string param; node_ptr body; };
struct binary_node { binop op = binop::add; node_ptr lhs, rhs; };
struct neg_node    { node_ptr operand; };
// Logical NOT. Separate from neg_node because `-` is arithmetic and `!` is a
// truthiness test -- `!0` is false, not -0.
struct not_node    { node_ptr operand; };
struct if_node     { node_ptr cond, then_, else_; };
struct array_node  { std::vector<node_ptr> items; };
// `match <ctx> { cond => value ... }`. The context becomes `this` for BOTH the
// conditions and the values; a null context leaves `this` as it was. A `_` arm
// has a null condition. No arm matching yields `nothing`, not an error --
// NewMatchFunction returns value.Nothing.
struct match_node  { node_ptr context;
                     std::vector<std::pair<node_ptr, node_ptr>> cases; };
// An object literal's key is an EXPRESSION, not a fixed string: Bloblang allows
// `{loc.state: [loc.name]}`, which is what makes `map_each(...).squash()` able
// to group by a field. A written-out key parses to a string literal, so the
// common case costs nothing at run time.
struct object_entry { node_ptr key, value; };
struct object_node  { std::vector<object_entry> entries; };

struct node {
    std::variant<lit_node, this_node, var_node, field_node, index_node,
                 call_node, method_node, lambda_node, binary_node, neg_node, not_node,
                 if_node, array_node, object_node, match_node> kind;
    source_span span;
};

template <class T, class... A>
node_ptr mk(source_span sp, A&&... a) {
    auto n = std::make_unique<node>();
    n->kind = T{std::forward<A>(a)...};
    n->span = sp;
    return n;
}

// ---- statements ------------------------------------------------------------

enum class target_kind { root, root_path, var, meta };

struct assign_target {
    target_kind kind = target_kind::root;
    std::vector<std::string> path;   // for root_path
    std::string name;                // for var / meta
};

// How an expression is named in a type error: `` field `this.a.b` ``,
// `method uppercase`, `function content`, `string literal ("x")`. Empty for an
// expression with no useful name -- an arithmetic result, say -- in which case
// the error is left undecorated rather than given a meaningless origin.
//
// Shared by both backends: the interpreter calls it at evaluation time and the
// emitter bakes the result into the generated code, so the two cannot word the
// same error differently.
struct source_desc {
    std::string text;
    // True when `text` already names the value, as `string literal ("x")` does;
    // the caller then must not append it a second time.
    bool        includes_value = false;
};
source_desc describe_source(const node& n);

struct statement {
    assign_target target;
    node_ptr      expr;
    std::string   source_text;       // echoed into generated code as a comment
};

// A named mapping declared with `map <name> { ... }` and invoked with
// `.apply("<name>")`. Stored beside the statements rather than in a global
// registry so a mapping stays a self-contained value: the emitter walks one of
// these and needs to see every map it can reach.
struct mapping;
struct named_map {
    std::string name;
    std::shared_ptr<mapping> body;      // shared_ptr, because mapping is incomplete here
};

struct mapping {
    std::vector<statement> statements;
    std::vector<named_map> maps;

    const mapping* find_map(std::string_view n) const {
        for (const auto& m : maps) if (m.name == n) return m.body.get();
        return nullptr;
    }
};

} // namespace sf::blobl
