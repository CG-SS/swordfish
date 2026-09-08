// `.bloblang(mapping)`: run a mapping supplied at run time.
//
// The reference disables two groups of functions inside a dynamic mapping, and
// both restrictions are enforced here rather than left to fail at evaluation
// time. Environment access (`file`, `env`) is refused because the mapping text
// is data -- often data that arrived in a message -- and a mapping that can read
// files is a mapping that can exfiltrate them. Message access (`content`,
// `json`, `meta`) is refused because a dynamic mapping is applied to a VALUE and
// has no message of its own; letting those reach the enclosing message would
// make the target argument a lie.
#include "swordfish/blobl/ast.hh"
#include "swordfish/blobl/interp.hh"
#include "swordfish/blobl/parse.hh"
#include "swordfish/methods.hh"
#include "swordfish/value.hh"

#include "methods_util.hh"

#include <map>
#include <memory>
#include <string>
#include <string_view>

namespace sf::m {

namespace {

bool forbidden_function(std::string_view n) {
    return n == "file" || n == "file_rel" || n == "env" ||
           n == "content" || n == "json" || n == "meta" || n == "metadata" ||
           n == "root_meta" || n == "error" || n == "errored" ||
           n == "error_source_name" || n == "error_source_label" ||
           n == "error_source_path" || n == "tracing_id" || n == "tracing_span" ||
           n == "batch_index" || n == "batch_size";
}

void reject_forbidden(const blobl::node& n);

void reject_all(const std::vector<blobl::node_ptr>& v) {
    for (const auto& e : v) if (e) reject_forbidden(*e);
}

void reject_forbidden(const blobl::node& n) {
    using namespace blobl;
    std::visit([&](const auto& k) {
        using T = std::decay_t<decltype(k)>;
        if constexpr (std::is_same_v<T, call_node>) {
            if (forbidden_function(k.name))
                throw eval_error("bloblang: the function " + k.name +
                                 " is not available in a dynamic mapping");
            reject_all(k.args);
        } else if constexpr (std::is_same_v<T, method_node>) {
            reject_forbidden(*k.target);
            reject_all(k.args);
        } else if constexpr (std::is_same_v<T, lambda_node>) {
            reject_forbidden(*k.body);
        } else if constexpr (std::is_same_v<T, field_node>) {
            reject_forbidden(*k.target);
        } else if constexpr (std::is_same_v<T, index_node>) {
            reject_forbidden(*k.target); reject_forbidden(*k.index);
        } else if constexpr (std::is_same_v<T, binary_node>) {
            reject_forbidden(*k.lhs); reject_forbidden(*k.rhs);
        } else if constexpr (std::is_same_v<T, neg_node>) {
            reject_forbidden(*k.operand);
        } else if constexpr (std::is_same_v<T, if_node>) {
            reject_forbidden(*k.cond); reject_forbidden(*k.then_);
            if (k.else_) reject_forbidden(*k.else_);
        } else if constexpr (std::is_same_v<T, array_node>) {
            reject_all(k.items);
        } else if constexpr (std::is_same_v<T, object_node>) {
            for (const auto& e : k.entries) {
                if (e.key)   reject_forbidden(*e.key);
                if (e.value) reject_forbidden(*e.value);
            }
        } else if constexpr (std::is_same_v<T, match_node>) {
            if (k.context) reject_forbidden(*k.context);
            for (const auto& [cond, val] : k.cases) {
                if (cond) reject_forbidden(*cond);
                if (val)  reject_forbidden(*val);
            }
        }
    }, n.kind);
}

// Parsing is far more expensive than evaluating, and a dynamic mapping usually
// arrives on every message with the same text, so the parse is cached per
// thread. A Seastar shard is a thread, hence thread_local rather than static.
const blobl::mapping& compiled(const std::string& text) {
    static thread_local std::map<std::string, std::shared_ptr<blobl::mapping>> cache;
    auto it = cache.find(text);
    if (it != cache.end()) return *it->second;

    blobl::mapping m;
    try {
        m = blobl::parse_mapping(text);
    } catch (const blobl::parse_error& e) {
        throw eval_error(std::string("bloblang: ") + e.what());
    }
    for (const auto& st : m.statements) if (st.expr) reject_forbidden(*st.expr);
    for (const auto& nm : m.maps)
        for (const auto& st : nm.body->statements) if (st.expr) reject_forbidden(*st.expr);

    // Bounded, because the text can come from message data: an attacker-shaped
    // stream of distinct mappings would otherwise grow the cache without limit.
    if (cache.size() >= 256) cache.clear();
    it = cache.emplace(text, std::make_shared<blobl::mapping>(std::move(m))).first;
    return *it->second;
}

} // namespace

value bloblang(const value& v, const value& mapping_text) {
    const blobl::mapping& m = compiled(want_string(mapping_text));
    // A fresh context: no message, no metadata and no inherited variables, so
    // the restrictions above are enforced by construction as well as by the
    // check.
    exec_ctx ctx;
    ctx.this_v = &v;
    return blobl::interp(m).run_ctx(ctx);
}

} // namespace sf::m
