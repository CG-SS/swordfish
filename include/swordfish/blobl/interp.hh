#pragma once
#include "swordfish/blobl/ast.hh"
#include "swordfish/runtime.hh"

#include <string_view>

namespace sf::blobl {

// The methods whose first argument is a regular expression. Both backends
// special-case them -- the interpreter caches the compiled pattern, the emitter
// hoists it -- so the membership test lives in one place.
bool is_regex_method(std::string_view name);

class interp {
public:
    explicit interp(const mapping& m) : m_(m), scope_(&m) {}
    // A map body is interpreted with the OUTER mapping as its map scope, so a
    // map can apply a sibling -- and itself.
    interp(const mapping& m, const interp& outer) : m_(m), scope_(outer.scope_) {}
    value run(const value& input, exec_ctx& ctx) const;
    // Runs with ctx.this_v already set, which may be null when the message is
    // not structured. Only statements that touch `this` then fail.
    value run_ctx(exec_ctx& ctx) const;
private:
    value eval(const node& n, exec_ctx& ctx) const;
    const mapping& m_;
    const mapping* scope_;
};

} // namespace sf::blobl
