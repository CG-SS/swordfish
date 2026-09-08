#pragma once
#include "swordfish/blobl/ast.hh"
#include <stdexcept>

namespace sf::blobl {

class parse_error : public std::runtime_error {
public:
    parse_error(source_span w, std::string msg)
        : std::runtime_error(std::move(msg)), where(w) {}
    source_span where;
    std::string render(std::string_view src) const;   // caret-annotated excerpt
};

mapping parse_mapping(std::string_view src);

// Parses a bare EXPRESSION rather than a list of assignments, and wraps it as
// `root = <expr>`. Benthos uses query fields in several places -- `switch` case
// checks, `while` conditions, batching `check` -- where a full mapping would be
// wrong.
mapping parse_query(std::string_view src);

} // namespace sf::blobl
