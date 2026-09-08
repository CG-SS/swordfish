#pragma once
#include "swordfish/blobl/ast.hh"
#include <string>
#include <string_view>
#include <vector>

namespace sf::blobl {

struct name_error {
    source_span where;
    std::string message;
};

// Returns every unrecognised function or method in the mapping. Empty means all
// names resolve.
// Mutates the mapping: named arguments are rewritten into positional form so
// that no backend has to know about them.
std::vector<name_error> validate_names(mapping& m);

} // namespace sf::blobl
