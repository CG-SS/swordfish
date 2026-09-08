// A transform is "the thing a mapping does": value in, value out.
//
// Both execution modes produce one. Interpreted mode wraps an AST walker;
// compiled mode passes a generated function directly. Components take a
// transform_fn and therefore have ONE implementation shared by both modes,
// which is the § 4.3 contract in its most concrete form.
#pragma once

#include "swordfish/runtime.hh"
#include "swordfish/blobl/ast.hh"

#include <functional>

namespace sf {

using transform_fn = std::function<value(exec_ctx&)>;

// Wraps a parsed mapping and an interpreter, keeping both alive for the life of
// the returned callable.
transform_fn interpreted_transform(blobl::mapping m);

// Compiled mode passes `&sf::gen::blobl_N` straight in; a function pointer
// stores inside std::function without allocating.

} // namespace sf
