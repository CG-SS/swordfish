// The method and function registry.
//
// A method used to be declared in five places: its signature, its
// implementation, the interpreter's dispatch chain, the emitter's dispatch
// chain, and the header. With 211 of 227 still to write, that is the same
// multiplier the component registry removed — so methods get the same
// treatment. A method is now declared ONCE:
//
//     {"uppercase", {}, SF_M(uppercase)},
//
// and the signature, arity checking, named-argument resolution, interpretation
// and emission all follow from it.
//
// Methods that cannot be table-driven — those taking a lambda, those hoisting a
// compiled regex, those with lazy argument evaluation — stay hand-written in the
// two backends and are marked `special` here so validation still knows them.
#pragma once

#include "swordfish/blobl/signatures.hh"
#include "swordfish/runtime.hh"
#include "swordfish/value.hh"

#include <cstdint>
#include <string>
#include <vector>

namespace sf::blobl {

// Uniform call shape: the target plus already-evaluated arguments. Missing
// optional arguments arrive as null, matching named-argument resolution.
using method_impl = value (*)(const value& target, const std::vector<value>& args);
using fn_impl     = value (*)(const std::vector<value>& args);

struct method_def {
    signature   sig;
    method_impl invoke = nullptr;    // null for `special`
    // Name of the runtime function the emitter calls. Empty means the emitter
    // handles this method itself.
    std::string_view cxx;
    // How many arguments that runtime function takes after the target. The
    // emitter must pass exactly this many, which is NOT the same as the number
    // of declared parameters: `number` documents an optional `default` but is
    // implemented as a one-argument function, and emitting two arguments for it
    // produced generated code that did not compile. Filled in by SF_M so the
    // two can no longer drift.
    uint8_t     cxx_arity = 0;
    // Hand-written in both backends: lambdas, hoisted regexes, lazy arguments.
    bool special = false;
};

struct function_def {
    signature   sig;
    fn_impl     invoke = nullptr;
    std::string_view cxx;
    uint8_t     cxx_arity = 0;       // as in method_def
    bool        special = false;
};

// A method whose argument is a query applied per element rather than a value.
// Both backends need to know which those are and where the query sits in the
// argument list, so the list lives here rather than being written twice.
struct lambda_method {
    std::string_view name;
    // Arguments evaluated normally BEFORE the query. Only fold has one: its
    // initial tally.
    uint8_t          leading_args = 0;
    // `sort()` and `unique()` work with or without a query. With one they take
    // this path; without one they fall through to the ordinary table-driven
    // implementation, so both backends test for the argument rather than for
    // the name.
    bool             query_optional = false;
};
const lambda_method* find_lambda_method(std::string_view name);

// Interpreter-side dispatch for those methods. The emitter needs no equivalent:
// it writes the call to sf::m::<name> directly, and the names match.
value call_lambda_method(std::string_view name, const value& target,
                         const value& initial, const m::lambda& fn);

const method_def*   find_method(std::string_view name);
const function_def* find_function(std::string_view name);

// Every registered name, for diagnostics and for `swordfish blobl --list`.

} // namespace sf::blobl

namespace sf::blobl {

// One adapter from a plain sf::m:: function to the uniform call shape, with
// the arity deduced from the function's own type. Deducing rather than naming
// it is the point: an `m2` adapter written over a two-argument function was a
// silent mismatch, and the emitter, which trusted the declared parameter list
// instead, generated a call that did not compile.
// A missing optional argument arrives as null, which is what named-argument
// resolution produces.
constexpr uint8_t variadic_arity = 255;

constexpr uint8_t arity_of(value (*)(const value&)) { return 0; }
constexpr uint8_t arity_of(value (*)(const value&, const value&)) { return 1; }
constexpr uint8_t arity_of(value (*)(const value&, const value&, const value&)) { return 2; }
constexpr uint8_t arity_of(value (*)(const value&, const value&, const value&,
                                     const value&)) { return 3; }
constexpr uint8_t arity_of(value (*)(const value&, const std::vector<value>&)) {
    return variadic_arity;
}

template <auto F>
value adapt(const value& t, const std::vector<value>& a) {
    constexpr uint8_t n = arity_of(F);
    [[maybe_unused]] auto at = [&](size_t i) { return i < a.size() ? a[i] : value(); };
    if constexpr (n == variadic_arity) return F(t, a);
    else if constexpr (n == 0) return F(t);
    else if constexpr (n == 1) return F(t, at(0));
    else if constexpr (n == 2) return F(t, at(0), at(1));
    else                       return F(t, at(0), at(1), at(2));
}

// Fills the invoke pointer, the emitted C++ name and the emitted arity from one
// mention of the runtime function, so a registry entry cannot disagree with the
// function it names.
#define SF_M(fname) &::sf::blobl::adapt<&::sf::m::fname>, #fname, \
                    ::sf::blobl::arity_of(&::sf::m::fname)

// The same for functions, which have no target argument.
constexpr uint8_t farity_of(value (*)()) { return 0; }
constexpr uint8_t farity_of(value (*)(const value&)) { return 1; }
constexpr uint8_t farity_of(value (*)(const value&, const value&)) { return 2; }
constexpr uint8_t farity_of(value (*)(const value&, const value&, const value&)) { return 3; }

template <auto F>
value fadapt(const std::vector<value>& a) {
    constexpr uint8_t n = farity_of(F);
    [[maybe_unused]] auto at = [&](size_t i) { return i < a.size() ? a[i] : value(); };
    if constexpr (n == 0) return F();
    else if constexpr (n == 1) return F(at(0));
    else if constexpr (n == 2) return F(at(0), at(1));
    else                       return F(at(0), at(1), at(2));
}

#define SF_F(fname) &::sf::blobl::fadapt<&::sf::fn::fname>, #fname, \
                    ::sf::blobl::farity_of(&::sf::fn::fname)

} // namespace sf::blobl
