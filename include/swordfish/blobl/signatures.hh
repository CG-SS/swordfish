// Function and method signatures.
//
// Bloblang accepts arguments positionally or by name — `range(start: 0, stop: 5)`
// is the same call as `range(0, 5)` — so resolving names to positions needs a
// parameter list per callable. Having one also gives arity checking, which
// Benthos performs when building a mapping rather than when running it.
#pragma once

#include <cstddef>
#include <string_view>
#include <vector>

namespace sf::blobl {

struct param {
    std::string_view name;
    bool             required = false;
};

struct signature {
    std::string_view   name;
    std::vector<param> params;
    // A variadic callable takes any number of trailing positional arguments and
    // cannot use named form, e.g. "%s-%s".format(a, b).
    bool               variadic = false;

    // Index of a parameter by name, or npos.
    size_t index_of(std::string_view n) const {
        for (size_t i = 0; i < params.size(); ++i)
            if (params[i].name == n) return i;
        return static_cast<size_t>(-1);
    }
    size_t required_count() const {
        size_t n = 0;
        for (const auto& p : params) if (p.required) ++n;
        return n;
    }
};

const signature* find_method_signature(std::string_view name);
const signature* find_function_signature(std::string_view name);

} // namespace sf::blobl
