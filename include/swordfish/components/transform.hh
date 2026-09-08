// The two ways to write a transformation, and the config that selects between
// them. Both compile to the same `sf::value(sf::exec_ctx&)` function, so the
// pipeline cannot tell them apart.
//
//   - mapping: root.sq = this.n * this.n          # Bloblang, translated
//   - cpp: |                                      # C++, pasted
//       root = self;
//       root.set("sq", sf::num::mul(self.get("n"), self.get("n")));
//
// The long form of `cpp` carries extra includes:
//
//   - cpp:
//       includes: ['<cmath>']
//       body: |
//         root = self;
//         root.set("d", sf::value(std::hypot(...)));
#pragma once

#include "swordfish/config/spec.hh"

namespace sf::proc {

struct cpp_config {
    std::string              body;
    std::vector<std::string> includes;
    bool operator==(const cpp_config&) const = default;
};

enum class transform_kind { bloblang, cpp };

struct transform {
    transform_kind   kind = transform_kind::bloblang;
    std::string      source;              // Bloblang text, or the C++ body
    std::vector<std::string> includes;    // cpp only
    cfg::position    where;               // for #line and diagnostics
};

} // namespace sf::proc

namespace sf::cfg {

template <> struct spec_of<sf::proc::cpp_config> {
    static constexpr std::string_view cpp_type = "sf::proc::cpp_config";
    static constexpr auto value = object(
        field("body", &sf::proc::cpp_config::body)
            .describe("C++ statements. In scope: `self` (input value), `root` (result), "
                      "`ctx` (execution context)."),
        field("includes", &sf::proc::cpp_config::includes)
            .describe("Additional headers to include, e.g. ['<cmath>'].")
    );
};

} // namespace sf::cfg
