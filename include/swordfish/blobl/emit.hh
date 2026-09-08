#pragma once
#include "swordfish/blobl/ast.hh"
#include <string>

namespace sf::blobl {

struct emit_options {
    std::string function_name = "blobl_0";
    bool        line_comments = true;   // echo the source Bloblang above each statement
};

// Returns a complete, compilable translation unit.
std::string emit_cpp(const mapping& m, const emit_options& opt = {});

// A `cpp:` block: C++ the user wrote in the config, pasted rather than
// translated. It compiles to the SAME signature a translated Bloblang mapping
// does, so the pipeline cannot tell the two apart.
//
//   pipeline:
//     processors:
//       - cpp: |
//           root = self;
//           root.set("sq", sf::num::mul(self.get("n"), self.get("n")));
//
// In scope for the body: `self` (the input value), `root` (the result, returned
// on fallthrough) and `ctx` (the execution context).
struct cpp_block {
    std::string              body;
    std::vector<std::string> includes;
    std::string              source_file;      // for #line, so compiler errors
    uint32_t                 source_line = 0;  // point at the YAML, not the .cc
};

std::string emit_cpp(const cpp_block& b, const emit_options& opt = {});

} // namespace sf::blobl
