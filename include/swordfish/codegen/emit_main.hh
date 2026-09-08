#pragma once
#include "swordfish/runtime/pipeline_spec.hh"
#include <string>
#include <vector>

namespace sf::codegen {

struct emitted {
    struct file { std::string name; std::string content; };
    std::string       main_cc;
    std::vector<file> units;     // one translation unit per transform
};

// Throws std::runtime_error if the pipeline contains something that cannot be
// compiled; the caller reports it as a build error.
emitted emit_program(const pipeline_spec& ps, const std::string& config_path);

} // namespace sf::codegen
