#pragma once
#include "swordfish/runtime/stream.hh"
#include "swordfish/runtime/pipeline_spec.hh"
#include "swordfish/config/yaml.hh"

namespace sf {
stream_spec build_stream_spec(const cfg::ynode& root);
stream_spec build_stream_spec(const pipeline_spec& ps);

// Exposed for `swordfish test`, which builds a processor chain WITHOUT a stream
// around it: a unit test feeds batches straight through the processors and
// inspects what comes out, with no input, output or transaction machinery.
// `path_prefix` is where this list sits in the document, e.g.
// "pipeline.processors"; each element appends its index. Empty means the caller
// has no position to offer, and error_source_path() reports "" as before.
std::vector<processor_ptr> build_processors(const std::vector<component_config>& specs,
                                            const std::string& path_prefix = {});
} // namespace sf
