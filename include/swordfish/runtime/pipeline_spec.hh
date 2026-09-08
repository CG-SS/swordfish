// The parsed pipeline, independent of how it will be executed.
//
// ONE parse of the YAML, TWO consumers: build_stream_spec() constructs
// components for `swordfish run`, and emit_main() writes C++ for
// `swordfish build`. Keeping the tree in the middle is what stops the two modes
// from drifting.
#pragma once

#include "swordfish/config/spec_error.hh"
#include "swordfish/config/yaml.hh"
#include "swordfish/components/registry.hh"
#include "swordfish/runtime/batching.hh"
#include "swordfish/runtime/observe.hh"   // http_spec
#include "swordfish/runtime/scanner.hh"   // scanner_spec
#include "swordfish/runtime/stream.hh"    // stream_config

#include <chrono>
#include <optional>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

namespace sf {

struct input_spec {
    std::string               kind;        // generate | file | stdin, or a
                                           // registered connector
    // Set when `kind` came from the input registry rather than from the
    // built-in three. The fields below are the built-ins' own, kept because
    // generate/file/stdin predate the registry.
    std::optional<component_config> comp;
    std::string               mapping;     // generate
    uint64_t                  count = 0;   // generate; 0 = unbounded
    std::chrono::milliseconds interval{0}; // generate
    // The reference's `file` input takes `paths`, a LIST, and expands globs in
    // each entry. Taking only a scalar `path` meant real Benthos configs failed
    // to parse here -- found while verifying `sequence` against the reference,
    // which rejected the config outright.
    std::vector<std::string>  paths;       // file
    // `broker` and `sequence`: the inputs they wrap. Nested rather than flat
    // because either may contain the other, and each child keeps its own kind's
    // rules -- a `file` inside a broker is still a shard-0 singleton.
    std::vector<input_spec>   children;
    uint64_t                  copies = 1;  // broker
    // The reference's `scanner` field is a COMPONENT -- `scanner: { csv: {} }`
    // -- so it carries a kind plus that kind's own options, and nests.
    scanner_spec              scanner;
    // `input.broker`'s `batching`: several source batches combined into one.
    // Its `processors` are lifted out for the usual reason -- a nested processor
    // list has no field type -- exactly as the output side's are.
    batch_policy_config           batching;
    std::vector<component_config> batching_processors;
    // `auto_replay_nacks`, on the three built-ins the reference also gives it
    // to. True means a nacked batch is offered again instead of dropped, which
    // is the reference's default and the difference between a failed write
    // costing latency and costing data.
    bool                      auto_replay_nacks = true;
    // `processors:` sits BESIDE the kind, as it does on an output, and runs on
    // what this input produced before the batch reaches the pipeline. The
    // reference gives every input one; swordfish used to refuse the whole
    // document with "`input` must be a single-key object", which made any
    // ordinary Benthos config that used either this or `label:` unloadable.
    std::vector<component_config> processors;
};

struct output_spec {
    std::string kind = "stdout";           // stdout | file | drop | reject |
                                           // broker | switch | fallback, or a
                                           // registered connector
    std::string path;                      // file
    std::optional<component_config> comp;  // as above
    // `broker`, `switch` and `fallback`: the outputs they wrap. Nested for the
    // same reason input_spec::children is -- any of them may contain any other,
    // and each child keeps its own kind's rules.
    std::vector<output_spec>  children;
    uint64_t                  copies  = 1;         // broker
    std::string               pattern = "fan_out"; // broker
    bool                      strict_mode = false;         // switch
    bool                      retry_until_success = false; // switch
    std::string               message;             // reject: the interpolated text

    // Set on a CHILD of a `switch`: the check that routes to it, and whether a
    // message that matched keeps being tested against later cases. They live on
    // the child rather than in a parallel array beside `children` so a case and
    // its output cannot get out of step.
    cfg::bloblang             check;               // empty source = always passes
    bool                      continue_ = false;

    // `processors:` sits alongside the kind on every output, and applies only to
    // the messages routed to THAT output. It is what makes a `broker` or
    // `switch` child able to reshape its own copy, which the reference's own
    // documentation examples rely on.
    std::vector<component_config> processors;

    // `batching:` sits beside `processors:` on an output and works the same
    // way: it applies to the messages routed to THAT output. Its own
    // `processors` are lifted out for the usual reason -- a nested processor
    // list has no field type.
    batch_policy_config           batching;
    std::vector<component_config> batching_processors;
};

// The `buffer` block. `none` is the default and means no buffer at all; `memory`
// parks messages and acknowledges the source on arrival, which is what makes it
// a deliberate weakening of the delivery guarantee rather than a free win.
struct buffer_spec {
    std::string          kind = "none";
    // The reference's default, 500 MiB, in bytes.
    int64_t              limit = 524288000;
    // `batch_policy.enabled`. Separate from the triggers because the reference
    // separates them, and because a policy with no trigger can never flush.
    bool                 batch_enabled = false;
    batch_policy_config  batching;
    std::vector<component_config> batching_processors;
};

struct pipeline_spec {
    input_spec             input;
    std::vector<component_config> processors;
    output_spec            output;
    http_spec              http;      // the `http` block; disabled unless present
    buffer_spec            buffer;
    // The engine-wide settings: `shutdown_timeout`, `shutdown_delay` and
    // `error_handling.strict`. Carried here so BOTH backends get them -- these
    // were parsed by nobody and left at their defaults whatever the config
    // said, which meant `error_handling.strict: true` ran non-strict in silence.
    stream_config          config;
};

// spec_error moved to config/spec_error.hh, which this header includes, so
// every existing user is unaffected. It had to leave: this header reaches
// Seastar through transaction.hh, and the config layer -- which is where a
// config error belongs -- is built without Seastar on its include path.

// Throws spec_error naming any unimplemented component, so `lint`, `run` and
// `build` all reject the same configs for the same reasons.
// A document with trivial `input` and `output` stages substituted in, so a
// config that does not carry a whole pipeline can still go through
// parse_pipeline and collect every one of its checks -- the root-field list,
// the `http` block's TLS refusals, the resource blocks.
//
// `keep` names the one pipeline section to take from the real config and is
// what `swordfish lint` uses to probe a document a section at a time; with no
// `keep` both stages are replaced, which is how `swordfish streams` validates a
// ROOT config that has no input or output by design. One function rather than
// two nearly-identical ones, because the two would have drifted the first time
// a root field was added.
cfg::ynode with_trivial_io(const cfg::ynode& root, std::string_view keep = {});

pipeline_spec parse_pipeline(const cfg::ynode& root);

// Exposed for `swordfish test`, whose `target_processors` pointer can select a
// single processor or a list from anywhere in the document -- not just
// /pipeline/processors -- so it needs the parser without the surrounding
// pipeline.
component_config              parse_processor(const cfg::ynode& node);
std::vector<component_config> parse_processors(const cfg::ynode& seq);

// Replaces `resource: <label>` with the definition from `processor_resources`.
// Exported because `swordfish test` builds a processor chain without going
// through parse_pipeline, and a `resource:` reaching the registry unresolved is
// an internal error rather than a useful message.
void resolve_resources(std::vector<component_config>& procs, const cfg::ynode& root);

} // namespace sf
