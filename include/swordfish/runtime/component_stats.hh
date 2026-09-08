// Per-component metric counters, and the identity a scrape names them by.
//
// Its own header because both ends of the pipeline need it and they already
// include each other: stream.hh takes `http_spec` from observe.hh, and observe.hh
// needs these to report one series per component. Neither type depends on the
// other, so lifting them here breaks the cycle rather than papering over it.
//
// The shape is the reference's: every /metrics series carries `path` (the
// component's place in the config, `root.pipeline.processors.1`) and `label`
// (its `label:`, empty when unset). A dashboard groups by those, so they are the
// part that has to match.
#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace sf {

// Which stage of the pipeline a component sits in. The reference names its
// metric families by this -- input_received, processor_error, output_sent -- so
// it decides both the metric name and the path shape.
enum class comp_kind { input, processor, output };

// What a scrape needs to name one component. Filled from the PARSED pipeline
// rather than from the built component, because a processor built by a factory
// does not know its own place in the config -- and that place is exactly what
// the reference keys every series on.
struct comp_ident {
    comp_kind   kind = comp_kind::input;
    std::string path;      // "root.input", "root.pipeline.processors.0", ...
    std::string label;     // the config's `label:`, empty when unset
};

// One component's counters. A plain aggregate with no strings, because these
// are incremented on the message path: the identity is looked up once per
// scrape, not once per message.
struct comp_counters {
    uint64_t received       = 0;
    uint64_t sent           = 0;
    uint64_t batch_received = 0;
    uint64_t batch_sent     = 0;
    uint64_t errors         = 0;
    uint64_t connection_up  = 0;   // inputs and outputs only

    comp_counters& operator+=(const comp_counters& o) noexcept {
        received       += o.received;        sent          += o.sent;
        batch_received += o.batch_received;  batch_sent    += o.batch_sent;
        errors         += o.errors;          connection_up += o.connection_up;
        return *this;
    }
};

// The two together, which is what a scrape consumes.
struct component_stats {
    comp_ident    id;
    comp_counters c;
};

} // namespace sf
