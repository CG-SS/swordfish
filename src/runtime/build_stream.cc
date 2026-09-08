// Config -> stream_spec. Builds per-shard factories from the parsed YAML.
//
// Everything here captures SOURCE TEXT rather than constructed objects: a
// mapping owns unique_ptr nodes and cannot be copied, and under share-nothing
// each shard should own its own AST and its own component state anyway.
#include "swordfish/runtime/build_stream.hh"
#include <algorithm>
#include "swordfish/runtime/pipeline_spec.hh"
#include "swordfish/runtime/components.hh"
#include "swordfish/blobl/parse.hh"
#include "swordfish/runtime/transform.hh"
#include "swordfish/config/spec.hh"

#include <seastar/core/smp.hh>

namespace sf {

using namespace sf::cfg;

namespace {

// Unreachable in practice: parse_pipeline rejects unknown kinds against the same
// registry, so a component_config that reaches here has already been validated.
// Kept as a defensive check, worded so that hitting it points at the real cause
// rather than looking like an ordinary unsupported-component error.
[[noreturn]] void unregistered(const std::string& kind) {
    throw std::runtime_error(
        "internal error: no registry entry for processor '" + kind +
        "' at build time, though parsing accepted it");
}

// Construction walks the registry: a component's entry knows how to build
// itself from its own config. `path` is the dotted position of this component
// in the config, which error provenance reports verbatim.
processor_ptr build_processor(const component_config& cc, const std::string& path) {
    const processor_def* def = find_processor(cc.kind);
    if (!def) unregistered(cc.kind);

    build_env env;
    env.transform_for = [](const cfg::bloblang& b) {
        return interpreted_transform(b.is_query ? blobl::parse_query(b.source)
                                                : blobl::parse_mapping(b.source));
    };
    // A nested list continues the path, and the segment it adds is the PARENT'S
    // KIND rather than the literal "processors": the reference names a mapping
    // inside a try at pipeline.processors.1 "pipeline.processors.1.try.0".
    const std::string kind = cc.kind;
    env.list_for = [path, kind](const std::vector<component_config>& l) {
        return build_processors(l, path.empty() ? std::string() : path + "." + kind);
    };
    env.label = cc.label;
    env.path  = path;
    return def->build(cc, env);
}

} // namespace

std::vector<processor_ptr> build_processors(const std::vector<component_config>& specs,
                                            const std::string& path_prefix) {
    std::vector<processor_ptr> out;
    out.reserve(specs.size());
    for (size_t i = 0; i < specs.size(); ++i)
        out.push_back(build_processor(
            specs[i],
            path_prefix.empty() ? std::string() : path_prefix + "." + std::to_string(i)));
    return out;
}

stream_spec build_stream_spec(const ynode& root) {
    return build_stream_spec(parse_pipeline(root));
}


// One input's per-shard factory. Recursive: `broker` and `sequence` build their
// children through this same function, so a nested `file` is still a shard-0
// singleton and a nested `generate` still divides its count -- rules that would
// otherwise have to be restated at every nesting level.
std::function<input_ptr()> build_input_kind(const input_spec& in);

// `processors:` beside the kind wraps whatever that kind turned out to be, and
// it wraps it OUTSIDE `auto_replay_nacks` -- the reference attaches its input
// pipelines to the already-retrying reader, so a nack replays the SOURCE read
// and the processors run again on what comes back. Applied here rather than in
// build_input_kind so a broker's children each get their own.
std::function<input_ptr()> build_input_factory(const input_spec& in) {
    auto f = build_input_kind(in);
    if (in.processors.empty()) return f;
    const auto procs = in.processors;
    return [f, procs]() -> input_ptr {
        return make_processed_input(f(), build_processors(procs));
    };
}

std::function<input_ptr()> build_input_kind(const input_spec& in) {
    // `auto_replay_nacks` wraps whatever this input turns out to be. Applied
    // here rather than inside each factory so there is one place that decides
    // what a nack means.
    auto replay = [&in](std::function<input_ptr()> f) {
        if (!in.auto_replay_nacks) return f;
        return std::function<input_ptr()>([f] { return make_auto_retry_input(f()); });
    };
    if (in.comp) {
        // A registered connector builds its own factory; it decides for itself
        // whether it is per-shard or a singleton.
        const input_def* def = find_input(in.kind);
        if (!def) throw std::runtime_error("input disappeared from the registry: " + in.kind);
        // WRAPPED, like every other branch. This returned the connector's own
        // factory untouched, so `auto_replay_nacks` reached the three built-ins
        // and nothing else: a nacked Kafka batch was never replayed, its
        // partition's commit stayed pinned at that offset, and the in-flight set
        // grew without bound. Measured against a live broker -- one record, an
        // output that always rejects -- zero replays in eight seconds, where
        // `generate` under the same output managed sixty-five in four.
        //
        // `http_client`, `websocket` and the socket inputs used to wrap
        // themselves inside their own factories, which is why they worked; that
        // is removed, so this comment's claim of "one place that decides what a
        // nack means" is now true.
        return replay(def->build(*in.comp));
    }
    if (in.kind == "broker" || in.kind == "sequence") {
        std::vector<std::function<input_ptr()>> kids;
        // `copies` repeats each child, which is how a broker scales one source.
        const uint64_t copies = in.kind == "broker" ? std::max<uint64_t>(in.copies, 1) : 1;
        for (uint64_t c = 0; c < copies; ++c)
            for (const auto& child : in.children) kids.push_back(build_input_factory(child));
        const bool is_broker = in.kind == "broker";
        // The broker's own `batching` wraps the assembled broker, not each
        // child: the policy combines across every source it fans in from, which
        // is the whole point of putting it on the broker.
        const bool batching = is_broker && !(in.batching.is_noop() &&
                                             in.batching_processors.empty());
        const auto policy = in.batching;
        const auto bprocs = in.batching_processors;
        return [kids, is_broker, batching, policy, bprocs]() -> input_ptr {
            std::vector<input_ptr> built;
            built.reserve(kids.size());
            for (const auto& k : kids) built.push_back(k());
            input_ptr self = is_broker ? make_broker_input(std::move(built))
                                       : make_sequence_input(std::move(built));
            if (!batching) return self;
            return make_batched_input(
                std::move(self),
                batch_policy(policy.count, policy.byte_size,
                             std::chrono::duration_cast<std::chrono::milliseconds>(
                                 policy.period.ns),
                             policy.check.source.empty()
                                 ? transform_fn{}
                                 : interpreted_transform(
                                       blobl::parse_query(policy.check.source))),
                build_processors(bprocs));
        };
    }
    if (in.kind == "generate") {
        const unsigned shards = seastar::this_smp().shard_count();
        auto mapping = in.mapping; auto count = in.count; auto interval = in.interval;
        return replay([mapping, count, interval, shards]() -> input_ptr {
            uint64_t mine = UINT64_MAX;
            if (count > 0) {
                const unsigned me = seastar::this_shard_id();
                mine = count / shards + (me < count % shards ? 1 : 0);
            }
            return make_generate_input(
                interpreted_transform(blobl::parse_mapping(mapping)), mine, interval);
        });
    }
    auto kind = in.kind; auto paths = in.paths; auto codec = in.scanner;
    // A file or stdin source is a SINGLETON: shard 0 only, or every shard would
    // read the same bytes.
    //
    // The glob expansion and the scanner lookup both live in the runtime rather
    // than here, so `emit_main.cc` can call exactly the same two functions. When
    // they were written out at each call site the two backends drifted: the
    // emitter used the singular `path` and never expanded anything.
    return replay([kind, paths, codec]() -> input_ptr {
        if (seastar::this_shard_id() != 0)
            return make_generate_input(
                interpreted_transform(blobl::parse_mapping("root = {}")), 0,
                std::chrono::milliseconds{0});
        if (kind == "stdin") return make_stdin_input(make_scanner(codec));
        return make_files_input(paths, codec);   // the filename reaches `switch` inside
    });
}

// One output's per-shard factory. Recursive for the same reason
// build_input_factory is: a `file` inside a `broker` inside a `switch` must get
// the same treatment as a top-level one.
// `strict_errors` travels down with the recursion rather than being read from a
// global: every wrapper that can introduce a processing error needs it, and the
// two backends must construct the same graph from the same flag.
std::function<output_ptr()> build_output_factory(const output_spec& out,
                                                 bool strict_errors) {
    std::function<output_ptr()> make;

    // The reference wraps each child of a non-fail-fast fan_out in an indefinite
    // retry, and does so BEFORE collapsing a one-child broker to that child --
    // so a broker of one still retries. Building the wrap into the child factory
    // keeps both cases on one line.
    const bool retry_children =
        out.kind == "broker" &&
        (out.pattern == "fan_out" || out.pattern == "fan_out_sequential");

    auto child_factory = [&](const output_spec& child) {
        auto f = build_output_factory(child, strict_errors);
        if (!retry_children) return f;
        return std::function<output_ptr()>(
            [f] { return make_retry_output(f()); });
    };

    if (out.comp) {
        // A registered connector builds its own factory; it decides for itself
        // whether it is per-shard or a singleton.
        const output_def* def = find_output(out.kind);
        if (!def) throw std::runtime_error("output disappeared from the registry: " + out.kind);
        make = def->build(*out.comp);
    } else if (out.kind == "broker" || out.kind == "fallback") {
        std::vector<std::function<output_ptr()>> kids;
        // `copies` repeats the whole child list, which is how a broker scales
        // one destination. It is a broker-only field.
        const uint64_t copies = out.kind == "broker" ? std::max<uint64_t>(out.copies, 1) : 1;
        for (uint64_t c = 0; c < copies; ++c)
            for (const auto& child : out.children) kids.push_back(child_factory(child));
        if (out.kind == "broker" && kids.size() == 1) {
            // A broker over exactly one output IS that output; the reference
            // collapses it too, so the shape of the config does not change how
            // many times a batch is written.
            make = kids[0];
        } else if (out.kind == "broker") {
            auto pattern = out.pattern;
            make = [kids, pattern] {
                std::vector<output_ptr> built;
                built.reserve(kids.size());
                for (const auto& k : kids) built.push_back(k());
                return make_broker_output(std::move(built), pattern);
            };
        } else {
            make = [kids] {
                std::vector<output_ptr> built;
                built.reserve(kids.size());
                for (const auto& k : kids) built.push_back(k());
                return make_fallback_output(std::move(built));
            };
        }
    } else if (out.kind == "switch") {
        struct case_factory {
            std::function<output_ptr()> out;
            cfg::bloblang               check;
            bool                        continue_ = false;
        };
        std::vector<case_factory> cases;
        cases.reserve(out.children.size());
        for (const auto& child : out.children) {
            auto f = build_output_factory(child, strict_errors);
            // `retry_until_success` wraps each CASE, not the switch: a message
            // routed to a failing case is retried there rather than nacking the
            // whole batch and re-routing it.
            if (out.retry_until_success)
                f = [inner = std::move(f)] { return make_retry_output(inner()); };
            cases.push_back({std::move(f), child.check, child.continue_});
        }
        // `switch.strict_mode` -- a message matching NO case is an error --
        // which is a different thing from `error_handling.strict` above. They
        // shared the name `strict` here until -Wshadow pointed it out.
        const bool switch_strict = out.strict_mode;
        make = [cases, switch_strict] {
            std::vector<switch_output_case> built;
            built.reserve(cases.size());
            for (const auto& c : cases)
                built.push_back({c.check.source.empty()
                                     ? transform_fn{}
                                     : interpreted_transform(blobl::parse_query(c.check.source)),
                                 c.out(), c.continue_});
            return make_switch_output(std::move(built), switch_strict);
        };
    } else if (out.kind == "reject") {
        // The interpolated message becomes a Bloblang QUERY, so it runs through
        // the same evaluator as every other expression.
        const std::string q = cfg::interpolation_to_query(out.message);
        make = [q] { return make_reject_output(interpreted_transform(blobl::parse_query(q))); };
    } else {
        auto okind = out.kind; auto opath = out.path;
        make = [okind, opath]() -> output_ptr {
            if (okind == "drop") return make_drop_output();
            if (okind == "file") {
                // Interpolation becomes a Bloblang QUERY, so it runs through the
                // same evaluator as every other expression. A path with nothing
                // to interpolate still compiles to a query, and passing the
                // literal alongside lets file_output open it once at connect().
                const std::string q = cfg::interpolation_to_query(opath);
                return make_file_output(
                    opath, cfg::is_interpolated(opath)
                               ? interpreted_transform(blobl::parse_query(q))
                               : transform_fn{});
            }
            return make_stdout_output();
        };
    }

    // Batching wraps BEFORE the output's own processors, matching the reference:
    // `processors` shape the messages routed to this output, and the policy then
    // groups what they produced.
    if (!out.batching.is_noop() || !out.batching_processors.empty()) {
        auto policy = out.batching;
        auto bprocs = out.batching_processors;
        make = [inner = std::move(make), policy, bprocs, strict_errors] {
            return make_batched_output(
                inner(), policy.count, policy.byte_size,
                std::chrono::duration_cast<std::chrono::milliseconds>(policy.period.ns),
                policy.check.source.empty()
                    ? transform_fn{}
                    : interpreted_transform(blobl::parse_query(policy.check.source)),
                build_processors(bprocs), strict_errors);
        };
    }

    if (!out.processors.empty()) {
        // `inner` is moved out first rather than captured from the variable being
        // assigned. Capturing `make` by value inside its own reassignment does
        // work -- the copy is taken before the assignment -- but it reads like a
        // self-referential loop, and this says what it is.
        auto procs = out.processors;
        make = [inner = std::move(make), procs, strict_errors] {
            return make_processed_output(inner(), build_processors(procs), strict_errors);
        };
    }
    return make;
}

stream_spec build_stream_spec(const pipeline_spec& ps) {
    stream_spec spec;
    spec.http = ps.http;
    spec.config = ps.config;

    spec.make_input = build_input_factory(ps.input);
    // The buffer WRAPS the input, because what it changes is when the source is
    // acknowledged. Applied here, outside the recursion, so it wraps the whole
    // input tree once rather than each leaf of a broker.
    if (ps.buffer.kind == "memory") {
        const auto& bs = ps.buffer;
        auto procs = bs.batching_processors;
        auto inner = spec.make_input;
        spec.make_input = [inner, bs, procs] {
            return make_buffered_input(
                inner(), bs.limit, bs.batch_enabled,
                batch_policy(bs.batching.count, bs.batching.byte_size,
                             std::chrono::duration_cast<std::chrono::milliseconds>(
                                 bs.batching.period.ns),
                             bs.batching.check.source.empty()
                                 ? transform_fn{}
                                 : interpreted_transform(
                                       blobl::parse_query(bs.batching.check.source))),
                build_processors(procs));
        };
    }

    auto procs = ps.processors;
    spec.make_processors = [procs] { return build_processors(procs, "pipeline.processors"); };

    // The identities every metric series is keyed on, in the order the stream
    // counts them: the input, then each processor, then the output.
    //
    // `root.` prefixed, because that is what the reference's /metrics emits --
    // `path="root.input"`, `path="root.pipeline.processors.0"`. Error
    // provenance uses the unprefixed form, and the two are deliberately not
    // shared: they are different strings in the reference too.
    spec.component_idents.push_back(
        {comp_kind::input, "root.input", ps.input.comp ? ps.input.comp->label : std::string()});
    for (size_t i = 0; i < procs.size(); ++i)
        spec.component_idents.push_back(
            {comp_kind::processor, "root.pipeline.processors." + std::to_string(i),
             procs[i].label});
    spec.component_idents.push_back(
        {comp_kind::output, "root.output",
         ps.output.comp ? ps.output.comp->label : std::string()});

    spec.make_output = build_output_factory(ps.output, ps.config.strict_errors);
    return spec;
}

} // namespace sf
