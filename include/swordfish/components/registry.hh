// The component registry: one declaration per component, read by every consumer.
//
// Before this, adding a processor meant editing five files — parse_pipeline.cc,
// build_stream.cc, emit_main.cc, components.hh and the implementation — and the
// three config sites could drift from each other. That is exactly what the
// descriptor exists to prevent, so the registry closes the gap: a component
// declares its config once and the parse,
// construct, emit and document paths are all derived from it.
#pragma once

#include "swordfish/config/spec.hh"
#include "swordfish/runtime/component.hh"
#include "swordfish/runtime/cache.hh"
#include "swordfish/runtime/rate_limit.hh"
#include "swordfish/runtime/scanner.hh"

#include <any>
#include <map>
#include <functional>
#include <memory>
#include <chrono>
#include <string>
#include <vector>

namespace sf {

// The `cpp:` body is the one config the code generator must look inside, because
// it becomes a translation unit rather than a constructor argument. Exposed as a
// layout-compatible view so the emitter need not include the definitions TU.
struct proc_cpp_view {
    std::string              body;
    std::vector<std::string> includes;
};

// A parsed component: its kind plus a type-erased config owned by the registry
// entry that produced it. Copyable, because every shard rebuilds from it.
struct component_config {
    std::string                   kind;
    // Benthos allows `label:` alongside the kind on every component; it names
    // the component in metrics paths and lets a resource be referenced by it.
    // Rejecting it made ordinary Redpanda Connect configs unparseable, not just
    // test files -- found while running their unit-test corpus.
    std::string                   label;
    // For `resource: <name>`: which resource this references. Distinct from
    // `label`, which is this component's OWN name.
    std::string                   label_ref;
    // Document-level state a component cannot look up for itself. `dedupe`
    // names an entry in `cache_resources`, which the registry never sees, so it
    // is resolved after parsing and handed to the build function through the
    // component_config it already receives.
    std::string                   cache_name;   // the label `dedupe` referenced
    cache_spec                    cache;
    bool                          cache_resolved = false;
    // Same shape for `rate_limit_resources`: the label a component named, and
    // the settings resolved from the document once it has been parsed whole.
    // The LABEL is carried through to construction, because two components
    // naming one limit must share its permits and the label is what identifies
    // it.
    std::string                   rate_limit_name;
    int64_t                       rate_limit_count = 0;
    std::chrono::nanoseconds      rate_limit_interval{0};
    bool                          rate_limit_resolved = false;
    // Scanners lifted out of the body before it was decoded, keyed by their
    // dotted config path. A field named `scanner` is always the reference's
    // scanner COMPONENT -- a single-key object whose key is the kind -- which
    // the spec_of field model cannot express, exactly as with `batching` and a
    // nested processor list.
    std::map<std::string, scanner_spec> scanners;
    std::shared_ptr<const void>   cfg;
    // Nested processor lists (try/catch/for_each) and switch cases, kept out of
    // the type-erased config so the tree stays walkable without knowing types.
    std::vector<component_config> children;
    std::vector<std::pair<cfg::bloblang, std::vector<component_config>>> cases;
    // `workflow`: its named branches and the groups they run in. Kept out of
    // the type-erased config for the same reason `children` and `cases` are --
    // a nested component tree has no field type, and the tree must stay
    // walkable without knowing the config type.
    // A named struct rather than std::pair, and that is not cosmetic: a vector
    // of INCOMPLETE type is well defined, but std::pair of one is not, and
    // clang refuses to instantiate std::find_if over the pair version while gcc
    // accepts it. The struct compiles under both.
    struct named_branch;
    std::vector<named_branch> branches;
    std::vector<std::vector<std::string>>                 order;
    cfg::position                 where;
};

struct component_config::named_branch {
    std::string      name;
    component_config config;
};

// What a component's emitter needs from its surroundings: a way to turn a
// Bloblang source into the name of a generated function, and a way to emit a
// nested processor list.
struct emit_env {
    std::function<std::string(const cfg::bloblang&)>              fn_for;
    std::function<std::string(const std::vector<component_config>&, int)> list_for;
    int indent = 0;
    // Where this component sits, as build_env carries it. Generated code gets
    // these as string literals so error_source_path() reads the same in both
    // modes -- a compiled pipeline reporting a different origin for the same
    // failure is the two backends disagreeing about what a config means.
    std::string path;
};

// What a component's builder needs: a way to turn Bloblang source into a
// transform, and to build nested processor lists.
struct build_env {
    std::function<transform_fn(const cfg::bloblang&)>                     transform_for;
    std::function<std::vector<processor_ptr>(const std::vector<component_config>&)> list_for;
    // Where this component sits, for the error_source_*() functions. A processor
    // that catches its own failures -- `mapping` and `try` do -- builds its own
    // error_source_info and cannot get these from the stream layer, which only
    // sees a processor that THREW. Both were left empty, so `error_source_label`
    // returned "" for every message however the config was labelled.
    std::string label;   // the user's `label:`, empty when none was set
    std::string path;    // e.g. "pipeline.processors.1"
};

struct processor_def {
    std::string_view kind;
    // Shapes a component's body can take in YAML.
    enum class body { object, list } shape = body::object;

    std::function<std::shared_ptr<const void>(const cfg::ynode&, const std::string&, cfg::lints&)> parse;
    std::function<processor_ptr(const component_config&, const build_env&)>  build;
    std::function<std::string(const component_config&, emit_env&)>           emit;
    std::function<std::string()>                                             describe;
    // A YAML skeleton of this component's default config, for `swordfish
    // create`. Filled from the same spec_of table describe() reads.
    std::function<std::string(int)>                                          scaffold;
    // Every Bloblang field of the config, so the caller can validate them at
    // load time without knowing the config's type.
    std::function<std::vector<const cfg::bloblang*>(const void*)>            bloblangs;
};

// Inputs and outputs go through the same machinery. They were hand-rolled in
// parse_pipeline/build_stream with a `kind` string and a struct of every field
// any of them might need, which was survivable for stdin/file/generate and
// stops being so at the first connector with real configuration -- Kafka's has
// thirteen fields, two of them nested.
//
// Unlike a processor, an input or output is built per SHARD, so `build` returns
// a factory rather than an instance.
struct input_def {
    std::string_view kind;
    // The config paths at which this component reads a `scanner:`. Declared,
    // because `lift_scanners` removes EVERY key named `scanner` from a body
    // before spec_of decodes it -- so without this list a scanner written on a
    // component that has none, or at the wrong path on one that does, was
    // accepted and then dropped. That changes the SHAPE of the data, which is
    // the failure make_scanner() was introduced to stop.
    std::vector<std::string> scanner_paths;
    std::function<std::shared_ptr<const void>(const cfg::ynode&, const std::string&, cfg::lints&)> parse;
    std::function<std::function<input_ptr()>(const component_config&)> build;
    // The C++ expression that constructs this input in a generated main().
    // Without it `swordfish build` would silently fall through to the built-in
    // inputs and produce a binary that reads something else entirely.
    std::function<std::string(const component_config&)> emit;
    // The header that expression needs. Carried here rather than hardcoded in
    // the emitter, which would need editing for every connector added -- the
    // exact coupling the registry exists to remove.
    std::string_view header;
    std::function<std::string()> describe;
    std::function<std::string(int)> scaffold;
    // As on processor_def: every Bloblang and interpolation field of the config,
    // so a syntax error in one is a config error with a position rather than a
    // failure on the first message. Connectors had no such hook at all, so a
    // broken `${! ... }` in an http_client payload or a kafka key lint clean and
    // then died at run time with no line and no field name.
    std::function<std::vector<const cfg::bloblang*>(const void*)> bloblangs;
};

struct output_def {
    std::string_view kind;
    std::vector<std::string> scanner_paths;   // as above; most outputs have none
    // Whether this output accepts a `batching:` policy. The reference allows it
    // only on the outputs that benefit -- `stdout` is rejected there with
    // "field batching is invalid when the component type is stdout" -- and
    // matching that is what keeps a config that lints here linting there too.
    bool batching = false;
    std::function<std::shared_ptr<const void>(const cfg::ynode&, const std::string&, cfg::lints&)> parse;
    std::function<std::function<output_ptr()>(const component_config&)> build;
    std::function<std::string(const component_config&)> emit;   // as above
    std::string_view header;                                    // as above
    std::function<std::string()> describe;
    std::function<std::string(int)> scaffold;
    std::function<std::vector<const cfg::bloblang*>(const void*)> bloblangs;   // as above
};

// The rate limit a component's `rate_limit:` field named, or nullptr when it
// named none. One helper rather than four copies of the same three lines, so a
// component cannot accidentally resolve a limit differently from its neighbour
// -- and so `build` and `emit` below cannot drift apart, which is the failure
// this whole registry exists to prevent.
//
// Safe to call from a PROCESSOR's build function, which runs once per shard.
rate_limit_ptr rate_limit_for(const component_config& cc);

// The same thing deferred, for an INPUT or OUTPUT. Their `build` returns a
// factory that is constructed once and then copied to every shard, so a limit
// resolved in `build` itself would be a single object shared across cores: a
// data race on its counter, and a budget already divided by the shard count then
// applied once for all of them instead of once per shard. Calling the returned
// resolver INSIDE the factory gives each shard its own.
std::function<rate_limit_ptr()> rate_limit_resolver(const component_config& cc);

// The C++ expression that reconstructs that limit in generated code, or an
// empty-shared_ptr literal when there is none.
std::string emit_rate_limit(const component_config& cc);

// The C++ expression that rebuilds a scanner_spec in generated code. Beside
// emit_rate_limit for the same reason: a connector's own emitter needs it, and
// a second copy in the code generator would be free to disagree with this one.
std::string emit_scanner_spec(const scanner_spec& sc);

// The same, for a cache_spec.
std::string emit_cache_spec(const cache_spec& c);

// The scanner a component's `scanner:` named, at the given config path
// ("scanner", or "stream.scanner" for `http_client`). Returns the default
// `lines` scanner when the config did not name one.
scanner_spec scanner_for(const component_config& cc, const std::string& path);

const input_def*  find_input(std::string_view kind);
const output_def* find_output(std::string_view kind);
void register_input(input_def def);
void register_output(output_def def);
std::vector<std::string_view> input_kinds();
std::vector<std::string_view> output_kinds();

// Registry lookup. Returns nullptr for an unknown kind, which is what turns an
// unsupported component into a named lint error rather than a crash.
const processor_def* find_processor(std::string_view kind);
std::vector<std::string_view> processor_kinds();

void register_processor(processor_def def);

// Registers the built-in components. Called on first registry access rather than
// relying on a static initialiser: the definitions live in their own translation
// unit inside a STATIC library, and a linker will drop an object file nothing
// references — which silently emptied the registry.
void register_builtin_processors();

// Registered the same way and for the same reason: a static library drops an
// object file nothing references. Called on first input/output lookup.
void register_builtin_io();

// Declares a processor whose config is a plain struct with a spec_of. Parsing,
// documentation and default-omitting emission all fall out of that one table.
template <class Config>
processor_def make_processor_def(
        std::string_view kind,
        std::function<processor_ptr(const Config&, const component_config&, const build_env&)> build,
        std::function<std::string(const Config&, const component_config&, emit_env&)> emit,
        processor_def::body shape = processor_def::body::object) {
    processor_def d;
    d.kind  = kind;
    d.shape = shape;
    d.parse = [](const cfg::ynode& n, const std::string& path, cfg::lints& ls)
            -> std::shared_ptr<const void> {
        auto c = std::make_shared<Config>();
        cfg::parse_into<Config>(n, *c, path, ls);
        return c;
    };
    d.build = [build](const component_config& cc, const build_env& env) {
        return build(*static_cast<const Config*>(cc.cfg.get()), cc, env);
    };
    d.emit = [emit](const component_config& cc, emit_env& env) {
        return emit(*static_cast<const Config*>(cc.cfg.get()), cc, env);
    };
    d.describe = [kind] { return cfg::describe<Config>(kind); };
    d.scaffold = [](int indent) { return cfg::scaffold<Config>(indent); };
    d.bloblangs = [](const void* cfg) {
        std::vector<const cfg::bloblang*> out;
        cfg::collect_bloblang(*static_cast<const Config*>(cfg), out);
        return out;
    };
    return d;
}

// The `component_config` reaches build and emit for the same reason it reaches
// a processor's: some settings are not in the component's own config and cannot
// be. A `rate_limit:` field holds a LABEL, and the count and interval behind it
// live in a document-level `rate_limit_resources` block the registry never sees.
// Most components ignore the parameter.
template <class Config>
input_def make_input_def(std::string_view kind,
                         std::function<std::function<input_ptr()>(const Config&, const component_config&)> build,
                         std::function<std::string(const Config&, const component_config&)> emit,
                         std::string_view header) {
    input_def d;
    d.kind = kind;
    d.header = header;
    d.parse = [](const cfg::ynode& n, const std::string& path, cfg::lints& ls)
            -> std::shared_ptr<const void> {
        auto c = std::make_shared<Config>();
        cfg::parse_into<Config>(n, *c, path, ls);
        // An optional per-config hook for what a field DESCRIPTOR cannot say:
        // a rule spanning two fields, such as "one of these two spellings must
        // be present". Runs inside parse, so `sfconfig lint` sees it rather than
        // the failure arriving at run time.
        if constexpr (requires { Config::validate(*c, n, path, ls); })
            Config::validate(*c, n, path, ls);
        return c;
    };
    d.build = [build](const component_config& cc) {
        return build(*static_cast<const Config*>(cc.cfg.get()), cc);
    };
    d.emit = [emit](const component_config& cc) {
        return emit(*static_cast<const Config*>(cc.cfg.get()), cc);
    };
    d.describe = [kind] { return cfg::describe<Config>(kind); };
    d.scaffold = [](int indent) { return cfg::scaffold<Config>(indent); };
    d.bloblangs = [](const void* cfg) {
        std::vector<const cfg::bloblang*> out;
        cfg::collect_bloblang(*static_cast<const Config*>(cfg), out);
        return out;
    };
    return d;
}

template <class Config>
output_def make_output_def(std::string_view kind,
                           std::function<std::function<output_ptr()>(const Config&, const component_config&)> build,
                           std::function<std::string(const Config&, const component_config&)> emit,
                           std::string_view header) {
    output_def d;
    d.kind = kind;
    d.header = header;
    d.parse = [](const cfg::ynode& n, const std::string& path, cfg::lints& ls)
            -> std::shared_ptr<const void> {
        auto c = std::make_shared<Config>();
        cfg::parse_into<Config>(n, *c, path, ls);
        // An optional per-config hook for what a field DESCRIPTOR cannot say:
        // a rule spanning two fields, such as "one of these two spellings must
        // be present". Runs inside parse, so `sfconfig lint` sees it rather than
        // the failure arriving at run time.
        if constexpr (requires { Config::validate(*c, n, path, ls); })
            Config::validate(*c, n, path, ls);
        return c;
    };
    d.build = [build](const component_config& cc) {
        return build(*static_cast<const Config*>(cc.cfg.get()), cc);
    };
    d.emit = [emit](const component_config& cc) {
        return emit(*static_cast<const Config*>(cc.cfg.get()), cc);
    };
    d.describe = [kind] { return cfg::describe<Config>(kind); };
    d.scaffold = [](int indent) { return cfg::scaffold<Config>(indent); };
    d.bloblangs = [](const void* cfg) {
        std::vector<const cfg::bloblang*> out;
        cfg::collect_bloblang(*static_cast<const Config*>(cfg), out);
        return out;
    };
    return d;
}

} // namespace sf
