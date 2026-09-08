// Every processor, declared once.
//
// Each entry gives a config struct (with its spec_of), how to build it, and how
// to emit it. Parsing, linting, documentation and default-omitting emission are
// all derived from the one table. Adding a processor now means: a config struct,
// a registration here, and the implementation — nothing in parse_pipeline,
// build_stream or emit_main.
#include "swordfish/codecs.hh"
#include "swordfish/components/registry.hh"
#include <type_traits>
#include <cstddef>
#include "swordfish/runtime/components.hh"

#include <chrono>

namespace sf::proc {

using namespace sf::cfg;

// ---- configs ----------------------------------------------------------------

struct mapping_config {
    bloblang mapping;
    bool operator==(const mapping_config&) const = default;
};
struct cpp_config {
    std::string              body;
    std::vector<std::string> includes;
    bool operator==(const cpp_config&) const = default;
};

// `emit_main.cc` reads a `cpp_config` back through `sf::proc_cpp_view`, a
// layout-compatible view declared in registry.hh so the emitter need not include
// this TU. Nothing tied the two together, so reordering or inserting a member
// here would have compiled clean and made every generated `cpp:` block read the
// wrong string. These turn that into a build failure.
static_assert(sizeof(cpp_config) == sizeof(sf::proc_cpp_view),
              "cpp_config and proc_cpp_view must stay layout-compatible");
static_assert(offsetof(cpp_config, body) == offsetof(sf::proc_cpp_view, body),
              "cpp_config::body moved away from proc_cpp_view::body");
static_assert(offsetof(cpp_config, includes) == offsetof(sf::proc_cpp_view, includes),
              "cpp_config::includes moved away from proc_cpp_view::includes");
static_assert(std::is_same_v<decltype(cpp_config::body), decltype(sf::proc_cpp_view::body)> &&
              std::is_same_v<decltype(cpp_config::includes),
                             decltype(sf::proc_cpp_view::includes)>,
              "cpp_config and proc_cpp_view member types diverged");
struct empty_config { bool operator==(const empty_config&) const = default; };
struct dedupe_config {
    std::string      cache;                 // the label of a cache_resources entry
    cfg::interpolation key;
    // TRUE, as the reference has it -- processor_dedupe.go's `drop_on_err` is
    // `.Default(true)`, and `redpanda-connect run` drops an errored message with
    // the field unset. Defaulting it to false here meant a message whose cache
    // lookup failed was PASSED ON where the reference discards it, so a cache
    // outage turned a deduplicating pipeline into a non-deduplicating one
    // without a word. Found while checking that a key the file cache cannot
    // store is refused rather than silently colliding.
    bool             drop_on_err = true;
    bool operator==(const dedupe_config&) const = default;
};
// Field names and defaults taken from `redpanda-connect create`, so a config
// written for the reference parses here unchanged.
struct workflow_config {
    // `branches` and `order` are lifted out during parsing; only meta_path is
    // an ordinary field.
    std::string meta_path = "meta.workflow";
    bool operator==(const workflow_config&) const = default;
};
struct branch_config {
    // `processors` is lifted out during parsing into component_config::children,
    // because a nested processor list has no field type here.
    bloblang request_map{"", false};
    bloblang result_map{"", false};
    bool operator==(const branch_config&) const = default;
};
struct codec_config {
    std::string algorithm;
    int         level = -1;          // ignored by the level-less codecs
    bool operator==(const codec_config&) const = default;
};
// The same shape, and a distinct TYPE only so the two directions can carry
// different option lists: swordfish (like the reference) ships a bzip2 decoder
// and no bzip2 encoder, so `bzip2` is valid on one and not the other.
struct decodec_config {
    std::string algorithm;
    int         level = -1;
    bool operator==(const decodec_config&) const = default;
};
struct archive_config {
    std::string format;
    bool operator==(const archive_config&) const = default;
};
struct select_parts_config {
    std::vector<int64_t> parts;
    bool operator==(const select_parts_config&) const = default;
};
struct insert_part_config {
    int64_t          index = -1;
    cfg::interpolation content;
    bool operator==(const insert_part_config&) const = default;
};
struct bounds_check_config {
    int64_t max_part_size = 1073741824;
    int64_t min_part_size = 1;
    int64_t max_parts     = 100;
    int64_t min_parts     = 1;
    bool operator==(const bounds_check_config&) const = default;
};
struct split_config {
    int64_t size      = 1;
    int64_t byte_size = 0;
    bool operator==(const split_config&) const = default;
};
struct unarchive_config {
    std::string format;
    bool operator==(const unarchive_config&) const = default;
};
struct log_config {
    std::string level = "INFO";
    // An INTERPOLATED STRING, as in the reference, where the field's type is
    // `string` and its default is empty. It was a Bloblang QUERY here, so the
    // reference's own documented usage -- `message: "unable to process: ${! error() }"`
    // -- was a parse error, and a plain `message: hello` was one too.
    interpolation message;
    bool operator==(const log_config&) const = default;
};
// `rate_limit` names a resource rather than configuring one, so its only field
// is the label. The settings are resolved from `rate_limit_resources` after the
// document is parsed, the way `dedupe` resolves its cache.
struct rate_limit_config {
    std::string resource;
    bool operator==(const rate_limit_config&) const = default;
};
struct sleep_config {
    duration duration_{};
    bool operator==(const sleep_config&) const = default;
};
} // namespace sf::proc

namespace sf::cfg {

template <> struct spec_of<sf::proc::mapping_config> {
    static constexpr std::string_view cpp_type = "sf::proc::mapping_config";
    static constexpr auto value = object(
        field("mapping", &sf::proc::mapping_config::mapping)
            .describe("A Bloblang mapping applied to each message."));
};
template <> struct spec_of<sf::proc::cpp_config> {
    static constexpr std::string_view cpp_type = "sf::proc::cpp_config";
    static constexpr auto value = object(
        field("body", &sf::proc::cpp_config::body)
            .describe("C++ statements. In scope: `self`, `root`, `ctx`."),
        field("includes", &sf::proc::cpp_config::includes)
            .describe("Extra headers, e.g. ['<cmath>']."));
};
template <> struct spec_of<sf::proc::empty_config> {
    static constexpr std::string_view cpp_type = "sf::proc::empty_config";
    static constexpr auto value = object();
};
template <> struct spec_of<sf::proc::dedupe_config> {
    static constexpr std::string_view cpp_type = "sf::proc::dedupe_config";
    static constexpr auto value = object(
        field("cache", &sf::proc::dedupe_config::cache)
            .describe("The label of a cache resource to store keys in.").require(),
        field("key", &sf::proc::dedupe_config::key)
            .describe("An interpolated string producing the deduplication key.").require(),
        field("drop_on_err", &sf::proc::dedupe_config::drop_on_err)
            .describe("Whether messages should be dropped when the cache returns a "
                      "general error such as a network issue."));
};
template <> struct spec_of<sf::proc::workflow_config> {
    static constexpr std::string_view cpp_type = "sf::proc::workflow_config";
    static constexpr auto value = object(
        field("meta_path", &sf::proc::workflow_config::meta_path)
            .describe("Dotted path IN THE DOCUMENT where the branches that succeeded and "
                      "failed are recorded. Not message metadata, despite the name. "
                      "Empty disables the record."));
};
template <> struct spec_of<sf::proc::branch_config> {
    static constexpr std::string_view cpp_type = "sf::proc::branch_config";
    static constexpr auto value = object(
        field("request_map", &sf::proc::branch_config::request_map)
            .describe("Builds the request sent to the child processors. `root` starts "
                      "EMPTY; a `deleted()` result skips the message, which is how a "
                      "branch is made conditional."),
        field("result_map", &sf::proc::branch_config::result_map)
            .describe("Maps the result back ONTO the original message: `root` starts at "
                      "the original and `this` is the branch result."));
};
template <> struct spec_of<sf::proc::codec_config> {
    static constexpr std::string_view cpp_type = "sf::proc::codec_config";
    // `pgzip` is Go's parallel gzip: the same wire format, so a config naming it
    // works here. `bzip2` is absent because there is no bzip2 ENCODER on either
    // side -- the reference accepts it at lint and then fails at run time, so
    // refusing it by name here is the same verdict said earlier.
    //
    // This list used to omit `zstd` as well, with a comment claiming bzip2 was
    // the only algorithm missing. codec::zstd_compress/zstd_decompress have been
    // in the binary all along -- used by Bloblang's compress(), by the
    // `decompress` scanner and by Kafka -- so a config the reference runs was
    // refused here by a list that had simply never been updated.
    static constexpr auto value = object(
        field("algorithm", &sf::proc::codec_config::algorithm)
            .describe("The compression algorithm.")
            .options(sf::codec::compress_algorithms).require(),
        field("level", &sf::proc::codec_config::level)
            .describe("Compression level, where the algorithm has one. -1 is its default."));
};

template <> struct spec_of<sf::proc::decodec_config> {
    static constexpr std::string_view cpp_type = "sf::proc::decodec_config";
    // Decoding additionally offers `bzip2`: there is a decoder and no encoder,
    // which is exactly the asymmetry the reference has.
    static constexpr auto value = object(
        field("algorithm", &sf::proc::decodec_config::algorithm)
            .describe("The compression algorithm.")
            .options(sf::codec::decompress_algorithms).require(),
        field("level", &sf::proc::decodec_config::level)
            .describe("Compression level, where the algorithm has one. -1 is its default."));
};
template <> struct spec_of<sf::proc::archive_config> {
    static constexpr std::string_view cpp_type = "sf::proc::archive_config";
    // tar, zip and binary need container formats not implemented yet, and are
    // left out so a config using them is rejected rather than surprising.
    static constexpr std::string_view formats[] = {"json_array", "lines", "concatenate"};
    static constexpr auto value = object(
        field("format", &sf::proc::archive_config::format)
            .describe("How the batch is combined into one message.")
            .options(formats).require());
};
template <> struct spec_of<sf::proc::select_parts_config> {
    static constexpr std::string_view cpp_type = "sf::proc::select_parts_config";
    static constexpr auto value = object(
        field("parts", &sf::proc::select_parts_config::parts)
            .describe("Message indices to keep, in order. A negative index counts "
                      "back from the end; one out of range is skipped."));
};
template <> struct spec_of<sf::proc::insert_part_config> {
    static constexpr std::string_view cpp_type = "sf::proc::insert_part_config";
    static constexpr auto value = object(
        field("index", &sf::proc::insert_part_config::index)
            .describe("Where to insert. -1 appends; past the end clamps."),
        field("content", &sf::proc::insert_part_config::content)
            .describe("The new message's content, interpolated against the first message."));
};
template <> struct spec_of<sf::proc::bounds_check_config> {
    static constexpr std::string_view cpp_type = "sf::proc::bounds_check_config";
    static constexpr auto value = object(
        field("max_part_size", &sf::proc::bounds_check_config::max_part_size)
            .describe("The largest a single message may be, in bytes."),
        field("min_part_size", &sf::proc::bounds_check_config::min_part_size)
            .describe("The smallest a single message may be, in bytes."),
        field("max_parts", &sf::proc::bounds_check_config::max_parts)
            .describe("The most messages a batch may contain.").advanced_(),
        field("min_parts", &sf::proc::bounds_check_config::min_parts)
            .describe("The fewest messages a batch may contain.").advanced_());
};
template <> struct spec_of<sf::proc::split_config> {
    static constexpr std::string_view cpp_type = "sf::proc::split_config";
    static constexpr auto value = object(
        field("size", &sf::proc::split_config::size)
            .describe("Messages per output batch."),
        field("byte_size", &sf::proc::split_config::byte_size)
            .describe("Cap each batch by accumulated bytes instead; takes precedence "
                      "over `size` when non-zero."));
};
template <> struct spec_of<sf::proc::unarchive_config> {
    static constexpr std::string_view cpp_type = "sf::proc::unarchive_config";
    // Only the formats that are actually implemented are offered. Listing tar
    // or zip here would let a config lint clean and then fail at runtime; the
    // options list is what makes the gap a config-time error naming the format.
    static constexpr std::string_view formats[] = {
        "json_array", "json_map", "json_documents", "lines"};
    static constexpr auto value = object(
        field("format", &sf::proc::unarchive_config::format)
            .describe("The archive format to expand.").options(formats).require());
};
template <> struct spec_of<sf::proc::log_config> {
    static constexpr std::string_view cpp_type = "sf::proc::log_config";
    static constexpr std::string_view levels[] = {"TRACE","DEBUG","INFO","WARN","ERROR"};
    static constexpr auto value = object(
        field("level", &sf::proc::log_config::level)
            .describe("The log level to emit at.").options(levels),
        field("message", &sf::proc::log_config::message)
            .describe("The message to log. Interpolated, so `${! ... }` is "
                      "evaluated against the message."));
};
template <> struct spec_of<sf::proc::rate_limit_config> {
    static constexpr std::string_view cpp_type = "sf::proc::rate_limit_config";
    static constexpr auto value = object(
        field("resource", &sf::proc::rate_limit_config::resource)
            .describe("The label of a `rate_limit_resources` entry to throttle by.")
            .require());
};
template <> struct spec_of<sf::proc::sleep_config> {
    static constexpr std::string_view cpp_type = "sf::proc::sleep_config";
    static constexpr auto value = object(
        field("duration", &sf::proc::sleep_config::duration_)
            .describe("How long to pause for, e.g. '100ms'.").require());
};

} // namespace sf::cfg

namespace sf {

namespace {

// `order` resolved against `branches`, shared by BOTH backends. The checks below
// decide what a workflow means, so they cannot live in only one of them: the
// interpreter accepting a config the compiler refuses -- or worse, the two
// running the branches in different orders -- is exactly the divergence the
// pipeline differential exists to catch, and it is cheaper to make impossible.
std::vector<std::vector<std::pair<std::string, const component_config*>>>
resolve_workflow_order(const component_config& cc) {
    if (cc.order.empty())
        throw std::runtime_error(
            "workflow requires `order`: swordfish does not infer branch "
            "dependencies from the request and result mappings, and running "
            "branches in the wrong order would produce wrong data silently");
    std::vector<std::vector<std::pair<std::string, const component_config*>>> groups;
    // `order` must name each branch EXACTLY once. Checking only that every name
    // exists left three ways to be silently wrong, all of which the reference
    // refuses by name (processor_workflow_branch_map.go:253-279): a branch
    // missing from `order` was never run and its result_map never applied, a
    // name listed twice ran twice, and an empty tier passed unnoticed.
    std::vector<std::string> seen;
    for (const auto& g : cc.order) {
        if (g.empty())
            throw std::runtime_error("workflow `order` has an empty tier; a tier "
                                     "names the branches that may run together, so "
                                     "an empty one asks for nothing");
        std::vector<std::pair<std::string, const component_config*>> built;
        for (const auto& nm : g) {
            const auto it = std::find_if(cc.branches.begin(), cc.branches.end(),
                [&](const auto& b) { return b.name == nm; });
            if (it == cc.branches.end())
                throw std::runtime_error("workflow `order` names branch '" + nm +
                                         "', which is not declared in `branches`");
            if (std::find(seen.begin(), seen.end(), nm) != seen.end())
                throw std::runtime_error("workflow `order` lists branch '" + nm +
                                         "' more than once, which would run it twice");
            seen.push_back(nm);
            built.emplace_back(nm, &it->config);
        }
        groups.push_back(std::move(built));
    }
    for (const auto& b : cc.branches)
        if (std::find(seen.begin(), seen.end(), b.name) == seen.end())
            throw std::runtime_error("workflow branch '" + b.name + "' is missing "
                                     "from `order`, so it would never run and its "
                                     "`result_map` would never be applied");
    return groups;
}

// `meta.workflow` -> {"meta", "workflow"}. Empty segments are dropped, so a
// trailing or doubled dot cannot produce an empty path component.
std::vector<std::string> split_meta_path(const std::string& path) {
    std::vector<std::string> out;
    for (size_t i = 0, j; i <= path.size(); i = j + 1) {
        j = path.find('.', i);
        if (j == std::string::npos) j = path.size();
        if (j > i) out.push_back(path.substr(i, j - i));
        if (j == path.size()) break;
    }
    return out;
}


using sf::cfg::cxx_string_literal;

using namespace sf::proc;

// A combinator: owns a nested processor list and differs only in which factory
// it calls. Declaring them together keeps the three that share a shape honest.
processor_def chain_def(std::string_view kind,
                        processor_ptr (*make)(std::vector<processor_ptr>),
                        std::string_view factory) {
    return make_processor_def<empty_config>(
        kind,
        [make](const empty_config&, const component_config& cc, const build_env& env) {
            return make(env.list_for(cc.children));
        },
        [factory](const empty_config&, const component_config& cc, emit_env& env) {
            return std::string(factory) + "(" + env.list_for(cc.children, env.indent) + ")";
        },
        processor_def::body::list);
}

void register_builtins_impl() {
        const auto mapping_def = make_processor_def<mapping_config>("mapping",
            [](const mapping_config& c, const component_config& cc, const build_env& e) {
                return make_mapping_processor(e.transform_for(c.mapping), false,
                                              cc.label, e.path);
            },
            [](const mapping_config& c, const component_config& cc, emit_env& e) {
                // The label and path go in as literals so error_source_*() reads
                // the same from a compiled binary as from the interpreter.
                return "sf::make_mapping_processor(&sf::gen::" + e.fn_for(c.mapping) +
                       ", false, " + cfg::cxx_string_literal(cc.label) + ", " +
                       cfg::cxx_string_literal(e.path) + ")";
            });
        register_processor(mapping_def);

        // `bloblang` IS an alias of `mapping` -- verified against
        // redpanda-connect 4.107.2, both start root empty. Built into a local
        // rather than read back from the registry: a lookup here would re-enter
        // the lazy initialiser that is still running.
        {
            auto d = mapping_def;
            d.kind = "bloblang";
            // `describe` closes over the kind it was BUILT with, so a clone that
            // only reassigns `kind` documents itself under the original name:
            // `sfconfig list` printed `mapping` twice and `bloblang` never.
            d.describe = [] { return cfg::describe<mapping_config>("bloblang"); };
            register_processor(std::move(d));
        }

        // `mutation` is NOT. Its root starts at the input document, so a
        // mapping that sets one field keeps the rest; treating it as an alias
        // dropped every field it did not mention. That is silent data loss in a
        // commonly used processor, and nothing in the suite caught it until the
        // Benthos unit-test corpus was run (05-compatibility-and-testing.md
        // § 8.7).
        register_processor(make_processor_def<mapping_config>("mutation",
            [](const mapping_config& c, const component_config& cc, const build_env& e) {
                return make_mapping_processor(e.transform_for(c.mapping), /*mutate=*/true,
                                              cc.label, e.path);
            },
            [](const mapping_config& c, const component_config& cc, emit_env& e) {
                return "sf::make_mapping_processor(&sf::gen::" + e.fn_for(c.mapping) +
                       ", /*mutate=*/true, " + cfg::cxx_string_literal(cc.label) + ", " +
                       cfg::cxx_string_literal(e.path) + ")";
            }));

        register_processor(make_processor_def<rate_limit_config>("rate_limit",
            [](const rate_limit_config&, const component_config& cc, const build_env&) {
                // A processor's build function already runs once per shard, so
                // the limit can be resolved here directly. rate_limit_for()
                // keys on the LABEL, which is what lets two components naming
                // one limit contend for the same permits -- the difference
                // between a resource and a setting.
                return make_rate_limit_processor(rate_limit_for(cc));
            },
            [](const rate_limit_config&, const component_config& cc, emit_env&) {
                return "sf::make_rate_limit_processor(" + emit_rate_limit(cc) + ")";
            }));

        register_processor(make_processor_def<dedupe_config>("dedupe",
            [](const dedupe_config& c, const component_config& cc, const build_env& e) {
                if (!cc.cache_resolved)
                    throw std::runtime_error("dedupe: cache '" + c.cache +
                        "' was not resolved against cache_resources");
                // The interpolated key becomes a Bloblang QUERY, so it runs
                // through the same evaluator as every other expression.
                cfg::bloblang q{cfg::interpolation_to_query(c.key.source), true};
                return make_dedupe_processor(make_cache(cc.cache),
                                             e.transform_for(q), c.drop_on_err);
            },
            [](const dedupe_config& c, const component_config& cc, emit_env& e) {
                // The same guard the build lambda carries. Without it an
                // unresolved cache emitted a DEFAULT-constructed cache_spec --
                // a 5-minute memory cache -- silently replacing whatever the
                // config declared, so `swordfish run` refused the config and
                // `swordfish build` shipped a different one. emit_rate_limit
                // has carried this check all along; this is the asymmetry the
                // shared helpers exist to prevent.
                if (!cc.cache_resolved)
                    throw std::runtime_error("dedupe: cache '" + c.cache +
                        "' was not resolved against cache_resources");
                cfg::bloblang q{cfg::interpolation_to_query(c.key.source), true};
                return "sf::make_dedupe_processor(sf::make_cache(" +
                       emit_cache_spec(cc.cache) + "), &sf::gen::" + e.fn_for(q) +
                       ", " + (c.drop_on_err ? "true" : "false") + ")";
            }));

        register_processor(make_processor_def<workflow_config>("workflow",
            [](const workflow_config& c, const component_config& cc, const build_env& e) {
                std::vector<std::vector<std::pair<std::string, processor_ptr>>> groups;
                for (const auto& g : resolve_workflow_order(cc)) {
                    std::vector<std::pair<std::string, processor_ptr>> built;
                    for (const auto& [name, branch] : g)
                        built.emplace_back(name, std::move(e.list_for({*branch}).front()));
                    groups.push_back(std::move(built));
                }
                return make_workflow_processor(std::move(groups),
                                               split_meta_path(c.meta_path));
            },
            [](const workflow_config& c, const component_config& cc, emit_env& env) {
                // The branch tree as nested factories. `order` and the branch
                // lookup go through the same resolver the interpreter uses, so
                // a config the two modes would disagree about is refused by
                // both rather than compiled into a silently mis-ordered
                // pipeline.
                const std::string pad(static_cast<size_t>(env.indent) * 4, ' ');
                std::string s = "sf::make_workflow_processor([]{\n" + pad +
                    "    std::vector<std::vector<std::pair<std::string, "
                    "sf::processor_ptr>>> gs;\n";
                for (const auto& g : resolve_workflow_order(cc)) {
                    s += pad + "    {\n" + pad +
                         "        std::vector<std::pair<std::string, "
                         "sf::processor_ptr>> g;\n";
                    for (const auto& [name, branch] : g)
                        // list_for yields a vector expression; a workflow slot
                        // holds one processor, so the single element is moved
                        // out of a named temporary rather than off a prvalue.
                        s += pad + "        g.emplace_back(" +
                             cfg::cxx_string_literal(name) + ", []{ auto v = " +
                             env.list_for({*branch}, env.indent + 2) +
                             "; return std::move(v.front()); }());\n";
                    s += pad + "        gs.push_back(std::move(g));\n" + pad + "    }\n";
                }
                s += pad + "    return gs;\n" + pad + "}(), std::vector<std::string>{";
                const auto path = split_meta_path(c.meta_path);
                for (size_t i = 0; i < path.size(); ++i)
                    s += (i ? ", " : "") + cfg::cxx_string_literal(path[i]);
                return s + "})";
            }));

        register_processor(make_processor_def<branch_config>("branch",
            [](const branch_config& c, const component_config& cc, const build_env& e) {
                return make_branch_processor(
                    c.request_map.source.empty() ? transform_fn{} : e.transform_for(c.request_map),
                    e.list_for(cc.children),
                    c.result_map.source.empty() ? transform_fn{} : e.transform_for(c.result_map));
            },
            [](const branch_config& c, const component_config& cc, emit_env& e) {
                std::string s = "sf::make_branch_processor(";
                s += c.request_map.source.empty() ? "sf::transform_fn{}"
                                                  : "&sf::gen::" + e.fn_for(c.request_map);
                s += ", " + e.list_for(cc.children, e.indent) + ", ";
                s += c.result_map.source.empty() ? "sf::transform_fn{}"
                                                 : "&sf::gen::" + e.fn_for(c.result_map);
                return s + ")";
            }));

        // The two directions no longer share a config TYPE, because they no
        // longer share an option list -- see decodec_config. Everything else is
        // identical, so both still build the one codec_processor.
        register_processor(make_processor_def<codec_config>("compress",
            [](const codec_config& c, const component_config&, const build_env&) {
                return make_codec_processor(c.algorithm, c.level, true);
            },
            [](const codec_config& c, const component_config&, emit_env&) {
                return "sf::make_codec_processor(" + cxx_string_literal(c.algorithm) +
                       ", " + std::to_string(c.level) + ", true)";
            }));
        register_processor(make_processor_def<decodec_config>("decompress",
            [](const decodec_config& c, const component_config&, const build_env&) {
                return make_codec_processor(c.algorithm, c.level, false);
            },
            [](const decodec_config& c, const component_config&, emit_env&) {
                return "sf::make_codec_processor(" + cxx_string_literal(c.algorithm) +
                       ", " + std::to_string(c.level) + ", false)";
            }));

        register_processor(make_processor_def<archive_config>("archive",
            [](const archive_config& c, const component_config&, const build_env&) {
                return make_archive_processor(c.format);
            },
            [](const archive_config& c, const component_config&, emit_env&) {
                return "sf::make_archive_processor(" + cxx_string_literal(c.format) + ")";
            }));

        register_processor(make_processor_def<select_parts_config>("select_parts",
            [](const select_parts_config& c, const component_config&, const build_env&) {
                return make_select_parts_processor(c.parts);
            },
            [](const select_parts_config& c, const component_config&, emit_env&) {
                std::string v = "std::vector<int64_t>{";
                for (size_t i = 0; i < c.parts.size(); ++i)
                    v += (i ? ", " : "") + std::to_string(c.parts[i]);
                return "sf::make_select_parts_processor(" + v + "})";
            }));

        register_processor(make_processor_def<insert_part_config>("insert_part",
            [](const insert_part_config& c, const component_config&, const build_env& e) {
                cfg::bloblang q{cfg::interpolation_to_query(c.content.source), true};
                return make_insert_part_processor(c.index, e.transform_for(q));
            },
            [](const insert_part_config& c, const component_config&, emit_env& e) {
                cfg::bloblang q{cfg::interpolation_to_query(c.content.source), true};
                return "sf::make_insert_part_processor(" + std::to_string(c.index) +
                       ", &sf::gen::" + e.fn_for(q) + ")";
            }));

        register_processor(make_processor_def<bounds_check_config>("bounds_check",
            [](const bounds_check_config& c, const component_config&, const build_env&) {
                return make_bounds_check_processor(c.max_part_size, c.min_part_size,
                                                   c.max_parts, c.min_parts);
            },
            [](const bounds_check_config& c, const component_config&, emit_env&) {
                return "sf::make_bounds_check_processor(" + std::to_string(c.max_part_size) +
                       ", " + std::to_string(c.min_part_size) + ", " +
                       std::to_string(c.max_parts) + ", " + std::to_string(c.min_parts) + ")";
            }));

        register_processor(make_processor_def<split_config>("split",
            [](const split_config& c, const component_config&, const build_env&) {
                return make_split_processor(c.size, c.byte_size);
            },
            [](const split_config& c, const component_config&, emit_env&) {
                return "sf::make_split_processor(" + std::to_string(c.size) + ", " +
                       std::to_string(c.byte_size) + ")";
            }));

        register_processor(make_processor_def<unarchive_config>("unarchive",
            [](const unarchive_config& c, const component_config&, const build_env&) {
                return make_unarchive_processor(c.format);
            },
            [](const unarchive_config& c, const component_config&, emit_env&) {
                return "sf::make_unarchive_processor(" + cxx_string_literal(c.format) + ")";
            }));

        register_processor(make_processor_def<cpp_config>("cpp",
            [](const cpp_config&, const component_config&, const build_env&) -> processor_ptr {
                throw std::runtime_error(
                    "`cpp:` blocks are compiled-mode only; use `swordfish build`");
            },
            [](const cpp_config&, const component_config& cc, emit_env& e) {
                // The body is emitted as its own translation unit, so it reaches
                // the emitter through the same function-naming path a mapping does.
                cfg::bloblang marker{"", false};
                return "sf::make_mapping_processor(&sf::gen::" + e.fn_for(marker) + ")";
            }));

        register_processor(make_processor_def<empty_config>("noop",
            [](const empty_config&, const component_config&, const build_env&) {
                return make_noop_processor();
            },
            [](const empty_config&, const component_config&, emit_env&) {
                return std::string("sf::make_noop_processor()");
            }));

        register_processor(make_processor_def<log_config>("log",
            [](const log_config& c, const component_config&, const build_env& e) {
                cfg::bloblang q{cfg::interpolation_to_query(c.message.source), true};
                return make_log_processor(c.level, e.transform_for(q));
            },
            [](const log_config& c, const component_config&, emit_env& e) {
                cfg::bloblang q{cfg::interpolation_to_query(c.message.source), true};
                return "sf::make_log_processor(" + cxx_string_literal(c.level) + ", &sf::gen::" +
                       e.fn_for(q) + ")";
            }));

        register_processor(make_processor_def<sleep_config>("sleep",
            [](const sleep_config& c, const component_config&, const build_env&) {
                return make_sleep_processor(
                    std::chrono::duration_cast<std::chrono::milliseconds>(c.duration_.ns));
            },
            [](const sleep_config& c, const component_config&, emit_env&) {
                return "sf::make_sleep_processor(std::chrono::milliseconds{" +
                       std::to_string(std::chrono::duration_cast<std::chrono::milliseconds>(
                           c.duration_.ns).count()) + "})";
            }));

        // `try` reports ITSELF as the error source when a child fails, so unlike
        // catch/for_each it needs its own label and path; the generated code gets
        // them as literals for the same reason.
        register_processor(make_processor_def<empty_config>(
            "try",
            [](const empty_config&, const component_config& cc, const build_env& env) {
                return make_try_processor(env.list_for(cc.children), env.label, env.path);
            },
            [](const empty_config&, const component_config& cc, emit_env& env) {
                return "sf::make_try_processor(" + env.list_for(cc.children, env.indent) +
                       ", " + cfg::cxx_string_literal(cc.label) + ", " +
                       cfg::cxx_string_literal(env.path) + ")";
            },
            processor_def::body::list));
        register_processor(chain_def("catch",    &make_catch_processor,    "sf::make_catch_processor"));
        register_processor(chain_def("for_each", &make_for_each_processor, "sf::make_for_each_processor"));

        const auto switch_def = make_processor_def<empty_config>("switch",
            [](const empty_config&, const component_config& cc, const build_env& env) {
                std::vector<std::pair<transform_fn, std::vector<processor_ptr>>> cases;
                for (const auto& [chk, children] : cc.cases)
                    cases.emplace_back(env.transform_for(chk), env.list_for(children));
                return make_switch_processor(std::move(cases));
            },
            [](const empty_config&, const component_config& cc, emit_env& env) {
                const std::string pad(env.indent * 4, ' ');
                std::string s = "sf::make_switch_processor([]{\n" + pad +
                    "    std::vector<std::pair<sf::transform_fn, "
                    "std::vector<sf::processor_ptr>>> c;\n";
                for (const auto& [chk, children] : cc.cases)
                    s += pad + "    c.emplace_back(&sf::gen::" + env.fn_for(chk) + ", " +
                         env.list_for(children, env.indent + 1) + ");\n";
                return s + pad + "    return c;\n" + pad + "}())";
            },
            processor_def::body::list);
        register_processor(switch_def);

        // `group_by` shares switch's config shape and routing rule -- a list of
        // {check, processors}, first match wins, unmatched passes through -- but
        // NOT its assembly, and it used to be registered as a clone that shared
        // both. The reference is explicit about the difference: switch returns
        // one batch in the incoming order (processor_switch.go:170-245), while
        // group_by returns one batch PER GROUP (processor_group_by.go:127-176).
        // Sharing the implementation meant every `switch` re-ordered and
        // re-batched its input.
        {
            auto g = switch_def;
            g.kind = "group_by";
            g.build = [](const component_config& cc, const build_env& env) {
                std::vector<std::pair<transform_fn, std::vector<processor_ptr>>> cases;
                for (const auto& [chk, children] : cc.cases)
                    cases.emplace_back(env.transform_for(chk), env.list_for(children));
                return make_group_by_processor(std::move(cases));
            };
            g.emit = [](const component_config& cc, emit_env& env) {
                const std::string pad(env.indent * 4, ' ');
                std::string s = "sf::make_group_by_processor([]{\n" + pad +
                    "    std::vector<std::pair<sf::transform_fn, "
                    "std::vector<sf::processor_ptr>>> c;\n";
                for (const auto& [chk, children] : cc.cases)
                    s += pad + "    c.emplace_back(&sf::gen::" + env.fn_for(chk) + ", " +
                         env.list_for(children, env.indent + 1) + ");\n";
                return s + pad + "    return c;\n" + pad + "}())";
            };
            // A clone that keeps the original `describe` reports the original
            // NAME, which is why `sfconfig list` printed `switch` twice and
            // `group_by` never.
            g.describe = [] { return cfg::describe<empty_config>("group_by"); };
            register_processor(std::move(g));
        }
}

} // namespace

void register_builtin_processors() {
    register_builtins_impl();
}

} // namespace sf
