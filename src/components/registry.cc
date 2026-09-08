#include "swordfish/components/registry.hh"
#include "swordfish/components/http.hh"
#include "swordfish/components/socket.hh"

#include <map>
#include <stdexcept>

namespace sf {

rate_limit_ptr rate_limit_for(const component_config& cc) {
    if (cc.rate_limit_name.empty()) return nullptr;
    // Unresolved here is an INTERNAL error, not a config one: parse_pipeline
    // refuses an undeclared label with a line number long before this point, so
    // reaching construction unresolved means a parse path forgot to resolve.
    if (!cc.rate_limit_resolved)
        throw std::runtime_error("internal error: rate limit '" + cc.rate_limit_name +
                                 "' reached component construction unresolved");
    return shared_rate_limit(cc.rate_limit_name, cc.rate_limit_count,
                             cc.rate_limit_interval);
}

std::function<rate_limit_ptr()> rate_limit_resolver(const component_config& cc) {
    // The label and settings are captured, not the limit: see the header. The
    // unresolved check runs HERE, at build time, so a parse bug is reported when
    // the pipeline is constructed rather than on whichever shard happens to
    // create its input first.
    if (cc.rate_limit_name.empty()) return [] { return rate_limit_ptr{}; };
    if (!cc.rate_limit_resolved)
        throw std::runtime_error("internal error: rate limit '" + cc.rate_limit_name +
                                 "' reached component construction unresolved");
    return [name = cc.rate_limit_name, count = cc.rate_limit_count,
            interval = cc.rate_limit_interval] {
        return shared_rate_limit(name, count, interval);
    };
}

// A `scanner_spec` as C++ that rebuilds it. Field-by-field assignment inside a
// lambda rather than a designated-initialiser literal: the fields a scanner uses
// depend on its kind, and designated initialisers must appear in declaration
// order, so a literal would have to be rebuilt every time the struct changed.
//
// Recursive, because `skip_bom`, `decompress` and `switch` each wrap a scanner.
std::string emit_scanner_spec(const scanner_spec& sc) {
    std::string body = "sf::scanner_spec s; s.kind = " + cfg::cxx_string_literal(sc.kind) + ";";
    const auto str = [&](const char* f, const std::string& v) {
        if (!v.empty()) body += std::string(" s.") + f + " = " + cfg::cxx_string_literal(v) + ";";
    };
    const auto num = [&](const char* f, int64_t v, int64_t dflt) {
        if (v != dflt) body += std::string(" s.") + f + " = " + std::to_string(v) + ";";
    };
    const auto flag = [&](const char* f, bool v, bool dflt) {
        if (v != dflt) body += std::string(" s.") + f + " = " + (v ? "true" : "false") + ";";
    };
    str("custom_delimiter", sc.custom_delimiter);
    num("max_buffer_size", sc.max_buffer_size, 65536);
    flag("omit_empty", sc.omit_empty, false);
    flag("parse_header_row", sc.parse_header_row, true);
    flag("lazy_quotes", sc.lazy_quotes, false);
    flag("continue_on_error", sc.continue_on_error, false);
    num("size", sc.size, 0);
    str("pattern", sc.pattern);
    str("algorithm", sc.algorithm);
    flag("raw_json", sc.raw_json, false);
    for (const auto& c : sc.children)
        body += " s.children.push_back(" + emit_scanner_spec(c) + ");";
    for (const auto& m : sc.child_matches)
        body += " s.child_matches.push_back(" + cfg::cxx_string_literal(m) + ");";
    return "[]{ " + body + " return s; }()";
}

std::string emit_cache_spec(const cache_spec& c) {
    // The label goes into the emitted spec too: it is what make_cache keys its
    // per-shard sharing on, so a compiled binary must share exactly as an
    // interpreted run does.
    std::string body = "sf::cache_spec s; s.kind = " + cfg::cxx_string_literal(c.kind) +
                       "; s.label = " + cfg::cxx_string_literal(c.label) + ";";
    const auto ms = [&](const char* f, std::chrono::milliseconds v) {
        body += std::string(" s.") + f + " = std::chrono::milliseconds{" +
                std::to_string(v.count()) + "};";
    };
    ms("default_ttl", c.default_ttl);
    ms("compaction_interval", c.compaction_interval);
    if (c.compaction_disabled) body += " s.compaction_disabled = true;";
    // `shards` decides when entries expire, not just how the map is striped, so
    // a compiled binary that dropped it would dedupe differently from an
    // interpreted run of the same config.
    if (c.shards != 1)         body += " s.shards = " + std::to_string(c.shards) + ";";
    if (c.cap != 1000)         body += " s.cap = " + std::to_string(c.cap) + ";";
    if (!c.directory.empty())
        body += " s.directory = " + cfg::cxx_string_literal(c.directory) + ";";
    for (const auto& k : c.init_keys)
        body += " s.init_keys.push_back(" + cfg::cxx_string_literal(k) + ");";
    return "[]{ " + body + " return s; }()";
}

scanner_spec scanner_for(const component_config& cc, const std::string& path) {
    auto it = cc.scanners.find(path);
    return it == cc.scanners.end() ? scanner_spec{} : it->second;
}

std::string emit_rate_limit(const component_config& cc) {
    if (cc.rate_limit_name.empty()) return "nullptr";
    if (!cc.rate_limit_resolved)
        throw std::runtime_error("internal error: rate limit '" + cc.rate_limit_name +
                                 "' reached code generation unresolved");
    return "sf::shared_rate_limit(" + cfg::cxx_string_literal(cc.rate_limit_name) + ", " +
           std::to_string(cc.rate_limit_count) + ", std::chrono::nanoseconds{" +
           std::to_string(cc.rate_limit_interval.count()) + "})";
}

namespace {
std::map<std::string, processor_def, std::less<>>& raw_registry() {
    static std::map<std::string, processor_def, std::less<>> r;
    return r;
}

// The first lookup pulls the built-ins in, which also creates the link-time
// reference that keeps their translation unit from being dropped.
std::map<std::string, processor_def, std::less<>>& registry() {
    // A type whose constructor does the work, rather than a bool that is
    // obviously always true. Function-local static initialisation is
    // thread-safe and runs exactly once.
    struct init { init() { register_builtin_processors(); } };
    static const init once;
    return raw_registry();
}
}

// Inputs and outputs, same shape and same lazy registration.
namespace {
template <class Def>
std::map<std::string, Def, std::less<>>& raw_io() {
    static std::map<std::string, Def, std::less<>> r;
    return r;
}
// One guard for BOTH registries: a per-template static would call
// register_builtin_io() once for inputs and again for outputs.
void ensure_builtin_io() {
    struct init { init() { register_builtin_io(); } };
    static const init once;
    (void)once;
}
template <class Def>
std::map<std::string, Def, std::less<>>& io_registry() {
    ensure_builtin_io();
    return raw_io<Def>();
}
}

void register_input(input_def def) {
    raw_io<input_def>().emplace(std::string(def.kind), std::move(def));
}
void register_output(output_def def) {
    raw_io<output_def>().emplace(std::string(def.kind), std::move(def));
}
// Component names the reference uses for something swordfish serves under
// another name. The reference ships TWO Kafka connectors -- `kafka` (stable,
// broker list `addresses`) and `kafka_franz` (beta, `seed_brokers`) -- and
// swordfish implements the franz-go shape under the name `kafka`, accepting
// `addresses` as a legacy alias for the broker list. Resolving `kafka_franz`
// here closes the other direction, so a config written for either of the
// reference's Kafka components runs unchanged.
//
// `kafka` is the preferred spelling, and the one every diagnostic and every
// piece of documentation uses.
std::string_view canonical_kind(std::string_view kind) {
    if (kind == "kafka_franz") return "kafka";
    return kind;
}

const input_def* find_input(std::string_view kind) {
    auto& r = io_registry<input_def>();
    auto it = r.find(canonical_kind(kind));
    return it == r.end() ? nullptr : &it->second;
}
const output_def* find_output(std::string_view kind) {
    auto& r = io_registry<output_def>();
    auto it = r.find(canonical_kind(kind));
    return it == r.end() ? nullptr : &it->second;
}
std::vector<std::string_view> input_kinds() {
    std::vector<std::string_view> out;
    for (const auto& [k, v] : io_registry<input_def>()) out.push_back(v.kind);
    return out;
}
std::vector<std::string_view> output_kinds() {
    std::vector<std::string_view> out;
    for (const auto& [k, v] : io_registry<output_def>()) out.push_back(v.kind);
    return out;
}

// The built-in inputs and outputs (stdin, file, generate, stdout, drop) are
// still constructed directly by build_stream; those are not registered here.
// Connectors that live in their OWN libraries register themselves --
// sf::kafka::register_components() -- because swordfish_rt must not depend on
// them, and because a static library's registrations are dropped by the linker
// unless something references them.
//
// The HTTP connectors are different: Seastar already provides their transport,
// so they sit in swordfish_rt itself. The call below is both their registration
// and the link-time reference that keeps their object file from being dropped.
void register_builtin_io() {
    http::register_components();
    sock::register_components();
}

void register_processor(processor_def def) {
    raw_registry().emplace(std::string(def.kind), std::move(def));
}

const processor_def* find_processor(std::string_view kind) {
    auto it = registry().find(kind);
    return it == registry().end() ? nullptr : &it->second;
}

std::vector<std::string_view> processor_kinds() {
    std::vector<std::string_view> out;
    for (const auto& [k, v] : registry()) out.push_back(v.kind);
    return out;
}

} // namespace sf
