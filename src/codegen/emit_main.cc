// pipeline_spec -> main.cc. The other consumer of the shared config tree.
//
// What is generated: the wiring, the config literals, and one function per
// transform. What is NOT generated: the components themselves. `generate`,
// `mapping`, `switch` and the rest are the same classes the interpreter uses --
// only the transform they are handed differs.
#include "swordfish/codegen/emit_main.hh"
#include "swordfish/blobl/parse.hh"
#include "swordfish/blobl/emit.hh"
#include "swordfish/components/registry.hh"

#include <algorithm>
#include <sstream>
#include <stdexcept>

namespace sf::codegen {

namespace {

using sf::cfg::cxx_string_literal;

// Walks the tree assigning an id to every transform, so the emitted translation
// units and the references in main.cc agree.
class assigner {
public:
    explicit assigner(const std::string& config_path) : _config_path(config_path) {}

    struct unit { std::string name; std::string source; };

    // One TU per transform.
    std::vector<unit> units;

    std::string add_bloblang(const std::string& src) {
        const std::string name = "blobl_" + std::to_string(units.size());
        auto m = blobl::parse_mapping(src);
        units.push_back({name, blobl::emit_cpp(m, {.function_name = name})});
        return name;
    }
    std::string add_query(const std::string& src) {
        const std::string name = "blobl_" + std::to_string(units.size());
        auto m = blobl::parse_query(src);
        units.push_back({name, blobl::emit_cpp(m, {.function_name = name})});
        return name;
    }
    std::string add_cpp(const component_config& cc) {
        const std::string name = "blobl_" + std::to_string(units.size());
        // The cpp: body and its includes come from the registered config.
        const auto& c = *static_cast<const sf::proc_cpp_view*>(cc.cfg.get());
        blobl::cpp_block b{
            .body = c.body, .includes = c.includes,
            .source_file = _config_path, .source_line = cc.where.line + 1,
        };
        units.push_back({name, blobl::emit_cpp(b, {.function_name = name})});
        return name;
    }

private:
    std::string _config_path;
};

// Emission walks the registry too: a component's own entry knows how to render
// itself, so this function never changes when one is added.
std::string emit_processor(const component_config& cc, assigner& a, int indent,
                           const std::string& path = {});

std::string emit_processor_list(const std::vector<component_config>& list,
                                assigner& a, int indent,
                                const std::string& path_prefix = {}) {
    const std::string pad(indent * 4, ' ');
    std::string s = "[]{\n" + pad + "    std::vector<sf::processor_ptr> v;\n";
    for (size_t i = 0; i < list.size(); ++i)
        s += pad + "    v.push_back(" +
             emit_processor(list[i], a, indent + 1,
                            path_prefix.empty() ? std::string()
                                                : path_prefix + "." + std::to_string(i)) +
             ");\n";
    s += pad + "    return v;\n" + pad + "}()";
    return s;
}

std::string emit_processor(const component_config& cc, assigner& a, int indent,
                           const std::string& path) {
    const processor_def* def = find_processor(cc.kind);
    if (!def)
        throw std::runtime_error("processor '" + cc.kind + "' cannot be compiled");

    emit_env env;
    env.indent = indent;
    env.path = path;
    env.fn_for = [&a, &cc](const cfg::bloblang& b) {
        // An empty source marks a `cpp:` body, which the assigner renders from
        // the component config rather than from Bloblang text.
        if (b.source.empty() && cc.kind == "cpp") return a.add_cpp(cc);
        return b.is_query ? a.add_query(b.source) : a.add_bloblang(b.source);
    };
    env.list_for = [&a, path, kind = cc.kind](const std::vector<component_config>& l, int ind) {
        return emit_processor_list(l, a, ind,
                                   path.empty() ? std::string() : path + "." + kind);
    };
    return def->emit(cc, env);
}

// One output, recursively -- the emitted counterpart of build_output_factory,
// and it has to stay in step with it: the pipeline-level differential runs each
// config both ways and requires byte-identical output, so a composite that
// compiled to a different shape than it interprets to fails that gate.
std::string emit_output(const output_spec& out, assigner& a, int indent,
                        std::vector<std::string>& includes, bool strict_errors) {
    const std::string pad(static_cast<size_t>(indent) * 4, ' ');
    std::string self;

    const bool retry_children =
        out.kind == "broker" &&
        (out.pattern == "fan_out" || out.pattern == "fan_out_sequential");
    auto child = [&](const output_spec& c) {
        std::string e = emit_output(c, a, indent + 1, includes, strict_errors);
        return retry_children ? "sf::make_retry_output(" + e + ")" : e;
    };

    if (out.comp) {
        const output_def* def = find_output(out.kind);
        if (!def || !def->emit)
            throw std::runtime_error("output `" + out.kind +
                                     "` cannot be compiled; use `swordfish run`");
        self = def->emit(*out.comp);
        if (!def->header.empty()) includes.emplace_back(def->header);
    } else if (out.kind == "broker" || out.kind == "fallback") {
        std::vector<std::string> kids;
        const uint64_t copies = out.kind == "broker" ? std::max<uint64_t>(out.copies, 1) : 1;
        for (uint64_t c = 0; c < copies; ++c)
            for (const auto& k : out.children) kids.push_back(child(k));
        if (out.kind == "broker" && kids.size() == 1) {
            self = kids[0];
        } else {
            self = "[]{\n" + pad + "    std::vector<sf::output_ptr> v;\n";
            for (const auto& k : kids) self += pad + "    v.push_back(" + k + ");\n";
            self += pad + "    return " +
                    (out.kind == "broker"
                         ? "sf::make_broker_output(std::move(v), " +
                               cxx_string_literal(out.pattern) + ")"
                         : "sf::make_fallback_output(std::move(v))") +
                    ";\n" + pad + "}()";
        }
    } else if (out.kind == "switch") {
        self = "[]{\n" + pad + "    std::vector<sf::switch_output_case> v;\n";
        for (const auto& c : out.children) {
            std::string e = emit_output(c, a, indent + 1, includes, strict_errors);
            if (out.retry_until_success) e = "sf::make_retry_output(" + e + ")";
            const std::string chk = c.check.source.empty()
                ? "sf::transform_fn{}"
                : "&sf::gen::" + a.add_query(c.check.source);
            self += pad + "    v.push_back({" + chk + ", " + e + ", " +
                    (c.continue_ ? "true" : "false") + "});\n";
        }
        self += pad + "    return sf::make_switch_output(std::move(v), " +
                std::string(out.strict_mode ? "true" : "false") + ");\n" + pad + "}()";
    } else if (out.kind == "reject") {
        self = "sf::make_reject_output(&sf::gen::" +
               a.add_query(cfg::interpolation_to_query(out.message)) + ")";
    } else if (out.kind == "drop") {
        self = "sf::make_drop_output()";
    } else if (out.kind == "file") {
        // The path interpolates, so the emitter has to carry the compiled query
        // alongside the literal -- exactly as the interpreter does. Emitting the
        // literal alone was the bug: `swordfish run` and `swordfish build` would
        // now disagree about how many files the pipeline writes.
        self = "sf::make_file_output(" + cxx_string_literal(out.path) + ", " +
               (cfg::is_interpolated(out.path)
                    ? "&sf::gen::" + a.add_query(cfg::interpolation_to_query(out.path))
                    : std::string("sf::transform_fn{}")) + ")";
    } else {
        self = "sf::make_stdout_output()";
    }

    if (!out.batching.is_noop() || !out.batching_processors.empty()) {
        const std::string check = out.batching.check.source.empty()
            ? "sf::transform_fn{}"
            : "&sf::gen::" + a.add_query(out.batching.check.source);
        self = "sf::make_batched_output(" + self + ", " +
               std::to_string(out.batching.count) + ", " +
               std::to_string(out.batching.byte_size) + ", std::chrono::milliseconds{" +
               std::to_string(std::chrono::duration_cast<std::chrono::milliseconds>(
                   out.batching.period.ns).count()) + "}, " + check + ", " +
               emit_processor_list(out.batching_processors, a, indent + 1) + ", " +
               (strict_errors ? "true" : "false") + ")";
    }
    if (!out.processors.empty())
        self = "sf::make_processed_output(" + self + ", " +
               emit_processor_list(out.processors, a, indent + 1) + ", " +
               (strict_errors ? "true" : "false") + ")";
    return self;
}

} // namespace

// One input, recursively. It mirrors build_input_factory() case for case, and
// that correspondence is the point: this function did not handle `broker` or
// `sequence` at all, so a config using either fell through to the file branch
// and compiled to `make_files_input({}, ...)` -- a binary that read nothing and
// said nothing about it, while `swordfish run` on the same config worked. The
// pipeline differential could not see it because its corpus had no nested input.
std::string emit_input(const input_spec& in, assigner& a,
                       std::vector<std::string>& extra_includes) {
    std::string self;
    if (in.comp) {
        // A registered connector emits its own construction. Falling through to
        // the built-ins here would compile a binary that reads something else
        // entirely and say nothing about it.
        const input_def* def = find_input(in.kind);
        if (!def || !def->emit)
            throw std::runtime_error("input `" + in.kind +
                                     "` cannot be compiled; use `swordfish run`");
        self = def->emit(*in.comp);
        if (!def->header.empty()) extra_includes.emplace_back(def->header);
    } else if (in.kind == "broker" || in.kind == "sequence") {
        const uint64_t copies = in.kind == "broker" ? std::max<uint64_t>(in.copies, 1) : 1;
        // One push_back per child: `copies` repeats the whole child list, which
        // is how a broker scales one source, and each repeat is its own input.
        std::string kids;
        for (uint64_t c = 0; c < copies; ++c)
            for (const auto& child : in.children)
                kids += "            v.push_back(" +
                        emit_input(child, a, extra_includes) + ");\n";
        self = "[&]{\n"
               "            std::vector<sf::input_ptr> v;\n" + kids +
               "            return " +
               (in.kind == "broker" ? "sf::make_broker_input(std::move(v))"
                                    : "sf::make_sequence_input(std::move(v))") +
               ";\n        }()";
    } else if (in.kind == "generate") {
        const std::string fn = a.add_bloblang(in.mapping);
        const std::string iv = "std::chrono::milliseconds{" +
                               std::to_string(in.interval.count()) + "}";
        if (in.count == 0) {
            self = "sf::make_generate_input(&sf::gen::" + fn + ", UINT64_MAX, " + iv + ")";
        } else {
            // `count` is the pipeline-wide total, divided across shards. The
            // lambda has no captures, so smp::count is read inside it.
            const std::string n = std::to_string(in.count);
            self = "[&]{\n"
                   "            const unsigned shards = seastar::this_smp().shard_count();\n"
                   "            const unsigned me = seastar::this_shard_id();\n"
                   "            const uint64_t mine = " + n + " / shards + (me < " + n +
                   " % shards ? 1 : 0);\n"
                   "            return sf::make_generate_input(&sf::gen::" + fn +
                   ", mine, " + iv + ");\n"
                   "        }()";
        }
    } else {
        // The scanner SPEC is emitted, not a constructor call, and the paths
        // are emitted whole: both go to the same runtime functions the
        // interpreter calls. Rendering the swordfish-only singular `path` here
        // instead meant a config using the reference's `paths:` compiled to a
        // binary that opened "".
        const std::string sc = emit_scanner_spec(in.scanner);
        std::string plist;
        for (const auto& p : in.paths)
            plist += (plist.empty() ? "" : ", ") + cxx_string_literal(p);
        const std::string mk = in.kind == "stdin"
            ? "sf::make_stdin_input(sf::make_scanner(" + sc + "))"
            : "sf::make_files_input({" + plist + "}, " + sc + ")";
        // Singleton source: shard 0 only.
        self = "(seastar::this_shard_id() == 0)\n            ? " + mk +
               "\n            : sf::make_generate_input(&sf::gen::" +
               a.add_bloblang("root = {}") + ", 0, std::chrono::milliseconds{0})";
    }
    // Matches build_input_factory, which applies its `replay` helper on the
    // GENERATE and FILE/STDIN branches only -- a registered connector wraps
    // itself inside its own factory, and broker/sequence are wrapped by nothing
    // because the reference has no `auto_replay_nacks` on either (its lint says
    // "field auto_replay_nacks not recognised" on input.broker).
    //
    // This used to read `if (!in.comp && ...)`, which caught broker and sequence
    // too. `input_spec::auto_replay_nacks` defaults to true and is only ever
    // READ from the config for those three kinds, so every compiled broker got an
    // auto-retry layer the interpreter never builds: a nacking output made the
    // compiled binary replay for ever where `swordfish run` exited. It also
    // reordered the wrappers, since the buffer and the broker's own batching wrap
    // whatever this produced. The third bug of this shape in this function.
    const bool wraps_replay = in.kind == "generate" || in.kind == "file" ||
                              in.kind == "stdin";
    if (!in.comp && wraps_replay && in.auto_replay_nacks)
        self = "sf::make_auto_retry_input(" + self + ")";

    // `input.broker`'s batching wraps the assembled broker, as it does there.
    if (in.kind == "broker" &&
        !(in.batching.is_noop() && in.batching_processors.empty())) {
        const std::string check = in.batching.check.source.empty()
            ? "sf::transform_fn{}"
            : "&sf::gen::" + a.add_query(in.batching.check.source);
        self = "sf::make_batched_input(" + self + ", sf::batch_policy(" +
               std::to_string(in.batching.count) + ", " +
               std::to_string(in.batching.byte_size) + ", std::chrono::milliseconds{" +
               std::to_string(std::chrono::duration_cast<std::chrono::milliseconds>(
                                  in.batching.period.ns).count()) + "}, " +
               check + "), " + emit_processor_list(in.batching_processors, a, 2) + ")";
    }

    // `processors:` beside the kind, OUTSIDE auto_replay_nacks and outside the
    // broker's batching -- the same order build_input_factory applies them in.
    // The two backends have disagreed about this function's wrapper order three
    // times already; the ordering here is the interpreter's, read off
    // build_input_factory / build_input_kind rather than reasoned about afresh.
    if (!in.processors.empty())
        self = "sf::make_processed_input(" + self + ", " +
               emit_processor_list(in.processors, a, 2) + ")";
    return self;
}

// Every member of `stream_config` is written out by emit_program below, one at a
// time, while the interpreted path copies the struct whole. Adding a member and
// forgetting this file would give a compiled binary that silently ignores that
// setting -- so adding one breaks the build here instead, and the fix is to emit
// it rather than to update the number.
static_assert(sizeof(stream_config) ==
                  sizeof(size_t) + 2 * sizeof(std::chrono::milliseconds) +
                      sizeof(bool) + 7 /* padding after the bool */,
              "stream_config gained or lost a member: emit_program writes each "
              "one by hand and must be updated too");

emitted emit_program(const pipeline_spec& ps, const std::string& config_path) {
    assigner a(config_path);
    emitted out;

    // Processors first, so ids are allocated in the order a reader meets them.
    std::string procs = emit_processor_list(ps.processors, a, 2, "pipeline.processors");

    // Extra headers a registered connector's emitted expression needs. Kept in
    // insertion order so the generated file is stable across builds.
    std::vector<std::string> extra_includes;

    std::string input = emit_input(ps.input, a, extra_includes);

    // The buffer wraps whatever the input turned out to be, exactly as
    // build_stream_spec wraps it -- the two must agree about when the source is
    // acknowledged, or a compiled pipeline would have different delivery
    // guarantees from the interpreted one running the same config.
    if (ps.buffer.kind == "memory") {
        const auto& bs = ps.buffer;
        const std::string check = bs.batching.check.source.empty()
            ? "sf::transform_fn{}"
            : "&sf::gen::" + a.add_query(bs.batching.check.source);
        input = "sf::make_buffered_input(" + input + ", " +
                std::to_string(bs.limit) + ", " +
                (bs.batch_enabled ? "true" : "false") + ", sf::batch_policy(" +
                std::to_string(bs.batching.count) + ", " +
                std::to_string(bs.batching.byte_size) + ", std::chrono::milliseconds{" +
                std::to_string(std::chrono::duration_cast<std::chrono::milliseconds>(
                                   bs.batching.period.ns).count()) + "}, " +
                check + "), " + emit_processor_list(bs.batching_processors, a, 2) + ")";
    }

    std::string output = emit_output(ps.output, a, 2, extra_includes,
                                     ps.config.strict_errors);

    std::ostringstream os;
    os << "// Generated by swordfish from " << config_path << ". Do not edit.\n"
          "#include <swordfish/runtime/stream.hh>\n"
          "#include <swordfish/runtime/components.hh>\n"
          "#include <swordfish/runtime.hh>\n"
          "#include <seastar/core/smp.hh>\n";
    // De-duplicated: input and output may both be the same connector.
    std::vector<std::string> seen;
    for (const auto& inc : extra_includes)
        if (std::find(seen.begin(), seen.end(), inc) == seen.end()) {
            seen.push_back(inc);
            os << "#include <" << inc << ">\n";
        }
    os << "\nnamespace sf::gen {\n";
    for (const auto& u : a.units)
        os << "sf::value " << u.name << "(sf::exec_ctx&);\n";
    os << "\nsf::stream_spec build_stream() {\n"
          "    sf::stream_spec spec;\n"
          "    spec.make_input = []() -> sf::input_ptr {\n"
          "        return " << input << ";\n"
          "    };\n"
          "    spec.make_processors = []() -> std::vector<sf::processor_ptr> {\n"
          "        return " << procs << ";\n"
          "    };\n"
          "    spec.make_output = []() -> sf::output_ptr {\n"
          "        return " << output << ";\n"
          "    };\n"
       << "    spec.http.enabled = " << (ps.http.enabled ? "true" : "false") << ";\n"
          // Through cxx_string_literal, not inline quotes: an address or root
          // path containing a quote or a backslash otherwise emitted C++ that
          // did not compile, or worse, compiled to something else.
       << "    spec.http.address = " << cxx_string_literal(ps.http.address) << ";\n"
          "    spec.http.root_path = " << cxx_string_literal(ps.http.root_path) << ";\n"
          // The engine settings go into the binary too. A compiled pipeline
          // that ignored `error_handling.strict` while the interpreted one
          // honoured it would be the two modes disagreeing about what a config
          // means, which is the one thing § 4.3 does not allow.
          //
          // EVERY member, and the static_assert below is what keeps it that way.
          // The interpreted path copies the whole struct in one statement
          // (`spec.config = ps.config`); this one writes the members out, and it
          // had already missed `queue_depth`. That cost nothing only because
          // nothing sets it from a config yet -- the same shape, in emit_input,
          // produced three separate miscompiles that DID.
       << "    spec.config.queue_depth = " << ps.config.queue_depth << ";\n"
          "    spec.config.shutdown_timeout = std::chrono::milliseconds{"
       << ps.config.shutdown_timeout.count() << "};\n"
          "    spec.config.shutdown_delay = std::chrono::milliseconds{"
       << ps.config.shutdown_delay.count() << "};\n"
          "    spec.config.strict_errors = "
       << (ps.config.strict_errors ? "true" : "false") << ";\n"
       << "    return spec;\n"
          "}\n\n"
          "} // namespace sf::gen\n\n"
          "int main(int argc, char** argv) {\n"
          "    return sf::run_generated(argc, argv, &sf::gen::build_stream);\n"
          "}\n";

    out.main_cc = os.str();
    for (auto& u : a.units) out.units.push_back({u.name + ".cc", u.source});
    return out;
}

} // namespace sf::codegen
