// sfconfig — the config CLI surface.
//
//   sfconfig lint <file.yaml>     parse + validate, reporting unimplemented components
//   sfconfig list [component]     documentation generated from spec_of
//   sfconfig emit <file.yaml>     the C++ literal `swordfish build` would embed
#include "swordfish/components/builtin_docs.hh"
#include "swordfish/config/spec.hh"
#include "swordfish/config/template.hh"
#include "swordfish/runtime/pipeline_spec.hh"
#include "swordfish/components/transform.hh"
#include "swordfish/blobl/parse.hh"
#include "swordfish/blobl/emit.hh"

#include <algorithm>
#include <functional>
#include <fstream>
#include <iostream>
#include <sstream>
#include "swordfish/kafka/components.hh"

#include "swordfish/cli/commands.hh"

using namespace sf::cfg;

namespace {

// Component support is decided by parse_pipeline -- the same code `run` and
// `build` use. Duplicating the lists here meant lint could quietly disagree with
// what the runtime actually accepts.
//
// parse_pipeline THROWS on the first problem, which is right for `run` and
// `build` -- neither should act on a half-understood config -- but it meant lint
// could report at most one diagnostic per file. A config with an unrecognised
// field in the input, another in a processor and a third in the output printed
// one line and exited 1; the reference prints all three, and the user fixes one,
// re-runs, and finds the next.
//
// Rather than turn all 112 throw sites into collect-and-continue -- which would
// change `run` and `build` too -- lint parses the document SEVERAL TIMES, each
// time with one section real and the others replaced by a minimal valid
// stand-in. Each parse reports its own first error, and the errors are merged.
// The cost falls only on configs that are already broken: a clean config takes
// exactly one parse, as before.
//
// This finds one error per SECTION, not per field. Two mistakes in the same
// processor list still surface one at a time, which is a smaller gap than the
// one it replaces and needs the parser change to close properly.

// A stand-in that parses cleanly and refers to nothing.
// The document with `section` taken from the real config and the other two
// pipeline stages replaced. Everything else -- the resource blocks especially --
// is carried over unchanged, so a `cache:` or `rate_limit:` label still
// resolves and a missing one is still reported.
//
// Shared with `swordfish streams`, which uses the no-section form to validate a
// ROOT config that has no input or output by design.
ynode probe_doc(const ynode& root, std::string_view section) {
    return sf::with_trivial_io(root, section);
}

void run_one(const ynode& doc, const ynode& root, lints& ls) {
    try {
        (void)sf::parse_pipeline(doc);
    } catch (const sf::spec_error& e) {
        ls.push_back({lint_level::error, e.where, "", e.what()});
    } catch (const std::exception& e) {
        ls.push_back({lint_level::error, root.pos, "", e.what()});
    }
}

void check_components(const ynode& root, lints& ls) {
    // The whole document first. A config that parses has nothing more to say,
    // and this is the only path a valid config takes.
    lints whole;
    run_one(root, root, whole);
    if (whole.empty()) return;

    // It failed, so each section is asked separately. The whole-document result
    // is included because it is the one that sees the top-level schema and the
    // document-level blocks -- `http`, `shutdown_timeout`, an unrecognised root
    // key -- which no section probe covers on its own.
    lints all = std::move(whole);
    for (std::string_view section : {"input", "pipeline", "output"})
        if (root.find(section))
            run_one(probe_doc(root, section), root, all);

    // Merged by POSITION and text: a resource block is present in every probe,
    // so an error in one would otherwise be reported four times. Ordered by
    // position so the output reads down the file, as the reference's does.
    std::stable_sort(all.begin(), all.end(), [](const lint& a, const lint& b) {
        if (a.where.line != b.where.line) return a.where.line < b.where.line;
        return a.where.col < b.where.col;
    });
    for (const auto& l : all) {
        const bool seen = std::any_of(ls.begin(), ls.end(), [&](const lint& p) {
            return p.where.line == l.where.line && p.where.col == l.where.col &&
                   p.message == l.message;
        });
        if (!seen) ls.push_back(l);
    }
}

int lint_file(const std::string& path) {
    ynode root;
    try {
        root = load_config(path);
    } catch (const yaml_error& e) {
        std::cout << path << ":" << e.where.line << ": error: " << e.what() << "\n";
        return 1;
    } catch (const std::exception& e) {
        std::cout << path << ": error: " << e.what() << "\n";
        return 1;
    }

    // Template documents have their own root schema (benthos-main/internal/template),
    // so linting them against the stream schema would produce a page of nonsense.
    const bool is_template = root.is_mapping() && root.find("name") && root.find("type") &&
                             root.find("mapping");

    lints ls;
    if (is_template) {
        ls.push_back({lint_level::warning, root.pos, "",
                      "this is a component template; swordfish does not implement templates yet"});
    } else if (root.is_mapping()) {
        // The root schema is checked by parse_pipeline, which check_components
        // calls -- deliberately NOT a second copy here. There used to be one,
        // and only here, so `run` and `build` accepted a top-level typo that
        // lint refused: a misspelt `pipeline` ran with no processors and
        // compiled into a shipped binary.
        check_components(root, ls);
    } else if (!root.is_null()) {
        ls.push_back({lint_level::error, root.pos, "", "expected a mapping at the document root"});
    }

    for (const auto& l : ls) std::cout << path << ":" << l.render() << "\n";
    return ls.empty() ? 0 : 1;
}

} // namespace

static int sf_cli_main(int argc, char** argv) {
    // Connectors in their own libraries register explicitly: a static
    // library's registrations are dropped by the linker otherwise.
    sf::kafka::register_components();
    if (argc < 2) { std::cerr << "usage: " << argv[0]
                              << " [-t <templates>] lint|list|echo|create|transforms|emit ...\n";
                       return 2; }

    // `-t` may appear anywhere among the arguments, and is removed before the
    // subcommand is read. Templates have to be registered before any config is
    // parsed, because a template usage is expanded at the point its kind is
    // looked up.
    std::vector<char*> rest;
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        if ((a == "-t" || a == "--templates") && i + 1 < argc) {
            try {
                sf::tmpl::load_templates(argv[++i]);
            } catch (const std::exception& e) {
                std::cerr << "templates: " << e.what() << "\n";
                return 1;
            }
            continue;
        }
        rest.push_back(argv[i]);
    }
    if (rest.empty()) { std::cerr << "usage: " << argv[0]
                                  << " [-t <templates>] lint|list|transforms|emit ...\n";
                        return 2; }
    argc = static_cast<int>(rest.size()) + 1;
    static std::vector<char*> shifted;
    shifted.clear();
    shifted.push_back(argv[0]);
    for (char* a : rest) shifted.push_back(a);
    argv = shifted.data();
    const std::string cmd = argv[1];

    // `template lint <paths>` -- parse each template, then run its own `tests:`.
    // A template is a component definition, so "does it parse" is only half the
    // question: the other half is whether the config it PRODUCES is one
    // swordfish would accept, and that can only be answered by expanding it.
    if (cmd == "template") {
        if (argc < 3 || std::string(argv[2]) != "lint") {
            std::cerr << "usage: " << argv[0] << " template lint <files...>\n";
            return 2;
        }
        if (argc < 4) { std::cerr << "swordfish template lint: no files given\n"; return 2; }
        int bad_files = 0;
        for (int i = 3; i < argc; ++i) {
            sf::tmpl::clear();          // each file judged on its own
            try {
                sf::tmpl::load_templates(argv[i]);
            } catch (const sf::spec_error& e) {
                std::cerr << argv[i] << ":line " << (e.where.line + 1) << ": error: "
                          << e.what() << "\n";
                ++bad_files;
                continue;
            } catch (const std::exception& e) {
                std::cerr << argv[i] << ": error: " << e.what() << "\n";
                ++bad_files;
                continue;
            }
            for (const auto& def : sf::tmpl::loaded()) {
                int failed = 0;
                for (const auto& t : def.tests) {
                    const std::string what =
                        def.source + ": " + def.name +
                        (t.name.empty() ? "" : " / " + t.name);
                    try {
                        // The usage as a config would write it: the template's
                        // name, then the test's config underneath.
                        sf::cfg::ynode usage;
                        usage.type = sf::cfg::ynode_type::mapping;
                        usage.pos = t.pos;
                        usage.map.push_back({def.name, t.config});
                        const sf::cfg::ynode got = sf::tmpl::expand(def, usage, t.label);

                        // Linted by building a document around it and running
                        // the real parser -- the same one `swordfish lint` uses,
                        // so a template cannot pass here and fail there.
                        sf::cfg::ynode doc;
                        doc.type = sf::cfg::ynode_type::mapping;
                        std::string section;
                        switch (def.type) {
                        case sf::tmpl::comp_type::input:
                            section = "input";
                            doc.map.push_back({"input", got});
                            break;
                        case sf::tmpl::comp_type::output:
                            section = "output";
                            doc.map.push_back({"output", got});
                            break;
                        case sf::tmpl::comp_type::processor: {
                            section = "pipeline";
                            sf::cfg::ynode procs;
                            procs.type = sf::cfg::ynode_type::sequence;
                            procs.seq.push_back(got);
                            sf::cfg::ynode pl;
                            pl.type = sf::cfg::ynode_type::mapping;
                            pl.map.push_back({"processors", procs});
                            doc.map.push_back({"pipeline", pl});
                            break;
                        }
                        case sf::tmpl::comp_type::cache:
                        case sf::tmpl::comp_type::rate_limit: {
                            // A resource needs a label to be addressable, so the
                            // test's own label is used and a default supplied.
                            sf::cfg::ynode res = got;
                            if (res.find("label") == nullptr) {
                                sf::cfg::ynode l;
                                l.type = sf::cfg::ynode_type::scalar;
                                l.scalar = t.label.empty() ? "t" : t.label;
                                l.quoted = true;
                                res.map.insert(res.map.begin(), {"label", l});
                            }
                            sf::cfg::ynode list;
                            list.type = sf::cfg::ynode_type::sequence;
                            list.seq.push_back(res);
                            doc.map.push_back(
                                {def.type == sf::tmpl::comp_type::cache ? "cache_resources"
                                                                        : "rate_limit_resources",
                                 list});
                            break;
                        }
                        }
                        (void)sf::parse_pipeline(sf::with_trivial_io(doc, section));

                        if (t.expected) {
                            // Compared as JSON, which sorts an object's keys, so
                            // a test does not have to write its expectation in
                            // the order the mapping happened to build it.
                            const std::string a = sf::cfg::node_to_value(got).to_json();
                            const std::string b = sf::cfg::node_to_value(*t.expected).to_json();
                            if (a != b) {
                                std::cerr << what << ": expected " << b << "\n"
                                          << std::string(what.size(), ' ')
                                          << "  but got " << a << "\n";
                                ++failed;
                                continue;
                            }
                        }
                        std::cout << "ok   " << what << "\n";
                    } catch (const sf::spec_error& e) {
                        std::cerr << what << ": line " << (e.where.line + 1) << ": "
                                  << e.what() << "\n";
                        ++failed;
                    } catch (const std::exception& e) {
                        std::cerr << what << ": " << e.what() << "\n";
                        ++failed;
                    }
                }
                if (def.tests.empty())
                    std::cout << "ok   " << def.source << ": " << def.name
                              << " (no tests)\n";
                if (failed) ++bad_files;
            }
        }
        return bad_files ? 1 : 0;
    }

    // `echo <config>` -- the config after `${VAR}` has been resolved, printed
    // back as YAML.
    //
    // Two differences from the reference, both deliberate and both stated in
    // README.md. It does NOT fill in defaults: the reference prints its entire
    // schema, `redpanda` block and all, and swordfish's schema is a different
    // one -- printing the reference's defaults would describe a program that is
    // not running, and printing only the subset swordfish models would be worse
    // still, because a reader could not tell which fields were omitted because
    // they are defaulted and which because they are unimplemented. And the
    // output is block style throughout, where the reference preserves whatever
    // style the input used.
    //
    // What it DOES match is the part that makes the command useful: `${VAR}` is
    // resolved, and a template usage is left as written (verified against
    // redpanda-connect 4.107.2, which does not expand them either).
    if (cmd == "echo") {
        if (argc < 3) {
            std::cerr << "usage: " << argv[0] << " " << cmd << " <config.yaml>\n";
            return 2;
        }
        int bad = 0;
        for (int i = 2; i < argc; ++i) {
            try {
                const ynode doc = load_config(argv[i]);
                // Parsed first, so `echo` refuses a config `lint` refuses
                // rather than pretty-printing something that cannot run. The
                // reference does the same -- its echo fails on a bad config.
                (void)sf::parse_pipeline(doc);
                std::cout << sf::cfg::to_yaml(doc);
            } catch (const sf::spec_error& e) {
                std::cerr << argv[i] << ":line " << e.where.line << ": error: " << e.what() << "\n";
                ++bad;
            } catch (const std::exception& e) {
                std::cerr << argv[i] << ": error: " << e.what() << "\n";
                ++bad;
            }
        }
        return bad ? 1 : 0;
    }

    // `roundtrip <config>...` -- a development check, not a documented command:
    // every file is parsed, written back out with to_yaml(), re-parsed, and the
    // two trees compared. It exists because the faithfulness of the writer is
    // the whole basis of `echo` and of the config the streams API reports, and
    // the cheapest way to be sure of it is to run it over every real config in
    // the tree rather than over examples chosen by whoever wrote it.
    if (cmd == "roundtrip") {
        // Reports WHERE two trees differ, not merely that they do. A serialiser
        // bug is a one-node problem inside a hundred-node document, and
        // "DIFFERS" alone sends the reader back to bisecting the file by hand.
        const std::function<std::string(const ynode&, const ynode&, const std::string&)> where =
            [&](const ynode& a, const ynode& b, const std::string& path) -> std::string {
            if (a.type != b.type)
                return path + ": type " + std::to_string(static_cast<int>(a.type)) +
                       " became " + std::to_string(static_cast<int>(b.type));
            switch (a.type) {
            case ynode_type::null: return "";
            case ynode_type::scalar:
                if (a.scalar != b.scalar)
                    return path + ": '" + a.scalar + "' became '" + b.scalar + "'";
                if (a.quoted != b.quoted)
                    return path + ": quoted " + (a.quoted ? "true" : "false") +
                           " became " + (b.quoted ? "true" : "false") + " ('" + a.scalar + "')";
                return "";
            case ynode_type::sequence: {
                if (a.seq.size() != b.seq.size())
                    return path + ": " + std::to_string(a.seq.size()) + " items became " +
                           std::to_string(b.seq.size());
                for (size_t i = 0; i < a.seq.size(); ++i) {
                    const auto w = where(a.seq[i], b.seq[i], path + "[" + std::to_string(i) + "]");
                    if (!w.empty()) return w;
                }
                return "";
            }
            case ynode_type::mapping: {
                if (a.map.size() != b.map.size())
                    return path + ": " + std::to_string(a.map.size()) + " keys became " +
                           std::to_string(b.map.size());
                for (size_t i = 0; i < a.map.size(); ++i) {
                    if (a.map[i].key != b.map[i].key)
                        return path + ": key '" + a.map[i].key + "' became '" +
                               b.map[i].key + "'";
                    const auto w = where(a.map[i].value, b.map[i].value,
                                         path + "." + a.map[i].key);
                    if (!w.empty()) return w;
                }
                return "";
            }
            }
            return "";
        };
        int bad = 0, n = 0;
        for (int i = 2; i < argc; ++i) {
            try {
                const ynode a = load_config(argv[i]);
                const ynode b = sf::cfg::parse_yaml(sf::cfg::to_yaml(a));
                ++n;
                if (!sf::cfg::same_shape(a, b)) {
                    std::cerr << "DIFFERS " << argv[i] << "\n         "
                              << where(a, b, "root") << "\n";
                    ++bad;
                }
            } catch (const std::exception& e) {
                // A file that does not parse is not this check's business.
                std::cerr << "skip    " << argv[i] << ": " << e.what() << "\n";
            }
        }
        std::cout << "roundtrip: " << (n - bad) << " of " << n << " identical\n";
        return bad ? 1 : 0;
    }

    // `create [inputs/processors/outputs]` -- a starting config for the named
    // components, every field at its default, required ones marked.
    //
    // The expression is the reference's: three comma-separated lists divided by
    // slashes, `stdin/bloblang,awk/nats`, and an omitted one means the default
    // for that stage. What swordfish emits differs from the reference's in one
    // stated way -- it does not print the engine blocks (`http`, `logger`,
    // `metrics`, the rest), because those are swordfish's own and defaulting
    // them into every scaffold would put a page of configuration in front of
    // someone who asked for a pipeline. The components' own fields, which is
    // what a scaffold is for, are all there.
    if (cmd == "create") {
        const std::string expr = argc > 2 ? argv[2] : "stdin//stdout";
        if (argc > 3) { std::cerr << "usage: " << argv[0] << " " << cmd
                                  << " [inputs/processors/outputs]\n"; return 2; }

        // Split on slashes, then on commas. An empty section is legal and means
        // "the default for this stage", which is how `stdin//stdout` asks for no
        // processors.
        //
        // A fourth section is refused rather than dropped. The loop below stops
        // after three, so `generate//stdout/extra` used to scaffold a perfectly
        // good pipeline and say nothing about `extra` -- a silent ignore, and
        // the reference rejects it ("more component separators than expected").
        if (std::count(expr.begin(), expr.end(), '/') > 2) {
            std::cerr << "swordfish create: '" << expr
                      << "' has more than three sections; the expression is "
                         "inputs/processors/outputs\n";
            return 1;
        }
        std::vector<std::vector<std::string>> parts(3);
        {
            size_t at = 0;
            for (int sec = 0; sec < 3; ++sec) {
                const size_t slash = expr.find('/', at);
                const std::string chunk =
                    expr.substr(at, slash == std::string::npos ? slash : slash - at);
                size_t cat = 0;
                while (cat <= chunk.size() && !chunk.empty()) {
                    const size_t comma = chunk.find(',', cat);
                    std::string one = chunk.substr(
                        cat, comma == std::string::npos ? comma : comma - cat);
                    // Trimmed, because the reference accepts
                    // `generate / mapping / stdout` and we reported
                    // "'generate ' is not an input" for it -- an expression
                    // written for redpanda-connect has to work here unchanged.
                    const size_t b = one.find_first_not_of(" \t");
                    const size_t e = one.find_last_not_of(" \t");
                    one = (b == std::string::npos) ? std::string()
                                                   : one.substr(b, e - b + 1);
                    if (!one.empty()) parts[sec].push_back(std::move(one));
                    if (comma == std::string::npos) break;
                    cat = comma + 1;
                }
                if (slash == std::string::npos) break;
                at = slash + 1;
            }
        }
        if (parts[0].empty()) parts[0].push_back("stdin");
        if (parts[2].empty()) parts[2].push_back("stdout");

        // Every name is resolved BEFORE anything is printed, so an unknown one
        // is an error rather than half a config followed by a complaint.
        const auto body = [&](const std::string& kind, sf::builtin_class cls,
                              int indent, std::string& out) -> bool {
            if (const auto* b = sf::find_builtin(cls, kind)) {
                out = sf::scaffold_builtin(*b, indent);
                return true;
            }
            if (cls == sf::builtin_class::input) {
                if (const auto* d = sf::find_input(kind)) { out = d->scaffold(indent); return true; }
            } else {
                if (const auto* d = sf::find_output(kind)) { out = d->scaffold(indent); return true; }
            }
            return false;
        };

        std::ostringstream os;
        const auto section = [&](const std::vector<std::string>& kinds,
                                 sf::builtin_class cls, const char* label) -> bool {
            const bool many = kinds.size() > 1;
            os << label << ":\n";
            if (many) {
                // Several components of one stage become a broker, which is what
                // the reference does with `file,http_server/.../...` too.
                os << "  broker:\n    " << (cls == sf::builtin_class::input ? "inputs" : "outputs")
                   << ":\n";
            }
            for (const auto& k : kinds) {
                std::string b;
                if (!body(k, cls, many ? 10 : 4, b)) {
                    std::cerr << "swordfish create: '" << k << "' is not an "
                              << (cls == sf::builtin_class::input ? "input" : "output")
                              << " swordfish implements\n";
                    return false;
                }
                os << (many ? "      - " : "  ") << k << ":";
                // `drop: {}` on one line. Hanging the `{}` on a continuation
                // line is valid YAML and parses the same, but it reads as
                // something half-written.
                if (b.empty()) os << " {}\n";
                else           os << "\n" << b;
            }
            return true;
        };

        if (!section(parts[0], sf::builtin_class::input, "input")) return 1;
        if (!parts[1].empty()) {
            os << "pipeline:\n  processors:\n";
            for (const auto& k : parts[1]) {
                const auto* d = sf::find_processor(k);
                if (d == nullptr) {
                    std::cerr << "swordfish create: '" << k
                              << "' is not a processor swordfish implements\n";
                    return 1;
                }
                const std::string b = d->scaffold(8);
                os << "    - " << k << ":";
                if (b.empty()) os << " {}\n";
                else           os << "\n" << b;
            }
        }
        if (!section(parts[2], sf::builtin_class::output, "output")) return 1;
        std::cout << os.str();
        return 0;
    }

    if (cmd == "list") {
        // Everything the registries know, rather than one hardcoded struct.
        // The previous version documented a config no component used any more,
        // so `list` and `lint` disagreed about what a `kafka` input accepts.
        for (auto kind : sf::input_kinds())
            if (const auto* d = sf::find_input(kind)) std::cout << d->describe();
        for (auto kind : sf::output_kinds())
            if (const auto* d = sf::find_output(kind)) std::cout << d->describe();
        for (auto kind : sf::processor_kinds())
            if (const auto* d = sf::find_processor(kind)) std::cout << d->describe();
        // The built-ins are not in those registries -- stdin, file, generate,
        // stdout, drop, broker, sequence and switch are constructed directly by
        // build_stream -- so nothing here described them and `list` omitted the
        // seven most basic components in the project. The reference lists all of
        // them. They come from the same table the parser takes its field names
        // from, so what is shown is what is accepted.
        for (const auto& b : sf::builtin_components())
            std::cout << sf::describe_builtin(b);
        return 0;
    }
    if (cmd == "lint") {
        int bad = 0;
        for (int i = 2; i < argc; ++i) bad += lint_file(argv[i]) != 0;
        return bad ? 1 : 0;
    }
    // Walk pipeline.processors and emit one C++ function per transform, whether
    // it was written as Bloblang or as C++. Both land in the same shape.
    if (cmd == "transforms" && argc > 2) {
        const std::string path = argv[2];
        auto root = load_config(path);
        const ynode* pl = root.find("pipeline");
        const ynode* ps = pl ? pl->find("processors") : nullptr;
        if (!ps || !ps->is_sequence()) { std::cerr << "no pipeline.processors\n"; return 1; }

        int n = 0;
        for (const auto& proc : ps->seq) {
            auto sk = proc.single_key();
            if (!sk) continue;
            const std::string& kind = sk->first;
            const ynode& body = *sk->second;
            sf::blobl::emit_options opt{.function_name = "blobl_" + std::to_string(n)};

            if (kind == "mapping" || kind == "bloblang" || kind == "mutation") {
                try {
                    auto m = sf::blobl::parse_mapping(body.scalar);
                    std::cout << sf::blobl::emit_cpp(m, opt);
                } catch (const sf::blobl::parse_error& e) {
                    std::cerr << path << ":" << (body.pos.line + e.where.line - 1)
                              << ": error: " << e.what() << "\n";
                    return 1;
                }
            } else if (kind == "cpp") {
                sf::proc::cpp_config c;
                if (body.is_scalar()) {
                    c.body = body.scalar;                 // shorthand: cpp: |
                } else {
                    auto r = from_yaml<sf::proc::cpp_config>(body, "pipeline.processors.cpp");
                    for (const auto& l : r.diagnostics) std::cerr << path << ":" << l.render() << "\n";
                    if (!r.ok()) return 1;
                    c = r.value;
                }
                sf::blobl::cpp_block b{
                    .body = c.body, .includes = c.includes,
                    .source_file = path,
                    // A block scalar's content starts on the line after its key.
                    .source_line = body.pos.line + 1,
                };
                std::cout << sf::blobl::emit_cpp(b, opt);
            } else {
                continue;
            }
            ++n;
            std::cout << "\n";
        }
        return 0;
    }

    if (cmd == "emit" && argc > 2) {
        // The C++ the build driver would generate for this config's input and
        // output, which is the useful thing to look at when a compiled pipeline
        // behaves unexpectedly.
        auto spec = sf::parse_pipeline(load_config(argv[2]));
        if (spec.input.comp) {
            const auto* d = sf::find_input(spec.input.kind);
            if (d && d->emit) std::cout << d->emit(*spec.input.comp) << "\n";
        }
        if (spec.output.comp) {
            const auto* d = sf::find_output(spec.output.kind);
            if (d && d->emit) std::cout << d->emit(*spec.output.comp) << "\n";
        }
        return 0;
    }
    std::cerr << argv[0] << ": '" << cmd
              << "' is not one of lint, list, echo, create, transforms, emit"
              << (argc <= 2 ? " (or it needs a file argument)" : "") << "\n";
    return 2;
}

// A top-level catch, because every path below can throw on ordinary user error
// -- an unreadable config, an output directory that cannot be created -- and
// without one those surface as `terminate called after throwing ...` and a core
// dump (SIGABRT, exit 134) rather than a message. Found by an audit:
// `sfconfig transforms missing.yaml` aborted instead of reporting.
int sf::cli::config_main(int argc, char** argv) {
    try {
        return sf_cli_main(argc, argv);
    } catch (const std::exception& e) {
        std::cerr << "error: " << e.what() << "\n";
        return 1;
    } catch (...) {
        std::cerr << "error: unknown failure\n";
        return 1;
    }
}
