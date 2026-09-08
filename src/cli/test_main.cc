// `swordfish test` — runs Benthos-format unit tests against a config.
//
// Usage mirrors the reference:
//   swordfish-test ./path/to/configs/...   (recursive)
//   swordfish-test ./foo.yaml ./bar.yaml
//
// Exits 1 if any case fails, matching `redpanda-connect test`, so it drops into
// the same CI step.
#include "swordfish/config/yaml.hh"
#include "swordfish/runtime/build_stream.hh"
#include "swordfish/components/registry.hh"
#include "swordfish/runtime/components.hh"
#include "swordfish/testing/unit_test.hh"
#include "swordfish/kafka/components.hh"

#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/util/log.hh>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>

#include "swordfish/cli/commands.hh"

namespace fs = std::filesystem;
using namespace sf;
using namespace sf::testing;

namespace {

std::string read_file(const std::string& p) {
    std::ifstream f(p, std::ios::binary);
    if (!f) throw std::runtime_error("cannot open " + p);
    std::ostringstream ss; ss << f.rdbuf();
    return ss.str();
}

// Benthos pairs `foo.yaml` with `foo_benthos_test.yaml`, and accepts either
// name as the target. Tests may also live inline in the config under `tests:`.
//
// The suffix is `_benthos_test`, not `_test` -- taken from
// config.OptTestSuffix in benthos-main/internal/cli/common/reader.go. Guessing
// `_test` mis-paired real files: `cities_test.yaml` is a standalone config with
// its own tests, and treating it as a DEFINITION sent the runner looking for a
// `cities.yaml` that does not exist.
constexpr std::string_view test_suffix = "_benthos_test";

std::pair<std::string, std::string> path_pair(const fs::path& p) {
    const std::string ext = p.extension().string();
    const std::string stem = p.stem().string();
    if (stem.size() > test_suffix.size() &&
        stem.compare(stem.size() - test_suffix.size(), test_suffix.size(), test_suffix) == 0)
        return {(p.parent_path() /
                 (stem.substr(0, stem.size() - test_suffix.size()) + ext)).string(),
                p.string()};
    return {p.string(), (p.parent_path() / (stem + std::string(test_suffix) + ext)).string()};
}

// Throws when the target names nothing. It used to return an empty vector and
// nothing downstream noticed: `pairs` stayed empty, the run loop never executed,
// and `swordfish test --targets /does/not/exist.yaml` printed
// "0 file(s), 0 case(s), 0 failed" and exited 0 -- a green run over no tests at
// all, which is the worst possible answer to a typo in a CI invocation. The
// reference exits 1 with "unable to access target config file".
std::vector<fs::path> expand(const std::string& target) {
    std::vector<fs::path> out;
    // `dir/...` means recurse, which is how the reference spells it.
    if (target.size() > 4 && target.compare(target.size() - 4, 4, "/...") == 0) {
        const fs::path root = target.substr(0, target.size() - 4);
        if (!fs::exists(root))
            throw std::runtime_error("unable to access target '" + target +
                                     "': no such directory");
        for (const auto& e : fs::recursive_directory_iterator(root)) {
            const auto x = e.path().extension();
            if (e.is_regular_file() && (x == ".yaml" || x == ".yml")) out.push_back(e.path());
        }
        return out;
    }
    if (fs::is_directory(target)) {
        for (const auto& e : fs::directory_iterator(target)) {
            const auto x = e.path().extension();
            if (e.is_regular_file() && (x == ".yaml" || x == ".yml")) out.push_back(e.path());
        }
        return out;
    }
    if (fs::exists(target)) { out.push_back(target); return out; }
    throw std::runtime_error("unable to access target '" + target + "': no such file or directory");
}

// A resource label names a component anywhere in the document. Benthos resolves
// these against the resource sections; searching the whole tree finds the same
// thing and does not need the section names hard-coded.
const cfg::ynode* find_labelled(const cfg::ynode& n, const std::string& label) {
    if (n.is_mapping()) {
        if (const auto* l = n.find("label"); l && l->scalar == label) return &n;
        for (const auto& e : n.map)
            if (const auto* hit = find_labelled(e.value, label)) return hit;
    } else if (n.is_sequence()) {
        for (const auto& e : n.seq)
            if (const auto* hit = find_labelled(e, label)) return hit;
    }
    return nullptr;
}

// Set for the duration of one case and restored afterwards, so a test that sets
// an environment variable cannot leak it into the next one.
class scoped_env {
public:
    explicit scoped_env(const std::map<std::string, std::string>& vars) {
        for (const auto& [k, v] : vars) {
            const char* old = ::getenv(k.c_str());
            _saved.emplace_back(k, old ? std::optional<std::string>(old) : std::nullopt);
            ::setenv(k.c_str(), v.c_str(), 1);
        }
    }
    ~scoped_env() {
        for (const auto& [k, v] : _saved) {
            if (v) ::setenv(k.c_str(), v->c_str(), 1); else ::unsetenv(k.c_str());
        }
    }
private:
    std::vector<std::pair<std::string, std::optional<std::string>>> _saved;
};

// Feeds a case's input batches through `procs` and checks the output
// conditions. Extracted so `target_processors` and `target_mapping` run the
// SAME comparison: two copies would be two things to keep in step, and the one
// used less often would be the one that drifted.
seastar::future<> run_case(const test_case& tc, const std::vector<processor_ptr>& procs,
                           const fs::path& cfg_path, std::vector<case_failure>& out) {
        std::vector<batch> produced;
        seastar::abort_source as;
        for (const auto& in_batch : tc.input_batches) {
            batch b;
            bool bad = false;
            for (const auto& im : in_batch) {
                std::string body;
                if (im.content)           body = *im.content;
                else if (im.json_content) body = *im.json_content;
                else if (im.file_content) {
                    try { body = read_file((cfg_path.parent_path() / *im.file_content).string()); }
                    catch (const std::exception& e) {
                        out.push_back(case_failure{tc.name, static_cast<int>(im.where.line),
                            std::string("file_content: ") + e.what()});
                        bad = true;
                    }
                }
                message m(std::move(body));
                for (const auto& [k, v] : im.metadata) m.meta().set(k, value(v));
                b.push_back(std::move(m));
            }
            if (bad) break;

            std::vector<batch> stage{std::move(b)};
            for (const auto& p : procs) {
                std::vector<batch> next;
                for (auto& sb : stage) {
                    if (sb.empty()) continue;
                    auto outs = co_await p->process(std::move(sb), as);
                    for (auto& o : outs) next.push_back(std::move(o));
                }
                stage = std::move(next);
            }
            for (auto& o : stage) produced.push_back(std::move(o));
        }

        if (out.empty()) {
            if (produced.size() != tc.output_batches.size()) {
                out.push_back(case_failure{tc.name, static_cast<int>(tc.where.line),
                    "expected " + std::to_string(tc.output_batches.size()) +
                    " output batch(es) but got " + std::to_string(produced.size())});
            } else {
                for (size_t bi = 0; bi < produced.size(); ++bi) {
                    if (produced[bi].size() != tc.output_batches[bi].size()) {
                        out.push_back(case_failure{tc.name, static_cast<int>(tc.where.line),
                            "batch " + std::to_string(bi) + ": expected " +
                            std::to_string(tc.output_batches[bi].size()) +
                            " message(s) but got " + std::to_string(produced[bi].size())});
                        continue;
                    }
                    for (size_t mi = 0; mi < produced[bi].size(); ++mi)
                        check_conditions(tc.output_batches[bi][mi], produced[bi][mi],
                                         cfg_path.parent_path().string(),
                                         tc.name, bi, mi, out);
                }
            }
        }
}

seastar::future<file_result> run_file(const fs::path& cfg_path,
                                      const fs::path& def_path) {
    file_result fr;
    fr.path = cfg_path.string();

    cfg::ynode def_root;
    try {
        // Lenient: this read only has to find the `tests:` block, and a case
        // may supply the variables the config requires. Each case re-reads it
        // strictly under its own environment.
        const cfg::ynode cfg_root =
            cfg::load_config(cfg_path.string(), /*require_env=*/false);
        def_root = fs::exists(def_path) && def_path != cfg_path
                       ? cfg::load_config(def_path.string(), /*require_env=*/false)
                       : cfg_root;
    } catch (const std::exception& e) {
        // A FAILURE, not a skip. It was counted with "no tests" and the run
        // exited 0, so a suite whose file did not parse reported success and
        // said nothing about the YAML error -- the one outcome a test harness
        // must never produce.
        fr.skip_reason = std::string("could not be parsed: ") + e.what();
        fr.skip_is_failure = true;
        co_return fr;
    }

    auto cases = parse_cases(def_root);
    if (cases.empty()) { fr.skip_reason = "no tests"; co_return fr; }
    fr.ran = true;
    fr.cases = cases.size();

    for (const auto& tc : cases) {
        // Reported rather than silently passed. A mocked component that is not
        // mocked would run for real -- which for `http` means a network call --
        // so the honest outcome is "unsupported", not a green tick.

        scoped_env env_guard(tc.environment);

        // `target_mapping` names a .blobl FILE relative to the test and
        // replaces the processor chain outright: the case exercises that
        // mapping alone, so target_processors is never consulted. Handled
        // here, before the pointer is resolved, for exactly that reason.
        if (!tc.target_mapping.empty()) {
            std::vector<processor_ptr> mprocs;
            try {
                const fs::path bl = tc.target_mapping.front() == '/'
                    ? fs::path(tc.target_mapping)
                    : cfg_path.parent_path() / tc.target_mapping;
                cfg::ynode m;
                m.type = cfg::ynode_type::mapping;
                cfg::ynode body;
                body.type = cfg::ynode_type::scalar;
                body.scalar = read_file(bl.string());
                m.map.push_back(cfg::ynode::entry{"mapping", std::move(body)});
                mprocs = build_processors({parse_processor(m)});
            } catch (const std::exception& e) {
                fr.failures.push_back(case_failure{tc.name, static_cast<int>(tc.where.line),
                    "target_mapping '" + tc.target_mapping + "': " + e.what()});
                continue;
            }
            std::vector<case_failure> mf;
            co_await run_case(tc, mprocs, cfg_path, mf);
            for (auto& f : mf) fr.failures.push_back(std::move(f));
            continue;
        }

    
        // Three spellings, all of which appear in the Benthos corpus:
        //   /pipeline/processors            a JSON pointer into this document
        //   woof_drop                       a resource LABEL
        //   ./other.yaml#/pipeline/procs    a pointer into a NEIGHBOURING file
        std::string ptr = tc.target_processors;
        // RE-READ under this case's environment rather than copied from the
        // load above. `${VAR:default}` is substituted when the file is read, so
        // a document parsed before `environment` was applied has already baked
        // in the defaults and the case's variables can never take effect.
        //
        // A fresh parse per case also keeps mocks honest: they rewrite the
        // document, and the next case must see the original.
        cfg::ynode case_doc;
        try {
            case_doc = cfg::load_config(cfg_path.string());
        } catch (const std::exception& e) {
            fr.failures.push_back(case_failure{tc.name, static_cast<int>(tc.where.line),
                                               e.what()});
            continue;
        }
        const cfg::ynode* doc = &case_doc;
        cfg::ynode other_doc;
        if (const size_t hash = ptr.find('#'); hash != std::string::npos) {
            const std::string rel = ptr.substr(0, hash);
            ptr = ptr.substr(hash + 1);
            try {
                other_doc = cfg::load_config((cfg_path.parent_path() / rel).string());
                doc = &other_doc;
            } catch (const std::exception& e) {
                fr.failures.push_back(case_failure{tc.name, static_cast<int>(tc.where.line),
                    "target_processors '" + tc.target_processors + "': " + e.what()});
                continue;
            }
        }
        if (!tc.mocks.empty()) {
            try {
                // Applied to whichever document the target resolves against --
                // a cross-file target mocks that file, not this one.
                apply_mocks(*const_cast<cfg::ynode*>(doc), tc.mocks);
            } catch (const std::exception& e) {
                fr.failures.push_back(case_failure{tc.name, static_cast<int>(tc.where.line),
                                                   e.what()});
                continue;
            }
        }

        const cfg::ynode* target = json_pointer(*doc, ptr);
        if (!target && !ptr.empty() && ptr.front() != '/')
            target = find_labelled(*doc, ptr);
        if (!target) {
            fr.failures.push_back(case_failure{tc.name, static_cast<int>(tc.where.line),
                "target_processors '" + tc.target_processors + "' matched nothing"});
            continue;
        }

        std::vector<processor_ptr> procs;
        try {
            // The pointer may name one processor or a list of them.
            std::vector<component_config> specs;
            const auto nodes = target->is_sequence()
                ? std::vector<const cfg::ynode*>([&]{
                      std::vector<const cfg::ynode*> v;
                      for (const auto& n : target->seq) v.push_back(&n);
                      return v; }())
                : std::vector<const cfg::ynode*>{target};
            for (const auto* n : nodes) specs.push_back(parse_processor(*n));
            // `resource:` names a component elsewhere in the document; without
            // this it reaches the registry unresolved and reports an internal
            // error instead of running the resource.
            resolve_resources(specs, *doc);
            procs = build_processors(specs);
        } catch (const std::exception& e) {
            fr.failures.push_back(case_failure{tc.name, static_cast<int>(tc.where.line),
                std::string("could not build the processors: ") + e.what()});
            continue;
        }

        std::vector<case_failure> failures;
        co_await run_case(tc, procs, cfg_path, failures);
        for (auto& f : failures) fr.failures.push_back(std::move(f));
    }
    co_return fr;
}

} // namespace

int sf::cli::test_main(int argc, char** argv) {
    seastar::app_template::seastar_options ac;
    ac.name = "swordfish-test";
    // One shard by default. The runner does every bit of its work on shard 0 --
    // it builds the processors and feeds each case's fixed message list through
    // them in place, with no submit_to and no sharded<> anywhere -- so extra
    // shards buy nothing and only reserve memory. They also changed the ANSWER:
    // components that refuse to run distributed, `dedupe` among them, are
    // refused by name against seastar::smp::count, so the same `swordfish test`
    // command reported 45/45 on a one-core machine and 43/45 on a 64-core one.
    // A test's outcome must not depend on the core count. `--smp N` still
    // overrides, for anyone who wants the old behaviour deliberately.
    ac.smp_opts.smp.set_default_value(1);
    seastar::app_template app(std::move(ac));
    // Registered ONCE, as a positional option, which also gives the `--targets`
    // spelling. The usage line below and the header comment both showed
    // `swordfish-test <target>...`, and boost rejected that with "too many
    // positional options have been specified" and exit 2 -- the documented
    // invocation did not work. Registering it in both places instead makes
    // `--targets` ambiguous, which is worse; this is the one registration that
    // serves both forms. `--targets` is what CMakeLists.txt and README.md use.
    app.add_positional_options({{"targets",
                                 boost::program_options::value<std::vector<std::string>>()
                                     ->multitoken(),
                                 "config or test files, or dir/...", -1}});
    app.add_options()("verbose,v", "print each test file as it runs");

    return app.run(argc, argv, [&app]() -> seastar::future<int> {
        // Static libraries drop their registrations without an explicit call.
        sf::register_builtin_processors();
        sf::register_builtin_io();
        sf::kafka::register_components();
        auto& args = app.configuration();
        if (!args.count("targets")) {
            std::cerr << "usage: swordfish-test <config-or-test.yaml|dir/...> ...\n";
            co_return 1;
        }
        const bool verbose = args.count("verbose") > 0;

        // Deduplicated: `dir/...` yields both foo.yaml and foo_test.yaml, which
        // pair to the same config, and running it twice would double-count.
        std::vector<std::pair<std::string, std::string>> pairs;
        try {
            for (const auto& t : args["targets"].as<std::vector<std::string>>())
                for (const auto& p : expand(t)) {
                    auto pr = path_pair(p);
                    if (std::find(pairs.begin(), pairs.end(), pr) == pairs.end())
                        pairs.push_back(std::move(pr));
                }
        } catch (const std::exception& e) {
            // Caught here so a bad target reads as a diagnostic rather than as
            // Seastar's "Exiting on unhandled exception", which buries the
            // message in a stack of framework noise.
            std::cerr << "swordfish test: " << e.what() << "\n";
            co_return 1;
        }

        size_t ran = 0, failed = 0, skipped = 0, total_cases = 0;
        for (const auto& [cfg, def] : pairs) {
            // A `foo_test.yaml` with no `foo.yaml` beside it is its own
            // config, which is what the reference does. Skipping it silently
            // made such a file invisible: not run, not reported.
            const std::string cfg_use = fs::exists(cfg) ? cfg : def;
            if (!fs::exists(cfg_use)) continue;
            if (verbose) std::cout << "Test '" << cfg_use << "'\n";
            const file_result r = co_await run_file(cfg_use, def);
            if (!r.ran) {
                if (r.skip_is_failure) {
                    ++failed;
                    std::cout << "Test '" << cfg_use << "' failed\n  "
                              << r.skip_reason << "\n";
                } else {
                    ++skipped;
                    if (verbose && !r.skip_reason.empty())
                        std::cout << "Test '" << cfg_use << "' skipped: "
                                  << r.skip_reason << "\n";
                }
                continue;
            }
            ++ran;
            total_cases += r.cases;
            if (r.failures.empty()) {
                std::cout << "Test '" << r.path << "' succeeded\n";
            } else {
                ++failed;
                std::cout << "Test '" << r.path << "' failed\n";
                for (const auto& f : r.failures) std::cout << "  " << f.str() << "\n";
            }
        }
        std::cout << "\n" << ran << " file(s), " << total_cases << " case(s), "
                  << failed << " failed, " << skipped << " skipped (no tests)\n";
        co_return failed ? 1 : 0;
    });
}
