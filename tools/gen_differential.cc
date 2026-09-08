// Generates the differential harness: one compiled C++ function per conformance
// fixture, plus a main() that runs each fixture through BOTH backends and
// compares.
//
// The interpreter and the emitter are two implementations of one language, and
// silent divergence between them is the project's largest risk. This is the
// gate for it: nothing here is
// hand-written per method, so a method added to one backend and forgotten in
// the other fails immediately across every fixture that uses it.
//
// Run it through tools/run_differential.sh, which generates, compiles and runs.
#include "swordfish/blobl/emit.hh"
#include "swordfish/blobl/parse.hh"
#include "swordfish/config/spec.hh"
#include "swordfish/config/yaml.hh"

#include <fstream>
#include <iostream>
#include <iterator>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

namespace {

using sf::cfg::cxx_string_literal;

// Only the input. This harness runs both backends and compares them to each
// other, so a fixture's recorded `out` is not an expectation here and was
// parsed into a field nothing read.
struct fx_case { std::string in; };
struct fx { std::string id, mapping; std::vector<fx_case> cases; bool strict = false; };

std::vector<fx> load(const std::string& path, bool strict) {
    std::ifstream f(path);
    if (!f) { std::cerr << "cannot open " << path << "\n"; std::exit(2); }
    std::ostringstream ss; ss << f.rdbuf();
    auto root = sf::cfg::parse_yaml(ss.str());
    std::vector<fx> out;
    if (!root.is_sequence()) return out;
    for (const auto& n : root.seq) {
        fx x;
        if (const auto* v = n.find("id"))      x.id = v->scalar;
        if (const auto* v = n.find("mapping")) x.mapping = v->scalar;
        if (const auto* cs = n.find("cases"); cs && cs->is_sequence())
            for (const auto& c : cs->seq) {
                // Keyed on `in` ALONE. Requiring `out` too meant a
                // conformance case written without a recorded output -- an
                // error case, say -- was skipped here silently, losing
                // differential coverage with nothing to show it had gone.
                if (const auto* i = c.find("in")) x.cases.push_back({i->scalar});
            }
        // The hand-written corpus lists bare `inputs` instead of in/out pairs:
        // this harness compares the two backends against each other, so there
        // is no expected answer to record.
        if (const auto* is = n.find("inputs"); is && is->is_sequence())
            for (const auto& i : is->seq) x.cases.push_back({i.scalar});
        x.strict = strict;
        if (!x.mapping.empty() && !x.cases.empty()) out.push_back(std::move(x));
    }
    return out;
}

// The emitter returns a whole translation unit. Concatenating many of them
// would repeat the includes and the namespace, so the body between the
// namespace braces is lifted out and the preamble written once.
std::string body_of(const std::string& tu) {
    const std::string open = "namespace sf::gen {";
    const std::string close = "} // namespace sf::gen";
    const size_t a = tu.find(open);
    const size_t b = tu.rfind(close);
    if (a == std::string::npos || b == std::string::npos) return {};
    return tu.substr(a + open.size(), b - a - open.size());
}

} // namespace

int main(int argc, char** argv) {
    if (argc < 3) {
        std::cerr << "usage: sfdiffgen <out.cc> [--strict] <fixtures.yaml>...\n"
                     "  --strict: files after it must parse; a fixture that does\n"
                     "            not is an error rather than a skip.\n";
        return 2;
    }
    // The extracted corpus grows faster than coverage does, so a fixture that
    // does not parse there is expected. The hand-written corpus is different:
    // every mapping in it was written to exercise something, and one that
    // silently fails to parse contributes nothing while looking like coverage.
    std::vector<fx> fixtures;
    bool strict = false;
    for (int i = 2; i < argc; ++i) {
        if (std::string_view(argv[i]) == "--strict") { strict = true; continue; }
        auto part = load(argv[i], strict);
        fixtures.insert(fixtures.end(), std::make_move_iterator(part.begin()),
                        std::make_move_iterator(part.end()));
    }

    std::ostringstream fns, table;
    size_t n = 0, skipped = 0, failed = 0;
    for (const auto& x : fixtures) {
        sf::blobl::mapping m;
        try {
            m = sf::blobl::parse_mapping(x.mapping);
        } catch (const sf::blobl::parse_error& e) {
            if (x.strict) {
                ++failed;
                std::cerr << "sfdiffgen: " << x.id << " does not parse: " << e.what() << "\n";
            } else {
                ++skipped;                // not yet supported: nothing to compare
            }
            continue;
        }
        const std::string name = "blobl_" + std::to_string(n);
        const std::string tu = sf::blobl::emit_cpp(m, {.function_name = name,
                                                       .line_comments = false});
        const std::string body = body_of(tu);
        if (body.empty()) { ++skipped; continue; }
        fns << body;

        table << "  {" << cxx_string_literal(x.id) << ", "
              << cxx_string_literal(x.mapping) << ", &sf::gen::" << name << ", {";
        for (const auto& c : x.cases) table << cxx_string_literal(c.in) << ", ";
        table << "}},\n";
        ++n;
    }

    std::ofstream out(argv[1]);
    out <<
R"(// Generated by sfdiffgen. Do not edit.
#include <swordfish/runtime.hh>
#include <swordfish/regex.hh>
#include <swordfish/blobl/parse.hh>
#include <swordfish/blobl/interp.hh>
#include <swordfish/message.hh>

#include <iostream>
#include <string>
#include <string_view>
#include <vector>

namespace sf::gen {
)" << fns.str() << R"(
} // namespace sf::gen

namespace {

struct entry {
    const char* id;
    const char* mapping;
    sf::value (*fn)(sf::exec_ctx&);
    std::vector<const char*> inputs;
};

const entry entries[] = {
)" << table.str() << R"(};

// Both backends see the same context shape the runtime gives them, so a
// difference here is a difference in the language, not in the harness.
struct answer { bool threw = false; std::string text; };

answer run_one(const std::string& input, sf::value (*fn)(sf::exec_ctx&),
               sf::blobl::interp* in) {
    answer a;
    sf::message msg(input);
    sf::value parsed;
    bool structured = true;
    try { parsed = msg.as_structured(); } catch (const sf::eval_error&) { structured = false; }
    sf::exec_ctx ctx;
    ctx.this_v = structured ? &parsed : nullptr;
    ctx.meta = &msg.meta();
    ctx.msg = &msg;
    try {
        sf::value r = fn ? fn(ctx) : in->run_ctx(ctx);
        sf::message out(input);
        out.set_mapped(std::move(r));
        a.text = out.as_bytes();
    } catch (const sf::eval_error& e) {
        a.threw = true;
        a.text = e.what();
    }
    return a;
}

} // namespace

int main() {
    size_t cases = 0, diverged = 0;
    for (const entry& e : entries) {
        sf::blobl::mapping m = sf::blobl::parse_mapping(e.mapping);
        sf::blobl::interp in(m);
        for (const char* input : e.inputs) {
            ++cases;
            const answer c = run_one(input, e.fn, nullptr);
            const answer i = run_one(input, nullptr, &in);
            // Error TEXT may differ between backends; whether it errored at all
            // may not, and neither may the value.
            if (c.threw != i.threw || (!c.threw && c.text != i.text)) {
                ++diverged;
                std::cout << "DIVERGENCE " << e.id << "\n"
                          << "  mapping:     " << e.mapping << "\n"
                          << "  input:       " << input << "\n"
                          << "  compiled:    " << (c.threw ? "error: " : "") << c.text << "\n"
                          << "  interpreted: " << (i.threw ? "error: " : "") << i.text << "\n";
            }
        }
    }
    std::cout << "differential: " << cases << " cases across "
              << (sizeof(entries) / sizeof(entries[0])) << " mappings, "
              << diverged << " divergences\n";
    return diverged == 0 ? 0 : 1;
}
)";
    std::cerr << "sfdiffgen: " << n << " mappings emitted, " << skipped
              << " skipped (do not parse yet)\n";
    if (failed) {
        std::cerr << "sfdiffgen: " << failed
                  << " fixture(s) in a --strict corpus do not parse\n";
        return 1;
    }
    return 0;
}
