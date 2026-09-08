// Bloblang conformance against the reference implementation.
//
// Fixtures are extracted by tools/extract_conformance.py from the Redpanda
// Connect docs and the Benthos source.
//
// The harness distinguishes three outcomes, and only one of them is a failure:
//
//   PASS        the mapping ran and matched the reference exactly
//   UNSUPPORTED it did not parse, or used a method we have not written yet
//   WRONG       it ran and produced a DIFFERENT answer
//
// UNSUPPORTED is expected while coverage grows from 16 of 227 and must not fail
// the build. WRONG is a semantic divergence — the failure mode the whole project
// is most at risk from — and fails immediately. That split is what lets this be
// a merge gate today rather than after every method lands.
#include "swordfish/blobl/parse.hh"
#include "swordfish/blobl/interp.hh"
#include "swordfish/config/yaml.hh"
#include "swordfish/message.hh"

#include <catch_amalgamated.hpp>

#include <fstream>
#include <map>
#include <sstream>
#include <string>
#include <algorithm>
#include <vector>

namespace {

struct fixture {
    std::string id, source, mapping;
    std::vector<std::pair<std::string, std::string>> cases;   // in, out
};

std::vector<fixture> load_fixtures(const std::string& path) {
    std::ifstream f(path);
    if (!f) return {};
    std::ostringstream ss;
    ss << f.rdbuf();
    auto root = sf::cfg::parse_yaml(ss.str());
    std::vector<fixture> out;
    if (!root.is_sequence()) return out;
    for (const auto& n : root.seq) {
        fixture fx;
        if (const auto* v = n.find("id"))      fx.id = v->scalar;
        if (const auto* v = n.find("source"))  fx.source = v->scalar;
        if (const auto* v = n.find("mapping")) fx.mapping = v->scalar;
        if (const auto* cs = n.find("cases"); cs && cs->is_sequence())
            for (const auto& c : cs->seq) {
                const auto* i = c.find("in");
                const auto* o = c.find("out");
                if (i && o) fx.cases.emplace_back(i->scalar, o->scalar);
            }
        if (!fx.mapping.empty() && !fx.cases.empty()) out.push_back(std::move(fx));
    }
    return out;
}

enum class outcome { pass, unsupported, wrong };

struct result {
    outcome     kind = outcome::pass;
    std::string detail;
};

// Comparing serialised JSON would make key order and number formatting part of
// every assertion. Parsing both sides and comparing structurally keeps the
// fixture honest about semantics; byte-level output is pinned separately in
// test_all.cc.
bool json_equal(const std::string& a, const std::string& b) {
    try { return sf::parse_json(a) == sf::parse_json(b); }
    catch (const sf::eval_error&) { return a == b; }
}

result run_fixture(const fixture& fx) {
    sf::blobl::mapping m;
    try {
        m = sf::blobl::parse_mapping(fx.mapping);
    } catch (const sf::blobl::parse_error& e) {
        return {outcome::unsupported, std::string("parse: ") + e.what()};
    }
    sf::blobl::interp in(m);
    sf::exec_ctx ctx;                       // shared across cases: counter() et al

    for (const auto& [input, expected] : fx.cases) {
        std::string got;
        try {
            // A fixture input is a MESSAGE, not necessarily a JSON value: the
            // content()-based examples pass raw text. Parsing eagerly rejected
            // them as malformed JSON and reported six false divergences.
            sf::message msg(input);
            sf::value parsed;
            bool structured = true;
            try { parsed = msg.as_structured(); }
            catch (const sf::eval_error&) { structured = false; }
            ctx.this_v = structured ? &parsed : nullptr;
            // Modelled on the mapping processor rather than simplified: writes
            // go to the OUTPUT message's metadata and reads come from the
            // input's. Pointing both at one message would make `meta x = ...`
            // visible to a later `meta("x")` here and nowhere else, so a
            // fixture covering that would be pinned to the wrong answer.
            sf::message out_msg = msg.shallow_copy();
            ctx.meta = &out_msg.meta();
            ctx.meta_in = &msg.meta();
            ctx.msg = &msg;
            sf::value result = in.run_ctx(ctx);
            ctx.meta = nullptr;
            ctx.meta_in = nullptr;
            // Compare the resulting MESSAGE, not the raw value: a string root
            // is written verbatim rather than JSON-quoted, which is what the
            // documented format_json outputs show.
            // A deleted root drops the message. The reference documentation
            // writes that outcome as <Message deleted>, so the harness has to
            // as well -- comparing the unchanged content would let a mapping
            // that silently failed to delete pass.
            if (result.is_deleted()) {
                got = "<Message deleted>";
            } else {
                sf::message out(input);
                out.set_mapped(std::move(result));
                got = out.as_bytes();
            }
        } catch (const sf::eval_error& e) {
            const std::string what = e.what();
            // A documented output of Error("...") means the reference itself
            // failed. Our wording differs from Go's, so agreeing that it is an
            // error is the whole assertion.
            if (expected.rfind("Error(", 0) == 0) continue;
            // "unknown method/function" means not written yet; any other error
            // is a real behavioural difference.
            // Unrecognised names are now rejected at parse time, so anything
            // reaching here is a behavioural difference rather than a gap --
            // except contexts the harness genuinely cannot provide.
            if (what.find("could not be parsed as structured data") != std::string::npos)
                return {outcome::unsupported, "needs structured input: " + what};
            return {outcome::wrong, "error: " + what + " (expected " + expected + ")"};
        }
        if (expected.rfind("Error(", 0) == 0)
            return {outcome::wrong, "expected an error, got " + got};
        if (got != expected && !json_equal(got, expected))
            return {outcome::wrong, "got " + got + ", expected " + expected};
    }
    return {outcome::pass, {}};
}

} // namespace

TEST_CASE("bloblang conformance corpus", "[conformance]") {
    const auto fixtures = load_fixtures("tests/fixtures/bloblang.yaml");
    REQUIRE(!fixtures.empty());          // the corpus must actually be present

    std::vector<const fixture*> wrong;
    std::map<std::string, int> unsupported_reasons;
    int passed = 0, unsup = 0;

    for (const auto& fx : fixtures) {
        const auto r = run_fixture(fx);
        switch (r.kind) {
        case outcome::pass: ++passed; break;
        case outcome::unsupported: {
            ++unsup;
            // Group by the missing thing so the report says what to build next.
            std::string key = r.detail.substr(0, r.detail.find(" ("));
            if (key.size() > 60) key = key.substr(0, 60);
            ++unsupported_reasons[key];
            break;
        }
        case outcome::wrong:
            wrong.push_back(&fx);
            UNSCOPED_INFO(fx.id << " (" << fx.source << ")\n"
                          << "    mapping: " << fx.mapping << "\n"
                          << "    " << r.detail);
            break;
        }
    }

    // Rank the missing names by how many fixtures each would unlock, so the
    // report is a work queue rather than a score.
    std::vector<std::pair<int, std::string>> ranked;
    for (const auto& [reason, n] : unsupported_reasons) ranked.emplace_back(n, reason);
    std::sort(ranked.rbegin(), ranked.rend());

    std::ostringstream report;
    report << "conformance: " << passed << " pass, " << unsup << " unsupported, "
           << wrong.size() << " WRONG, of " << fixtures.size() << " fixtures\n"
           << "  most-blocking gaps (fixtures unlocked if implemented):\n";
    for (size_t i = 0; i < ranked.size() && i < 12; ++i)
        report << "    " << ranked[i].first << "  " << ranked[i].second << "\n";
    WARN(report.str());

    // Only semantic divergence fails. Growing coverage is tracked, not enforced.
    CHECK(wrong.empty());
}
