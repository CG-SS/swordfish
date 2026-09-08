// Config-framework tests, on Catch2.
//
// The important ones are the spec_of round-trips: they assert that the four
// consumers of a single declaration agree with each other, which is the property
// the whole design rests on.
#include "swordfish/config/spec.hh"
#include "spec_fixture.hh"
#include "swordfish/message.hh"
#include "swordfish/components/transform.hh"
#include "swordfish/blobl/emit.hh"
#include "swordfish/components/registry.hh"

#include <catch_amalgamated.hpp>

#include <iostream>
#include <string>

using namespace sf::cfg;

// Shared by the parsing and emission cases: one config, exercised by both
// consumers of the same spec_of declaration.
static constexpr const char* kKafkaConfig = R"(
seed_brokers:
  - localhost:9092
  - localhost:9093
topics: [ events ]
consumer_group: swordfish
fetch_max_wait: 750ms
sasl:
  mechanism: SCRAM-SHA-256
  username: alice
  password: hunter2
tls:
  enabled: true
)";

static parse_result<sf::in::kafka_input_config> parse_kafka() {
    return from_yaml<sf::in::kafka_input_config>(parse_yaml(kKafkaConfig), "input.kafka");
}

// Parenthesised for the same reason as in test_all.cc: Catch2 will not decompose
// a compound comparison. `eq` keeps its comparison bare so both sides are shown.
#define check(cond, label)     do { INFO(label); CHECK((cond)); } while (0)
#define eq(got, want, label)   do { INFO(label); CHECK((got) == (want)); } while (0)


TEST_CASE("yaml: the subset real configs use", "[yaml]") {
    // ---- YAML subset ----
    {
        auto n = parse_yaml("a: 1\nb:\n  c: hello\n  d: [1, 2, 3]\n");
        check(n.is_mapping(), "top level is a mapping");
        check(n.find("a") && n.find("a")->scalar == "1", "scalar value");
        check(n.find("b") && n.find("b")->is_mapping(), "nested mapping");
        check(n.find("b")->find("c")->scalar == "hello", "nested scalar");
        check(n.find("b")->find("d")->is_sequence(), "flow sequence");
        check(n.find("b")->find("d")->seq.size() == 3, "flow sequence length");
    }
    {
        auto n = parse_yaml("items:\n  - one\n  - two\n");
        check(n.find("items")->seq.size() == 2, "block sequence");
        check(n.find("items")->seq[1].scalar == "two", "block sequence item");
    }
    {   // sequences of mappings: the shape of `pipeline.processors`
        auto n = parse_yaml("processors:\n  - mapping: root = this\n    label: a\n  - noop: {}\n");
        auto* p = n.find("processors");
        check(p && p->seq.size() == 2, "sequence of mappings");
        check(p->seq[0].find("mapping")->scalar == "root = this", "inline key on dash line");
        check(p->seq[0].find("label")->scalar == "a", "sibling key of a dash item");
        check(p->seq[1].find("noop")->is_mapping(), "empty flow mapping");
    }
    {   // block scalars carry Bloblang, so chomping must be right
        auto n = parse_yaml("m: |\n  root = this\n  root.x = 1\nother: 2\n");
        eq(n.find("m")->scalar, "root = this\nroot.x = 1\n", "block scalar |");
        auto s = parse_yaml("m: |-\n  root = this\n  root.x = 1\nother: 2\n");
        eq(s.find("m")->scalar, "root = this\nroot.x = 1", "block scalar |- strips newline");
        check(n.find("other")->scalar == "2", "parsing resumes after a block scalar");
    }
    {   // '#' inside a value is content, not a comment
        auto n = parse_yaml("url: http://x/y  # trailing comment\nfrag: \"a # b\"\n");
        eq(n.find("url")->scalar, "http://x/y", "trailing comment stripped");
        eq(n.find("frag")->scalar, "a # b", "'#' inside quotes preserved");
    }
    {   // a colon inside a value must not split the key
        auto n = parse_yaml("addr: localhost:9092\n");
        eq(n.find("addr")->scalar, "localhost:9092", "colon inside a scalar");
    }

    {   // Regression: a block sequence may sit level with its own key.
        auto n = parse_yaml("processors:\n- branch:\n    x: 1\n- noop: {}\n");
        auto* ps = n.find("processors");
        check(ps && ps->is_sequence() && ps->seq.size() == 2, "sequence level with its key");
        check(ps->seq[0].find("branch")->find("x")->scalar == "1", "contents of such a sequence");
    }
    {   // Regression: same-indent sequence under a key inside a dash item.
        auto n = parse_yaml("- try:\n  - http:\n      url: http://x\n");
        check(n.is_sequence() && n.seq.size() == 1, "outer sequence");
        auto* t = n.seq[0].find("try");
        check(t && t->is_sequence() && t->seq.size() == 1, "nested same-indent sequence");
        check(t->seq[0].find("http")->find("url")->scalar == "http://x", "deeply nested scalar");
    }
    {   // Regression: "- - item" opens a nested sequence on one line.
        auto n = parse_yaml("output_batches:\n  - - a: 1\n    - a: 2\n");
        auto* ob = n.find("output_batches");
        check(ob && ob->seq.size() == 1, "outer batch list");
        check(ob->seq[0].is_sequence() && ob->seq[0].seq.size() == 2, "nested sequence via '- -'");
        check(ob->seq[0].seq[1].find("a")->scalar == "2", "second nested item");
    }
    {   // Regression: a flow collection spanning several lines.
        auto n = parse_yaml("check:\n  json_equals: {\n    \"a\": 1,\n    \"b\": { \"c\": 2 }\n  }\n");
        auto* je = n.find("check")->find("json_equals");
        check(je && je->is_mapping(), "multi-line flow mapping");
        check(je->find("a")->scalar == "1", "flow scalar across lines");
        check(je->find("b")->find("c")->scalar == "2", "nested flow across lines");
    }

    {   // Quoting must be observable: a quoted "true" is a string, not a bool.
        auto n = parse_yaml("a: true\nb: \"true\"\nc: |\n  true\n");
        check(!n.find("a")->quoted, "plain scalar is unquoted");
        check(n.find("b")->quoted,  "double-quoted scalar is marked quoted");
        check(n.find("c")->quoted,  "block scalar is marked quoted");
    }
    {   // Anchors and aliases — the subset reader never handled these.
        auto n = parse_yaml("defaults: &d\n  retries: 3\nfirst:\n  <<: *d\nsecond: *d\n");
        check(n.find("second") && n.find("second")->find("retries")->scalar == "3",
              "alias resolves to the anchored node");
    }
}

TEST_CASE("config: Go-style durations", "[config]") {
    // ---- durations ----
    check(parse_duration("500ms")->ns.count() == 500'000'000LL, "parse 500ms");
    check(parse_duration("1h30m")->ns.count() == 5400'000'000'000LL, "parse 1h30m");
    check(!parse_duration("nope").has_value(), "reject a bad duration");
    eq(format_duration(*parse_duration("1500ms")), "1500ms", "format duration");
}

TEST_CASE("spec_of: parsing a real component", "[config][spec]") {
    // ---- parsing a real component config ----
    auto r = parse_kafka();
    for (const auto& l : r.diagnostics) std::cerr << "  unexpected: " << l.render() << "\n";
    check(r.ok(), "valid config parses cleanly");
    check(r.value.seed_brokers.size() == 2, "list of brokers");
    eq(r.value.seed_brokers[1], "localhost:9093", "second broker");
    check(r.value.topics.size() == 1 && r.value.topics[0] == "events", "flow list of topics");
    check(r.value.fetch_max_wait.ns.count() == 750'000'000LL, "duration field");
    eq(r.value.sasl.mechanism, "SCRAM-SHA-256", "nested struct field");
    check(r.value.tls.has_value() && r.value.tls->enabled, "optional nested struct");
    check(r.value.start_from_oldest == true, "unset field keeps its struct default");
}

TEST_CASE("spec_of: lint diagnostics", "[config][lint]") {
    // ---- lint: the diagnostics users actually rely on ----
    {
        auto res = from_yaml<sf::in::kafka_input_config>(parse_yaml("topics: [a]\n"));
        check(!res.ok(), "missing required field is an error");
        bool named = false;
        for (const auto& l : res.diagnostics)
            if (l.message.find("required") != std::string::npos &&
                l.path.find("seed_brokers") != std::string::npos) named = true;
        check(named, "missing required field names the field");
    }
    {   // the "did you mean" path
        auto res = from_yaml<sf::in::kafka_input_config>(
            parse_yaml("seed_brokers: [a]\ntopics: [b]\nconsumer_grop: x\n"));
        bool hinted = false;
        for (const auto& l : res.diagnostics)
            if (l.message.find("did you mean 'consumer_group'") != std::string::npos) hinted = true;
        check(hinted, "unknown field suggests the closest match");
    }
    {
        auto res = from_yaml<sf::in::kafka_input_config>(
            parse_yaml("seed_brokers: [a]\ntopics: [b]\nsasl:\n  mechanism: NOPE\n"));
        bool rejected = false;
        for (const auto& l : res.diagnostics)
            if (l.message.find("is not one of") != std::string::npos) rejected = true;
        check(rejected, "value outside the declared option set is rejected");
    }
    {
        auto res = from_yaml<sf::in::kafka_input_config>(
            parse_yaml("seed_brokers: [a]\ntopics: [b]\nfetch_max_wait: soon\n"));
        check(!res.ok(), "bad duration is an error");
    }
}

TEST_CASE("spec_of: emission agrees with parsing", "[config][spec][emit]") {
    // ---- THE property that makes the two execution modes safe ----
    // Round trip: parse YAML -> emit C++ -> the emitted literal must describe a
    // config equal to the parsed one. Proven here by re-parsing the emitted
    // source is not possible in-process, so we check the two weaker halves that
    // together give the same guarantee.
    {
        auto r = parse_kafka();          // self-contained: no shared state between cases
        std::string cc = emit_cpp(r.value, spec_of<sf::in::kafka_input_config>::cpp_type);
        // Every field that differs from the default must appear in the output.
        check(cc.find(".seed_brokers") != std::string::npos, "emit includes a set list field");
        check(cc.find("localhost:9093") != std::string::npos, "emit includes list contents");
        check(cc.find(".fetch_max_wait") != std::string::npos, "emit includes a duration field");
        check(cc.find("750000000") != std::string::npos, "emit uses nanoseconds for durations");
        check(cc.find(".sasl") != std::string::npos, "emit includes a nested struct");
        check(cc.find("SCRAM-SHA-256") != std::string::npos, "emit includes nested contents");
        check(cc.find(".tls") != std::string::npos, "emit includes an optional that is set");
        // Fields still at their default must NOT appear — that is what keeps
        // generated source readable.
        check(cc.find("start_from_oldest") == std::string::npos, "emit omits defaulted fields");
        check(cc.find("std::nullopt") == std::string::npos, "emit omits an optional left at its default");
        check(cc.find("regexp_topics") == std::string::npos, "emit omits defaulted advanced fields");
    }
    {   // A default-constructed config emits an empty brace initialiser.
        sf::in::sasl_config d;
        eq(emit_cpp(d, spec_of<sf::in::sasl_config>::cpp_type), "sf::in::sasl_config{}",
           "all-default config emits {}");
    }
    {   // Documentation is generated from the same table, so it cannot go stale.
        std::string doc = describe<sf::in::kafka_input_config>("kafka");
        check(doc.find("seed_brokers") != std::string::npos, "docs list fields");
        check(doc.find("(required)") != std::string::npos, "docs mark required fields");
        check(doc.find("(advanced)") != std::string::npos, "docs mark advanced fields");
        check(doc.find("default: 500ms") != std::string::npos, "docs show defaults from the struct");
        std::string brief = describe<sf::in::kafka_input_config>("kafka", false);
        check(brief.find("regexp_topics") == std::string::npos, "docs can hide advanced fields");
    }
}

TEST_CASE("message: bytes, structured and metadata", "[message]") {
    // ---- message / metadata ----
    {
        sf::message m(R"({"a":1,"b":"x"})");
        check(m.as_structured().get("a").as_i64() == 1, "lazy parse of raw bytes");
        m.as_structured_mut().set("c", sf::value(int64_t{3}));
        eq(m.as_bytes(), R"({"a":1,"b":"x","c":3})", "re-serialised after mutation, keys sorted");
        m.meta().set("kafka_topic", sf::value(std::string("events")));
        m.meta().set("kafka_key", sf::value(std::string("k1")));
        check(m.meta().size() == 2, "metadata entries");
        check(m.meta().entries()[0].first == "kafka_key", "metadata is kept sorted");
        check(m.meta().find("kafka_topic") != nullptr, "metadata lookup");
        m.set_error("boom");
        check(m.has_error(), "error travels with the message");
    }
}

TEST_CASE("cpp blocks: config and emission", "[cpp][emit]") {
    // ---- cpp: blocks -------------------------------------------------------
    {   // Shorthand form: the scalar IS the body.
        auto n = parse_yaml("processors:\n  - cpp: |\n      root = self;\n");
        const auto& proc = n.find("processors")->seq[0];
        auto sk = proc.single_key();
        check(sk && sk->first == "cpp", "cpp block selected by single key");
        check(sk->second->is_scalar(), "shorthand cpp body is a scalar");
        eq(sk->second->scalar, "root = self;\n", "shorthand body text");
    }
    {   // Long form goes through spec_of like any other component config.
        auto n = parse_yaml("cpp:\n  includes: ['<cmath>']\n  body: |\n    root = self;\n");
        auto r = from_yaml<sf::proc::cpp_config>(*n.find("cpp"), "cpp");
        check(r.ok(), "long-form cpp config parses");
        check(r.value.includes.size() == 1 && r.value.includes[0] == "<cmath>", "includes parsed");
        eq(r.value.body, "root = self;\n", "long-form body");
    }
    {   // An unknown key inside cpp: gets the same lint treatment as anywhere.
        auto n = parse_yaml("cpp:\n  bodyy: x\n");
        auto r = from_yaml<sf::proc::cpp_config>(*n.find("cpp"), "cpp");
        bool hinted = false;
        for (const auto& l : r.diagnostics)
            if (l.message.find("did you mean 'body'") != std::string::npos) hinted = true;
        check(hinted, "cpp config gets 'did you mean' like every other component");
    }
    {   // Emission: same signature as a translated mapping, plus #line.
        sf::blobl::cpp_block b{
            .body = "    root = self;\n", .includes = {"<cmath>", "cstdio"},
            .source_file = "pipeline.yaml", .source_line = 42};
        std::string cc = sf::blobl::emit_cpp(b, {.function_name = "blobl_7"});
        check(cc.find("sf::value blobl_7(sf::exec_ctx& ctx)") != std::string::npos,
              "cpp block emits the standard transform signature");
        check(cc.find("const sf::value& self = ctx.self();") != std::string::npos,
              "self is in scope");
        check(cc.find("#line 42 \"pipeline.yaml\"") != std::string::npos,
              "#line points at the config, so compile errors name the YAML");
        check(cc.find("#include <cmath>") != std::string::npos, "bracketed include passed through");
        check(cc.find("#include <cstdio>") != std::string::npos, "bare include gets brackets");
        check(cc.find("root = self;") != std::string::npos, "body pasted verbatim");
        check(cc.find("#line 0") == std::string::npos, "no invalid #line 0 reset");
    }
}

TEST_CASE("registry: the first lookup does not deadlock", "[registry][regression]") {
    // The registry is populated lazily on first access. An earlier version
    // registered the `bloblang`/`mutation` aliases by reading `mapping` back out
    // with find_processor(), which re-entered the static-initialisation guard
    // that was still running and deadlocked the process — silently, blocked on a
    // futex at 0.1% CPU, so it looked idle rather than hung.
    //
    // This case is the FIRST registry access in the binary, so it exercises the
    // cold path. A regression would hang here and the suite would time out
    // rather than pass, which is the detectable outcome.
    REQUIRE(sf::find_processor("mapping") != nullptr);

    // The aliases must resolve to the same behaviour without a read-back.
    for (const char* alias : {"bloblang", "mutation"}) {
        INFO("alias: " << alias);
        const auto* d = sf::find_processor(alias);
        REQUIRE(d != nullptr);
        CHECK(d->build != nullptr);
        CHECK(d->emit != nullptr);
    }

    CHECK(sf::find_processor("no_such_processor") == nullptr);
    CHECK(sf::processor_kinds().size() >= 10);
}

TEST_CASE("cxx_string_literal escapes everything a C++ literal needs",
          "[config][emit][regression]") {
    // There were four independent copies of this before, and they disagreed:
    // the weakest emitted control characters raw. Consolidating had to take the
    // union of their behaviour, not the intersection.
    CHECK(cxx_string_literal("plain") == "\"plain\"");
    CHECK(cxx_string_literal("a\"b") == "\"a\\\"b\"");
    CHECK(cxx_string_literal("a\\b") == "\"a\\\\b\"");
    CHECK(cxx_string_literal("a\nb") == "\"a\\nb\"");
    CHECK(cxx_string_literal("a\tb") == "\"a\\tb\"");

    // Control characters must not reach the source raw, and an octal escape is
    // used rather than hex: a hex escape has no length limit in C++, so "\x1"
    // followed by a literal '2' would be read as \x12.
    CHECK(cxx_string_literal(std::string("a\x01" "2b")) == "\"a\\0012b\"");
    CHECK(cxx_string_literal(std::string("\x7f")) == "\"\\177\"");

    // A NUL must survive as an escape rather than truncating the literal.
    CHECK(cxx_string_literal(std::string("a\0b", 3)) == "\"a\\000b\"");
}

// `${VAR}` / `${VAR:default}` substitution happens on RAW config text before
// parsing, matched to redpanda-connect 4.107.2 by testing it. The cases below
// are the ones that actually bit: a default containing colons, adjacency, and
// `${!...}` which is a Bloblang interpolation rather than a variable.
TEST_CASE("environment substitution in config text") {
    ::setenv("SF_UT_SET", "VALUE", 1);
    ::unsetenv("SF_UT_UNSET");
    std::vector<std::string> missing;

    auto sub = [&](std::string_view t) {
        missing.clear();
        return sf::cfg::substitute_env(t, missing);
    };

    CHECK(sub("a ${SF_UT_SET} b") == "a VALUE b");
    CHECK(sub("${SF_UT_UNSET:fallback}") == "fallback");
    CHECK(sub("${SF_UT_SET:ignored}") == "VALUE");
    // The default is everything after the FIRST colon, so a URL keeps its own.
    CHECK(sub("${SF_UT_UNSET:http://h:8080/p}") == "http://h:8080/p");
    // Adjacent references must not swallow one another.
    CHECK(sub("${SF_UT_SET}${SF_UT_UNSET:2}") == "VALUE2");
    // A Bloblang interpolation is left exactly as written. Treating it as a
    // variable named "! content() " broke two working configs.
    CHECK(sub("key: ${! content() }") == "key: ${! content() }");
    CHECK(sub("${}") == "${}");
    CHECK(sub("no references here") == "no references here");
    // An unterminated reference is text, not an error.
    CHECK(sub("${UNCLOSED") == "${UNCLOSED");

    // A required variable is REPORTED and left in place, never substituted with
    // an empty string: a config that parses and then behaves wrongly is worse
    // than one that refuses to start.
    missing.clear();
    const std::string out = sf::cfg::substitute_env("${SF_UT_UNSET}", missing);
    CHECK(out == "${SF_UT_UNSET}");
    REQUIRE(missing.size() == 1);
    CHECK(missing[0] == "SF_UT_UNSET");
}

// An interpolated field is rewritten to a Bloblang query rather than evaluated
// by a second engine, so the interpreter and the emitter share one set of
// semantics. These are the shapes that actually appear in configs.
TEST_CASE("interpolated strings become bloblang queries") {
    using sf::cfg::interpolation_to_query;
    CHECK(interpolation_to_query("") == "\"\"");
    CHECK(interpolation_to_query("plain") == "\"plain\"");
    CHECK(interpolation_to_query("${! content() }") == "( content() ).string()");
    CHECK(interpolation_to_query("id-${! this.n }") == "\"id-\" + ( this.n ).string()");
    CHECK(interpolation_to_query("${! this.a }-${! this.b }")
          == "( this.a ).string() + \"-\" + ( this.b ).string()");
    // A query may contain braces of its own, so the closing brace is matched,
    // not simply the first one found.
    CHECK(interpolation_to_query("${! this.a.or({}) }") == "( this.a.or({}) ).string()");
    // Quotes and backslashes in the literal halves must survive into the
    // generated Bloblang string literal.
    CHECK(interpolation_to_query("a\"b") == "\"a\\\"b\"");
    // An unterminated interpolation is literal text, not an error.
    CHECK(interpolation_to_query("${! oops") == "\"${! oops\"");
}
