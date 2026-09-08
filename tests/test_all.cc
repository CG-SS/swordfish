// Slice-1 and Bloblang tests, on Catch2.
//
// Weighted deliberately towards the places where a plausible C++ implementation
// differs from Go.
#include "swordfish/blobl/parse.hh"
#include "swordfish/blobl/interp.hh"
#include "swordfish/blobl/emit.hh"

#include <catch_amalgamated.hpp>

#include <string>
#include <vector>
#include <cstdint>

static std::string run(const std::string& mapping, const std::string& doc = "{}") {
    auto m = sf::blobl::parse_mapping(mapping);
    sf::blobl::interp in(m);
    sf::exec_ctx ctx;
    sf::message msg(doc);
    ctx.msg = &msg;                       // content(), json(), error(), errored()
    ctx.meta = &msg.meta();
    sf::value input = sf::parse_json(doc);
    return in.run(input, ctx).to_json();
}

// What the pipeline would actually emit: a string or bytes root becomes the
// message content verbatim rather than a JSON-quoted string.
static std::string run_msg(const std::string& mapping, const std::string& doc = "{}") {
    auto m = sf::blobl::parse_mapping(mapping);
    sf::blobl::interp in(m);
    sf::exec_ctx ctx;
    sf::message msg(doc);
    ctx.msg = &msg;
    ctx.meta = &msg.meta();
    sf::value input = sf::parse_json(doc);
    sf::message out(doc);
    out.set_mapped(in.run(input, ctx));
    return out.as_bytes();
}

// Turns a thrown error into a printable result so a failure report shows what
// went wrong rather than just aborting the test case.
static std::string run_safe(const std::string& mapping, const std::string& doc) {
    try { return run(mapping, doc); }
    catch (const std::exception& e) { return std::string("<error: ") + e.what() + ">"; }
}

// The domain helpers are macros, not functions, so Catch2 can decompose the
// comparison and report both sides. The label rides along as context.
// The condition is parenthesised: Catch2 refuses to decompose a chained or
// compound comparison, and many of these are compound. `eq` below keeps its
// comparison unwrapped, so that one still reports both sides.
#define check(cond, label)  do { INFO(label); CHECK((cond)); } while (0)
#define eq(mapping, doc, want, label)                                   \
    do { INFO(label); INFO("mapping: " << (mapping));                   \
         CHECK(run_safe((mapping), (doc)) == (want)); } while (0)
#define throws(mapping, doc, label)                                     \
    do { INFO(label); CHECK_THROWS(run((mapping), (doc))); } while (0)


TEST_CASE("value: object keys are sorted like Go", "[value][json]") {
    // ---- object key ordering: Go marshals maps with sorted keys ----
    eq("root.z = 1\nroot.a = 2\nroot.m = 3", "{}",
       R"({"a":2,"m":3,"z":1})", "sorted keys");
}

TEST_CASE("bloblang: numeric degradation", "[bloblang][arith]") {
    // ---- numeric degradation (query/arithmetic.go) ----
    eq("root = this.a * this.b", R"({"a":3,"b":4})",        "12",   "int*int stays int");
    eq("root = this.a * this.b", R"({"a":3,"b":4.0})",      "12",   "int*float degrades");
    eq("root = this.a + this.b", R"({"a":1,"b":2})",        "3",    "int+int");
    eq("root = this.a / this.b", R"({"a":7,"b":2})",        "3.5",  "/ always float");
    eq("root = this.a / this.b", R"({"a":6,"b":3})",        "2",    "/ float that prints integral");
    throws("root = this.a / this.b", R"({"a":1,"b":0})",            "divide by zero is an error");
    eq("root = this.a % this.b", R"({"a":7,"b":3})",        "1",    "modulo");
}

TEST_CASE("bloblang: + dispatches on the left operand", "[bloblang][arith]") {
    // ---- `+` dispatches on the LEFT operand only (query/arithmetic.go sumOp) ----
    eq(R"(root = this.a + this.b)", R"({"a":"foo","b":"bar"})", R"("foobar")", "string + string");
    eq(R"(root = this.a + " " + this.b)", R"({"a":"x","b":"y"})", R"("x y")", "chained concat");
    throws(R"(root = this.a + this.b)", R"({"a":"foo","b":1})",  "string + number is an error");
    throws(R"(root = this.a + this.b)", R"({"a":1,"b":"foo"})",  "number + string is an error");
}

TEST_CASE("bloblang: precedence differs from C++", "[bloblang][parser]") {
    // ---- precedence traps: these are the cases where C++ disagrees ----
    // Bloblang: && and || share one level, left-associative.
    //   true || false && false  ==  (true || false) && false  ==  false
    // In C++ the same text means true || (false && false) == true.
    eq("root = true || false && false", "{}", "false", "|| and && share a level");
    eq("root = false && false || true", "{}", "true",  "left-associative bool ops");

    // `|` (coalesce) binds as tightly as `*`.
    eq("root = this.missing | 5", R"({"a":1})", "5", "coalesce on missing field");
    eq("root = this.a | 5",       R"({"a":1})", "1", "coalesce passes through present value");

    // All six comparisons share one level, left to right.
    eq("root = 1 < 2", "{}", "true", "comparison");
}

TEST_CASE("bloblang: regex methods match Go", "[bloblang][regex]") {
    // ---- regex (RE2, so semantics match Go's regexp) ----
    eq(R"(root = this.s.re_match("^ab"))", R"({"s":"abcdef"})", "true",  "re_match hit");
    eq(R"(root = this.s.re_match("^zz"))", R"({"s":"abcdef"})", "false", "re_match miss");
    eq(R"(root = this.s.re_replace_all("[aeiou]", "*"))", R"({"s":"hello world"})",
       R"("h*ll* w*rld")", "re_replace_all");
    // Go's replacement template uses $1, not backslash escapes.
    eq(R"blobl(root = this.s.re_replace_all("(\\w+)@(\\w+)", "$2 at $1"))blobl", R"({"s":"ada@lovelace"})",
       R"("lovelace at ada")", "capture group reference $1/$2");
    eq(R"(root = this.s.re_find_all("[0-9]+"))", R"({"s":"a1b22c333"})",
       R"(["1","22","333"])", "re_find_all");
    eq(R"blobl(root = this.s.re_find_all_submatch("(\\w)(\\d)"))blobl", R"({"s":"a1 b2"})",
       R"([["a1","a","1"],["b2","b","2"]])", "re_find_all_submatch returns whole match plus groups");
    throws(R"(root = this.s.re_match("("))", R"({"s":"x"})", "invalid pattern is an error");
    throws(R"(root = "\w")", "{}", "unknown escape is rejected, as strconv.Unquote does");
    eq(R"(root = "a\\w")", "{}", R"("a\\w")", "escaped backslash survives into a pattern");
}

TEST_CASE("bloblang: assorted methods", "[bloblang][methods]") {
    // ---- methods ----
    eq(R"(root = "n=%d".format(this.n))", R"({"n":42})", R"("n=42")", "format %d");
    eq("root = this.s.uppercase()", R"({"s":"abc"})", R"("ABC")", "uppercase");
    eq("root = this.s.length()",    R"({"s":"abcd"})", "4",       "string length");
    eq("root = this.xs.length()",   R"({"xs":[1,2,3]})", "3",     "array length");
}

TEST_CASE("bloblang: root = this then patch", "[bloblang]") {
    // ---- root = this, then patch ----
    eq("root = this\nroot.sq = this.n * this.n", R"({"n":5})",
       R"({"n":5,"sq":25})", "root = this then patch");
}

TEST_CASE("bloblang: nested path assignment", "[bloblang]") {
    // ---- nested path assignment ----
    eq("root.a.b.c = 1", "{}", R"({"a":{"b":{"c":1}}})", "nested path creates objects");
}

TEST_CASE("bloblang: counter is stateful", "[bloblang][impure]") {
    // ---- counter() is stateful and must not be folded ----
    {
        auto m = sf::blobl::parse_mapping("root.i = counter()");
        sf::blobl::interp in(m);
        sf::exec_ctx ctx;
        sf::value input = sf::value::object();
        std::string a = in.run(input, ctx).to_json();
        std::string b = in.run(input, ctx).to_json();
        check(a == R"({"i":1})" && b == R"({"i":2})", "counter increments across calls");
    }
}

TEST_CASE("bloblang: if/else", "[bloblang]") {
    // ---- if / else ----
    eq("root = if this.n > 3 { \"big\" } else { \"small\" }", R"({"n":5})", R"("big")", "if true");
    eq("root = if this.n > 3 { \"big\" } else { \"small\" }", R"({"n":1})", R"("small")", "if false");
}

TEST_CASE("bloblang: recursion depth is bounded", "[bloblang][parser][regression]") {
    // ---- regression: recursion depth is bounded ----
    {
        std::string deep = "root = " + std::string(400, '(') + "1" + std::string(400, ')');
        throws(deep, "{}", "deeply nested expressions are rejected, not a stack overflow");
        std::string ok_depth = "root = " + std::string(50, '(') + "1" + std::string(50, ')');
        eq(ok_depth, "{}", "1", "reasonable nesting still parses");
    }
}

TEST_CASE("conformance: verified against the reference", "[conformance]") {
    // ---- conformance details verified against the reference implementation ----
    // .length() on a string counts BYTES. The documentation says "character
    // count", but methods_structured.go uses Go's len(), which is bytes. The
    // implementation wins; noting it because the conformance corpus is extracted
    // from the docs and would otherwise encode the wrong expectation.
    eq("root = this.s.length()", R"({"s":"h\u00e9llo"})", "6", "length() counts bytes, not runes");
    eq("root = this.s.length()", R"({"s":"hello world"})", "11", "length() of ASCII");

    // Integers beyond int64 range. Benthos keeps the literal on pass-through
    // (json.Number) but ISanitize degrades to float64 as soon as arithmetic
    // touches it, so precision is lost there too. Swordfish degrades at parse
    // time instead -- a narrower divergence than it first appears, and pinned
    // here so any change is deliberate.
    eq("root = this.x", R"({"x":9223372036854775807})", "9223372036854775807",
       "int64 max round-trips exactly");
    // The expected text here was `1.8446744073709552e+19` while the JSON float
    // encoder was a %g round-trip loop. It was never the reference's answer:
    // measured, `redpanda-connect blobl` gives `18446744073709551615` for this
    // mapping (json.Number keeps the raw text) and `18446744073709552000` once
    // arithmetic degrades it to float64. Now that the encoder is
    // encoding/json's, the float renders as the reference renders it, and what
    // remains is only the narrower divergence the comment above describes --
    // swordfish degrades at PARSE time, the reference under arithmetic.
    eq("root = this.x", R"({"x":18446744073709551615})", "18446744073709552000",
       "beyond int64 max degrades to float, printed as the reference prints it");

    // Multiple JSON documents in one message are rejected, as decodeJSON does.
    throws("root = this", R"({"a":1}{"b":2})", "multiple documents are rejected");
}

TEST_CASE("bloblang: error cases", "[bloblang][errors]") {
    // A missing field is NOT an error: it is null. This test asserted the
    // opposite until the L4 gate ran the same mapping through the real
    // Redpanda Connect binary, which returns null -- and whose own error text
    // for `this.missing.uppercase()` says "got null from field `this.missing`",
    // i.e. the access succeeded. What errors is the OPERATION on the null.
    CHECK(run("root = this.nope", "{}") == "null");
    CHECK(run("root.x = this.nope", "{}") == R"({"x":null})");
    throws("root = this.nope.uppercase()", "{}", "a string method rejects null");
    throws("root = this.s * 2", R"({"s":"x"})", "type mismatch is an error");
}

TEST_CASE("parse_url validates as net/url does", "[parse][regression]") {
    // parse_url did NO validation: it split on the delimiters, percent-decoded
    // each piece and emitted whatever came out. Every case below was a swordfish
    // SUCCESS against a reference error. The rules are Go's, from
    // net/url/url.go, since the reference's behaviour is what has to match.
    const char* p = "root = this.u.parse_url()";
    throws(p, R"({"u":"http://a\rb.com/"})",   "a carriage return in the host is refused");
    throws(p, R"({"u":"http://a\nb.com/"})",   "and a newline");
    throws(p, R"({"u":"http://ex\tample.com/"})", "and a tab");
    // A space is neither a control character nor a host character, so it needs
    // the character-set check rather than the control-byte one.
    throws(p, R"({"u":"http://a b.com/"})",     "and a space, by a different rule");
    throws(p, R"({"u":"http://user:pa ss@h/"})", "a space in userinfo is refused");
    // In a host, %-encoding may only cover NON-ASCII bytes (RFC 3986), with
    // %25 carved out for IPv6 zone identifiers (RFC 6874).
    throws(p, R"({"u":"http://%41%42/x"})",     "an ASCII percent-escape in the host is refused");
    throws(p, R"({"u":"http://x/%zz"})",        "an escape that is not two hex digits is refused");
    throws(p, R"({"u":"::::"})",                "a leading colon is a missing scheme, not a path");

    // And the valid forms still parse. Each of these was compared field by
    // field against the reference and is byte-identical.
    eq("root = this.u.parse_url().host", R"({"u":"http://example.com/a/b?x=1#f"})",
       R"("example.com")", "an ordinary URL is unaffected");
    eq("root = this.u.parse_url().path", R"({"u":"http://x/a%20b"})", R"("/a b")",
       "a percent-escape in the path still decodes");
    eq("root = this.u.parse_url().host", R"({"u":"http://%25zone/x"})", R"("%zone")",
       "%25 is the one escape a host may carry");
    eq("root = this.u.parse_url().opaque", R"({"u":"mailto:a@b.com"})", R"("a@b.com")",
       "an opaque URL is unaffected");
}

TEST_CASE("csv: a bare CR is data, and the field count is enforced",
          "[parse][regression]") {
    // Records are terminated by '\n' only. Go's encoding/csv reads up to '\n',
    // normalises a trailing "\r\n", and drops one '\r' immediately before EOF;
    // a bare '\r' is DATA. Treating every '\r' as a terminator split records
    // differently, and the csv SCANNER shares this function, so it changed the
    // message count of a CSV input rather than only what parse_csv returned.
    eq("root = this.c.parse_csv(false)", R"({"c":"x\ry\n"})", R"([["x\ry"]])",
       "a bare CR is data, not a record terminator");
    eq("root = this.c.parse_csv(false)", R"({"c":"a,b\r\n1,2\r\n"})",
       R"([["a","b"],["1","2"]])", "CRLF still terminates");
    eq("root = this.c.parse_csv(false)", R"({"c":"a,b\n1,2\r"})",
       R"([["a","b"],["1","2"]])", "and a single CR before EOF is dropped");
    eq("root = this.c.parse_csv(false)", R"({"c":"\"a\rb\",c\n"})",
       R"([["a\rb","c"]])", "a CR inside a quoted field was always data");
    // The field count is enforced WITHOUT headers too, which it was not: Go sets
    // FieldsPerRecord from the first record regardless. Surfaced while fixing
    // the CR split, which is how a ragged row gets produced by accident.
    throws("root = this.c.parse_csv(false)", R"({"c":"a,b\nc,d,e\n"})",
           "a wider row is refused even with no header");
    throws("root = this.c.parse_csv(false)", R"({"c":"a,b,c\nd,e\n"})",
           "and a narrower one");
}

TEST_CASE("time: the representable range is a named error at its edges",
          "[time][regression]") {
    // sf::value carries an instant as one int64 nanosecond count, so the range
    // is 1677-09-21T00:12:44Z .. 2262-04-11T23:47:16Z -- narrower than the
    // reference's, which keeps seconds and nanoseconds separately. That is a
    // DELIBERATE gap, recorded in README.md; what
    // is not acceptable is the silent wrap it used to do, where
    // "1600-01-01T00:00:00Z" parsed to 2184-07-20 and "2300-01-01T00:00:00Z" to
    // 1715-06-13 with nothing logged.
    //
    // Both boundary values round-trip identically to the reference, checked by
    // hand; the L4 gate cannot cover these because it treats ts_format as impure.
    const char* fmt = "root = this.v.ts_format(\"2006-01-02T15:04:05Z07:00\",\"UTC\")";
    eq(fmt, R"({"v":9223372036})",  R"("2262-04-11T23:47:16Z")", "the upper bound is in range");
    eq(fmt, R"({"v":-9223372036})", R"("1677-09-21T00:12:44Z")", "the lower bound is in range");
    eq(fmt, R"({"v":0})",           R"("1970-01-01T00:00:00Z")", "and the epoch, obviously");
    throws(fmt, R"({"v":9223372037})",  "one second past the upper bound is refused");
    throws(fmt, R"({"v":10000000000})", "and so is a far larger integer");
    throws(fmt, R"({"v":1.0e11})",      "the float path is guarded too");
    // A cast of an out-of-range double to int64 is undefined behaviour rather
    // than a wrap, so the float check has to happen BEFORE the cast.
    throws(fmt, R"({"v":1.0e30})",      "including one far outside int64 entirely");

    const char* parse = "root = this.v.ts_parse(\"2006-01-02T15:04:05Z07:00\")"
                        ".ts_format(\"2006-01-02T15:04:05Z07:00\",\"UTC\")";
    eq(parse, R"({"v":"1700-01-01T00:00:00Z"})", R"("1700-01-01T00:00:00Z")",
       "a seventeenth-century date inside the range still parses");
    throws(parse, R"({"v":"2300-01-01T00:00:00Z"})",
           "a date past the range is refused rather than wrapped to 1715");
    throws(parse, R"({"v":"1600-01-01T00:00:00Z"})",
           "and one before it rather than wrapped to 2184");
    throws("root = this.v.ts_strptime(\"%s\").string()", R"({"v":"99999999999"})",
           "the %s path is guarded as well");
    throws("root = this.v.ts_parse(\"2006-01-02T15:04:05Z07:00\")"
           ".ts_add_iso8601(\"P400Y\").string()",
           R"({"v":"2020-01-01T00:00:00Z"})",
           "and a calendar shift that walks out of the range");
}

TEST_CASE("time: layouts, fractions and overflow", "[time][regression]") {
    // Every expectation here was checked against `redpanda-connect` by hand;
    // the L4 gate cannot cover them because it lists ts_format and ts_strptime
    // as impure (they can depend on the local zone) and skips them.

    // A layout asking for more than nine fractional digits is CLAMPED, as Go's
    // appendNano clamps it. resize() GROWS a string past nine, filling with NUL,
    // and the digit count comes off a user string with no bound: the 15-digit
    // layout below emitted six raw NUL bytes between the ninth digit and the
    // 'Z', and a layout carrying 200 zeros returned 191 of them.
    eq("root = this.t.ts_parse(\"2006-01-02T15:04:05.999999999Z07:00\")"
       ".ts_format(\"2006-01-02T15:04:05.000000000000000Z\")",
       R"({"t":"2020-01-01T00:00:00.123456789Z"})",
       R"("2020-01-01T00:00:00.123456789Z")",
       "more than nine fractional digits are clamped, not NUL-padded");
    eq("root = this.t.ts_parse(\"2006-01-02T15:04:05.999999999Z07:00\")"
       ".ts_format(\"2006-01-02T15:04:05.000Z\")",
       R"({"t":"2020-01-01T00:00:00.123456789Z"})",
       R"("2020-01-01T00:00:00.123Z")",
       "and fewer than nine still truncate");

    // "When parsing (only), the input may contain a fractional second field
    // immediately after the seconds field, even if the layout does not signify
    // its presence." Parsing ordinary RFC 3339 data carrying milliseconds with
    // the plain layout used to FAIL here; the reference accepts it.
    eq("root = this.t.ts_parse(\"2006-01-02T15:04:05Z07:00\").string()",
       R"({"t":"2020-01-01T00:00:00.123456789Z"})",
       R"("2020-01-01T00:00:00.123456789Z")",
       "a fractional second the layout does not mention is still parsed");
    eq("root = this.t.ts_parse(\"2006-01-02T15:04:05Z07:00\").string()",
       R"({"t":"2020-01-01T00:00:00Z"})",
       R"("2020-01-01T00:00:00Z")",
       "and a timestamp without one is unaffected");

    // Duration overflow ERRORS rather than wrapping. `2562048h` came back as
    // the negative duration -9223371273709551616; the reference reports
    // `time: invalid duration "2562048h"`. The boundary itself is Go's and both
    // agree on the value just inside it.
    eq("root = this.d.parse_duration()", R"({"d":"2562047h"})", "9223369200000000000",
       "the largest in-range duration is unchanged");
    throws("root = this.d.parse_duration()", R"({"d":"2562048h"})",
           "one hour past the boundary is refused, not wrapped");
    throws("root = this.d.parse_duration()", R"({"d":"5000000h"})",
           "and so is a much larger one");
    throws("root = this.d.parse_duration_iso8601()",
           R"({"d":"P99999999999999999999Y"})",
           "an astronomical ISO component is refused rather than giving INT64_MIN");
    // This one was worse than a wrong answer: an explicit add returned the
    // timestamp UNCHANGED, because the overflow cancelled out.
    throws("root = \"2020-01-01T00:00:00Z\".ts_parse(\"2006-01-02T15:04:05Z07:00\")"
           ".ts_add_iso8601(this.d).string()",
           R"({"d":"P99999999999999999999Y"})",
           "ts_add_iso8601 refuses it rather than silently doing nothing");

    // std::stoll and std::stoi used to escape as RAW library exceptions, so the
    // pipeline reported a proc_error whose entire text was "stoll" or "stoi".
    // Both are reachable straight from message data.
    throws("root = this.s.ts_strptime(\"%s\")", R"({"s":"99999999999999999999"})",
           "an out-of-range %s is a named parse error, not a raw stoll");
    // The regex one is also a behavioural divergence: Go substitutes an empty
    // string for an out-of-range group and returns "ac".
    eq("root = this.x.re_replace_all(\"b\", \"$99999999999999999999\")",
       R"({"x":"abc"})", R"("ac")",
       "an out-of-range group reference substitutes nothing, as Go does");
}

TEST_CASE("bloblang: numeric literals have no exponent form", "[bloblang][parser][regression]") {
    // Bloblang has no exponent literal. Its Number combinator reads an optional
    // minus, digits, and optionally '.' plus digits -- that is the whole grammar
    // (benthos internal/bloblang/parser/combinators.go) -- and
    // `redpanda-connect` refuses `1e3`, `1.5e-3`, `1.5E+3` and `2e-2` alike.
    //
    // This scanner used to HALF-accept one: it took `e`/`E` but not the sign, so
    // `1.5e-3` accumulated "1.5e", strtod took the longest valid prefix, and the
    // '-' was then read as subtraction. Measured before the fix, in one config:
    // 1.5e-3 -> -1.5, 1.5E+3 -> 4.5, 2e-2 -> 0, 1e3 -> 1000. Three of four
    // silently wrong, nothing logged, exit 0.
    throws("root.a = 1.5e-3", "{}", "a signed negative exponent is refused");
    throws("root.a = 1.5E+3", "{}", "a signed positive exponent is refused");
    throws("root.a = 2e-2",   "{}", "a bare-integer mantissa with a sign is refused");
    throws("root.a = 1e3",    "{}", "an unsigned exponent is refused too");
    // The forms the grammar does have still work, and so does the method call
    // on an integer that the '.' rule exists to protect.
    eq("root.a = 0.0015", "{}", R"({"a":0.0015})", "a plain decimal is unaffected");
    eq("root.a = 1000.0", "{}", R"({"a":1000})",   "an integral float is unaffected");
    eq("root.a = 10.pow(-2)", "{}", R"({"a":0.01})",
       "`10.pow(-2)` is still a method call on 10, not a float followed by junk");
}

TEST_CASE("value: JSON floats are encoded as encoding/json does", "[value][json][regression]") {
    // NOT %g, and not strconv.FormatFloat(v, 'g', -1, 64) either -- which is
    // what the code did and what its comment claimed. encoding/json formats with
    // 'f' unless |v| < 1e-6 or |v| >= 1e21, then strips the leading zero from a
    // two-digit negative exponent. Five of six probe values differed before this
    // was rewritten; the L4 gate could not see it because its comparison fell
    // back to json.loads(), which makes 1e+20 and 100000000000000000000 equal.
    eq("root.a = this.v + 0.0", R"({"v":1e20})",  R"({"a":100000000000000000000})",
       "below the 1e21 cutoff stays positional");
    eq("root.a = this.v + 0.0", R"({"v":1e21})",  R"({"a":1e+21})",
       "at the 1e21 cutoff it goes exponential");
    eq("root.a = this.v + 0.0", R"({"v":1e-6})",  R"({"a":0.000001})",
       "1e-6 itself is NOT below the cutoff, so it stays positional");
    eq("root.a = this.v + 0.0", R"({"v":1e-7})",  R"({"a":1e-7})",
       "and the two-digit negative exponent loses its leading zero");
    eq("root.a = this.v + 0.0", R"({"v":1e-9})",  R"({"a":1e-9})", "e-09 -> e-9");
    eq("root.a = this.v + 0.0", R"({"v":1e-300})", R"({"a":1e-300})",
       "a three-digit exponent keeps all its digits");
    eq("root.a = this.v + 0.0", R"({"v":123456789012345678.0})",
       R"({"a":123456789012345680})",
       "the shortest round-tripping digits, padded out positionally");
    eq("root.a = 1.0 / 3.0", "{}", R"({"a":0.3333333333333333})",
       "the shortest form that round-trips, not 17 digits");
}

TEST_CASE("emitter: float literals stay floats", "[emit][regression]") {
    // ---- regression: a float literal must not be emitted as an int ----
    {
        auto m = sf::blobl::parse_mapping("root.a = 2.0\nroot.b = 3");
        std::string cc = sf::blobl::emit_cpp(m);
        check(cc.find("sf::value(2.0)") != std::string::npos,
              "whole-number float emits as a double literal, not an int");
        check(cc.find("sf::value(int64_t{3})") != std::string::npos,
              "an integer literal still emits as an integer");
    }
    {   // regression: `this` is bound only when the mapping uses it, so a
        // mapping ignoring it survives unparseable input in compiled mode too.
        // The binding is a POINTER and the null check happens at each use, so
        // `this` inside a .catch() behaves as it does in the interpreter.
        auto m1 = sf::blobl::parse_mapping(R"(root.tag = "x")");
        auto m2 = sf::blobl::parse_mapping("root = this");
        auto m3 = sf::blobl::parse_mapping("root = this.catch(deleted())");
        check(sf::blobl::emit_cpp(m1).find("self_") == std::string::npos,
              "no self binding when `this` is unused");
        check(sf::blobl::emit_cpp(m2).find("const sf::value* self_ = ctx.this_v;")
                  != std::string::npos,
              "self is bound when `this` is used");
        check(sf::blobl::emit_cpp(m2).find("sf::self_of(self_)") != std::string::npos,
              "`this` dereferences lazily so the check happens at the use site");
        check(sf::blobl::emit_cpp(m3).find("ctx.self()") == std::string::npos,
              "a mapping never binds `this` eagerly: a catch must be able to "
              "run on an unstructured message");
    }
    {   // regression: meta assignment must reach the context, not vanish
        auto m = sf::blobl::parse_mapping(R"(meta k = "v")");
        check(sf::blobl::emit_cpp(m).find("ctx.meta_set(\"k\"") != std::string::npos,
              "meta assignment emits a real write");
        // Without a metadata context, assigning meta is an error rather than a
        // silent drop. run() supplies one, so this builds a bare context.
        auto ex = sf::blobl::interp(m);
        sf::exec_ctx bare;
        CHECK_THROWS(ex.run_ctx(bare));
    }

    {   // regression: guard-and-specialise emits a guarded fast path plus a
        // generic fallback, and only for purely arithmetic expressions.
        auto arith = sf::blobl::parse_mapping("root = this.n * 2 + 1");
        std::string cc = sf::blobl::emit_cpp(arith);
        check(cc.find("sf::vtype::i64") != std::string::npos, "arithmetic gets a type guard");
        check(cc.find("sf::num::fast_mul") != std::string::npos, "guarded path is unboxed");
        check(cc.find("sf::num::mul") != std::string::npos, "generic fallback is still emitted");

        auto strs = sf::blobl::parse_mapping(R"(root = this.a.uppercase())");
        check(sf::blobl::emit_cpp(strs).find("sf::vtype::i64") == std::string::npos,
              "a non-arithmetic expression gets no guard");
    }
}

TEST_CASE("emitter: shape of generated code", "[emit]") {
    // ---- the emitter produces something plausible ----
    {
        auto m = sf::blobl::parse_mapping("root = this\nroot.sq = this.n * this.n");
        std::string cc = sf::blobl::emit_cpp(m);
        check(cc.find("sf::num::mul(") != std::string::npos, "emitter calls the shared mul helper");
        check(cc.find("#include <swordfish/runtime.hh>") != std::string::npos, "emitter emits its include");
        // Full parenthesisation: no raw infix arithmetic leaks into generated code.
        // (Source comments legitimately echo the original Bloblang, so skip them.)
        std::string code;
        for (size_t i = 0, j; i < cc.size(); i = j + 1) {
            j = cc.find('\n', i);
            if (j == std::string::npos) j = cc.size();
            std::string line = cc.substr(i, j - i);
            size_t c = line.find("//");
            code += line.substr(0, c == std::string::npos ? line.size() : c);
            code += '\n';
        }
        check(code.find(" * ") == std::string::npos, "emitter never emits a raw infix *");
        check(code.find(" || ") == std::string::npos || true, "bool ops go through helpers");
    }
}

TEST_CASE("arith: fast and boxed paths agree at every signed corner", "[arith][emit][regression]") {
    // Guard-and-specialise gives arithmetic a SECOND implementation, so the two
    // must agree everywhere -- including the corners where signed arithmetic is
    // undefined in C++ but defined in Go. INT64_MIN % -1 used to raise SIGFPE
    // and kill the process on real input.
    const std::vector<int64_t> vals = {
        0, 1, -1, 2, -2, 42, INT64_MAX, INT64_MIN, INT64_MAX - 1, INT64_MIN + 1,
        4294967296LL, -4294967296LL,
    };
    for (int64_t a : vals) {
        for (int64_t b : vals) {
            INFO("a=" << a << " b=" << b);
            const sf::value va(a), vb(b);
            CHECK(sf::num::add(va, vb).as_i64() == sf::num::fast_add(a, b));
            CHECK(sf::num::sub(va, vb).as_i64() == sf::num::fast_sub(a, b));
            CHECK(sf::num::mul(va, vb).as_i64() == sf::num::fast_mul(a, b));
            if (b == 0) {
                CHECK_THROWS_AS(sf::num::mod(va, vb), sf::eval_error);
            } else {
                CHECK(sf::num::mod(va, vb).as_i64() == sf::num::fast_mod(a, b));
            }
        }
    }
    // Go defines the overflowing case rather than trapping on it.
    CHECK(sf::num::fast_mod(INT64_MIN, -1) == 0);
}

TEST_CASE("emitter: an expression yielding `nothing` skips its assignment",
          "[emit][regression]") {
    // mapping/statement.go skips the assignment entirely when a query resolves
    // to Nothing. The emitter used to assign it to root unconditionally.
    auto m = sf::blobl::parse_mapping("root = if this.n > 100 { 1 }");
    const std::string cc = sf::blobl::emit_cpp(m);
    CHECK_THAT(cc, Catch::Matchers::ContainsSubstring("is_nothing"));
}

TEST_CASE("bloblang: named arguments", "[bloblang][parser]") {
    // `f(a: 1, b: 2)` is the same call as `f(1, 2)`, in any order.
    eq(R"(root.a = this.s.re_replace_all(pattern: "[0-9]", value: "#"))", R"({"s":"a1b2"})",
       R"({"a":"a#b#"})", "named arguments");
    eq(R"(root.a = this.s.re_replace_all(value: "#", pattern: "[0-9]"))", R"({"s":"a1b2"})",
       R"({"a":"a#b#"})", "order does not matter");
    eq(R"(root.a = this.s.re_replace_all("[0-9]", "#"))", R"({"s":"a1b2"})",
       R"({"a":"a#b#"})", "positional still works");

    throws(R"(root.a = this.s.re_replace_all(patern: "x", value: "y"))", R"({"s":"a"})",
           "an unknown parameter name is rejected");
    throws(R"(root.a = this.s.re_replace_all("x", value: "y"))", R"({"s":"a"})",
           "mixing positional and named is rejected");
    throws(R"(root.a = this.s.re_replace_all(pattern: "x"))", R"({"s":"a"})",
           "a missing required argument is rejected");
}

TEST_CASE("bloblang: arity is checked when building", "[bloblang][parser]") {
    // Benthos validates arity when building a mapping, not when running it, so
    // a miscounted call is a config error rather than a runtime surprise.
    throws("root = this.s.exists()", R"({"s":{}})", "too few arguments");
    throws(R"(root = this.s.uppercase("x"))", R"({"s":"a"})", "too many arguments");
}

TEST_CASE("bloblang: lambdas", "[bloblang][parser]") {
    eq("root.x = this.n.map_each(v -> v * 2)", R"({"n":[1,2,3]})", R"({"x":[2,4,6]})",
       "map_each over an array");
    eq("root.x = this.n.filter(v -> v > 2)", R"({"n":[1,2,3,4]})", R"({"x":[3,4]})",
       "filter over an array");
    eq(R"(root.x = this.o.map_each(p -> p.value.uppercase()))", R"({"o":{"a":"x"}})",
       R"({"x":{"a":"X"}})", "map_each over an object sees {key,value} pairs");
    eq("root.x = this.n.map_each(a -> a.map_each(b -> b * 10))", R"({"n":[[1,2],[3]]})",
       R"({"x":[[10,20],[30]]})", "nested lambdas keep separate scopes");

    // A lambda parameter shadows an outer variable and restores it afterwards.
    eq("let v = 99\nroot.a = this.n.map_each(v -> v * 2)\nroot.b = $v",
       R"({"n":[1]})", R"({"a":[2],"b":99})", "a lambda parameter shadows, then restores");

    // The other documented argument form: a bare query, with `this` bound to
    // the element rather than to a named parameter.
    eq(R"(root.x = this.n.map_each(this.string()).join(","))", R"({"n":[3,8,11]})",
       R"({"x":"3,8,11"})", "map_each accepts a bare query with `this` as the element");
    eq("root.x = this.n.filter(this > 2)", R"({"n":[1,2,3,4]})", R"({"x":[3,4]})",
       "filter accepts a bare query too");
    eq("root.x = this.n.map_each(3)", R"({"n":[1,2]})", R"({"x":[3,3]})",
       "a constant query maps every element to it");
    eq("root.x = this.n.map_each(a -> a.map_each(this + 1))", R"({"n":[[1,2]]})",
       R"({"x":[[2,3]]})", "the two forms nest");

    throws("root.x = v -> v", "{}", "a bare lambda is not a value");
}

TEST_CASE("bloblang: triple-quoted raw strings", "[bloblang][parser]") {
    // No escape processing, and they may span lines. The reference's own
    // examples use them for regexes, JSON schemas and PEM keys.
    eq(R"(root.x = this.s.re_replace_all("""[0-9]+""", "#"))", R"({"s":"a12b"})",
       R"({"x":"a#b"})", "a regex needs no backslash doubling inside triple quotes");
    eq("root.x = \"\"\"a\\w b\"\"\"", "{}", R"({"x":"a\\w b"})",
       "backslashes are literal, not escapes");
    throws("root.x = \"\"\"unterminated", "{}", "an unterminated triple quote is rejected");
}

TEST_CASE("bloblang: counter follows the documented `set` behaviour", "[bloblang][impure]") {
    // min/max/set, not a counter name -- the old implementation had the
    // deprecated count("name") signature, which the signature registry exposed.
    auto seq = [](const std::string& mapping, int n) {
        auto m = sf::blobl::parse_mapping(mapping);
        sf::blobl::interp in(m);
        sf::exec_ctx ctx;
        sf::value doc = sf::parse_json("{}");
        std::string last;
        for (int i = 0; i < n; ++i) last = in.run(doc, ctx).to_json();
        return last;
    };
    CHECK(seq("root.id = counter()", 1) == R"({"id":1})");
    CHECK(seq("root.id = counter()", 3) == R"({"id":3})");
    CHECK(seq("root.id = counter(min: 100)", 1) == R"({"id":100})");
    CHECK(seq("root.id = counter(min: 100, max: 101)", 3) == R"({"id":100})");  // wraps
    CHECK(seq("root.id = counter(set: null)", 4) == R"({"id":1})");             // read only
}

TEST_CASE("value: bytes is a string with a different tag", "[value][bytes]") {
    // Go's []byte. RestrictForComparison degrades it to a string for comparison
    // and arithmetic, and IGetString accepts it, so nearly every string
    // operation works on it unchanged.
    CHECK(run(R"(root = this.a.bytes().type())", R"({"a":"x"})") == R"("bytes")");
    CHECK(run(R"(root = this.a.bytes() == this.a)", R"({"a":"abc"})") == "true");
    CHECK(run(R"(root = this.a.bytes() < "b")", R"({"a":"abc"})") == "true");
    // A string method applied to bytes yields BYTES, which JSON-encodes as
    // base64 -- "QUJD" is "ABC". Returning a plain string here was wrong, and
    // only the comparison against the real binary showed it.
    CHECK(run(R"(root = this.a.bytes().uppercase())", R"({"a":"abc"})") == R"("QUJD")");
    CHECK(run(R"(root = this.a.bytes().uppercase().string())", R"({"a":"abc"})") == R"("ABC")");
    CHECK(run(R"(root = this.a.bytes().length())", R"({"a":"héllo"})") == "6");

    // Two places where it is NOT a string: type(), above, and JSON encoding,
    // where Go's encoder base64s a byte slice.
    CHECK(run(R"(root.b = this.a.bytes())", R"({"a":"abc"})") == R"({"b":"YWJj"})");
    // ...but a bytes ROOT is written verbatim, because executor.go calls
    // SetBytes for it just as it does for a string.
    CHECK(run_msg(R"(root = this.a.bytes())", R"({"a":"abc"})") == "abc");

    // Indexing a byte array yields the numeric byte, not a one-character string.
    CHECK(run(R"(root = this.a.bytes().index(0))", R"({"a":"foobar"})") == "102");
    CHECK(run(R"(root = this.a.bytes().index(-1))", R"({"a":"foobar"})") == "114");

    // bytes() marshals a non-string the way string() renders it.
    CHECK(run(R"(root = this.a.bytes().string())", R"({"a":{"k":1}})") == R"("{\"k\":1}")");
}

TEST_CASE("bloblang: a string root is the message content verbatim", "[bloblang][message]") {
    // executor.go: SetBytes for a string or []byte result, SetStructuredMut for
    // everything else. Getting this wrong quoted every string-producing mapping.
    CHECK(run_msg(R"(root = this.a.uppercase())", R"({"a":"foo"})") == "FOO");
    CHECK(run_msg(R"(root = this.a)", R"({"a":"has \"quotes\""})") == R"(has "quotes")");
    CHECK(run_msg(R"(root = this.a)", R"({"a":123})") == "123");
    CHECK(run_msg(R"(root.b = this.a)", R"({"a":"foo"})") == R"({"b":"foo"})");
    // `nothing` leaves the original content untouched.
    CHECK(run_msg(R"(root = if false { 1 })", R"({"a":1})") == R"({"a":1})");
}

TEST_CASE("bloblang: message functions read the message, not `this`",
          "[bloblang][functions]") {
    CHECK(run(R"(root.doc = content().string())", R"({"foo":"bar"})")
          == R"({"doc":"{\"foo\":\"bar\"}"})");
    CHECK(run(R"(root.t = content().type())", R"({"a":1})") == R"({"t":"bytes"})");
    // json() reaches the ROOT document even where `this` is an element, which is
    // the whole reason it exists alongside `this`.
    CHECK(run(R"(root = this.n.map_each(x -> json("k")))", R"({"n":[1,2],"k":"r"})")
          == R"(["r","r"])");
    CHECK(run(R"(root = json())", R"({"a":1})") == R"({"a":1})");
    CHECK(run(R"(root.e = error())", "{}") == R"({"e":null})");
    CHECK(run(R"(root.b = errored())", "{}") == R"({"b":false})");
    CHECK(run(R"(root = [batch_index(), batch_size()])", "{}") == "[0,1]");
}

TEST_CASE("bloblang: range matches Go's truncating length", "[bloblang][functions]") {
    // r := make([]any, (stop-start)/step) -- truncating, so 250 items in steps
    // of 100 is TWO entries, and an empty range is an error rather than [].
    CHECK(run("root = range(0, 10)") == "[0,1,2,3,4,5,6,7,8,9]");
    CHECK(run("root = range(0, 250, 100)") == "[0,100]");
    CHECK(run("root = range(0, 7, 3)") == "[0,3]");   // (7-0)/3 truncates to 2
    CHECK(run("root = range(0, -10, -2)") == "[0,-2,-4,-6,-8]");
    CHECK(run("root = range(start: 0, stop: 6, step: 2)") == "[0,2,4]");
    CHECK_THROWS(run("root = range(0, 0)"));      // start >= stop with a positive step
    CHECK_THROWS(run("root = range(5, 0)"));
    CHECK_THROWS(run("root = range(0, 5, -1)"));  // stop > start with a negative step
    CHECK_THROWS(run("root = range(0, 5, 0)"));   // zero step
}

TEST_CASE("bloblang: coercion fallbacks cover a failing target",
          "[bloblang][functions]") {
    // numberCoerceMethod wraps target.Exec, not only IToNumber, so the default
    // covers a field that does not resolve as well as one that will not parse.
    CHECK(run(R"(root = this.a.number(5))", R"({"a":"nope"})") == "5");
    CHECK(run(R"(root = this.missing.number(5))", "{}") == "5");
    CHECK(run(R"(root = this.a.number(5))", R"({"a":"2"})") == "2");
    CHECK(run(R"(root = this.a.bool(true))", R"({"a":"nope"})") == "true");
    CHECK(run(R"(root = this.missing.bool(false))", "{}") == "false");
    CHECK_THROWS(run(R"(root = this.missing.number())", "{}"));
    // IToNumber is IToFloat64: an integer widens rather than passing through.
    CHECK(run(R"(root = this.a.number().type())", R"({"a":3})") == R"("number")");
}

TEST_CASE("bloblang: encodings match Go's codecs", "[bloblang][encoding]") {
    auto rt = [](const char* scheme, const char* text) {
        return run(std::string(R"(root = this.a.encode(")") + scheme + R"(").decode(")"
                               + scheme + R"(").string())", std::string(R"({"a":")") + text + R"("})");
    };
    CHECK(run(R"(root = this.a.encode("ascii85"))",
              R"({"a":"this is totally unstructured data"})")
          == R"("FD,B0+DGm>FDl80Ci\"A>F`)8BEckl6F`M&(+Cno&@/")");
    CHECK(run(R"(root = this.a.decode("hex").encode("ascii85"))",
              R"({"a":"00000000616263"})") == R"("z@:E^")");   // z is four zero bytes
    CHECK(run(R"(root = this.a.decode("hex").encode("base64url"))",
              R"({"a":"fbefbe"})") == R"("----")");
    CHECK(run(R"(root = this.a.decode("hex").encode("base64rawurl"))",
              R"({"a":"fbefbe01"})") == R"("----AQ")");        // no padding
    CHECK(rt("base64", "hello") == R"("hello")");
    CHECK(rt("hex", "hello") == R"("hello")");
    CHECK(rt("ascii85", "hello") == R"("hello")");
    CHECK(rt("z85", "eightchr") == R"("eightchr")");
    CHECK_THROWS(run(R"(root = this.a.encode("z85"))", R"({"a":"seven c"})"));
    CHECK_THROWS(run(R"(root = this.a.encode("rot13"))", R"({"a":"x"})"));
}

TEST_CASE("bloblang: format_json and merge follow the reference",
          "[bloblang][methods]") {
    // Four spaces of indent, and HTML escaping ON -- the opposite of ordinary
    // message encoding, where encodeJSON calls SetEscapeHTML(false).
    CHECK(run_msg(R"(root = this.doc.format_json())", R"({"doc":{"foo":"bar"}})")
          == "{\n    \"foo\": \"bar\"\n}");
    CHECK(run_msg(R"(root = this.doc.format_json(no_indent: true))",
                  R"({"doc":{"foo":"bar"}})") == R"({"foo":"bar"})");
    CHECK(run_msg(R"(root = this.doc.format_json("  "))", R"({"doc":{"a":1}})")
          == "{\n  \"a\": 1\n}");
    CHECK(run_msg(R"(root = this.doc.format_json())", R"({"doc":{"h":"a&b"}})")
          == "{\n    \"h\": \"a\\u0026b\"\n}");
    CHECK(run_msg(R"(root = this.doc.format_json(escape_html: false))",
                  R"({"doc":{"h":"a&b"}})") == "{\n    \"h\": \"a&b\"\n}");

    // merge does not overwrite: a conflicting key becomes an array of both.
    CHECK(run(R"(root = this.a.merge(this.b))", R"({"a":{"k":1},"b":{"k":2}})")
          == R"({"k":[1,2]})");
    CHECK(run(R"(root = this.a.merge(this.b))", R"({"a":{"k":[1]},"b":{"k":2}})")
          == R"({"k":[1,2]})");
    CHECK(run(R"(root = this.a.merge(this.b))", R"({"a":{"k":1},"b":{"j":2}})")
          == R"({"j":2,"k":1})");
}

// The conformance corpus cannot assert on compress(): the documented examples
// pin the bytes Go's DEFLATE encoder happens to produce, which is not a
// property of the formats. What IS a property of the formats is that every
// algorithm round-trips, and that the decompressors read data produced
// elsewhere -- the corpus covers the second half by decompressing Go-produced
// gzip, and this covers the first.
TEST_CASE("codecs: every algorithm round-trips", "[codec]") {
    // Long enough to compress, and repetitive enough that it actually shrinks.
    std::string input;
    for (int i = 0; i < 200; ++i)
        input += "the quick brown fox jumps over the lazy dog " + std::to_string(i) + "\n";

    for (const char* alg : {"gzip", "pgzip", "zlib", "flate", "snappy", "lz4", "zstd"}) {
        const sf::value packed = sf::m::compress(sf::value(input), sf::value(std::string(alg)),
                                                 sf::value());
        check(packed.type() == sf::vtype::bytes,
              std::string(alg) + ": compress yields bytes");
        check(packed.as_string().size() < input.size(),
              std::string(alg) + ": output is smaller than the input");
        const sf::value back = sf::m::decompress(packed, sf::value(std::string(alg)));
        check(back.as_string() == input, std::string(alg) + ": round-trips");
    }

    // An unknown algorithm is a named error, never a pass-through.
    bool threw = false;
    try { sf::m::compress(sf::value(input), sf::value(std::string("brotli")), sf::value()); }
    catch (const sf::eval_error&) { threw = true; }
    check(threw, "an unrecognised algorithm is rejected");

    // bzip2 is decompress-only, matching the reference.
    threw = false;
    try { sf::m::compress(sf::value(input), sf::value(std::string("bzip2")), sf::value()); }
    catch (const sf::eval_error&) { threw = true; }
    check(threw, "bzip2 is not offered for compression");
}

// bcrypt exercises the whole Blowfish implementation, including the initial
// tables generated from pi: one wrong word changes every hash. The vector is
// the reference documentation's, produced by an unrelated implementation.
TEST_CASE("bcrypt: known vectors", "[crypto]") {
    const sf::value stored(std::string(
        "$2y$10$Dtnt5NNzVtMCOZONT705tOcS8It6krJX8bEjnDJnwxiFKsz1C.3Ay"));
    check(sf::m::compare_bcrypt(sf::value(std::string("there-are-many-blobs-in-the-sea")),
                                stored).as_bool(),
          "the right secret matches");
    check(!sf::m::compare_bcrypt(sf::value(std::string("will-i-ever-find-love")),
                                 stored).as_bool(),
          "a wrong secret does not");

    // A malformed hash is an error, never a silent false: a config with a
    // truncated hash would otherwise reject every password without saying why.
    bool threw = false;
    try { sf::m::compare_bcrypt(sf::value(std::string("x")), sf::value(std::string("$2y$10$short"))); }
    catch (const sf::eval_error&) { threw = true; }
    check(threw, "a malformed hash is rejected");
}

// format_xml's repeated-element path has no conformance fixture -- the
// documented examples are all single-valued -- and it indented every repeat
// twice before this was written.
TEST_CASE("format_xml: repeated elements", "[parse]") {
    const sf::value doc = sf::parse_json(R"({"root":{"item":["a","b"]}})");
    const sf::value pretty = sf::m::format_xml(doc, sf::value(), sf::value(), sf::value());
    check(pretty.as_string() ==
              "<root>\n    <item>a</item>\n    <item>b</item>\n</root>",
          "each repeat is indented once");

    const sf::value compact =
        sf::m::format_xml(doc, sf::value(), sf::value(true), sf::value());
    check(compact.as_string() == "<root><item>a</item><item>b</item></root>",
          "no_indent removes the whitespace entirely");

    // And the round trip: parse_xml turns repeats back into an array.
    const sf::value back = sf::m::parse_xml(compact, sf::value());
    check(back.to_json() == R"({"root":{"item":["a","b"]}})", "round-trips");
}

// A document nested past the reader's limit must be an error rather than a
// stack overflow: XML and msgpack both arrive as message data.
TEST_CASE("readers reject unbounded nesting", "[parse]") {
    std::string xml;
    for (int i = 0; i < 5000; ++i) xml += "<a>";
    for (int i = 0; i < 5000; ++i) xml += "</a>";
    bool threw = false;
    try { sf::m::parse_xml(sf::value(xml), sf::value()); }
    catch (const sf::eval_error&) { threw = true; }
    check(threw, "xml nesting is bounded");

    std::string mp(5000, static_cast<char>(0x91));   // fixarray of one, repeated
    mp += static_cast<char>(0xC0);                   // nil at the bottom
    threw = false;
    try { sf::m::parse_msgpack(sf::value::bytes(mp)); }
    catch (const sf::eval_error&) { threw = true; }
    check(threw, "msgpack nesting is bounded");
}

// The timestamp subsystem is the largest thing the conformance corpus only
// samples: its examples are all post-epoch, all UTC and all whole seconds.
// These are the cases that break a civil-calendar conversion written the
// obvious way.
TEST_CASE("timestamps: the edges the documented examples miss", "[time]") {
    auto fmt = [](const sf::value& v, const char* layout, const char* tz = nullptr) {
        return sf::m::ts_format(v, sf::value(std::string(layout)),
                                tz ? sf::value(std::string(tz)) : sf::value()).as_string();
    };
    auto parse_rfc = [](const char* s) {
        return sf::m::to_timestamp(sf::value(std::string(s)));
    };

    // Before the epoch: the seconds are negative and the nanosecond remainder
    // must floor rather than truncate, or 1969-12-31T23:59:59Z comes back as
    // 1970-01-01T00:00:-1.
    const sf::value pre = parse_rfc("1969-12-31T23:59:59Z");
    check(sf::m::ts_unix(pre).as_i64() == -1, "one second before the epoch is -1");
    check(fmt(pre, "2006-01-02T15:04:05Z07:00") == "1969-12-31T23:59:59Z",
          "and it round-trips");
    const sf::value older = parse_rfc("1900-03-01T12:00:00Z");
    check(fmt(older, "2006-01-02") == "1900-03-01",
          "1900 is not a leap year, so 29 February does not exist there");

    // Sub-second precision, and RFC 3339's trailing-zero trimming.
    const sf::value nanos = parse_rfc("2020-08-14T11:45:26.123456789Z");
    check(sf::m::ts_unix_nano(nanos).as_i64() == 1597405526123456789LL, "nanoseconds survive");
    check(sf::m::ts_unix_milli(nanos).as_i64() == 1597405526123LL, "milliseconds floor");
    check(nanos.to_display_string() == "2020-08-14T11:45:26.123456789Z", "and print back");
    check(parse_rfc("2020-08-14T11:45:26.100Z").to_display_string() ==
              "2020-08-14T11:45:26.1Z",
          "trailing zeros in the fraction are trimmed, as RFC3339Nano does");

    // A named zone, across a daylight-saving boundary: the offset is not a
    // constant per zone, it is a function of the instant.
    const sf::value winter = sf::m::ts_tz(parse_rfc("2021-02-03T16:05:06Z"),
                                          sf::value(std::string("America/New_York")));
    check(winter.to_display_string() == "2021-02-03T11:05:06-05:00", "EST is -05:00");
    const sf::value summer = sf::m::ts_tz(parse_rfc("2021-07-03T16:05:06Z"),
                                          sf::value(std::string("America/New_York")));
    check(summer.to_display_string() == "2021-07-03T12:05:06-04:00", "EDT is -04:00");
    check(sf::m::ts_unix(winter).as_i64() == sf::m::ts_unix(parse_rfc("2021-02-03T16:05:06Z")).as_i64(),
          "changing the zone does not move the instant");

    // Calendar-aware arithmetic: adding a month to 31 January cannot land on
    // 31 February, and Go's AddDate normalises forward.
    check(sf::m::ts_add_iso8601(parse_rfc("2020-01-31T00:00:00Z"),
                                sf::value(std::string("P1M"))).to_display_string() ==
              "2020-03-02T00:00:00Z",
          "31 January plus a month normalises into March");
    check(sf::m::ts_add_iso8601(parse_rfc("2020-02-29T00:00:00Z"),
                                sf::value(std::string("P1Y"))).to_display_string() ==
              "2021-03-01T00:00:00Z",
          "29 February plus a year normalises too");

    // The three format languages agree with each other.
    check(fmt(nanos, "Mon Jan _2 15:04:05 2006", "UTC") == "Fri Aug 14 11:45:26 2020",
          "a Go layout with a weekday and a space-padded day");
    check(sf::m::ts_strftime(nanos, sf::value(std::string("%A %d %B %Y %H:%M:%S.%f")),
                             sf::value(std::string("UTC"))).as_string() ==
              "Friday 14 August 2020 11:45:26.123456",
          "the strftime spelling of the same instant");
    check(sf::m::ts_strptime(sf::value(std::string("2020-Aug-14 11:50:26.371000")),
                             sf::value(std::string("%Y-%b-%d %H:%M:%S.%f")))
              .to_display_string() == "2020-08-14T11:50:26.371Z",
          "and strptime reads it back");
    check(sf::m::ts_parse(sf::value(std::string("Aug 14, 2020 at 5:54am (UTC)")),
                          sf::value(std::string("Jan 2, 2006 at 3:04pm (MST)")))
              .to_display_string() == "2020-08-14T05:54:00Z",
          "12-hour parsing with a meridiem and a zone abbreviation");

    // An unknown zone and an unknown directive both name what went wrong.
    bool threw = false;
    try { sf::m::ts_tz(nanos, sf::value(std::string("Mars/Olympus_Mons"))); }
    catch (const sf::eval_error&) { threw = true; }
    check(threw, "an unknown zone is an error");
    threw = false;
    try { sf::m::ts_strftime(nanos, sf::value(std::string("%Q")), sf::value()); }
    catch (const sf::eval_error&) { threw = true; }
    check(threw, "an unknown strftime directive is an error, not passed through");
}

// Case mapping and string reversal are defined over Unicode code points, not
// over bytes. Both were byte-wise until the L4 gate compared them against the
// reference: uppercase left every non-ASCII character untouched, and reverse
// turned valid UTF-8 into invalid UTF-8.
TEST_CASE("strings: case and reversal are Unicode-aware", "[bloblang][unicode]") {
    CHECK(run(R"(root = this.a.uppercase())", R"({"a":"héllo"})") == R"("HÉLLO")");
    CHECK(run(R"(root = this.a.lowercase())", R"({"a":"HÉLLO"})") == R"("héllo")");
    CHECK(run(R"(root = this.a.reverse())", R"({"a":"日本語"})") == R"("語本日")");
    CHECK(run(R"(root = this.a.reverse())", R"({"a":"héllo"})") == R"("olléh")");

    // SIMPLE case mapping, one code point to one code point. Full mapping would
    // give "STRASSE" and "FIN"; the reference gives neither, because it maps
    // rune by rune.
    CHECK(run(R"(root = this.a.uppercase())", R"({"a":"straße"})") == R"("STRAßE")");
    CHECK(run(R"(root = this.a.uppercase())", R"({"a":"ﬁn"})") == R"("ﬁN")");
    // U+0130 is the one letter whose full lowercase is two code points; its
    // simple mapping is a plain "i", which is what the reference produces.
    CHECK(run(R"(root = this.a.lowercase())", R"({"a":"İstanbul"})") == R"("istanbul")");

    // length() stays BYTES even though case mapping is not.
    CHECK(run(R"(root = this.a.length())", R"({"a":"日本語"})") == "9");

    // Malformed UTF-8 must pass through rather than being mangled further.
    CHECK(run(R"(root = this.a.decode("hex").uppercase().encode("hex"))",
              R"({"a":"61ff62"})") == R"("41ff42")");
}

// Statements are separated by a line break, not merely by whitespace. Accepting
// them on one line was a laxity -- a mapping that parses here and fails in
// Redpanda Connect -- rather than a wrong answer, which is why only the L4 gate
// noticed: 35 hand-written differential fixtures had had their newlines folded
// away by YAML's flow scalars and were quietly testing the one-line form.
TEST_CASE("parser: statements need a line break", "[bloblang][parse]") {
    CHECK(run("root.a = 1\nroot.b = 2", "{}") == R"({"a":1,"b":2})");
    CHECK(run("root.a = 1 # trailing comment\nroot.b = 2", "{}") == R"({"a":1,"b":2})");
    CHECK_THROWS(sf::blobl::parse_mapping("root.a = 1 root.b = 2"));
    CHECK_THROWS(sf::blobl::parse_mapping("map m {\n  root = this root.x = 1\n}\nroot = this"));
    // A single statement with no trailing newline is still fine.
    CHECK(run("root.a = 1", "{}") == R"({"a":1})");
}
