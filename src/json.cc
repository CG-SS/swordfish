// JSON reader, backed by simdjson.
//
// Replaces an earlier hand-rolled reader. The DOM API is used here for a
// straight port; the on-demand API is what the compiled path wants, and it needs
// the mapping's read-set to drive it, so it belongs with the emitter rather than
// here.
#include "swordfish/methods.hh"
#include "swordfish/value.hh"

#include <simdjson.h>

#include <cctype>
#include <cerrno>
#include <cstdlib>
#include <string>

namespace sf {
namespace {

value convert(simdjson::dom::element e) {
    using namespace simdjson;
    switch (e.type()) {
    case dom::element_type::OBJECT: {
        value o = value::object();
        for (auto [key, val] : dom::object(e)) o.set(std::string_view(key), convert(val));
        return o;
    }
    case dom::element_type::ARRAY: {
        std::vector<value> items;
        for (auto item : dom::array(e)) items.push_back(convert(item));
        return value::array(std::move(items));
    }
    case dom::element_type::STRING:
        return value(std::string(std::string_view(e.get_string().value_unsafe())));
    case dom::element_type::INT64:
        return value(static_cast<int64_t>(e.get_int64().value_unsafe()));
    case dom::element_type::UINT64:
        // Bloblang's numeric tower degrades uint64 -> int64 -> double; values
        // above int64 max become doubles rather than being truncated.
        {
            uint64_t u = e.get_uint64().value_unsafe();
            if (u <= static_cast<uint64_t>(INT64_MAX)) return value(static_cast<int64_t>(u));
            return value(static_cast<double>(u));
        }
    case dom::element_type::DOUBLE:
        return value(e.get_double().value_unsafe());
    case dom::element_type::BOOL:
        return value(e.get_bool().value_unsafe());
    case dom::element_type::NULL_VALUE:
    default:
        return value();
    }
}

// ---- parse_json(use_number: true) -------------------------------------------
//
// A second traversal, over the On Demand API rather than the DOM, for one
// reason: only On Demand exposes raw_json_token(), the characters the document
// actually used to spell a number. The DOM has already narrowed the number to
// an int64 or a double by the time it can be inspected, and
// 11380878173205700000000000000000000000000000000 does not survive that.
//
// The DOM path above stays the default because it is the fast one and because
// use_number is rare.

// An int64 when the text is an integer that fits, a double otherwise. The
// degradation is Go's: encoding/json turns an out-of-range integer literal into
// a float64 rather than refusing the document.
value number_from_token(const std::string& tok) {
    if (tok.find_first_of(".eE") == std::string::npos) {
        errno = 0;
        char* end = nullptr;
        const long long i = std::strtoll(tok.c_str(), &end, 10);
        if (errno != ERANGE && end && *end == '\0') return value(static_cast<int64_t>(i));
    }
    return value(std::strtod(tok.c_str(), nullptr));
}

// The number token, trimmed: On Demand returns it with whatever delimiter or
// whitespace followed it still attached.
std::string number_token(std::string_view t) {
    size_t n = 0;
    while (n < t.size() && (std::isdigit(static_cast<unsigned char>(t[n])) ||
                            t[n] == '-' || t[n] == '+' || t[n] == '.' ||
                            t[n] == 'e' || t[n] == 'E')) ++n;
    return std::string(t.substr(0, n));
}

// `raw_numbers` is parse_json's use_number flag. With it off this path is the
// BIGINT fallback: the number is rebuilt from its text as a double, which is
// what Go's encoding/json does with an integer too large for 64 bits.
// `depth` bounds the RECURSION, which is otherwise one C++ frame per level of
// JSON nesting: 50,000 open brackets is 100 KB of message and a stack overflow,
// reachable from any source that carries JSON. simdjson's own DOM parser refuses
// past DEFAULT_MAX_DEPTH (1024) with DEPTH_ERROR, and the on-demand path this
// uses does not, so the same document was accepted by one reader and fatal to
// the other. The limit matches simdjson's so both refuse the same documents, and
// it is the same number the `xml` reader and the scanners already use.
constexpr int max_json_depth = 1024;

value convert_raw(simdjson::ondemand::value v, bool raw_numbers, int depth = 0) {
    using namespace simdjson;
    if (depth > max_json_depth)
        throw eval_error("invalid JSON: exceeds the maximum nesting depth of " +
                         std::to_string(max_json_depth));
    // `v.type()` is a simdjson_result: switching on it directly converts, and a
    // FAILED conversion used to land on `default:` and return null. That is how
    // `{"x": NaN}` became `{"x":null}` -- a well-formed document out of invalid
    // JSON, which is silent corruption rather than a parse error.
    ondemand::json_type t;
    if (const auto e = v.type().get(t); e != SUCCESS)
        throw eval_error(std::string("invalid JSON: ") + error_message(e));
    switch (t) {
    case ondemand::json_type::object: {
        value o = value::object();
        for (auto field : v.get_object()) {
            const std::string_view key = field.unescaped_key();
            o.set(key, convert_raw(field.value(), raw_numbers, depth + 1));
        }
        return o;
    }
    case ondemand::json_type::array: {
        std::vector<value> items;
        for (auto item : v.get_array())
            items.push_back(convert_raw(item.value(), raw_numbers, depth + 1));
        return value::array(std::move(items));
    }
    case ondemand::json_type::string:
        return value(std::string(std::string_view(v.get_string())));
    case ondemand::json_type::number: {
        const std::string tok = number_token(v.raw_json_token());
        if (raw_numbers) return value::raw_number(tok);
        return number_from_token(tok);
    }
    case ondemand::json_type::boolean:
        return value(bool(v.get_bool()));
    case ondemand::json_type::null:
        return value();
    case ondemand::json_type::unknown:
        break;
    }
    // Every enumerator is named, so -Wswitch flags a simdjson upgrade that adds
    // one rather than letting it fall silently into a null.
    throw eval_error("invalid JSON: unrecognised value");
}

// One parser per thread: it owns reusable internal buffers, which is where a
// good part of simdjson's speed comes from. A Seastar shard is a thread, so a
// plain static would be shared across shards, which costs both correctness and
// throughput.
thread_local simdjson::dom::parser g_parser;
thread_local simdjson::ondemand::parser g_od_parser;

} // namespace

namespace {
// The On Demand traversal, used both for use_number and as the fallback when
// the DOM parser refuses a document over an oversized integer.
// On Demand consumes `simdjson_result` values by implicit conversion, and each
// of those THROWS simdjson_error -- a std::runtime_error, not an sf::eval_error
// -- when the document turns out to be malformed further in. Only iterate()'s
// error was checked, so three things went wrong at once: `.catch()` never fired
// because the exception was not an eval_error; some invalid documents became
// well-formed values with null fields (`{"x": NaN}` parsed to `{"x":null}`);
// and iterate() alone cannot see either, because On Demand validates lazily.
//
// So the traversal is wrapped and every escaping error becomes a named
// eval_error. The DOM path was always correct; this brings `use_number: true`
// into line with it.
value parse_ondemand_inner(simdjson::padded_string& padded, bool raw_numbers);

value parse_ondemand(simdjson::padded_string& padded, bool raw_numbers) {
    try {
        return parse_ondemand_inner(padded, raw_numbers);
    } catch (const eval_error&) {
        throw;
    } catch (const simdjson::simdjson_error& e) {
        throw eval_error(std::string("invalid JSON: ") + e.what());
    }
}

value parse_ondemand_inner(simdjson::padded_string& padded, bool raw_numbers) {
    auto doc = g_od_parser.iterate(padded);
    if (doc.error() != simdjson::SUCCESS)
        throw eval_error(std::string("invalid JSON: ") +
                         simdjson::error_message(doc.error()));
    auto& d = doc.value_unsafe();
    // Same care as convert_raw: is_scalar() is a simdjson_result, and a failed
    // conversion here used to fall through to a null document. `"}"` parsed to
    // null instead of failing.
    bool scalar = false;
    if (const auto e = d.is_scalar().get(scalar); e != simdjson::SUCCESS)
        throw eval_error(std::string("invalid JSON: ") + simdjson::error_message(e));
    if (scalar) {
        simdjson::ondemand::json_type t;
        if (d.type().get(t) != simdjson::SUCCESS)
            throw eval_error("invalid JSON: unrecognised document");
        if (t == simdjson::ondemand::json_type::number) {
            const std::string tok = number_token(d.raw_json_token());
            return raw_numbers ? value::raw_number(tok) : number_from_token(tok);
        }
        if (t == simdjson::ondemand::json_type::string)
            return value(std::string(std::string_view(d.get_string())));
        if (t == simdjson::ondemand::json_type::boolean) return value(bool(d.get_bool()));
        // ONLY a real null is null. Anything else reaching here is a document
        // On Demand could not classify -- `"}"` took this path and came back as
        // a null value rather than a parse error.
        if (t != simdjson::ondemand::json_type::null)
            throw eval_error("invalid JSON: unrecognised document");
        // A root scalar must still consume the whole input: `1 2` is not one
        // document.
        if (!bool(d.at_end()))
            throw eval_error("invalid JSON: trailing content after the document");
        return value();
    }
    value out = convert_raw(d.get_value(), raw_numbers);
    // On Demand is LAZY: a document can traverse without complaint and still be
    // malformed after the part that was read, so the traversal is only complete
    // once the input is consumed.
    if (!bool(doc.at_end()))
        throw eval_error("invalid JSON: trailing content after the document");
    return out;
}
} // namespace

value parse_json(std::string_view text, bool use_number) {
    simdjson::padded_string padded(text);
    if (use_number) {
        return parse_ondemand(padded, /*raw_numbers=*/true);
    }
    auto result = g_parser.parse(padded);
    // An integer too large for 64 bits makes the DOM parser reject the WHOLE
    // document, which would make any message containing one unparseable. Go
    // degrades such a literal to a float64 instead, so the On Demand traversal
    // is used to do the same rather than losing the message.
    if (result.error() == simdjson::BIGINT_ERROR)
        return parse_ondemand(padded, /*raw_numbers=*/false);
    if (result.error() != simdjson::SUCCESS)
        throw eval_error(std::string("invalid JSON: ") + simdjson::error_message(result.error()));
    return convert(result.value_unsafe());
}

} // namespace sf
