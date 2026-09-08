// The config descriptor framework — the load-bearing abstraction.
//
// A component declares its fields ONCE as a constexpr table. Three consumers
// read that one declaration:
//
//   from_yaml<T>()  parse          (swordfish run)
//   describe<T>()   document       (swordfish list, --help)
//   emit_cpp<T>()   C++ literal    (swordfish build)
//
// Linting is NOT a fourth consumer, though this header used to name a
// `lint<T>()` that has never existed. It is the `lints&` out-parameter every
// `parse` takes: parsing and validating are one pass, so a field cannot be read
// by one and checked by the other. Someone adding a field type implements
// parse/describe/emit and greps for the fourth name in vain.
//
// Emission is why this exists. If emission were hand-written per component,
// a field added to the parser but forgotten in the emitter would produce a
// compiled binary that silently ignores configuration. Deriving all four from
// one table makes that bug unrepresentable.
//
// Defaults are NOT stored in the descriptor: the struct's own default member
// initialisers are the defaults. So `T{}` is the default config, `describe`
// reads defaults from it, and `emit_cpp` omits fields that still match it.
#pragma once

#include "swordfish/config/yaml.hh"

#include <chrono>
#include <map>
#include <optional>
#include <sstream>
#include <string>
#include <tuple>
#include <type_traits>
#include <vector>

namespace sf::cfg {

// Renders a string as a C++ source literal. Everything that emits generated
// code needs this; there were four independent copies before, which is exactly
// the kind of divergence that produces one escaping bug nobody can reproduce.
std::string cxx_string_literal(std::string_view s);

// ---- diagnostics -----------------------------------------------------------

enum class lint_level { error, warning };

struct lint {
    lint_level  level = lint_level::error;
    position    where;
    std::string path;        // dotted config path, e.g. input.kafka.seed_brokers
    std::string message;
    std::string render() const;
};

using lints = std::vector<lint>;

// ---- duration --------------------------------------------------------------
// Go-style duration strings ("500ms", "1h30m"), because that is what Benthos
// configs contain.
struct duration {
    std::chrono::nanoseconds ns{};
    constexpr duration() = default;
    constexpr duration(std::chrono::nanoseconds v) : ns(v) {}
    template <class R, class P>
    constexpr duration(std::chrono::duration<R, P> v)
        : ns(std::chrono::duration_cast<std::chrono::nanoseconds>(v)) {}
    constexpr bool operator==(const duration&) const = default;
};

std::optional<duration> parse_duration(std::string_view s);
std::string             format_duration(duration d);

// ---- bloblang-typed field ---------------------------------------------------
// A field holding a Bloblang mapping or query. It carries the SOURCE, because
// the two execution modes need different things from it: the interpreter parses
// it into an AST, and the emitter turns it into a generated function. Parsing at
// load time means a syntax error is a config error, reported with a line number,
// rather than a surprise at first message.
struct bloblang {
    std::string source;
    bool        is_query = false;   // an expression (a `check`) rather than a mapping
    bool operator==(const bloblang&) const = default;
};

// An interpolated string field: literal text with embedded `${! query }`
// segments evaluated per message. Benthos uses these widely -- a Kafka topic, a
// log message, a dedupe key.
struct interpolation {
    std::string source;
    bool operator==(const interpolation&) const = default;
};

// A field written as a SINGLE-KEY OBJECT naming a variant, of which only the
// name is needed here:
//
//   scanner:
//     lines: {}
//
// The reference writes several fields this way -- scanners above all -- and
// accepting a bare string instead would mean a config that parses here is
// rejected there. That exact mismatch (`path` versus `paths` on the file input)
// has already cost this project a comparison that looked like agreement.
struct choice {
    std::string name;
    bool operator==(const choice&) const = default;
};

// Rewrites an interpolated string as an equivalent Bloblang QUERY, so the
// existing machinery evaluates it: both the interpreter and the emitter already
// handle queries, and a second evaluator would be a second set of semantics to
// keep in step.
//
//   `id-${! this.n }`  ->  `"id-" + (this.n).string()`
//
// Segments are concatenated with `.string()` applied to each query, because
// interpolation always yields text even where the query does not.
std::string interpolation_to_query(std::string_view src);

// Whether `src` actually contains a `${! ... }` segment. Every interpolated
// field can be run through interpolation_to_query, but a field with nothing to
// interpolate is a plain literal, and some callers -- `output.file`'s path, for
// one -- take a cheaper and more useful path when they know it is fixed.
bool is_interpolated(std::string_view src);

// ---- field descriptor ------------------------------------------------------

template <class C, class M>
struct field_desc {
    using owner = C;
    using member = M;

    std::string_view name;
    M C::*           ptr = nullptr;
    std::string_view description = {};
    bool             required    = false;
    bool             advanced    = false;
    bool             deprecated  = false;
    std::string_view const* options_begin = nullptr;
    std::string_view const* options_end   = nullptr;

    constexpr field_desc describe(std::string_view d)  const { auto f = *this; f.description = d;  return f; }
    constexpr field_desc require()                     const { auto f = *this; f.required    = true; return f; }
    constexpr field_desc advanced_()                   const { auto f = *this; f.advanced    = true; return f; }
    constexpr field_desc deprecated_()                 const { auto f = *this; f.deprecated  = true; return f; }
    template <size_t N>
    constexpr field_desc options(const std::string_view (&o)[N]) const {
        auto f = *this; f.options_begin = o; f.options_end = o + N; return f;
    }
    constexpr bool has_options() const { return options_begin != options_end; }
};

template <class C, class M>
constexpr auto field(std::string_view name, M C::* ptr) {
    return field_desc<C, M>{name, ptr};
}

template <class... F>
constexpr auto object(F... f) { return std::tuple<F...>{f...}; }

// Components specialise this next to their config struct.
template <class T> struct spec_of;

template <class T>
concept has_spec = requires { spec_of<T>::value; };

// ---- per-type codec --------------------------------------------------------
// Each supported field type says how to parse, emit and name itself. Adding a
// type here makes it usable by every component at once.

template <class M, class Enable = void> struct codec;

struct emit_ctx;

#define SF_SCALAR_CODEC(TYPE, TYPENAME)                                        \
    template <> struct codec<TYPE> {                                           \
        static void parse(const ynode&, TYPE&, const std::string&, lints&);    \
        static std::string emit(const TYPE&, emit_ctx&);                       \
        static std::string type_name() { return TYPENAME; }                    \
        static std::string show(const TYPE&);                                  \
    };

SF_SCALAR_CODEC(std::string, "string")
SF_SCALAR_CODEC(bool,        "bool")
SF_SCALAR_CODEC(int,         "int")
SF_SCALAR_CODEC(int64_t,     "int")
SF_SCALAR_CODEC(double,      "float")
SF_SCALAR_CODEC(duration,    "duration")
SF_SCALAR_CODEC(bloblang,    "bloblang mapping")
SF_SCALAR_CODEC(interpolation, "interpolated string")
SF_SCALAR_CODEC(choice,        "object")
#undef SF_SCALAR_CODEC

struct emit_ctx {
    int indent = 0;   // top level renders flush; nested structs step in
    std::string pad() const {
        return std::string(static_cast<size_t>(indent < 0 ? 0 : indent) * 4, ' ');
    }
};

// vector<T>
template <class T>
struct codec<std::vector<T>> {
    static void parse(const ynode& n, std::vector<T>& out, const std::string& path, lints& ls) {
        if (n.is_null()) return;
        if (!n.is_sequence()) {
            ls.push_back({lint_level::error, n.pos, path, "expected a list"});
            return;
        }
        out.clear();
        out.reserve(n.seq.size());
        for (size_t i = 0; i < n.seq.size(); ++i) {
            T item{};
            codec<T>::parse(n.seq[i], item, path + "[" + std::to_string(i) + "]", ls);
            out.push_back(std::move(item));
        }
    }
    static std::string emit(const std::vector<T>& v, emit_ctx& c) {
        std::string s = "{";
        for (size_t i = 0; i < v.size(); ++i) { if (i) s += ", "; s += codec<T>::emit(v[i], c); }
        return s + "}";
    }
    static std::string type_name() { return "array of " + codec<T>::type_name(); }
    static std::string show(const std::vector<T>& v) {
        std::string s = "[";
        for (size_t i = 0; i < v.size(); ++i) { if (i) s += ", "; s += codec<T>::show(v[i]); }
        return s + "]";
    }
};

// optional<T>
template <class T>
struct codec<std::optional<T>> {
    static void parse(const ynode& n, std::optional<T>& out, const std::string& path, lints& ls) {
        if (n.is_null()) { out.reset(); return; }
        T v{};
        codec<T>::parse(n, v, path, ls);
        out = std::move(v);
    }
    static std::string emit(const std::optional<T>& v, emit_ctx& c) {
        return v ? codec<T>::emit(*v, c) : "std::nullopt";
    }
    static std::string type_name() { return codec<T>::type_name() + " (optional)"; }
    static std::string show(const std::optional<T>& v) { return v ? codec<T>::show(*v) : "(unset)"; }
};

// map<string, T>
template <class T>
struct codec<std::map<std::string, T>> {
    static void parse(const ynode& n, std::map<std::string, T>& out, const std::string& path, lints& ls) {
        if (n.is_null()) return;
        if (!n.is_mapping()) {
            ls.push_back({lint_level::error, n.pos, path, "expected an object"});
            return;
        }
        out.clear();
        for (const auto& [k, v] : n.map) {
            T item{};
            codec<T>::parse(v, item, path + "." + k, ls);
            out.emplace(k, std::move(item));
        }
    }
    static std::string emit(const std::map<std::string, T>& v, emit_ctx& c) {
        std::string s = "{";
        bool first = true;
        for (const auto& [k, val] : v) {
            if (!first) s += ", ";
            first = false;
            s += "{" + codec<std::string>::emit(k, c) + ", " + codec<T>::emit(val, c) + "}";
        }
        return s + "}";
    }
    static std::string type_name() { return "object of " + codec<T>::type_name(); }
    static std::string show(const std::map<std::string, T>& v) {
        return "{" + std::to_string(v.size()) + " entries}";
    }
};

// Nested config structs, detected by having their own spec.
template <class T>
struct codec<T, std::enable_if_t<has_spec<T>>> {
    static void parse(const ynode& n, T& out, const std::string& path, lints& ls);
    static std::string emit(const T& v, emit_ctx& c);
    static std::string type_name() { return "object"; }
    static std::string show(const T&) { return "{...}"; }
};

// ---- the four consumers ----------------------------------------------------

template <class T> void        parse_into(const ynode& n, T& out, const std::string& path, lints& ls);
template <class T> std::string emit_cpp(const T& cfg, std::string_view type_name);
template <class T> std::string describe(std::string_view name, bool include_advanced = true);

// A YAML skeleton of T's DEFAULT config, for `swordfish create`: every field at
// its default, required ones marked with a trailing comment.
//
// This is the fourth consumer the header above says does not exist, and it is
// worth being precise about why it is allowed to. It reads the same one table,
// so it cannot document a field the parser does not have. What it does NOT do
// is add a method to every `codec`: it renders only `T{}`, so every container
// is empty and every scalar goes through the `show()` that `describe` already
// uses. A general YAML writer for an arbitrary T would need that fourth method
// on each codec; a writer for the default value does not.
//
// `indent` is the column its fields start at. Output has no trailing newline
// rules of its own: each line ends in one.
template <class T> std::string scaffold(int indent);

// Convenience: parse and collect diagnostics in one call.
template <class T>
struct parse_result {
    T     value{};
    lints diagnostics;
    bool  ok() const {
        for (const auto& l : diagnostics) if (l.level == lint_level::error) return false;
        return true;
    }
};

template <class T>
parse_result<T> from_yaml(const ynode& n, const std::string& path = "") {
    parse_result<T> r;
    parse_into<T>(n, r.value, path, r.diagnostics);
    return r;
}

} // namespace sf::cfg

#include "swordfish/config/spec_impl.hh"
