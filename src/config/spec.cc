#include "swordfish/config/spec.hh"
#include <string>
#include <vector>

#include <charconv>
#include <cstdio>
#include <cstdlib>

namespace sf::cfg {

std::string lint::render() const {
    std::ostringstream os;
    os << "line " << where.line << ": " << (level == lint_level::error ? "error" : "warning")
       << ": ";
    if (!path.empty()) os << path << ": ";
    os << message;
    return os.str();
}

// ---- duration --------------------------------------------------------------

std::optional<duration> parse_duration(std::string_view s) {
    if (s.empty()) return std::nullopt;
    using namespace std::chrono;
    nanoseconds total{};
    size_t i = 0;
    bool neg = false;
    if (s[0] == '-') { neg = true; i = 1; }
    if (i >= s.size()) return std::nullopt;
    bool any = false;
    while (i < s.size()) {
        size_t start = i;
        while (i < s.size() && (std::isdigit(static_cast<unsigned char>(s[i])) || s[i] == '.')) ++i;
        if (start == i) return std::nullopt;
        double mag = std::strtod(std::string(s.substr(start, i - start)).c_str(), nullptr);
        size_t us = i;
        while (i < s.size() && !std::isdigit(static_cast<unsigned char>(s[i])) && s[i] != '.') ++i;
        std::string_view unit = s.substr(us, i - us);
        double mult;
        if      (unit == "ns") mult = 1.0;
        else if (unit == "us" || unit == "µs") mult = 1e3;
        else if (unit == "ms") mult = 1e6;
        else if (unit == "s")  mult = 1e9;
        else if (unit == "m")  mult = 6e10;
        else if (unit == "h")  mult = 3.6e12;
        else return std::nullopt;
        total += nanoseconds(static_cast<int64_t>(mag * mult));
        any = true;
    }
    if (!any) return std::nullopt;
    return duration{neg ? -total : total};
}

std::string format_duration(duration d) {
    using namespace std::chrono;
    auto ns = d.ns.count();
    if (ns == 0) return "0s";
    auto emit = [](int64_t v, const char* u) { return std::to_string(v) + u; };
    if (ns % 3600000000000LL == 0) return emit(ns / 3600000000000LL, "h");
    if (ns % 60000000000LL   == 0) return emit(ns / 60000000000LL,   "m");
    if (ns % 1000000000LL    == 0) return emit(ns / 1000000000LL,    "s");
    if (ns % 1000000LL       == 0) return emit(ns / 1000000LL,       "ms");
    if (ns % 1000LL          == 0) return emit(ns / 1000LL,          "us");
    return emit(ns, "ns");
}

// ---- scalar codecs ---------------------------------------------------------

std::string cxx_string_literal(std::string_view s) {
    std::string out = "\"";
    for (unsigned char c : s) {
        switch (c) {
        case '"':  out += "\\\""; break;
        case '\\': out += "\\\\"; break;
        case '\n': out += "\\n";  break;
        case '\t': out += "\\t";  break;
        case '\r': out += "\\r";  break;
        default:
            if (c < 0x20 || c == 0x7f) {
                // Octal rather than \xNN: a hex escape has no length limit in
                // C++, so "\x1" followed by a literal '2' would be read as
                // \x12. Three octal digits are unambiguous.
                char b[5];
                std::snprintf(b, sizeof b, "\\%03o", c);
                out += b;
            } else {
                out += static_cast<char>(c);
            }
        }
    }
    return out + '"';
}

static bool want_scalar(const ynode& n, const std::string& path, lints& ls, const char* what) {
    if (n.is_scalar()) return true;
    ls.push_back({lint_level::error, n.pos, path, std::string("expected a ") + what});
    return false;
}

void codec<std::string>::parse(const ynode& n, std::string& out, const std::string& p, lints& ls) {
    if (want_scalar(n, p, ls, "string")) out = n.scalar;
}
std::string codec<std::string>::emit(const std::string& v, emit_ctx&) { return cxx_string_literal(v); }
std::string codec<std::string>::show(const std::string& v) { return v.empty() ? "\"\"" : cxx_string_literal(v); }

void codec<bool>::parse(const ynode& n, bool& out, const std::string& p, lints& ls) {
    if (!want_scalar(n, p, ls, "bool")) return;
    if (n.scalar == "true" || n.scalar == "yes" || n.scalar == "on")        out = true;
    else if (n.scalar == "false" || n.scalar == "no" || n.scalar == "off")  out = false;
    else ls.push_back({lint_level::error, n.pos, p, "expected a bool, got '" + n.scalar + "'"});
}
std::string codec<bool>::emit(const bool& v, emit_ctx&) { return v ? "true" : "false"; }
std::string codec<bool>::show(const bool& v) { return v ? "true" : "false"; }

template <class I>
static void parse_int(const ynode& n, I& out, const std::string& p, lints& ls) {
    if (!want_scalar(n, p, ls, "int")) return;
    const std::string& s = n.scalar;
    I v{};
    auto [end, ec] = std::from_chars(s.data(), s.data() + s.size(), v);
    if (ec != std::errc{} || end != s.data() + s.size())
        ls.push_back({lint_level::error, n.pos, p, "expected an integer, got '" + s + "'"});
    else out = v;
}

void codec<int>::parse(const ynode& n, int& out, const std::string& p, lints& ls) { parse_int(n, out, p, ls); }
std::string codec<int>::emit(const int& v, emit_ctx&) { return std::to_string(v); }
std::string codec<int>::show(const int& v) { return std::to_string(v); }

void codec<int64_t>::parse(const ynode& n, int64_t& out, const std::string& p, lints& ls) { parse_int(n, out, p, ls); }
std::string codec<int64_t>::emit(const int64_t& v, emit_ctx&) { return "int64_t{" + std::to_string(v) + "}"; }
std::string codec<int64_t>::show(const int64_t& v) { return std::to_string(v); }

void codec<double>::parse(const ynode& n, double& out, const std::string& p, lints& ls) {
    if (!want_scalar(n, p, ls, "float")) return;
    char* end = nullptr;
    double v = std::strtod(n.scalar.c_str(), &end);
    if (end == n.scalar.c_str() || *end)
        ls.push_back({lint_level::error, n.pos, p, "expected a number, got '" + n.scalar + "'"});
    else out = v;
}
std::string codec<double>::emit(const double& v, emit_ctx&) {
    std::ostringstream os; os.precision(17); os << v; return os.str();
}
std::string codec<double>::show(const double& v) {
    std::ostringstream os; os << v; return os.str();
}

void codec<duration>::parse(const ynode& n, duration& out, const std::string& p, lints& ls) {
    if (!want_scalar(n, p, ls, "duration")) return;
    if (auto d = parse_duration(n.scalar)) out = *d;
    else ls.push_back({lint_level::error, n.pos, p,
                       "expected a duration like '500ms' or '1h30m', got '" + n.scalar + "'"});
}
std::string codec<duration>::emit(const duration& v, emit_ctx&) {
    return "std::chrono::nanoseconds{" + std::to_string(v.ns.count()) + "}";
}
std::string codec<duration>::show(const duration& v) { return format_duration(v); }

void codec<bloblang>::parse(const ynode& n, bloblang& out, const std::string& p, lints& ls) {
    if (!want_scalar(n, p, ls, "bloblang mapping")) return;
    out.source = n.scalar;
    // Validated by the caller, which knows whether a mapping or a query is
    // expected; the codec cannot depend on the Bloblang parser without making
    // the config framework depend on it.
}

// Emitted as a function pointer into the generated blobl_*.cc, never as a
// string: the compiled binary holds compiled code, not source to re-parse.
std::string codec<bloblang>::emit(const bloblang& v, emit_ctx&) {
    return v.source.empty() ? "nullptr" : "/* bloblang */ nullptr";
}
std::string codec<bloblang>::show(const bloblang& v) {
    return v.source.empty() ? "(none)" : "\"" + v.source.substr(0, 40) + "...\"";
}

void codec<interpolation>::parse(const ynode& n, interpolation& out,
                                 const std::string& p, lints& ls) {
    if (!want_scalar(n, p, ls, "interpolated string")) return;
    out.source = n.scalar;
}

// Like bloblang, an interpolation compiles to a function in the generated code
// rather than surviving as source to re-parse at runtime.
std::string codec<interpolation>::emit(const interpolation& v, emit_ctx&) {
    return v.source.empty() ? "nullptr" : "/* interpolation */ nullptr";
}
std::string codec<interpolation>::show(const interpolation& v) {
    return "\"" + v.source + "\"";
}

void codec<choice>::parse(const ynode& n, choice& out, const std::string& p, lints& ls) {
    // A bare scalar is accepted as well as the object form. The reference only
    // writes the object, but `scanner: lines` is what a reader reaches for
    // first, and refusing it would be pedantry rather than a real distinction.
    if (n.is_scalar()) { out.name = n.scalar; return; }
    auto sk = n.single_key();
    if (!sk) {
        ls.push_back({lint_level::error, n.pos, p,
                      "expected a single-key object naming one option"});
        return;
    }
    out.name = sk->first;
}

std::string codec<choice>::emit(const choice& v, emit_ctx&) {
    return "sf::cfg::choice{" + cxx_string_literal(v.name) + "}";
}
std::string codec<choice>::show(const choice& v) { return v.name; }

} // namespace sf::cfg

namespace sf::cfg {

bool is_interpolated(std::string_view src) {
    return src.find("${!") != std::string_view::npos;
}

std::string interpolation_to_query(std::string_view src) {
    // Bloblang string literal: the literal halves are quoted, the queries are
    // spliced in as expressions.
    const auto quote = [](std::string_view t) {
        std::string out = "\"";
        for (char c : t) {
            switch (c) {
            case '"':  out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\n': out += "\\n";  break;
            case '\t': out += "\\t";  break;
            case '\r': out += "\\r";  break;
            default:   out += c;
            }
        }
        return out + "\"";
    };

    std::vector<std::string> parts;
    size_t i = 0, lit_start = 0;
    while (i < src.size()) {
        if (src[i] != '$' || i + 2 >= src.size() || src[i + 1] != '{' || src[i + 2] != '!') {
            ++i;
            continue;
        }
        // Brace-matched, not first-`}`: a query may contain braces of its own,
        // as `${! this.a.or({}) }` does.
        // Starts at the OPENING brace, not past it: starting at `!` left depth
        // at zero, so the closing brace underflowed the counter and nothing
        // ever matched.
        size_t depth = 0, j = i + 1;
        for (; j < src.size(); ++j) {
            if (src[j] == '{') ++depth;
            else if (src[j] == '}' && --depth == 0) break;
        }
        if (j >= src.size()) { ++i; continue; }        // unterminated: literal text
        if (i > lit_start) parts.push_back(quote(src.substr(lit_start, i - lit_start)));
        // Skip the `{!` and take the body up to the matching brace.
        parts.push_back("(" + std::string(src.substr(i + 3, j - (i + 3))) + ").string()");
        i = lit_start = j + 1;
    }
    if (lit_start < src.size()) parts.push_back(quote(src.substr(lit_start)));
    if (parts.empty()) return "\"\"";

    std::string q = parts[0];
    for (size_t k = 1; k < parts.size(); ++k) q += " + " + parts[k];
    return q;
}

} // namespace sf::cfg
