// Bloblang parsing and serialisation methods: CSV, logfmt, URLs, YAML, XML and
// JSONPath.
//
// Each of these is a small, self-contained format reader. They are hand-written
// rather than delegated to a library for two reasons: the observable behaviour
// has to match Go's (Go's csv.Reader has a `LazyQuotes` mode with specific
// semantics, and net/url distinguishes `path` from `raw_path` in a way most
// parsers do not), and a dependency per format would be seven dependencies.
// YAML is the exception -- yaml-cpp is already linked for the config reader.
#include <optional>
#include "swordfish/methods.hh"
#include "swordfish/value.hh"

#include "encoding_util.hh"
#include "methods_util.hh"

#include <yaml-cpp/yaml.h>

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace sf::m {

namespace {

// Percent-decoding shared by parse_url and parse_form_url_encoded. `plus` picks
// between query semantics (+ is a space) and path semantics (it is not).
std::string pct_decode(std::string_view s, bool plus) {
    std::string out;
    out.reserve(s.size());
    for (size_t i = 0; i < s.size(); ++i) {
        if (plus && s[i] == '+') { out += ' '; continue; }
        if (s[i] != '%' || i + 2 >= s.size()) { out += s[i]; continue; }
        const int hi = enc::hex_nibble(s[i + 1]), lo = enc::hex_nibble(s[i + 2]);
        if (hi < 0 || lo < 0) { out += s[i]; continue; }
        out += static_cast<char>(hi * 16 + lo);
        i += 2;
    }
    return out;
}

// Go's url.escape in encodePath / encodeFragment mode. Only `?` is escaped out
// of the RFC 3986 reserved set for a path, and none of it for a fragment, so
// slashes and colons survive while a space becomes %20.
std::string pct_encode(std::string_view s, bool fragment) {
    static const char* H = "0123456789ABCDEF";
    std::string out;
    out.reserve(s.size());
    for (char ch : s) {
        const auto c = static_cast<unsigned char>(ch);
        const bool unreserved = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                                (c >= '0' && c <= '9') ||
                                c == '-' || c == '_' || c == '.' || c == '~';
        bool keep = unreserved;
        switch (c) {
        case '$': case '&': case '+': case ',': case '/': case ':':
        case ';': case '=': case '@':
            keep = true;
            break;
        case '?':
            keep = fragment;
            break;
        default: break;
        }
        if (keep) { out += ch; continue; }
        out += '%';
        out += H[c >> 4];
        out += H[c & 15];
    }
    return out;
}

// ---- CSV ---------------------------------------------------------------------

} // namespace

// Declared in methods.hh: the `csv` scanner shares it.
bool read_csv_record(std::string_view s, size_t& i, char delim, bool lazy,
                     std::vector<std::string>& out, bool at_end) {
    if (i >= s.size()) return false;
    const size_t start = i;
    out.clear();
    std::string field;
    bool quoted = false;
    bool field_started = false;
    for (;;) {
        if (i >= s.size()) {
            // Out of input part-way through a record. Whether that is an ERROR
            // or merely a record still arriving is not something this function
            // can know, so the caller says: the scanner reads one buffer at a
            // time and a quoted field straddling a buffer boundary is ordinary,
            // while `parse_csv` holds the whole document and a quote left open
            // is genuinely unterminated. Rewind so the caller can retry the
            // same record once it has more bytes.
            if (!at_end) { i = start; return false; }
            if (quoted && !lazy) throw eval_error("csv: unterminated quoted field");
            out.push_back(std::move(field));
            return true;
        }
        const char c = s[i];
        if (quoted) {
            if (c == '"') {
                if (i + 1 < s.size() && s[i + 1] == '"') { field += '"'; i += 2; continue; }
                quoted = false;
                ++i;
                continue;
            }
            field += c;
            ++i;
            continue;
        }
        if (c == '"' && !field_started) { quoted = true; field_started = true; ++i; continue; }
        if (c == '"' && field_started) {
            // A quote inside an unquoted field is an error unless lazy_quotes
            // is on, in which case it is data.
            if (!lazy) throw eval_error("csv: bare \" in non-quoted field");
            field += c;
            ++i;
            continue;
        }
        if (c == delim) {
            out.push_back(std::move(field));
            field.clear();
            field_started = false;
            ++i;
            continue;
        }
        // Records are terminated by '\n' ONLY. A bare '\r' is DATA, which is
        // what Go's encoding/csv does -- readLine reads up to '\n', normalises
        // a trailing "\r\n" to "\n", and drops a single '\r' immediately
        // before EOF "for backwards compatibility". Treating every '\r' as a
        // terminator split records differently: `x\ry` came out as two records
        // here against one field `x\ry` there, and a file mixing "\r\n" and
        // bare '\r' endings produced three well-formed records here where the
        // reference reports `record on line 2: wrong number of fields`. The
        // csv SCANNER shares this function, so it changed the message count of a
        // CSV input, not just what parse_csv returned.
        if (c == '\r' && i + 1 < s.size() && s[i + 1] == '\n') {
            out.push_back(std::move(field));
            i += 2;
            return true;
        }
        if (c == '\r' && i + 1 == s.size()) {   // trailing '\r' before EOF
            out.push_back(std::move(field));
            ++i;
            return true;
        }
        if (c == '\n') {
            out.push_back(std::move(field));
            ++i;
            return true;
        }
        field += c;
        field_started = true;
        ++i;
    }
}

namespace {

// ---- XML ---------------------------------------------------------------------

// Casts a text node the way the `cast` option describes: integers, floats and
// the four boolean spellings Go's strconv.ParseBool accepts.
value xml_cast(const std::string& text, bool cast) {
    if (!cast) return value(text);
    if (text.empty()) return value(text);
    if (text == "true" || text == "True" || text == "TRUE" || text == "1")
        return value(true);
    if (text == "false" || text == "False" || text == "FALSE" || text == "0")
        return value(false);
    const char* b = text.c_str();
    char* end = nullptr;
    const long long i = std::strtoll(b, &end, 10);
    if (end && *end == '\0') return value(static_cast<int64_t>(i));
    end = nullptr;
    const double d = std::strtod(b, &end);
    if (end && *end == '\0' && end != b) return value(d);
    return value(text);
}

std::string xml_unescape(std::string_view s) {
    std::string out;
    out.reserve(s.size());
    for (size_t i = 0; i < s.size();) {
        if (s[i] != '&') { out += s[i++]; continue; }
        const size_t semi = s.find(';', i);
        if (semi == std::string_view::npos || semi - i > 10) { out += s[i++]; continue; }
        const std::string_view e = s.substr(i + 1, semi - i - 1);
        if      (e == "amp")  out += '&';
        else if (e == "lt")   out += '<';
        else if (e == "gt")   out += '>';
        else if (e == "quot") out += '"';
        else if (e == "apos") out += '\'';
        else if (!e.empty() && e[0] == '#') {
            const bool hex = e.size() > 1 && (e[1] == 'x' || e[1] == 'X');
            uint32_t cp = 0;
            // At least ONE digit. `ok` used to start from `!e.empty()`, which is
            // already known true here, so `&#;` and `&#x;` ran a loop that never
            // executed, left cp at zero and wrote a raw NUL byte into the text.
            // Go's xml.Decoder leaves an entity it cannot resolve as literal
            // text, which is what the guard below already does for `&#zz;`.
            const size_t first_digit = hex ? 2 : 1;
            bool ok = e.size() > first_digit;
            for (size_t k = first_digit; ok && k < e.size(); ++k) {
                const int d = hex ? enc::hex_nibble(e[k]) : (e[k] >= '0' && e[k] <= '9' ? e[k] - '0' : -1);
                if (d < 0) { ok = false; break; }
                cp = cp * (hex ? 16u : 10u) + static_cast<uint32_t>(d);
            }
            if (!ok) { out += s[i++]; continue; }
            enc::append_utf8(out, cp);
        } else { out += s[i++]; continue; }
        i = semi + 1;
    }
    return out;
}

// Adds a child under `key`, turning a repeated key into an array as the
// documented rules require.
void xml_add(value& obj, const std::string& key, value child) {
    const value* existing = obj.find(key);
    if (!existing) { obj.set(key, std::move(child)); return; }
    if (existing->type() != vtype::array) {
        obj.set(key, value::array({*existing, std::move(child)}));
        return;
    }
    // Append in place. Copying the array out, pushing, and setting it back cost
    // O(N^2) on a document whose root has N children under one tag -- which is
    // the ordinary shape of an XML list, not a pathological input.
    auto& entries = obj.obj_mut();          // may unshare, invalidating `existing`
    auto it = std::lower_bound(entries.begin(), entries.end(), key,
                               [](const auto& e, const std::string& k) { return e.first < k; });
    it->second.arr_mut().push_back(std::move(child));
}

struct xml_parser {
    std::string_view s;
    size_t i = 0;
    bool cast = false;
    // Element nesting is bounded because the document is message data: without
    // a limit, `<a><a><a>...` a million deep overflows the stack, which is a
    // crash reachable from the network rather than a parse error. The number
    // matches simdjson's default JSON depth, so the two readers agree about
    // what is too deep.
    int depth = 0;
    static constexpr int max_depth = 1024;

    void skip_space() {
        while (i < s.size() && std::isspace(static_cast<unsigned char>(s[i]))) ++i;
    }

    // Skips comments, declarations and processing instructions, which the
    // documented rules say to ignore.
    bool skip_ignorable() {
        if (s.compare(i, 4, "<!--") == 0) {
            const size_t e = s.find("-->", i + 4);
            i = e == std::string_view::npos ? s.size() : e + 3;
            return true;
        }
        if (s.compare(i, 2, "<?") == 0) {
            const size_t e = s.find("?>", i + 2);
            i = e == std::string_view::npos ? s.size() : e + 2;
            return true;
        }
        if (s.compare(i, 9, "<![CDATA[") == 0) return false;    // content, not markup
        if (s.compare(i, 2, "<!") == 0) {
            const size_t e = s.find('>', i + 2);
            i = e == std::string_view::npos ? s.size() : e + 1;
            return true;
        }
        return false;
    }

    // Stops at '=' as well as at whitespace and the tag terminators: an
    // attribute name runs up to its equals sign, and swallowing it produced
    // keys like `-id="99"`.
    std::string read_name() {
        const size_t start = i;
        while (i < s.size() && s[i] != ' ' && s[i] != '\t' && s[i] != '\n' && s[i] != '\r' &&
               s[i] != '>' && s[i] != '/' && s[i] != '=')
            ++i;
        return std::string(s.substr(start, i - start));
    }

    // Parses one element, having already consumed its '<'. Returns the tag name
    // through `name` and the element's value as the result.
    value parse_element(std::string& name) {
        if (++depth > max_depth) throw eval_error("xml: document is nested too deeply");
        struct pop { int& d; ~pop() { --d; } } pop_{depth};
        name = read_name();
        value attrs = value::object();
        bool self_closing = false;
        for (;;) {
            skip_space();
            if (i >= s.size()) throw eval_error("xml: unexpected end of document");
            if (s[i] == '/') { self_closing = true; ++i; }
            if (s[i] == '>') { ++i; break; }
            const std::string an = read_name();
            if (an.empty()) throw eval_error("xml: malformed attribute");
            skip_space();
            if (i < s.size() && s[i] == '=') {
                ++i;
                skip_space();
                if (i >= s.size() || (s[i] != '"' && s[i] != '\'')) 
                    throw eval_error("xml: unquoted attribute value");
                const char q = s[i++];
                const size_t vstart = i;
                while (i < s.size() && s[i] != q) ++i;
                const std::string av = xml_unescape(s.substr(vstart, i - vstart));
                if (i < s.size()) ++i;
                // Attributes are prefixed with a hyphen, which is the documented
                // rule and is what keeps them from colliding with child elements.
                attrs.set("-" + an, xml_cast(av, cast));
            } else {
                attrs.set("-" + an, value(std::string{}));
            }
        }
        if (self_closing) {
            if (attrs.obj().empty()) return value(std::string{});
            return attrs;
        }

        value children = value::object();
        std::string text;
        bool has_children = false;
        for (;;) {
            if (i >= s.size()) throw eval_error("xml: unclosed element <" + name + ">");
            if (s[i] == '<') {
                if (s.compare(i, 9, "<![CDATA[") == 0) {
                    const size_t e = s.find("]]>", i + 9);
                    const size_t stop = e == std::string_view::npos ? s.size() : e;
                    text.append(s.substr(i + 9, stop - i - 9));
                    i = e == std::string_view::npos ? s.size() : e + 3;
                    continue;
                }
                if (skip_ignorable()) continue;
                if (s.compare(i, 2, "</") == 0) {
                    i += 2;
                    const std::string close = read_name();
                    if (close != name)
                        throw eval_error("xml: </" + close + "> closes <" + name + ">");
                    skip_space();
                    if (i < s.size() && s[i] == '>') ++i;
                    break;
                }
                ++i;                                   // consume '<'
                std::string child_name;
                value child = parse_element(child_name);
                xml_add(children, child_name, std::move(child));
                has_children = true;
                continue;
            }
            const size_t start = i;
            while (i < s.size() && s[i] != '<') ++i;
            text.append(s.substr(start, i - start));
        }

        const bool has_attrs = !attrs.obj().empty();
        std::string trimmed = xml_unescape(text);
        {
            const size_t b = trimmed.find_first_not_of(" \t\n\r");
            if (b == std::string::npos) trimmed.clear();
            else trimmed = trimmed.substr(b, trimmed.find_last_not_of(" \t\n\r") - b + 1);
        }
        if (!has_children && !has_attrs) return xml_cast(trimmed, cast);
        value out = has_children ? children : value::object();
        if (has_attrs) for (const auto& [k, v] : attrs.obj()) out.set(k, v);
        // Text alongside markup goes into #text, which is the documented rule.
        if (!trimmed.empty()) out.set("#text", xml_cast(trimmed, cast));
        return out;
    }
};

// ---- YAML ------------------------------------------------------------------------

// `budget` is a NODE COUNT shared across the whole walk, and `depth` bounds the
// recursion. Neither existed, and YAML aliases are what makes that fatal: an
// alias is expanded every time it is referenced, so nine levels of nine-way
// aliasing -- the classic billion-laughs bomb, 342 bytes of input -- becomes
// 387 million nodes and takes the process down with it. yaml-cpp hands aliases
// back as ordinary nodes, so the budget has to be enforced in this walk; the
// parser cannot see it. The reference refuses the same document by name in
// microseconds.
constexpr int    max_yaml_depth  = 1024;
constexpr size_t max_yaml_nodes  = 5'000'000;

value yaml_to_value(const YAML::Node& n, size_t& budget, int depth = 0) {
    if (depth > max_yaml_depth)
        throw eval_error("yaml: document exceeds the maximum nesting depth of " +
                         std::to_string(max_yaml_depth));
    if (budget-- == 0)
        throw eval_error("yaml: document expands to more than " +
                         std::to_string(max_yaml_nodes) +
                         " nodes, which usually means recursive aliasing");
    switch (n.Type()) {
    case YAML::NodeType::Null:
    case YAML::NodeType::Undefined:
        return value();
    case YAML::NodeType::Scalar: {
        // yaml-cpp keeps the tag, so an explicitly quoted scalar stays a string
        // and a bare one is resolved by the core schema, as go-yaml does.
        const std::string& t = n.Tag();
        const std::string s = n.Scalar();
        if (t == "!" || t == "tag:yaml.org,2002:str") return value(s);
        if (s == "true" || s == "True" || s == "TRUE")   return value(true);
        if (s == "false" || s == "False" || s == "FALSE") return value(false);
        if (s == "null" || s == "Null" || s == "NULL" || s == "~" || s.empty())
            return value();
        const char* b = s.c_str();
        char* end = nullptr;
        const long long i = std::strtoll(b, &end, 10);
        if (end && *end == '\0') return value(static_cast<int64_t>(i));
        end = nullptr;
        const double d = std::strtod(b, &end);
        if (end && *end == '\0' && end != b) return value(d);
        return value(s);
    }
    case YAML::NodeType::Sequence: {
        std::vector<value> out;
        out.reserve(n.size());
        for (const auto& e : n) out.push_back(yaml_to_value(e, budget, depth + 1));
        return value::array(std::move(out));
    }
    case YAML::NodeType::Map: {
        value out = value::object();
        for (const auto& kv : n)
            out.set(kv.first.Scalar(), yaml_to_value(kv.second, budget, depth + 1));
        return out;
    }
    }
    return value();
}

// A YAML scalar needs quoting when it would otherwise resolve to another type,
// or when it carries characters that would break block context.
bool yaml_needs_quotes(const std::string& s) {
    if (s.empty()) return true;
    static const char* const RESERVED[] = {"true","True","TRUE","false","False","FALSE",
                                           "null","Null","NULL","~","yes","no","on","off",
                                           "y","n","Y","N"};
    for (const char* r : RESERVED) if (s == r) return true;
    if (s.find_first_of(":#{}[]&*!|>'\"%@`,\n\t") != std::string::npos) return true;
    if (std::isspace(static_cast<unsigned char>(s.front())) ||
        std::isspace(static_cast<unsigned char>(s.back()))) return true;
    if (s.front() == '-' && s.size() > 1 && s[1] == ' ') return true;
    // A string that looks like a number must be quoted or it comes back as one.
    char* end = nullptr;
    std::strtod(s.c_str(), &end);
    if (end && *end == '\0') return true;
    return false;
}

std::string yaml_quote(const std::string& s) {
    std::string out = "\"";
    for (char c : s) {
        switch (c) {
        case '"':  out += "\\\""; break;
        case '\\': out += "\\\\"; break;
        case '\n': out += "\\n"; break;
        case '\t': out += "\\t"; break;
        case '\r': out += "\\r"; break;
        default:   out += c;
        }
    }
    return out + "\"";
}

void yaml_emit(const value& v, std::string& out, int indent);

void yaml_emit_scalar(const value& v, std::string& out) {
    switch (v.type()) {
    case vtype::null: case vtype::deleted: case vtype::nothing: out += "null"; break;
    case vtype::boolean: out += v.as_bool() ? "true" : "false"; break;
    case vtype::string: case vtype::bytes: case vtype::timestamp: {
        const std::string s = v.to_display_string();
        out += yaml_needs_quotes(s) ? yaml_quote(s) : s;
        break;
    }
    default: out += v.to_json(); break;               // numbers
    }
}

void yaml_emit(const value& v, std::string& out, int indent) {
    const std::string pad(static_cast<size_t>(indent), ' ');
    if (v.type() == vtype::object) {
        if (v.obj().empty()) { out += "{}\n"; return; }
        bool first = true;
        for (const auto& [k, el] : v.obj()) {
            if (!first) out += pad;
            first = false;
            out += yaml_needs_quotes(k) ? yaml_quote(k) : k;
            out += ':';
            if (el.type() == vtype::object && !el.obj().empty()) {
                out += '\n';
                out += std::string(static_cast<size_t>(indent + 4), ' ');
                yaml_emit(el, out, indent + 4);
            } else if (el.type() == vtype::array && !el.arr().empty()) {
                out += '\n';
                out += std::string(static_cast<size_t>(indent + 4), ' ');
                yaml_emit(el, out, indent + 4);
            } else {
                out += ' ';
                if (el.type() == vtype::array) out += "[]\n";
                else if (el.type() == vtype::object) out += "{}\n";
                else { yaml_emit_scalar(el, out); out += '\n'; }
            }
        }
        return;
    }
    if (v.type() == vtype::array) {
        if (v.arr().empty()) { out += "[]\n"; return; }
        bool first = true;
        for (const auto& el : v.arr()) {
            if (!first) out += pad;
            first = false;
            out += "- ";
            if ((el.type() == vtype::object && !el.obj().empty()) ||
                (el.type() == vtype::array && !el.arr().empty()))
                yaml_emit(el, out, indent + 2);
            else if (el.type() == vtype::object) out += "{}\n";
            else if (el.type() == vtype::array) out += "[]\n";
            else { yaml_emit_scalar(el, out); out += '\n'; }
        }
        return;
    }
    yaml_emit_scalar(v, out);
    out += '\n';
}

// ---- JSONPath ----------------------------------------------------------------------

// The JSONPath subset the reference's own examples use: `$`, `.name`, `..name`,
// `[n]`, `[*]`, `['name']` and a `[?(@.k=='v')]` filter. Anything else is a
// named error rather than an empty result, so a config using a construct we do
// not implement fails loudly instead of quietly matching nothing.
struct jsonpath {
    std::string_view p;
    size_t i = 0;

    // Matches at this level are emitted BEFORE recursing, so `$..name` on
    // {"name":"alice","foo":{"name":"bob"}} yields alice then bob. Recursing
    // key by key instead put the nested match first, because the keys are
    // sorted and "foo" precedes "name".
    static void collect_descend(const value& v, std::string_view name,
                                std::vector<value>& out) {
        if (v.type() == vtype::object) {
            for (const auto& [k, el] : v.obj()) if (k == name) out.push_back(el);
            for (const auto& [k, el] : v.obj()) { (void)k; collect_descend(el, name, out); }
            return;
        }
        if (v.type() == vtype::array)
            for (const auto& el : v.arr()) collect_descend(el, name, out);
    }

    std::string read_name() {
        const size_t start = i;
        while (i < p.size() && (std::isalnum(static_cast<unsigned char>(p[i])) ||
                                p[i] == '_' || p[i] == '-'))
            ++i;
        return std::string(p.substr(start, i - start));
    }

    // `@.key=='literal'` or `@.key==42`.
    static bool eval_filter(const value& el, std::string_view expr) {
        const size_t eq = expr.find("==");
        if (eq == std::string_view::npos)
            throw eval_error("jsonpath: only equality filters are supported: " +
                             std::string(expr));
        std::string_view lhs = expr.substr(0, eq);
        std::string_view rhs = expr.substr(eq + 2);
        auto trim = [](std::string_view x) {
            while (!x.empty() && x.front() == ' ') x.remove_prefix(1);
            while (!x.empty() && x.back() == ' ') x.remove_suffix(1);
            return x;
        };
        lhs = trim(lhs);
        rhs = trim(rhs);
        if (lhs.size() < 2 || lhs[0] != '@' || lhs[1] != '.')
            throw eval_error("jsonpath: filter must test @.<field>");
        lhs.remove_prefix(2);
        if (el.type() != vtype::object) return false;
        const value* got = el.find(std::string(lhs));
        if (!got) return false;
        if (rhs.size() >= 2 && (rhs.front() == '\'' || rhs.front() == '"') &&
            rhs.back() == rhs.front())
            return got->is_stringy() && got->as_string() == rhs.substr(1, rhs.size() - 2);
        if (rhs == "true" || rhs == "false")
            return got->type() == vtype::boolean && got->as_bool() == (rhs == "true");
        char* end = nullptr;
        const double d = std::strtod(std::string(rhs).c_str(), &end);
        if (end && *end == '\0') return got->is_number() && got->as_f64() == d;
        throw eval_error("jsonpath: unsupported filter literal " + std::string(rhs));
    }

    std::vector<value> run(const value& root) {
        if (p.empty() || p[0] != '$')
            throw eval_error("jsonpath: expression must start with $");
        i = 1;
        std::vector<value> cur{root};
        while (i < p.size()) {
            std::vector<value> next;
            if (p.compare(i, 2, "..") == 0) {
                i += 2;
                const std::string name = read_name();
                if (name.empty()) throw eval_error("jsonpath: .. must be followed by a name");
                for (const auto& v : cur) collect_descend(v, name, next);
            } else if (p[i] == '.') {
                ++i;
                if (i < p.size() && p[i] == '*') {
                    ++i;
                    for (const auto& v : cur) {
                        if (v.type() == vtype::object)
                            for (const auto& [k, el] : v.obj()) { (void)k; next.push_back(el); }
                        else if (v.type() == vtype::array)
                            for (const auto& el : v.arr()) next.push_back(el);
                    }
                } else {
                    const std::string name = read_name();
                    if (name.empty()) throw eval_error("jsonpath: expected a field name");
                    for (const auto& v : cur)
                        if (v.type() == vtype::object)
                            if (const value* el = v.find(name)) next.push_back(*el);
                }
            } else if (p[i] == '[') {
                const size_t close = p.find(']', i);
                if (close == std::string_view::npos)
                    throw eval_error("jsonpath: unterminated [");
                std::string_view inner = p.substr(i + 1, close - i - 1);
                i = close + 1;
                if (inner == "*") {
                    for (const auto& v : cur) {
                        if (v.type() == vtype::array)
                            for (const auto& el : v.arr()) next.push_back(el);
                        else if (v.type() == vtype::object)
                            for (const auto& [k, el] : v.obj()) { (void)k; next.push_back(el); }
                    }
                } else if (inner.size() > 3 && inner[0] == '?' && inner[1] == '(' &&
                           inner.back() == ')') {
                    const std::string_view expr = inner.substr(2, inner.size() - 3);
                    for (const auto& v : cur) {
                        if (v.type() != vtype::array) continue;
                        for (const auto& el : v.arr())
                            if (eval_filter(el, expr)) next.push_back(el);
                    }
                } else if (inner.size() >= 2 && (inner.front() == '\'' || inner.front() == '"') &&
                           inner.back() == inner.front()) {
                    const std::string name(inner.substr(1, inner.size() - 2));
                    for (const auto& v : cur)
                        if (v.type() == vtype::object)
                            if (const value* el = v.find(name)) next.push_back(*el);
                } else {
                    char* end = nullptr;
                    const long idx = std::strtol(std::string(inner).c_str(), &end, 10);
                    if (!end || *end != '\0')
                        throw eval_error("jsonpath: unsupported subscript [" +
                                         std::string(inner) + "]");
                    for (const auto& v : cur) {
                        if (v.type() != vtype::array) continue;
                        long k = idx < 0 ? idx + static_cast<long>(v.arr().size()) : idx;
                        if (k >= 0 && static_cast<size_t>(k) < v.arr().size())
                            next.push_back(v.arr()[static_cast<size_t>(k)]);
                    }
                }
            } else {
                throw eval_error("jsonpath: unexpected character in expression");
            }
            cur = std::move(next);
        }
        return cur;
    }
};

} // namespace

// ---- the methods -----------------------------------------------------------------

value parse_csv(const value& v, const value& header, const value& delimiter,
                const value& lazy_quotes) {
    const std::string& s = want_string(v);
    const bool with_header = header.type() != vtype::boolean || header.as_bool();
    const std::string d = delimiter.is_stringy() ? delimiter.as_string() : std::string(",");
    if (d.size() != 1) throw eval_error("csv delimiter must be a single character");
    const bool lazy = lazy_quotes.type() == vtype::boolean && lazy_quotes.as_bool();

    size_t i = 0;
    std::vector<std::string> row;
    std::vector<std::string> head;
    std::vector<value> out;
    bool first = true;
    std::optional<size_t> first_width;
    size_t line = 0;
    while (read_csv_record(s, i, d[0], lazy, row)) {
        ++line;
        if (row.size() == 1 && row[0].empty()) continue;      // a blank line
        if (first && with_header) { head = row; first = false; continue; }
        first = false;
        if (with_header) {
            if (row.size() != head.size())
                throw eval_error("csv: record has " + std::to_string(row.size()) +
                                 " fields, header has " + std::to_string(head.size()));
            value o = value::object();
            for (size_t k = 0; k < row.size(); ++k) o.set(head[k], value(row[k]));
            out.push_back(std::move(o));
        } else {
            // The field count is enforced WITHOUT headers too. Go's
            // encoding/csv sets FieldsPerRecord from the first record and
            // errors on any mismatch regardless of headers, and swordfish
            // checked it only on the header path: `a,b\nc,d,e` came back as
            // two well-formed rows here where the reference reports
            // `record on line 2: wrong number of fields`. Surfaced while fixing
            // the bare-CR split, which is how a ragged row gets produced by
            // accident in the first place.
            if (!first_width) first_width = row.size();
            else if (row.size() != *first_width)
                throw eval_error("csv: record on line " + std::to_string(line) +
                                 ": wrong number of fields");
            std::vector<value> cells;
            cells.reserve(row.size());
            for (const auto& c : row) cells.push_back(value(c));
            out.push_back(value::array(std::move(cells)));
        }
    }
    return value::array(std::move(out));
}

value parse_form_url_encoded(const value& v) {
    const std::string& s = want_string(v);
    value out = value::object();
    size_t i = 0;
    while (i < s.size()) {
        size_t end = s.find_first_of("&;", i);
        if (end == std::string::npos) end = s.size();
        const std::string_view pair(s.data() + i, end - i);
        i = end + 1;
        if (pair.empty()) continue;
        const size_t eq = pair.find('=');
        const std::string key = pct_decode(pair.substr(0, eq), true);
        const std::string val = eq == std::string_view::npos
            ? std::string{} : pct_decode(pair.substr(eq + 1), true);
        // A repeated key collects into an array, which is what a form with
        // multiple checkboxes of the same name produces.
        const value* existing = out.find(key);
        if (!existing) { out.set(key, value(val)); continue; }
        if (existing->type() == vtype::array) {
            std::vector<value> a = existing->arr();
            a.push_back(value(val));
            out.set(key, value::array(std::move(a)));
        } else {
            out.set(key, value::array({*existing, value(val)}));
        }
    }
    return out;
}

value parse_logfmt(const value& v) {
    const std::string& s = want_string(v);
    value out = value::object();
    size_t i = 0;
    while (i < s.size()) {
        while (i < s.size() && (s[i] == ' ' || s[i] == '\t')) ++i;
        if (i >= s.size()) break;
        const size_t kstart = i;
        while (i < s.size() && s[i] != '=' && s[i] != ' ' && s[i] != '\t') ++i;
        const std::string key = s.substr(kstart, i - kstart);
        if (key.empty()) { ++i; continue; }
        if (i >= s.size() || s[i] != '=') {
            // A bare key is a flag, and reads as true.
            out.set(key, value(true));
            continue;
        }
        ++i;
        if (i < s.size() && s[i] == '"') {
            ++i;
            std::string val;
            while (i < s.size() && s[i] != '"') {
                if (s[i] == '\\' && i + 1 < s.size()) {
                    ++i;
                    switch (s[i]) {
                    case '"': case '\\': case '/': val += s[i]; break;
                    case 'n': val += '\n'; break;
                    case 'r': val += '\r'; break;
                    case 't': val += '\t'; break;
                    case 'b': val += '\b'; break;
                    case 'f': val += '\f'; break;
                    default:  val += '\\'; val += s[i]; break;
                    }
                } else {
                    val += s[i];
                }
                ++i;
            }
            if (i >= s.size())
                throw eval_error("logfmt: unterminated quoted value for key \"" + key + "\"");
            ++i;
            out.set(key, value(std::move(val)));
        } else {
            const size_t vstart = i;
            while (i < s.size() && s[i] != ' ' && s[i] != '\t') ++i;
            out.set(key, value(s.substr(vstart, i - vstart)));
        }
    }
    return out;
}

// ---- net/url validation ------------------------------------------------------
//
// parse_url did NONE of this: it split on '#', '?', ':' and "//", percent-decoded
// each piece, and emitted whatever came out. Eight probes were each a swordfish
// success against a reference error -- a carriage return or a tab or a space in
// the host came back as part of the host, `http://%41%42/x` gave host "AB",
// `http://x/%zz` gave path "/%zz", `http://user:pa ss@h/` gave a password with a
// space in it, and `::::` gave a path of four colons. Go's net/url.Parse, which
// the reference calls, rejects all of them.
//
// The rules are Go's, taken from net/url/url.go rather than from the RFC, since
// the reference's behaviour is what has to match.

// `b < ' ' || b == 0x7f`, anywhere in the raw URL (stringContainsCTLByte).
void reject_control_chars(std::string_view s) {
    for (unsigned char b : s)
        if (b < 0x20 || b == 0x7f)
            throw eval_error("net/url: invalid control character in URL");
}

bool is_hex_digit(char c) {
    return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
}

// Every '%' must introduce two hex digits (unescape's opening loop). In the HOST
// a percent-escape may only encode a NON-ASCII byte -- RFC 3986 -- so a first
// nibble below 8 is refused unless the escape is exactly "%25", the one
// exception RFC 6874 carves out for IPv6 zone identifiers.
void reject_bad_escapes(std::string_view s, bool host) {
    for (size_t i = 0; i < s.size(); ++i) {
        if (s[i] != '%') continue;
        if (i + 2 >= s.size() || !is_hex_digit(s[i + 1]) || !is_hex_digit(s[i + 2]))
            throw eval_error("invalid URL escape \"" +
                             std::string(s.substr(i, std::min<size_t>(3, s.size() - i))) + "\"");
        if (host && enc::hex_nibble(s[i + 1]) < 8 && s.substr(i, 3) != "%25")
            throw eval_error("invalid URL escape \"" + std::string(s.substr(i, 3)) + "\"");
    }
}

// The ASCII characters a host may contain UNESCAPED, from Go's
// gen_encoding_table.go: alphanumerics, RFC 3986 §3.2.2 sub-delims, the
// unreserved marks, and the handful Go adds for ":port", "[ipv6]" and the
// characters it says are "the only ones left that we could possibly allow".
// Anything else below 0x80 is InvalidHostError; bytes at or above it pass
// through. A SPACE is not in the set and is not a control character either,
// which is why `http://a b.com/` needed this check rather than the one above.
void reject_bad_host_chars(std::string_view host) {
    for (unsigned char c : host) {
        if (c >= 0x80) continue;
        if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9'))
            continue;
        switch (c) {
        case '!': case '$': case '&': case '\'': case '(': case ')': case '*':
        case '+': case ',': case ';': case '=': case ':': case '[': case ']':
        case '<': case '>': case '"':
        case '-': case '_': case '.': case '~':
        case '%':                       // handled by reject_bad_escapes
            continue;
        default:
            throw eval_error(std::string("invalid character \"") +
                             static_cast<char>(c) + "\" in host name");
        }
    }
}

// validUserinfo: letters, digits, and a fixed punctuation set. Notably NOT a
// space, which is how `http://user:pa ss@h/` slipped through.
void reject_bad_userinfo(std::string_view s) {
    for (char c : s) {
        if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9'))
            continue;
        switch (c) {
        case '-': case '.': case '_': case ':': case '~': case '!': case '$':
        case '&': case '\'': case '(': case ')': case '*': case '+': case ',':
        case ';': case '=': case '%': case '@':
            continue;
        default:
            throw eval_error("net/url: invalid userinfo");
        }
    }
}

value parse_url(const value& v) {
    const std::string& s = want_string(v);
    reject_control_chars(s);
    // getScheme: a ':' at offset zero is not an empty scheme, it is an error.
    if (!s.empty() && s[0] == ':') throw eval_error("missing protocol scheme");
    value out = value::object();
    std::string rest = s;
    std::string scheme, opaque, host, path, raw_query, fragment, raw_fragment, raw_path;
    value user;

    // Fragment first: everything after the first '#' is the fragment, wherever
    // the '#' falls.
    if (const size_t h = rest.find('#'); h != std::string::npos) {
        const std::string raw = rest.substr(h + 1);
        fragment = pct_decode(raw, false);
        // raw_fragment records the original spelling ONLY when the default
        // encoding of the decoded value does not reproduce it. `%20` re-encodes
        // to `%20`, so it is not "different" in the sense that matters; `%2F`
        // re-encodes to `/`, so it is. Testing whether decoding changed
        // anything instead reported a raw form for every escaped character.
        if (pct_encode(fragment, /*fragment=*/true) != raw) raw_fragment = raw;
        rest.resize(h);
    }
    if (const size_t q = rest.find('?'); q != std::string::npos) {
        raw_query = rest.substr(q + 1);
        rest.resize(q);
    }
    if (const size_t c = rest.find(':'); c != std::string::npos && c > 0) {
        bool valid = std::isalpha(static_cast<unsigned char>(rest[0])) != 0;
        for (size_t k = 1; k < c && valid; ++k)
            valid = std::isalnum(static_cast<unsigned char>(rest[k])) || rest[k] == '+' ||
                    rest[k] == '-' || rest[k] == '.';
        if (valid) { scheme = rest.substr(0, c); rest = rest.substr(c + 1); }
    }
    if (rest.rfind("//", 0) == 0) {
        rest = rest.substr(2);
        const size_t slash = rest.find('/');
        std::string authority = slash == std::string::npos ? rest : rest.substr(0, slash);
        path = slash == std::string::npos ? std::string{} : rest.substr(slash);
        if (const size_t at = authority.rfind('@'); at != std::string::npos) {
            const std::string info = authority.substr(0, at);
            authority = authority.substr(at + 1);
            reject_bad_userinfo(info);
            reject_bad_escapes(info, /*host=*/false);
            user = value::object();
            const size_t colon = info.find(':');
            user.set("name", value(pct_decode(info.substr(0, colon), false)));
            if (colon != std::string::npos)
                user.set("password", value(pct_decode(info.substr(colon + 1), false)));
        }
        reject_bad_escapes(authority, /*host=*/true);
        reject_bad_host_chars(authority);
        host = pct_decode(authority, false);
        reject_bad_escapes(path, /*host=*/false);
        const std::string decoded = pct_decode(path, false);
        if (pct_encode(decoded, /*fragment=*/false) != path) raw_path = path;
        path = decoded;
    } else if (!scheme.empty()) {
        // No authority: the remainder is opaque, as it is for "mailto:x@y".
        opaque = rest;
    } else {
        reject_bad_escapes(rest, /*host=*/false);
        const std::string decoded = pct_decode(rest, false);
        if (pct_encode(decoded, /*fragment=*/false) != rest) raw_path = rest;
        path = decoded;
    }

    out.set("fragment",     value(fragment));
    out.set("host",         value(host));
    out.set("opaque",       value(opaque));
    out.set("path",         value(path));
    out.set("raw_fragment", value(raw_fragment));
    out.set("raw_path",     value(raw_path));
    out.set("raw_query",    value(raw_query));
    out.set("scheme",       value(scheme));
    if (!user.is_null()) out.set("user", user);
    return out;
}

value parse_yaml(const value& v) {
    try {
        size_t budget = max_yaml_nodes;
        return yaml_to_value(YAML::Load(want_string(v)), budget);
    } catch (const YAML::Exception& e) {
        throw eval_error(std::string("failed to parse YAML: ") + e.what());
    }
}

value format_yaml(const value& v) {
    std::string out;
    yaml_emit(v, out, 0);
    // Bytes, not a string: the documented example calls .string() on the result,
    // which would be a no-op if it were already one.
    return value::bytes(std::move(out));
}

value parse_xml(const value& v, const value& cast) {
    xml_parser p{want_string(v), 0, cast.type() == vtype::boolean && cast.as_bool()};
    value out = value::object();
    bool any = false;
    while (p.i < p.s.size()) {
        p.skip_space();
        if (p.i >= p.s.size()) break;
        if (p.s[p.i] != '<') { ++p.i; continue; }
        if (p.skip_ignorable()) continue;
        ++p.i;
        std::string name;
        value el = p.parse_element(name);
        xml_add(out, name, std::move(el));
        any = true;
    }
    if (!any) throw eval_error("failed to parse XML: no elements found");
    return out;
}

namespace {
void xml_write(const value& v, const std::string& tag, std::string& out,
               const std::string& indent, int depth) {
    const std::string pad = indent.empty() ? std::string{}
                                           : std::string(static_cast<size_t>(depth) * indent.size(), ' ');
    auto nl = [&] { if (!indent.empty()) out += '\n'; };
    auto escape = [](const std::string& s) {
        std::string e;
        for (char c : s) {
            switch (c) {
            case '&':  e += "&amp;";  break;
            case '<':  e += "&lt;";   break;
            case '>':  e += "&gt;";   break;
            case '"':  e += "&#34;";  break;
            case '\'': e += "&#39;";  break;
            default:   e += c;
            }
        }
        return e;
    };

    if (v.type() == vtype::array) {
        // A repeated element: the same tag once per item, which is the inverse
        // of the array rule parse_xml applies. No padding is written here --
        // each recursive call writes its own, and doing it in both places
        // indented every repeated element twice.
        bool first = true;
        for (const auto& el : v.arr()) {
            if (!first) nl();
            first = false;
            xml_write(el, tag, out, indent, depth);
        }
        return;
    }
    out += pad;
    out += '<';
    out += tag;
    if (v.type() == vtype::object) {
        for (const auto& [k, el] : v.obj())
            if (!k.empty() && k[0] == '-')
                out += " " + k.substr(1) + "=\"" + escape(el.to_display_string()) + "\"";
        out += '>';
        bool wrote_child = false;
        for (const auto& [k, el] : v.obj()) {
            if (!k.empty() && k[0] == '-') continue;
            if (k == "#text") { out += escape(el.to_display_string()); continue; }
            nl();
            xml_write(el, k, out, indent, depth + 1);
            wrote_child = true;
        }
        if (wrote_child) { nl(); out += pad; }
        out += "</";
        out += tag;
        out += '>';
        return;
    }
    out += '>';
    out += escape(v.to_display_string());
    out += "</";
    out += tag;
    out += '>';
}
} // namespace

value format_xml(const value& v, const value& indent_v, const value& no_indent,
                 const value& root_tag) {
    if (v.type() != vtype::object) wrong("object", v);
    const bool compact = no_indent.type() == vtype::boolean && no_indent.as_bool();
    const std::string indent = compact ? std::string{}
        : (indent_v.is_stringy() ? indent_v.as_string() : std::string(4, ' '));
    std::string out;
    if (root_tag.is_stringy()) {
        xml_write(v, root_tag.as_string(), out, indent, 0);
    } else {
        // With no root_tag the object's own keys become top-level elements,
        // which is what "derived from the first key" means for a single-key
        // document.
        bool first = true;
        for (const auto& [k, el] : v.obj()) {
            if (!first && !indent.empty()) out += '\n';
            first = false;
            xml_write(el, k, out, indent, 0);
        }
    }
    return value::bytes(std::move(out));
}

value json_path(const value& v, const value& expression) {
    jsonpath jp{want_string(expression), 0};
    std::vector<value> hits = jp.run(v);
    return value::array(std::move(hits));
}

} // namespace sf::m
