// YAML reader, backed by yaml-cpp.
//
// Replaces the hand-rolled subset reader. The `ynode` interface is unchanged, so
// nothing downstream moves; what changes is coverage of the long tail the subset
// reader never handled — anchors and aliases, multi-document streams, explicit
// tags, and the full range of scalar styles.
#include "swordfish/config/yaml.hh"
#include <stdexcept>
#include <sstream>
#include <fstream>
#include <cstdlib>

#include <yaml-cpp/yaml.h>

namespace sf::cfg {
namespace {

position pos_of(const YAML::Node& n) {
    const auto m = n.Mark();
    // yaml-cpp counts from zero; every diagnostic we print counts from one.
    return position{static_cast<uint32_t>(m.line + 1), static_cast<uint32_t>(m.column + 1)};
}

ynode convert(const YAML::Node& n) {
    ynode out;
    out.pos = pos_of(n);
    switch (n.Type()) {
    case YAML::NodeType::Null:
    case YAML::NodeType::Undefined:
        out.type = ynode_type::null;
        break;
    case YAML::NodeType::Scalar:
        out.type = ynode_type::scalar;
        out.scalar = n.Scalar();
        // Quoted and block scalars must never be re-read as bool or number:
        // `auto_replay_nacks: "true"` is a string field in some components.
        // yaml-cpp gives a plain scalar the non-specific tag "?" and a quoted or
        // block scalar the tag "!".
        out.quoted = n.Tag() == "!";
        break;
    case YAML::NodeType::Sequence:
        out.type = ynode_type::sequence;
        out.seq.reserve(n.size());
        for (const auto& item : n) out.seq.push_back(convert(item));
        break;
    case YAML::NodeType::Map:
        out.type = ynode_type::mapping;
        out.map.reserve(n.size());
        for (const auto& kv : n)
            out.map.emplace_back(kv.first.Scalar(), convert(kv.second));
        break;
    }
    return out;
}

} // namespace

ynode parse_yaml(std::string_view text) {
    try {
        YAML::Node root = YAML::Load(std::string(text));
        return convert(root);
    } catch (const YAML::ParserException& e) {
        throw yaml_error({static_cast<uint32_t>(e.mark.line + 1),
                          static_cast<uint32_t>(e.mark.column + 1)}, e.msg);
    } catch (const YAML::Exception& e) {
        throw yaml_error({0, 0}, e.what());
    }
}

} // namespace sf::cfg

namespace sf::cfg {

std::string substitute_env(std::string_view text, std::vector<std::string>& missing) {
    std::string out;
    out.reserve(text.size());
    for (size_t i = 0; i < text.size();) {
        if (text[i] != '$' || i + 1 >= text.size() || text[i + 1] != '{') {
            out += text[i++];
            continue;
        }
        const size_t close = text.find('}', i + 2);
        if (close == std::string_view::npos) { out += text[i++]; continue; }
        const std::string_view body = text.substr(i + 2, close - (i + 2));
        // `${}` is not a reference, and neither is `${! ... }` -- that is a
        // Bloblang INTERPOLATION, evaluated per message at runtime. Treating it
        // as an environment variable broke two working configs by demanding a
        // variable named "! content() ".
        //
        // The interpolation may itself contain `}` (`${! this.a.or({}) }`), so
        // the closing brace found above is not necessarily its end; it is left
        // alone byte for byte either way.
        if (body.empty() || body.front() == '!') {
            out += text.substr(i, close - i + 1);
            i = close + 1;
            continue;
        }
        // First colon only: a default may itself contain colons, which a URL
        // default routinely does.
        const size_t colon = body.find(':');
        const std::string name(body.substr(0, colon));
        const bool has_default = colon != std::string_view::npos;
        const char* env = ::getenv(name.c_str());
        if (env)                out += env;
        else if (has_default)   out += std::string(body.substr(colon + 1));
        else {
            // Left verbatim, and reported. Substituting an empty string would
            // produce a config that parses and then behaves wrongly, which is
            // worse than refusing to start.
            missing.push_back(name);
            out += text.substr(i, close - i + 1);
        }
        i = close + 1;
    }
    return out;
}

namespace {

// A plain (unquoted) scalar is only safe when re-reading it gives back the same
// STRING and the same "was it quoted" answer. Everything a parse produced as
// plain already satisfies that -- YAML would not have parsed it plain
// otherwise -- but a node built in memory (a template's output, a stream posted
// to the API) has never been through a parser, so the check is made here rather
// than assumed.
bool plain_safe(const std::string& s) {
    if (s.empty()) return false;                       // an empty plain scalar is null
    if (s.front() == ' ' || s.back() == ' ') return false;
    if (s.find_first_of("\n\r\t") != std::string::npos) return false;
    // Indicators, which change what the line means when they lead.
    //
    // `-`, `?` and `:` are indicators only when a space (or the end) follows:
    // `- x` is a sequence entry, `-1` and `--kafka-addr ...` are ordinary plain
    // scalars. Treating them as indicators unconditionally quoted both, which
    // flipped their `quoted` flag and made `-1` into the STRING "-1" on the next
    // read. The corpus round trip found exactly these two, in a docker-compose
    // command line and in an `insert_part.index: -1`.
    if (std::string_view("-?:").find(s.front()) != std::string_view::npos) {
        if (s.size() == 1 || s[1] == ' ' || s[1] == '\t') return false;
    } else if (std::string_view(",[]{}#&*!|>'\"%@`").find(s.front()) != std::string_view::npos) {
        return false;
    }
    // `: ` starts a mapping and ` #` starts a comment wherever they appear.
    if (s.find(": ") != std::string::npos) return false;
    if (s.find(" #") != std::string::npos) return false;
    // A plain scalar that reads back as null, a bool or a number is still the
    // same STRING, and scalar_to_value() is what decides the type -- so those
    // are safe here. What is not safe is anything with a control character.
    for (const unsigned char c : s)
        if (c < 0x20 && c != '\t') return false;
    return true;
}

// A block scalar (`|`) is the readable way to write a mapping or a script, and
// it is what makes an echoed config worth looking at. It cannot represent every
// string: a line with trailing whitespace loses it, and content starting with a
// space needs an explicit indentation indicator this does not emit.
bool block_safe(const std::string& s) {
    if (s.empty()) return false;
    if (s.front() == ' ' || s.front() == '\t') return false;
    size_t start = 0;
    for (;;) {
        const size_t nl = s.find('\n', start);
        const std::string line = s.substr(start, nl == std::string::npos ? nl : nl - start);
        // A trailing space would be stripped when the block is read back.
        if (!line.empty() && (line.back() == ' ' || line.back() == '\t')) return false;
        for (const unsigned char c : line)
            if (c < 0x20 && c != '\t') return false;
        if (nl == std::string::npos) break;
        start = nl + 1;
    }
    return true;
}

void write_double_quoted(std::string& out, const std::string& s) {
    out += '"';
    for (const char ch : s) {
        const auto c = static_cast<unsigned char>(ch);
        switch (c) {
        case '"':  out += "\\\""; break;
        case '\\': out += "\\\\"; break;
        case '\n': out += "\\n";  break;
        case '\r': out += "\\r";  break;
        case '\t': out += "\\t";  break;
        default:
            if (c < 0x20) {
                char b[8];
                std::snprintf(b, sizeof b, "\\x%02x", c);
                out += b;
            } else {
                out += ch;
            }
        }
    }
    out += '"';
}

void write_single_quoted(std::string& out, const std::string& s) {
    out += '\'';
    for (const char c : s) {
        out += c;
        if (c == '\'') out += '\'';        // '' is how a quote escapes itself
    }
    out += '\'';
}

// Writes a scalar in the least noisy form that reads back identically.
// `force_quoted` is the node's own `quoted` flag: a scalar that arrived quoted
// has to leave quoted, or `"5"` becomes the number 5 on the next read.
void write_scalar(std::string& out, const std::string& s, bool force_quoted, int indent) {
    if (!force_quoted && plain_safe(s)) { out += s; return; }
    if (s.find('\n') != std::string::npos && block_safe(s)) {
        // `|` keeps one trailing newline, `|-` keeps none. Anything else --
        // two or more trailing newlines -- needs `|+`, and rather than emit a
        // form whose chomping is easy to get wrong, those fall through to
        // double quotes below.
        size_t trailing = 0;
        for (auto it = s.rbegin(); it != s.rend() && *it == '\n'; ++it) ++trailing;
        if (trailing <= 1) {
            out += (trailing == 1) ? "|" : "|-";
            out += '\n';
            const std::string pad(static_cast<size_t>(indent) + 2, ' ');
            size_t start = 0;
            const std::string body = trailing == 1 ? s.substr(0, s.size() - 1) : s;
            for (;;) {
                const size_t nl = body.find('\n', start);
                out += pad;
                out += body.substr(start, nl == std::string::npos ? nl : nl - start);
                out += '\n';
                if (nl == std::string::npos) break;
                start = nl + 1;
            }
            return;
        }
    }
    if (s.find_first_of("\n\r") == std::string::npos) {
        bool ctrl = false;
        for (const unsigned char c : s) if (c < 0x20) ctrl = true;
        if (!ctrl) { write_single_quoted(out, s); return; }
    }
    write_double_quoted(out, s);
}

void write_node(std::string& out, const ynode& n, int indent);

void write_key(std::string& out, const std::string& key) {
    if (plain_safe(key)) out += key;
    else                 write_single_quoted(out, key);
}

// A value that follows `key:` on the same line, or opens a block beneath it.
void write_value_after_key(std::string& out, const ynode& v, int indent) {
    switch (v.type) {
    case ynode_type::null:
        out += " null\n";
        return;
    case ynode_type::scalar: {
        out += ' ';
        const size_t before = out.size();
        write_scalar(out, v.scalar, v.quoted, indent);
        // A block scalar writes its own newline; every other form does not.
        if (out.size() > before && out.back() != '\n') out += '\n';
        return;
    }
    case ynode_type::mapping:
        if (v.map.empty()) { out += " {}\n"; return; }
        out += '\n';
        write_node(out, v, indent + 2);
        return;
    case ynode_type::sequence:
        if (v.seq.empty()) { out += " []\n"; return; }
        out += '\n';
        write_node(out, v, indent + 2);
        return;
    }
}

void write_node(std::string& out, const ynode& n, int indent) {
    const std::string pad(static_cast<size_t>(indent), ' ');
    switch (n.type) {
    case ynode_type::mapping:
        for (const auto& [k, v] : n.map) {
            out += pad;
            write_key(out, k);
            out += ':';
            write_value_after_key(out, v, indent);
        }
        return;
    case ynode_type::sequence:
        for (const auto& item : n.seq) {
            out += pad;
            out += '-';
            switch (item.type) {
            case ynode_type::null:   out += " null\n"; break;
            case ynode_type::scalar: {
                out += ' ';
                const size_t before = out.size();
                write_scalar(out, item.scalar, item.quoted, indent);
                if (out.size() > before && out.back() != '\n') out += '\n';
                break;
            }
            case ynode_type::mapping:
                if (item.map.empty()) { out += " {}\n"; break; }
                // The first key shares the `- ` line, which is how YAML is
                // conventionally written and what the reference emits.
                out += ' ';
                {
                    std::string inner;
                    write_node(inner, item, indent + 2);
                    // Drop the leading indent of the first line only.
                    out += inner.substr(static_cast<size_t>(indent) + 2);
                }
                break;
            case ynode_type::sequence:
                if (item.seq.empty()) { out += " []\n"; break; }
                out += '\n';
                write_node(out, item, indent + 2);
                break;
            }
        }
        return;
    case ynode_type::scalar: {
        out += pad;
        const size_t before = out.size();
        write_scalar(out, n.scalar, n.quoted, indent);
        if (out.size() > before && out.back() != '\n') out += '\n';
        return;
    }
    case ynode_type::null:
        out += pad;
        out += "null\n";
        return;
    }
}

} // namespace

std::string yaml_scalar(std::string_view s, bool quoted) {
    std::string out;
    write_scalar(out, std::string(s), quoted, 0);
    // write_scalar ends a block scalar with a newline; a caller placing this
    // after `key:` wants a value, not a line.
    while (!out.empty() && out.back() == '\n') out.pop_back();
    return out;
}

std::string to_yaml(const ynode& n) {
    std::string out;
    if (n.type == ynode_type::mapping && n.map.empty()) return "{}\n";
    if (n.type == ynode_type::sequence && n.seq.empty()) return "[]\n";
    write_node(out, n, 0);
    return out;
}

bool same_shape(const ynode& a, const ynode& b) {
    if (a.type != b.type) return false;
    switch (a.type) {
    case ynode_type::null:   return true;
    case ynode_type::scalar: return a.scalar == b.scalar && a.quoted == b.quoted;
    case ynode_type::sequence:
        if (a.seq.size() != b.seq.size()) return false;
        for (size_t i = 0; i < a.seq.size(); ++i)
            if (!same_shape(a.seq[i], b.seq[i])) return false;
        return true;
    case ynode_type::mapping:
        if (a.map.size() != b.map.size()) return false;
        for (size_t i = 0; i < a.map.size(); ++i)
            if (a.map[i].key != b.map[i].key || !same_shape(a.map[i].value, b.map[i].value))
                return false;
        return true;
    }
    return false;
}

value scalar_to_value(const ynode& n) {
    if (n.quoted) return value(n.scalar);
    const std::string& s = n.scalar;
    if (s.empty() || s == "null" || s == "~")   return value();
    if (s == "true")  return value(true);
    if (s == "false") return value(false);
    // strtoll/strtod rather than a regex: the whole string must be consumed, or
    // "5x" would silently become 5.
    char* end = nullptr;
    const long long i = std::strtoll(s.c_str(), &end, 10);
    if (end && *end == '\0' && end != s.c_str()) return value(static_cast<int64_t>(i));
    end = nullptr;
    const double d = std::strtod(s.c_str(), &end);
    // A plain double, NOT a raw_number. `swordfish test` compares against this
    // and has done since it was written; making the numeric type depend on how
    // the scalar happened to be spelt would change what `json_equals` means, and
    // this function moved here to be SHARED, not to be changed on the way.
    if (end && *end == '\0' && end != s.c_str()) return value(d);
    return value(s);
}

ynode value_to_node(const value& v, position pos) {
    ynode out;
    out.pos = pos;
    switch (v.type()) {
    case vtype::null:
    case vtype::deleted:
    case vtype::nothing:
        out.type = ynode_type::null;
        return out;
    case vtype::array: {
        out.type = ynode_type::sequence;
        out.seq.reserve(v.arr().size());
        for (const auto& e : v.arr()) out.seq.push_back(value_to_node(e, pos));
        return out;
    }
    case vtype::object: {
        out.type = ynode_type::mapping;
        out.map.reserve(v.obj().size());
        for (const auto& [k, e] : v.obj()) out.map.push_back({k, value_to_node(e, pos)});
        return out;
    }
    case vtype::string:
    case vtype::bytes:
        out.type = ynode_type::scalar;
        out.scalar = v.as_string();
        // QUOTED, so the scalar is read back as the string it is. Without this
        // a template that produced the string "5" -- or "true", or "null" --
        // would have it inferred back into a number the next time the document
        // was read, which is precisely the round trip the two functions here
        // exist to make faithful.
        out.quoted = true;
        return out;
    default:
        out.type = ynode_type::scalar;
        // to_display_string() rather than to_json(): a string would come back
        // quoted, and every case that reaches here is a scalar already.
        out.scalar = v.to_display_string();
        return out;
    }
}

value node_to_value(const ynode& n) {
    switch (n.type) {
    case ynode_type::sequence: {
        std::vector<value> v;
        v.reserve(n.seq.size());
        for (const auto& e : n.seq) v.push_back(node_to_value(e));
        return value::array(std::move(v));
    }
    case ynode_type::mapping: {
        value o = value::object();
        for (const auto& e : n.map) o.set(e.key, node_to_value(e.value));
        return o;
    }
    case ynode_type::null:   return value();
    case ynode_type::scalar: return scalar_to_value(n);
    }
    return value();
}

ynode load_config_text(std::string_view text, const std::string& where, bool require_env) {
    std::vector<std::string> missing;
    const std::string subst = substitute_env(text, missing);
    if (require_env && !missing.empty()) {
        std::string names;
        for (const auto& m : missing) names += (names.empty() ? "" : " ") + m;
        // No prefix when `where` is empty: every FILE caller already prefixes
        // the path, and a bare leading ": " would have changed a message the
        // gates assert on.
        throw std::runtime_error((where.empty() ? "" : where + ": ") +
                                 "required environment variables were not set: [" + names + "]");
    }
    return parse_yaml(subst);
}

ynode load_config(const std::string& path, bool require_env) {
    std::ifstream f(path, std::ios::binary);
    if (!f) throw std::runtime_error("cannot open " + path);
    std::ostringstream ss;
    ss << f.rdbuf();
    // The message here has never carried the path -- every caller prefixes it
    // -- so the file case keeps that shape by passing an empty `where`.
    return load_config_text(ss.str(), "", require_env);
}

} // namespace sf::cfg
