// Bloblang regex methods, backed by RE2.
//
// RE2 specifically, because Go's `regexp` IS RE2: pattern syntax, capture
// semantics and the replacement template language all match, which is what keeps
// these ~40 methods conformant. std::regex or PCRE would silently differ.
//
// Patterns are compiled once and cached. In compiled mode the emitter goes
// further and hoists each literal pattern to a file-scope `static const sf::re`,
// removing even the cache lookup.
#include "swordfish/regex.hh"

#include "encoding_util.hh"

#include <re2/re2.h>
#include <re2/stringpiece.h>

#include <cctype>
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace sf {

struct re::impl {
    RE2 rx;
    // Computed once at compile time: RE2 exposes the mapping the other way
    // round (name -> index), and re_find_object needs index -> key for every
    // group, unnamed ones included.
    std::vector<std::string> names;
    explicit impl(std::string_view pattern) : rx(re2::StringPiece(pattern.data(), pattern.size())) {
        if (!rx.ok()) return;
        names.assign(static_cast<size_t>(rx.NumberOfCapturingGroups()) + 1, std::string{});
        for (size_t i = 0; i < names.size(); ++i) names[i] = std::to_string(i);
        for (const auto& [name, idx] : rx.NamedCapturingGroups())
            if (idx >= 0 && static_cast<size_t>(idx) < names.size()) names[static_cast<size_t>(idx)] = name;
    }
};

re::re(std::string_view pattern) : p_(std::make_shared<impl>(pattern)) {
    if (!p_->rx.ok())
        throw eval_error("failed to compile regexp: " + p_->rx.error());
}

re::~re() = default;

// Go's regexp.MatchString.
bool re::match(std::string_view s) const {
    return RE2::PartialMatch(re2::StringPiece(s.data(), s.size()), p_->rx);
}

// Bloblang replacement templates use Go's syntax -- $1, ${1}, ${name}, $$ --
// while RE2's rewrite syntax uses \1 and \\. Translate before handing over, or
// every capture group reference silently becomes literal text.
//
// Go's quirk is faithfully reproduced: $1x parses as ${1x}, not ${1} followed by
// "x", which is why the documentation recommends ${1}x.
std::string re::go_template_to_re2(std::string_view repl) const {
    std::string out;
    out.reserve(repl.size() + 8);
    for (size_t i = 0; i < repl.size(); ++i) {
        char c = repl[i];
        if (c == '\\') { out += "\\\\"; continue; }      // literal backslash for RE2
        if (c != '$')    { out += c; continue; }
        if (i + 1 < repl.size() && repl[i + 1] == '$') { out += '$'; ++i; continue; }

        std::string name;
        if (i + 1 < repl.size() && repl[i + 1] == '{') {
            size_t close = repl.find('}', i + 2);
            if (close == std::string_view::npos) { out += '$'; continue; }
            name = std::string(repl.substr(i + 2, close - i - 2));
            i = close;
        } else {
            size_t j = i + 1;
            while (j < repl.size() &&
                   (std::isalnum(static_cast<unsigned char>(repl[j])) || repl[j] == '_')) ++j;
            if (j == i + 1) { out += '$'; continue; }
            name = std::string(repl.substr(i + 1, j - i - 1));
            i = j - 1;
        }

        bool numeric = !name.empty() &&
                       name.find_first_not_of("0123456789") == std::string::npos;
        int index = -1;
        if (numeric) {
            // std::stoi THROWS std::out_of_range for a group number too large
            // for an int -- a raw library exception, not an eval_error, so the
            // pipeline reported the single word "stoi" and failed the message.
            // Go substitutes an empty string for such a reference and carries
            // on: `"abc".re_replace_all("b", "$99999999999999999999")` returns
            // "ac" there. A number that cannot be an index is simply left at -1,
            // which the guard below already treats as out of range -- the very
            // branch this throw was jumping over.
            try {
                index = std::stoi(name);
            } catch (const std::exception&) {
                index = -1;
            }
        } else {
            const auto& named = p_->rx.NamedCapturingGroups();
            auto it = named.find(name);
            if (it != named.end()) index = it->second;
        }
        // Go substitutes an empty string for an unknown or out-of-range group.
        if (index >= 0 && index <= p_->rx.NumberOfCapturingGroups())
            out += "\\" + std::to_string(index);
    }
    return out;
}

// Go's regexp.ReplaceAllString.
std::string re::replace_all(std::string_view s, std::string_view repl) const {
    std::string out(s);
    const std::string rewrite = go_template_to_re2(repl);
    RE2::GlobalReplace(&out, p_->rx, rewrite);
    return out;
}

// FindAndConsume cannot be used here: it requires at least one capturing group,
// while these patterns usually have none. Match() with submatch 0 gives the whole
// match, which is what Go's FindAllString returns.

// Go's two rules for iterating matches, from regexp.allMatches:
//
//   * after an EMPTY match, advance by one RUNE rather than one byte; and
//   * DISCARD an empty match whose start equals the previous match's end.
//
// Neither was implemented. The old loop advanced a single byte and tracked no
// previous end, which produced spurious empties on pure ASCII already --
// `"abc".re_find_all("(?s).*")` gave ["abc",""] against the reference's ["abc"]
// -- and multiplied them on UTF-8, where one byte lands inside a character:
// `"héllo wörld".re_find_all("[a-z]*")` returned ten entries against four.
//
// `prev_end` starts at npos, standing in for Go's -1: no match has ended yet, so
// the first empty match is always accepted.
struct match_walker {
    std::string_view s;
    size_t pos = 0;
    size_t prev_end = std::string::npos;

    // Given the match at [ms, me), decides whether to keep it and advances.
    bool step(size_t ms, size_t me) {
        bool accept = true;
        if (me == pos) {                       // an empty match at the cursor
            if (ms == prev_end) accept = false;
            const size_t w = pos < s.size() ? enc::utf8_rune_len(s, pos) : 0;
            if (w > 0) pos += w;
            else       pos = s.size() + 1;     // past the end: the loop stops
        } else {
            pos = me;
        }
        prev_end = me;                         // recorded even when not accepted
        return accept;
    }
};

std::vector<std::string> re::find_all(std::string_view s) const {
    std::vector<std::string> out;
    re2::StringPiece input(s.data(), s.size());
    re2::StringPiece m;
    match_walker w{s};
    while (w.pos <= input.size() &&
           p_->rx.Match(input, w.pos, input.size(), RE2::UNANCHORED, &m, 1)) {
        const size_t ms = static_cast<size_t>(m.data() - input.data());
        if (w.step(ms, ms + m.size())) out.emplace_back(m.data(), m.size());
    }
    return out;
}

// The whole match plus each capture group, as Go's FindStringSubmatch returns.
std::vector<std::vector<std::string>> re::find_all_submatch(std::string_view s) const {
    const int ngroups = p_->rx.NumberOfCapturingGroups();
    std::vector<std::vector<std::string>> out;
    std::vector<re2::StringPiece> groups(ngroups + 1);
    re2::StringPiece input(s.data(), s.size());
    // The same two rules as find_all; see match_walker above.
    match_walker w{s};
    while (w.pos <= input.size() &&
           p_->rx.Match(input, w.pos, input.size(), RE2::UNANCHORED,
                        groups.data(), ngroups + 1)) {
        const size_t ms = static_cast<size_t>(groups[0].data() - input.data());
        const bool keep = w.step(ms, ms + groups[0].size());
        if (!keep) continue;
        std::vector<std::string> row;
        row.reserve(ngroups + 1);
        for (const auto& g : groups) row.emplace_back(g.data() ? std::string(g.data(), g.size()) : std::string());
        out.push_back(std::move(row));
    }
    return out;
}

std::vector<std::string> re::find_submatch(std::string_view s) const {
    const int ngroups = p_->rx.NumberOfCapturingGroups();
    std::vector<re2::StringPiece> groups(static_cast<size_t>(ngroups) + 1);
    re2::StringPiece input(s.data(), s.size());
    if (!p_->rx.Match(input, 0, input.size(), RE2::UNANCHORED,
                      groups.data(), ngroups + 1))
        return {};
    std::vector<std::string> row;
    row.reserve(groups.size());
    for (const auto& g : groups)
        row.emplace_back(g.data() ? std::string(g.data(), g.size()) : std::string());
    return row;
}

const std::vector<std::string>& re::group_names() const { return p_->names; }

// Cache for the interpreter, which meets each pattern as a runtime string.
const re& cached_re(const std::string& pattern) {
    static thread_local std::map<std::string, re> cache;
    auto it = cache.find(pattern);
    if (it == cache.end()) it = cache.emplace(pattern, re(pattern)).first;
    return it->second;
}

namespace m {

value re_match(const value& v, const re& r)  { return value(r.match(v.as_string())); }

value re_replace_all(const value& v, const re& r, const value& repl) {
    return value(r.replace_all(v.as_string(), repl.as_string()));
}

value re_find_all(const value& v, const re& r) {
    std::vector<value> out;
    for (auto& s : r.find_all(v.as_string())) out.push_back(value(std::move(s)));
    return value::array(std::move(out));
}

value re_find_all_submatch(const value& v, const re& r) {
    std::vector<value> out;
    for (auto& row : r.find_all_submatch(v.as_string())) {
        std::vector<value> inner;
        inner.reserve(row.size());
        for (auto& g : row) inner.push_back(value(std::move(g)));
        out.push_back(value::array(std::move(inner)));
    }
    return value::array(std::move(out));
}

namespace {
value groups_to_object(const std::vector<std::string>& names,
                       const std::vector<std::string>& groups) {
    value o = value::object();
    for (size_t i = 0; i < groups.size() && i < names.size(); ++i)
        o.set(names[i], value(groups[i]));
    return o;
}
} // namespace

value re_find_object(const value& v, const re& r) {
    // No match yields an empty object rather than an error: Go ranges over a
    // nil FindStringSubmatch, which contributes no keys.
    return groups_to_object(r.group_names(), r.find_submatch(v.as_string()));
}

value re_find_all_object(const value& v, const re& r) {
    std::vector<value> out;
    for (const auto& row : r.find_all_submatch(v.as_string()))
        out.push_back(groups_to_object(r.group_names(), row));
    return value::array(std::move(out));
}

} // namespace m
} // namespace sf
