#pragma once
#include "swordfish/value.hh"
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace sf {

// A compiled pattern. Copyable and cheap to copy: the compiled RE2 is shared.
class re {
public:
    explicit re(std::string_view pattern);
    ~re();
    re(const re&) = default;
    re& operator=(const re&) = default;

    bool                                  match(std::string_view s) const;
    std::string                           replace_all(std::string_view s, std::string_view repl) const;
    std::vector<std::string>              find_all(std::string_view s) const;
    std::vector<std::vector<std::string>> find_all_submatch(std::string_view s) const;
    // The first match only, as Go's FindStringSubmatch returns it. Empty when
    // the pattern does not match at all -- which is distinct from a match whose
    // groups are all empty, so the emptiness of the outer vector is the test.
    std::vector<std::string>              find_submatch(std::string_view s) const;
    // Group 0 is the whole match. An unnamed group is keyed by its index, as a
    // string, which is what re_find_object's documented output shows.
    const std::vector<std::string>&       group_names() const;

    // Go template ($1, ${name}, $$) -> RE2 rewrite (\\1). Exposed for testing.
    std::string                           go_template_to_re2(std::string_view repl) const;

private:
    struct impl;
    std::shared_ptr<impl> p_;
};

// Interpreter path: patterns arrive as runtime strings, so they are cached.
// Generated code hoists them to file-scope statics instead.
const re& cached_re(const std::string& pattern);

namespace m {
value re_match(const value& v, const re& r);
value re_replace_all(const value& v, const re& r, const value& repl);
value re_find_all(const value& v, const re& r);
value re_find_all_submatch(const value& v, const re& r);
value re_find_object(const value& v, const re& r);
value re_find_all_object(const value& v, const re& r);
}

} // namespace sf
