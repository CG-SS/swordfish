// Helpers shared by the method implementation files.
//
// methods.cc grew these in an anonymous namespace when it was the only such
// file. There are now several, split by category, and duplicating the argument
// checks across them would be the same drift risk the registry removed: two
// copies of `want_string` could disagree about whether bytes count as a string.
#pragma once

#include "swordfish/value.hh"

#include <cstdint>
#include <string>

namespace sf::m {

// Throws a type_error so the calling expression can say where the value came
// from -- see attach_source in runtime.hh. Previously a plain eval_error, which
// carried the same text but nothing an outer layer could recognise.
[[noreturn]] inline void wrong(const char* what, const value& v) {
    throw type_error(what, v.type_name());
}

// IGetString accepts a []byte as well as a string, so every string method does.
inline const std::string& want_string(const value& v) {
    if (!v.is_stringy()) wrong("string", v);
    return v.as_string();
}

inline bool is_seq(const value& v) { return v.type() == vtype::array; }

inline const std::vector<value>& want_array(const value& v) {
    if (!is_seq(v)) wrong("array", v);
    return v.arr();
}

inline const std::vector<std::pair<std::string, value>>& want_object(const value& v) {
    if (v.type() != vtype::object) wrong("object", v);
    return v.obj();
}

// Normalises a possibly-negative index against a length, Go-style: -1 is the
// last element. The result may still be out of range.
inline int64_t norm_index(int64_t i, size_t len) {
    if (i < 0) i += static_cast<int64_t>(len);
    return i;
}

// A string result keeps the target's stringiness: a method applied to bytes
// returns bytes, which is what Go's `switch t := v.(type)` arms do.
inline value same_stringy(const value& target, std::string s) {
    return target.type() == vtype::bytes ? value::bytes(std::move(s))
                                         : value(std::move(s));
}

} // namespace sf::m
