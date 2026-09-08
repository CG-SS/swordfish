#include "swordfish/message.hh"

#include <algorithm>

namespace sf {

static auto lower(std::vector<std::pair<std::string, value>>& e, std::string_view k) {
    return std::lower_bound(e.begin(), e.end(), k,
        [](const auto& p, std::string_view key) { return p.first < key; });
}

const value* metadata::find(std::string_view k) const noexcept {
    auto it = std::lower_bound(e_.begin(), e_.end(), k,
        [](const auto& p, std::string_view key) { return p.first < key; });
    return (it != e_.end() && it->first == k) ? &it->second : nullptr;
}

void metadata::set(std::string_view k, value v) {
    auto it = lower(e_, k);
    if (it != e_.end() && it->first == k) it->second = std::move(v);
    else e_.insert(it, {std::string(k), std::move(v)});
}

void metadata::remove(std::string_view k) {
    auto it = lower(e_, k);
    if (it != e_.end() && it->first == k) e_.erase(it);
}

const std::string& message::as_bytes() const {
    static const std::string empty;
    if (!raw_) {
        if (!structured_) return empty;
        raw_ = std::make_shared<const std::string>(structured_->to_json());
    }
    return *raw_;
}

const value& message::as_structured() const {
    if (!structured_) structured_ = parse_json(as_bytes());
    return *structured_;
}

// cppcheck-suppress unusedFunction
//   Public API for `cpp:` blocks, and used by the config tests.
value& message::as_structured_mut() {
    if (!structured_) structured_ = parse_json(as_bytes());
    raw_.reset();                     // bytes are now stale
    return *structured_;
}

void message::set_bytes(std::string raw) {
    raw_ = std::make_shared<const std::string>(std::move(raw));
    structured_.reset();
}

void message::set_structured(value v) {
    structured_ = std::move(v);
    raw_.reset();
}

bool message::set_mapped(value v) {
    if (v.is_nothing()) return false;
    if (v.is_stringy()) {              // string or []byte: written verbatim
        set_bytes(v.as_string());
        return true;
    }
    set_structured(std::move(v));
    return true;
}

} // namespace sf
