// Generic algorithms over spec_of<T>. Every one of these walks the same table.
#pragma once

#include <map>

#include <algorithm>

namespace sf::cfg {

// ---- helpers ---------------------------------------------------------------

inline std::string join_path(const std::string& base, std::string_view name) {
    return base.empty() ? std::string(name) : base + "." + std::string(name);
}

// Levenshtein, capped — used for "did you mean" on unknown keys, which is a
// large part of why `benthos lint` is pleasant to use.
inline size_t edit_distance(std::string_view a, std::string_view b) {
    std::vector<size_t> prev(b.size() + 1), cur(b.size() + 1);
    for (size_t j = 0; j <= b.size(); ++j) prev[j] = j;
    for (size_t i = 1; i <= a.size(); ++i) {
        cur[0] = i;
        for (size_t j = 1; j <= b.size(); ++j)
            cur[j] = std::min({prev[j] + 1, cur[j - 1] + 1, prev[j - 1] + (a[i - 1] != b[j - 1])});
        prev = cur;
    }
    return prev[b.size()];
}

// ---- parse -----------------------------------------------------------------

template <class T>
void parse_into(const ynode& n, T& out, const std::string& path, lints& ls) {
    static_assert(has_spec<T>, "type has no spec_of specialisation");

    if (!n.is_null() && !n.is_mapping()) {
        ls.push_back({lint_level::error, n.pos, path, "expected an object"});
        return;
    }

    // Collect declared names first so unknown keys can be reported with a hint.
    std::vector<std::string_view> known;
    std::apply([&](auto const&... f) { (known.push_back(f.name), ...); }, spec_of<T>::value);

    if (n.is_mapping()) {
        for (const auto& [key, child] : n.map) {
            if (std::find(known.begin(), known.end(), key) != known.end()) continue;
            std::string msg = "field '" + key + "' is not recognised";
            std::string_view best;
            size_t best_d = 3;                       // only suggest close matches
            for (auto k : known) {
                size_t d = edit_distance(key, k);
                if (d < best_d) { best_d = d; best = k; }
            }
            if (!best.empty()) msg += "; did you mean '" + std::string(best) + "'?";
            ls.push_back({lint_level::error, child.pos, join_path(path, key), std::move(msg)});
        }
    }

    std::apply([&](auto const&... f) {
        auto one = [&](auto const& fd) {
            using M = typename std::decay_t<decltype(fd)>::member;
            const std::string fpath = join_path(path, fd.name);
            const ynode* child = n.is_mapping() ? n.find(fd.name) : nullptr;

            if (!child || child->is_null()) {
                if (fd.required)
                    ls.push_back({lint_level::error, n.pos, fpath, "required field is missing"});
                return;                              // leave the struct default in place
            }
            if (fd.deprecated)
                ls.push_back({lint_level::warning, child->pos, fpath, "field is deprecated"});

            codec<M>::parse(*child, out.*(fd.ptr), fpath, ls);

            // `choice` is validated against options the same way a string is:
            // it IS a string, wearing an object's clothes in the YAML.
            if constexpr (std::is_same_v<M, std::string> || std::is_same_v<M, choice>) {
                if (fd.has_options()) {
                    const std::string& v = [&]() -> const std::string& {
                        if constexpr (std::is_same_v<M, choice>) return (out.*(fd.ptr)).name;
                        else                                    return out.*(fd.ptr);
                    }();
                    if (std::find(fd.options_begin, fd.options_end, v) == fd.options_end) {
                        std::string msg = "value '" + v + "' is not one of: ";
                        for (auto o = fd.options_begin; o != fd.options_end; ++o) {
                            if (o != fd.options_begin) msg += ", ";
                            msg += std::string(*o);
                        }
                        ls.push_back({lint_level::error, child->pos, fpath, std::move(msg)});
                    }
                }
            }
        };
        (one(f), ...);
    }, spec_of<T>::value);
}

template <class T>
void codec<T, std::enable_if_t<has_spec<T>>>::parse(
        const ynode& n, T& out, const std::string& path, lints& ls) {
    parse_into<T>(n, out, path, ls);
}

// ---- emit ------------------------------------------------------------------

template <class T>
std::string emit_struct(const T& cfg, std::string_view type_name, emit_ctx& c) {
    static_assert(has_spec<T>, "type has no spec_of specialisation");
    const T defaults{};

    std::vector<std::string> parts;
    ++c.indent;                     // nested structs must render at the deeper level
    std::apply([&](auto const&... f) {
        auto one = [&](auto const& fd) {
            using M = typename std::decay_t<decltype(fd)>::member;
            const M& v = cfg.*(fd.ptr);
            // Emit only what differs from the struct's own defaults, so the
            // generated source stays readable and diffable. This requires the
            // field type to be equality-comparable -- config structs should
            // declare `bool operator==(const T&) const = default;` or their
            // defaulted fields will be emitted redundantly.
            if constexpr (requires { v == defaults.*(fd.ptr); })
                if (v == defaults.*(fd.ptr)) return;
            parts.push_back("." + std::string(fd.name) + " = " + codec<M>::emit(v, c));
        };
        (one(f), ...);
    }, spec_of<T>::value);

    --c.indent;
    if (parts.empty()) return std::string(type_name) + "{}";

    std::string s = std::string(type_name) + "{\n";
    ++c.indent;
    for (const auto& p : parts) s += c.pad() + p + ",\n";
    --c.indent;
    return s + c.pad() + "}";
}

template <class T>
std::string codec<T, std::enable_if_t<has_spec<T>>>::emit(const T& v, emit_ctx& c) {
    return emit_struct(v, spec_of<T>::cpp_type, c);
}

template <class T>
std::string emit_cpp(const T& cfg, std::string_view type_name) {
    emit_ctx c;
    return emit_struct(cfg, type_name, c);
}

// ---- bloblang field collection ---------------------------------------------
// Lets a caller validate every Bloblang field of a config without knowing its
// type. Derived from the same table as everything else.
// An `interpolation` seen as the query it becomes. Stored per source string in a
// function-local cache so the pointer stays valid after collect_bloblang returns
// -- the interface hands out raw pointers, and an interpolation has no bloblang
// of its own to point at.
inline const bloblang& interp_as_query(const interpolation& iv) {
    static std::map<std::string, bloblang> cache;
    auto it = cache.find(iv.source);
    if (it == cache.end())
        it = cache.emplace(iv.source,
                           bloblang{interpolation_to_query(iv.source), true}).first;
    return it->second;
}

template <class T>
void collect_bloblang(const T& cfg, std::vector<const bloblang*>& out) {
    static_assert(has_spec<T>, "type has no spec_of specialisation");
    std::apply([&](auto const&... f) {
        auto one = [&](auto const& fd) {
            using M = typename std::decay_t<decltype(fd)>::member;
            if constexpr (std::is_same_v<M, bloblang>) {
                out.push_back(&(cfg.*(fd.ptr)));
            } else if constexpr (std::is_same_v<M, interpolation>) {
                // An `interpolation` is Bloblang too -- every consumer runs it
                // through interpolation_to_query and then parse_query -- and it
                // was invisible here, so a syntax error inside `${! ... }` was
                // not a config error but a failure on the first message, with no
                // line and no field name. The bloblang is a per-config cache,
                // because the pointers handed out must outlive this call.
                const interpolation& iv = cfg.*(fd.ptr);
                if (!iv.source.empty())
                    out.push_back(&interp_as_query(iv));
            } else if constexpr (has_spec<M>) {
                collect_bloblang(cfg.*(fd.ptr), out);      // nested config
            }
        };
        (one(f), ...);
    }, spec_of<T>::value);
}

// ---- describe --------------------------------------------------------------

// Traits used only by scaffold(): whether a member is a container it should
// render structurally rather than through show(). show() is a DOCUMENTATION
// renderer -- it prints "{0 entries}" for a map -- so a scaffold that used it
// for containers would emit YAML that does not parse.
template <class> struct sf_is_vector : std::false_type {};
template <class T> struct sf_is_vector<std::vector<T>> : std::true_type {};
template <class> struct sf_is_map : std::false_type {};
template <class T> struct sf_is_map<std::map<std::string, T>> : std::true_type {};
template <class> struct sf_is_optional : std::false_type {};
template <class T> struct sf_is_optional<std::optional<T>> : std::true_type {};

// A field's default as YAML. NOT codec<M>::show(): that renders for
// documentation -- it truncates a Bloblang mapping to forty characters with an
// ellipsis, prints "(none)" for an empty one and "{0 entries}" for a map -- and
// a scaffold has to parse. Only the types whose show() is not already valid
// YAML are named here; bool, int, double and duration render the same either
// way.
template <class M>
std::string scaffold_scalar(const M& v) {
    if constexpr (std::is_same_v<M, std::string>)
        return yaml_scalar(v, true);
    else if constexpr (std::is_same_v<M, bloblang>)
        return yaml_scalar(v.source, true);
    else if constexpr (std::is_same_v<M, interpolation>)
        return yaml_scalar(v.source, true);
    else if constexpr (std::is_same_v<M, choice>)
        return v.name.empty() ? std::string("{}") : yaml_scalar(v.name, false);
    else
        return codec<M>::show(v);
}

template <class T>
std::string scaffold(int indent) {
    static_assert(has_spec<T>, "type has no spec_of specialisation");
    const T defaults{};
    const std::string pad(static_cast<size_t>(indent), ' ');
    std::ostringstream os;
    std::apply([&](auto const&... f) {
        auto one = [&](auto const& fd) {
            using M = typename std::decay_t<decltype(fd)>::member;
            // Deprecated fields are left out: a scaffold is a starting point,
            // and starting someone on a field the reference has deprecated
            // would be actively unhelpful.
            if (fd.deprecated) return;
            os << pad << fd.name << ":";
            if constexpr (has_spec<M>) {
                // A nested object renders its own fields underneath. `{}` when
                // it has none, or the key would have no value at all.
                const std::string inner = scaffold<M>(indent + 2);
                if (inner.empty()) os << " {}\n";
                else               os << "\n" << inner;
                return;
            } else if constexpr (sf_is_vector<M>::value) {
                os << " []";
            } else if constexpr (sf_is_map<M>::value) {
                os << " {}";
            } else if constexpr (sf_is_optional<M>::value) {
                // Unset by default. `null` rather than an omitted key, so the
                // field is visible in the scaffold and can be filled in.
                os << " null";
            } else {
                os << " " << scaffold_scalar<M>(defaults.*(fd.ptr));
            }
            // The comment carries what a reader needs to fill the field in: that
            // it is required, and what it is for. The reference marks required
            // fields the same way ("# No default (required)").
            if (fd.required)              os << "   # (required)";
            else if (fd.advanced)         os << "   # (advanced)";
            if (!fd.description.empty())  os << (fd.required || fd.advanced ? " " : "   # ")
                                             << fd.description;
            os << "\n";
        };
        (one(f), ...);
    }, spec_of<T>::value);
    return os.str();
}

template <class T>
std::string describe(std::string_view name, bool include_advanced) {
    static_assert(has_spec<T>, "type has no spec_of specialisation");
    const T defaults{};
    std::ostringstream os;
    os << name << "\n";
    std::apply([&](auto const&... f) {
        auto one = [&](auto const& fd) {
            using M = typename std::decay_t<decltype(fd)>::member;
            if (fd.advanced && !include_advanced) return;
            os << "  " << fd.name << "  <" << codec<M>::type_name() << ">";
            if (fd.required)   os << "  (required)";
            if (fd.advanced)   os << "  (advanced)";
            if (fd.deprecated) os << "  (deprecated)";
            if (!fd.required)  os << "  default: " << codec<M>::show(defaults.*(fd.ptr));
            os << "\n";
            if (!fd.description.empty()) os << "      " << fd.description << "\n";
            if (fd.has_options()) {
                os << "      one of: ";
                for (auto o = fd.options_begin; o != fd.options_end; ++o) {
                    if (o != fd.options_begin) os << ", ";
                    os << *o;
                }
                os << "\n";
            }
        };
        (one(f), ...);
    }, spec_of<T>::value);
    return os.str();
}

} // namespace sf::cfg
