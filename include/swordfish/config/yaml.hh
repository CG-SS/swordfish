// A YAML subset reader for configuration files.
//
// Backed by yaml-cpp, which Seastar already links. What matters here is the
// node model —
// every node carries a source position, because lint messages that cannot point
// at a line are useless, and matching `benthos lint` output is a project goal.
//
// Covers what Redpanda Connect configs actually use: block mappings, block
// sequences, plain/quoted scalars, block scalars (| > with - + chomping), flow
// {} and [], comments, and empty values.
#pragma once

#include "swordfish/value.hh"

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

namespace sf::cfg {

struct position { uint32_t line = 0, col = 0; };

class yaml_error : public std::runtime_error {
public:
    yaml_error(position p, std::string msg) : std::runtime_error(std::move(msg)), where(p) {}
    position where;
};

enum class ynode_type { null, scalar, mapping, sequence };

class ynode {
public:
    ynode_type            type = ynode_type::null;
    position              pos;
    std::string           scalar;
    bool                  quoted = false;      // was the scalar quoted or a block scalar
    // A mapping's entries, in the order they were written. Deliberately NOT
    // std::vector<std::pair<std::string, ynode>>: instantiating that pair asks
    // whether ynode is implicitly default-constructible, which needs the
    // vector's destructor, which needs the pair -- a cycle GCC tolerates and
    // clang rejects. A plain aggregate asks nothing until ynode is complete,
    // and vector over an incomplete element type is explicitly allowed.
    struct entry;
    std::vector<entry>    map;
    std::vector<ynode>    seq;

    bool is_null()     const { return type == ynode_type::null; }
    bool is_scalar()   const { return type == ynode_type::scalar; }
    bool is_mapping()  const { return type == ynode_type::mapping; }
    bool is_sequence() const { return type == ynode_type::sequence; }

    // Defined out of line, below: they read `map`, whose element type is only
    // complete once ynode is.
    const ynode* find(std::string_view key) const;
    std::vector<std::string> keys() const;
    // A single-key mapping is how Benthos selects a component: input: { kafka: {...} }
    std::optional<std::pair<std::string, const ynode*>> single_key() const;
};

struct ynode::entry {
    std::string key;
    ynode       value;
};

inline const ynode* ynode::find(std::string_view key) const {
    for (const auto& [k, v] : map) if (k == key) return &v;
    return nullptr;
}

inline std::vector<std::string> ynode::keys() const {
    std::vector<std::string> ks;
    ks.reserve(map.size());
    for (const auto& [k, v] : map) ks.push_back(k);
    return ks;
}

inline std::optional<std::pair<std::string, const ynode*>> ynode::single_key() const {
    if (type != ynode_type::mapping || map.size() != 1) return std::nullopt;
    return std::pair{map[0].key, &map[0].value};
}

ynode parse_yaml(std::string_view text);

// ---- ynode <-> value -------------------------------------------------------
//
// Templates need both directions -- a template's fields are handed to a
// Bloblang mapping as a `value`, and the config the mapping returns is spliced
// back into the document as a `ynode` -- and `swordfish test` already needed the
// first one. It lived as a static helper in unit_test.cc; two implementations of
// "what does this YAML scalar mean" is exactly the kind of pair that drifts, so
// there is one.
//
// A YAML scalar is untyped text, so the usual inference is applied: `5` is a
// number, `true` a boolean, `null`/`~`/empty a null. `quoted` suppresses it,
// which is how a config says it means the STRING "5".
value scalar_to_value(const ynode& n);
value node_to_value(const ynode& n);

// The other way. `pos` is stamped on every node produced, so an error reported
// against a template's OUTPUT points at the template usage in the user's config
// rather than at line 0 of nowhere.
//
// A number keeps the text it round-trips through, not a re-rendered form: a
// config value that arrived as `1e21` must not reach a component as
// `1000000000000000000000`.
ynode value_to_node(const value& v, position pos = {});

// ---- writing YAML back out -------------------------------------------------
//
// `swordfish echo` prints a config after `${VAR}` has been resolved, and the
// streams API reports a stream's config back through the same path. The
// requirement on this function is not that it is pretty but that it is
// FAITHFUL: parse_yaml(to_yaml(n)) has to give back n, including whether each
// scalar was quoted -- a config where `"true"` came back as `true` would change
// what the field means.
//
// Block style always, and flow style never, which is one cosmetic difference
// from the reference: its echo preserves whatever style the input used.
std::string to_yaml(const ynode& n);

// One scalar, quoted exactly as to_yaml() would quote it. Exposed so
// `swordfish create` can render a field value without a second quoting
// implementation -- the codecs' own show() is a DOCUMENTATION renderer, which
// truncates a long Bloblang mapping to forty characters and prints "(none)" for
// an empty one, and neither is YAML.
std::string yaml_scalar(std::string_view s, bool quoted);

// Structural equality, ignoring source positions -- which differ by
// construction after a round trip. Exposed because the round-trip property is
// worth asserting in more than one place.
bool same_shape(const ynode& a, const ynode& b);

// Substitutes `${VAR}` and `${VAR:default}` in RAW config text, before parsing.
// Matched to redpanda-connect 4.107.2 by testing it:
//
//   ${VAR:default}   VAR if set, otherwise the default. The default is
//                    everything after the FIRST colon, so
//                    `${URL:http://h:8080/p}` keeps its own colons.
//   ${VAR}           VAR if set; otherwise the name is appended to `missing`
//                    and the text is left alone. The reference rejects such a
//                    config with `required environment variables were not set`,
//                    so this is an error rather than an empty string.
//   ${}              left as written.
//
// There is no `$$` escape: the reference treats `$${VAR}` as a reference to
// VAR, so neither does this.
std::string substitute_env(std::string_view text, std::vector<std::string>& missing);

// Reads a config file and substitutes environment variables, throwing when a
// required one is unset. Every entry point goes through this rather than
// read_file + parse_yaml, so `${...}` means the same thing in `run`, `build`,
// `lint` and `test`.
// `require_env` false substitutes what it can and leaves the rest as written
// instead of throwing. `swordfish test` needs that for its FIRST read of a
// file: it is looking for the `tests:` block, and a case may supply the very
// variables the config requires through its own `environment`. Enforcing them
// before the case has been applied dropped such a file entirely -- reported as
// "no tests", which is indistinguishable from a file that has none.
ynode load_config(const std::string& path, bool require_env = true);

// The same thing for a config that did not come from a file -- `swordfish
// streams` accepts one in the body of a POST. It goes through this rather than
// parse_yaml directly so that `${VAR}` means in an API request exactly what it
// means in a file; `where` names the source in any error.
ynode load_config_text(std::string_view text, const std::string& where,
                       bool require_env = true);

} // namespace sf::cfg
