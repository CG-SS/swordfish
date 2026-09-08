// The built-in components' fields, declared once.
//
// `stdin`, `file`, `generate`, `stdout`, `drop`, `broker`, `sequence` and
// `switch` predate the component registry and are still parsed by hand in
// parse_pipeline.cc. Because they are not registered, nothing described them:
// `swordfish list` -- the command whose whole job is answering "what does this
// build implement?" -- reported `http_client` and `kafka` and omitted `stdout`.
// The reference lists all of them.
//
// So the fields live here, and BOTH consumers read this one table:
//
//   * `only_fields()` in the parser takes the allowed names from it, so a field
//     accepted by the parser is a field the documentation shows, and vice versa.
//   * `swordfish list` renders it beside the registered components.
//
// That pairing is the point. registry.hh's spec_of exists so a field cannot be
// added to the parser and forgotten in the docs, and config_main.cc records what
// happened the last time documentation was kept in a separate hardcoded struct:
// "`list` and `lint` disagreed about what a `kafka` input accepts". A second
// hand-written list would have reintroduced exactly that, so there is one.
//
// What this table does NOT do is parse. The built-ins' hand-written parsers
// carry behaviour a descriptor cannot express -- `generate`'s interval default,
// `file`'s glob expansion, `broker`'s child recursion -- and rewriting them
// through spec_of would be a change to the runtime disguised as a change to the
// docs. The names are shared; the parsing is not.
#pragma once

#include <string_view>
#include <vector>

namespace sf {

// Which section of a config the component belongs to. Not `comp_kind` from
// runtime/component_stats.hh: that one names a metrics stage and has no
// `processor` distinction to make here.
enum class builtin_class { input, output };

struct builtin_field {
    std::string_view name;
    std::string_view type;          // "string", "duration", "int", "bool", ...
    std::string_view default_text;  // rendered default; empty when required
    std::string_view description;
    bool             required = false;
    // Refused BY NAME when present, rather than accepted and ignored. Shown by
    // `list` rather than hidden, because someone porting a config needs to know
    // the difference between a field swordfish has not built and one that does
    // not exist.
    bool             unimplemented = false;
    // Whether the parser's only_fields() list includes it. An unimplemented
    // field is usually absent from that list, because unimplemented_fields()
    // has already refused it -- but `sequence.sharded_join` is in both: it is
    // accepted as a key and then refused with its own message. Getting this
    // wrong in either direction changes what the parser accepts, which is why
    // it is a separate flag and not inferred from `unimplemented`.
    bool             in_allowed = true;
    // Accepted, but superseded -- `codec` on an input is the old spelling of
    // `scanner`. `list` still shows it, because a config in the wild uses it and
    // a reader needs to know it is understood; `create` leaves it OUT, because
    // the two are mutually exclusive and a scaffold emitting both produced a
    // config the parser refuses ("sets both `scanner` and the deprecated
    // `codec`"). The registered components' scaffold skips their `deprecated`
    // fields for the same reason.
    bool             deprecated = false;
};

struct builtin_component {
    // Defaulted, like every other member here: the table below is written with
    // designated aggregate initialisation and always sets it, but a struct with
    // one uninitialised member is a trap for whoever adds the next entry.
    builtin_class                    cls = builtin_class::input;
    std::string_view                 kind;
    std::string_view                 summary;
    std::vector<builtin_field>       fields;
};

// Every built-in, in a stable order.
const std::vector<builtin_component>& builtin_components();

// One of them, or null.
const builtin_component* find_builtin(builtin_class cls, std::string_view kind);

// The field names, for the parser's only_fields() check. Returns an empty list
// for a kind that is not in the table, which is what a caller that has not been
// converted yet gets -- and an empty allowed-list would reject every field, so
// callers must check before using it.
std::vector<std::string_view> builtin_field_names(builtin_class cls, std::string_view kind);

// The same text shape `describe<T>()` produces for a registered component, so
// `swordfish list` reads as one document rather than two.
std::string describe_builtin(const builtin_component& c);

// A YAML skeleton of the component's default config, for `swordfish create`.
// The same shape cfg::scaffold<T>() produces for a registered component.
std::string scaffold_builtin(const builtin_component& c, int indent);

} // namespace sf
