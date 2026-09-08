// Config templates: a new component defined by a Bloblang mapping.
//
// A template file declares a name, the component type it produces, the fields a
// user may set, and a mapping from those fields to a real component config. Used
// in a config, the template's name sits where a component kind would:
//
//     # templates/sqs_list.yaml            # config.yaml
//     name: aws_sqs_list                   input:
//     type: input                            aws_sqs_list:
//     fields:                                  urls: [ ..., ... ]
//       - name: urls
//         type: string
//         kind: list
//     mapping: |
//       root.broker.inputs = this.urls.map_each(url -> {"aws_sqs":{"url":url}})
//
// Templates are loaded with `-t` and are EXPERIMENTAL upstream, which is worth
// knowing before matching a detail exactly: the reference says so itself.
//
// The expansion hook is at the point a component kind is looked up, not a
// pre-pass over the document. Every nested input goes through parse_input, so
// hooking there covers a template inside a `broker`, inside a `switch` case,
// inside a resource -- positions a document walk would have to know about one
// at a time, and would eventually miss one.
#pragma once

#include "swordfish/config/yaml.hh"

#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace sf::tmpl {

// The component types a template may produce. `buffer` and `scanner` are NOT in
// this list because the reference does not offer them either.
enum class comp_type { input, output, processor, cache, rate_limit };

std::string_view to_string(comp_type t);
std::optional<comp_type> type_from_string(std::string_view s);

// A field's scalar type, from the template schema. `unknown` means "whatever the
// user writes here is passed through", which is how a template wraps a config
// block it does not want to describe.
enum class field_type { string, string_enum, string_annotated_enum, int_, float_, bool_, bloblang, unknown };

// scalar, or a list/map OF the scalar type.
enum class field_kind { scalar, list, map };

struct field {
    std::string  name;
    std::string  description;
    field_type   type = field_type::string;
    field_kind   kind = field_kind::scalar;
    bool         advanced = false;
    // Absent means the field is REQUIRED: "if a default value is not specified
    // then a configuration without the field is considered incorrect".
    std::optional<cfg::ynode> default_value;
    std::vector<std::string>  options;      // string_enum / string_annotated_enum
    cfg::position pos;
};

// One `tests:` entry: a config to expand and, optionally, what it must expand
// to. Run by `swordfish template lint`.
struct test_case {
    std::string   name;
    std::string   label;
    cfg::ynode    config;
    std::optional<cfg::ynode> expected;
    cfg::position pos;
};

struct definition {
    std::string   name;
    comp_type     type = comp_type::input;
    std::string   status = "stable";
    std::string   summary;
    std::string   description;
    std::vector<std::string> categories;
    std::vector<field>       fields;
    std::string   mapping;          // Bloblang
    std::vector<test_case> tests;
    std::string   source;           // the file it came from, for errors
    cfg::position pos;
};

// Parses one template document. Throws sf::spec_error, so a bad template is
// reported with a line number like any other config mistake.
definition parse_template(const cfg::ynode& doc, const std::string& source);

// Loads every template in a file, a directory, or a glob, and registers it.
// Registration is process-wide because parse_pipeline() is a free function with
// no context to thread one through -- the same reason the component registries
// are global.
void load_templates(const std::string& path_or_glob);

// Everything loaded, for `template lint` and for an error that wants to say what
// IS available.
const std::vector<definition>& loaded();
void clear();                       // tests, and `template lint` between files

// The template registered for this type and name, or null.
const definition* find(comp_type type, std::string_view name);

// Applies a template to the config written at `usage` -- the body under the
// template's name -- and returns the component config it produces.
//
// `usage` is the whole component node, so `label` and any sibling keys are
// preserved onto the result: a labelled template usage keeps its label.
cfg::ynode expand(const definition& def, const cfg::ynode& usage,
                  const std::string& label);

// Bounds how deep template expansion may go, and MUST be held across the
// recursive parse that follows an expansion -- not merely inside expand().
//
// That distinction is the whole reason this is a separate object. The counter
// started inside expand(), where it was useless: expand() returns the produced
// config and the PARSER then recurses into it, so the guard had already been
// destroyed by the time the next expansion began and the depth never rose above
// one. A template whose mapping produces a usage of itself expanded for ever and
// took the stack with it -- a config file, not a hostile one, dumping core.
//
// Constructing it past the limit throws spec_error naming the position, so the
// answer is a diagnostic rather than a crash.
class expansion_guard {
public:
    explicit expansion_guard(cfg::position where);
    ~expansion_guard();
    expansion_guard(const expansion_guard&) = delete;
    expansion_guard& operator=(const expansion_guard&) = delete;
};

} // namespace sf::tmpl
