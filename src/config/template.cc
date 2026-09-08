// Config templates. See include/swordfish/config/template.hh for what they are.

#include "swordfish/config/template.hh"

#include "swordfish/blobl/interp.hh"
#include "swordfish/blobl/parse.hh"
#include "swordfish/config/spec_error.hh"

#include <algorithm>
#include <filesystem>
#include <glob.h>
#include <map>
#include <stdexcept>

namespace sf::tmpl {
namespace {

[[noreturn]] void bad(const cfg::ynode& at, const std::string& msg) {
    throw sf::spec_error(at.pos, msg);
}

std::string join(const std::vector<std::string_view>& v) {
    std::string out;
    for (const auto& s : v) out += (out.empty() ? "" : ", ") + std::string(s);
    return out;
}

// Every key is accounted for. A template file with a misspelt `mappping` would
// otherwise register a component whose mapping is empty -- which produces an
// empty config, which fails somewhere else entirely.
void only_keys(const cfg::ynode& n, const char* what,
               std::vector<std::string_view> known) {
    for (const auto& [k, v] : n.map) {
        if (std::find(known.begin(), known.end(), k) != known.end()) continue;
        bad(v, std::string(what) + " has no field '" + k + "'; expected " + join(known));
    }
}

const cfg::ynode* need(const cfg::ynode& n, const char* key, const char* what) {
    const cfg::ynode* v = n.find(key);
    if (!v) bad(n, std::string(what) + " needs a `" + key + "`");
    return v;
}

std::string scalar_or(const cfg::ynode* n, const std::string& dflt) {
    return n && n->is_scalar() ? n->scalar : dflt;
}

field_type parse_field_type(const cfg::ynode& n) {
    static const std::map<std::string, field_type, std::less<>> m{
        {"string", field_type::string},
        {"string_enum", field_type::string_enum},
        {"string_annotated_enum", field_type::string_annotated_enum},
        {"int", field_type::int_},
        {"float", field_type::float_},
        {"bool", field_type::bool_},
        {"bloblang", field_type::bloblang},
        {"unknown", field_type::unknown},
    };
    const auto it = m.find(n.scalar);
    if (it == m.end())
        bad(n, "a template field's `type` must be one of string, string_enum, "
               "string_annotated_enum, int, float, bool, bloblang, unknown; got '" +
               n.scalar + "'");
    return it->second;
}

field_kind parse_field_kind(const cfg::ynode& n) {
    if (n.scalar == "scalar") return field_kind::scalar;
    if (n.scalar == "list")   return field_kind::list;
    if (n.scalar == "map")    return field_kind::map;
    bad(n, "a template field's `kind` must be scalar, list or map; got '" + n.scalar + "'");
}

field parse_field(const cfg::ynode& n) {
    only_keys(n, "a template field",
              {"name", "description", "type", "kind", "default", "advanced", "options"});
    field f;
    f.pos = n.pos;
    f.name = need(n, "name", "a template field")->scalar;
    if (f.name.empty()) bad(n, "a template field's `name` cannot be empty");
    f.description = scalar_or(n.find("description"), "");
    f.type = parse_field_type(*need(n, "type", "a template field"));
    if (const cfg::ynode* k = n.find("kind")) f.kind = parse_field_kind(*k);
    if (const cfg::ynode* d = n.find("default")) f.default_value = *d;
    if (const cfg::ynode* a = n.find("advanced"))
        f.advanced = a->scalar == "true";

    if (const cfg::ynode* o = n.find("options")) {
        // A list for `string_enum`; a map of value -> description for
        // `string_annotated_enum`. Both reduce to the set of allowed values.
        if (o->is_sequence())      for (const auto& e : o->seq) f.options.push_back(e.scalar);
        else if (o->is_mapping())  for (const auto& e : o->map) f.options.push_back(e.key);
        else bad(*o, "a template field's `options` must be a list or a map");
    }
    if ((f.type == field_type::string_enum || f.type == field_type::string_annotated_enum) &&
        f.options.empty())
        bad(n, "a `" + std::string(f.type == field_type::string_enum ? "string_enum"
                                                                    : "string_annotated_enum") +
               "` field needs `options`");
    return f;
}

test_case parse_test(const cfg::ynode& n) {
    only_keys(n, "a template test", {"name", "label", "config", "expected"});
    test_case t;
    t.pos = n.pos;
    t.name = scalar_or(n.find("name"), "");
    t.label = scalar_or(n.find("label"), "");
    t.config = *need(n, "config", "a template test");
    if (const cfg::ynode* e = n.find("expected")) t.expected = *e;
    return t;
}

// The registry. Keyed by type AND name, because the reference allows an input
// and an output to share a name -- they are different components.
std::map<std::pair<int, std::string>, definition>& registry() {
    static std::map<std::pair<int, std::string>, definition> r;
    return r;
}

std::vector<definition>& all() {
    static std::vector<definition> v;
    return v;
}

// A template whose mapping produces a usage of itself would expand for ever.
// The limit is generous -- a template built out of three others is ordinary --
// and the error names the position so the cycle can be found.
constexpr int max_expansion_depth = 16;
thread_local int expansion_depth = 0;

} // namespace

std::string_view to_string(comp_type t) {
    switch (t) {
    case comp_type::input:      return "input";
    case comp_type::output:     return "output";
    case comp_type::processor:  return "processor";
    case comp_type::cache:      return "cache";
    case comp_type::rate_limit: return "rate_limit";
    }
    return "input";
}

std::optional<comp_type> type_from_string(std::string_view s) {
    if (s == "input")      return comp_type::input;
    if (s == "output")     return comp_type::output;
    if (s == "processor")  return comp_type::processor;
    if (s == "cache")      return comp_type::cache;
    if (s == "rate_limit") return comp_type::rate_limit;
    return std::nullopt;
}

definition parse_template(const cfg::ynode& doc, const std::string& source) {
    if (!doc.is_mapping()) bad(doc, "a template must be a mapping");
    only_keys(doc, "a template",
              {"name", "type", "status", "categories", "summary", "description",
               "fields", "mapping", "metrics_mapping", "tests"});

    definition def;
    def.source = source;
    def.pos = doc.pos;
    def.name = need(doc, "name", "a template")->scalar;
    if (def.name.empty()) bad(doc, "a template's `name` cannot be empty");

    const cfg::ynode* type = need(doc, "type", "a template");
    const auto t = type_from_string(type->scalar);
    if (!t)
        bad(*type, "a template's `type` must be one of input, output, processor, "
                   "cache, rate_limit; got '" + type->scalar + "'");
    def.type = *t;

    def.status = scalar_or(doc.find("status"), "stable");
    for (const char* s : {"stable", "beta", "experimental", "deprecated"})
        if (def.status == s) goto status_ok;
    bad(*doc.find("status"), "a template's `status` must be stable, beta, "
                             "experimental or deprecated; got '" + def.status + "'");
status_ok:
    def.summary = scalar_or(doc.find("summary"), "");
    def.description = scalar_or(doc.find("description"), "");
    if (const cfg::ynode* c = doc.find("categories"))
        for (const auto& e : c->seq) def.categories.push_back(e.scalar);

    if (const cfg::ynode* f = doc.find("fields")) {
        if (!f->is_sequence()) bad(*f, "a template's `fields` must be a list");
        for (const auto& e : f->seq) def.fields.push_back(parse_field(e));
    }

    // `metrics_mapping` renames metric paths, and swordfish's metrics are not
    // per-component -- there are no paths to rename. Named rather than accepted
    // and ignored: a template relying on it would silently export the metrics it
    // was written to suppress.
    if (doc.find("metrics_mapping") != nullptr)
        bad(*doc.find("metrics_mapping"),
            "a template's `metrics_mapping` is not implemented by swordfish; it "
            "renames per-component metric paths, and swordfish reports one set of "
            "counters for the whole stream");

    def.mapping = need(doc, "mapping", "a template")->scalar;
    if (def.mapping.empty()) bad(doc, "a template's `mapping` cannot be empty");
    // Parsed HERE so a broken mapping is reported when the template is loaded,
    // with the template's own file named, rather than on the first config that
    // happens to use it.
    try {
        (void)sf::blobl::parse_mapping(def.mapping);
    } catch (const std::exception& e) {
        bad(*doc.find("mapping"),
            "the template's `mapping` does not parse: " + std::string(e.what()));
    }

    if (const cfg::ynode* ts = doc.find("tests")) {
        if (!ts->is_sequence()) bad(*ts, "a template's `tests` must be a list");
        for (const auto& e : ts->seq) def.tests.push_back(parse_test(e));
    }
    return def;
}

void load_templates(const std::string& path_or_glob) {
    namespace fs = std::filesystem;
    std::vector<std::string> files;
    std::error_code ec;

    if (fs::is_directory(path_or_glob, ec)) {
        for (fs::recursive_directory_iterator it(path_or_glob, ec), end; it != end;
             it.increment(ec)) {
            if (ec) break;
            if (!it->is_regular_file()) continue;
            const auto ext = it->path().extension().string();
            if (ext == ".yaml" || ext == ".yml") files.push_back(it->path().string());
        }
    } else if (path_or_glob.find_first_of("*?[") != std::string::npos) {
        // A glob, which the reference documents ("supports glob patterns
        // (requires quotes)") because a shell that expanded it would pass many
        // arguments where the flag takes one.
        glob_t g{};
        if (::glob(path_or_glob.c_str(), 0, nullptr, &g) == 0)
            for (size_t i = 0; i < g.gl_pathc; ++i) files.emplace_back(g.gl_pathv[i]);
        ::globfree(&g);
        if (files.empty())
            throw std::runtime_error("no template files matched '" + path_or_glob + "'");
    } else {
        if (!fs::exists(path_or_glob, ec))
            throw std::runtime_error("no such template file: " + path_or_glob);
        files.push_back(path_or_glob);
    }
    // Sorted, so a duplicate name is always reported against the same file.
    std::sort(files.begin(), files.end());

    for (const auto& f : files) {
        definition def = parse_template(cfg::load_config(f), f);
        const auto key = std::make_pair(static_cast<int>(def.type), def.name);
        if (const auto it = registry().find(key); it != registry().end())
            throw std::runtime_error(
                "template '" + def.name + "' (" + std::string(to_string(def.type)) +
                ") is defined twice: " + it->second.source + " and " + f);
        all().push_back(def);
        registry().emplace(key, std::move(def));
    }
}

const std::vector<definition>& loaded() { return all(); }

void clear() {
    registry().clear();
    all().clear();
}

const definition* find(comp_type type, std::string_view name) {
    const auto it = registry().find(std::make_pair(static_cast<int>(type), std::string(name)));
    return it == registry().end() ? nullptr : &it->second;
}

expansion_guard::expansion_guard(cfg::position where) {
    if (++expansion_depth > max_expansion_depth) {
        --expansion_depth;
        throw sf::spec_error(
            where, "a template is still expanding after " +
                   std::to_string(max_expansion_depth) +
                   " levels; a template whose mapping produces a usage of itself, "
                   "directly or through others, never finishes");
    }
}

expansion_guard::~expansion_guard() { --expansion_depth; }

cfg::ynode expand(const definition& def, const cfg::ynode& usage, const std::string& label) {
    // The body written under the template's name, which may be absent when
    // every field has a default: `my_template: {}` and a bare `my_template:`
    // are both legal.
    const cfg::ynode* body = usage.find(def.name);
    static const cfg::ynode empty;
    const cfg::ynode& in = (body && !body->is_null()) ? *body : empty;
    if (!in.is_mapping() && !in.is_null())
        bad(in, "the config for template '" + def.name + "' must be a mapping of its fields");

    // Every declared field, with the user's value or the template's default. A
    // field with neither is the error the schema calls for: "if a default value
    // is not specified then a configuration without the field is considered
    // incorrect".
    cfg::ynode args;
    args.type = cfg::ynode_type::mapping;
    args.pos = usage.pos;
    for (const auto& f : def.fields) {
        const cfg::ynode* given = in.find(f.name);
        if (given == nullptr) {
            if (!f.default_value)
                bad(in.is_mapping() ? in : usage,
                    "template '" + def.name + "' needs a `" + f.name +
                    "`, which has no default");
            args.map.push_back({f.name, *f.default_value});
            continue;
        }
        // The declared kind is checked, because getting it wrong is a mistake
        // the mapping would otherwise turn into something stranger: a `list`
        // field given a scalar makes `this.urls.map_each(...)` fail with a
        // Bloblang type error naming neither the field nor the template.
        if (f.kind == field_kind::list && !given->is_sequence())
            bad(*given, "template '" + def.name + "' field `" + f.name +
                        "` is a list");
        if (f.kind == field_kind::map && !given->is_mapping())
            bad(*given, "template '" + def.name + "' field `" + f.name +
                        "` is a map");
        if (f.kind == field_kind::scalar && (given->is_sequence() || given->is_mapping()) &&
            f.type != field_type::unknown)
            bad(*given, "template '" + def.name + "' field `" + f.name +
                        "` is a single value, not a list or a mapping");
        if (!f.options.empty()) {
            // Checked across a list or map too, so an enum with kind: list
            // validates every element rather than only the first.
            std::vector<const cfg::ynode*> values;
            if (given->is_sequence())     for (const auto& e : given->seq) values.push_back(&e);
            else if (given->is_mapping()) for (const auto& e : given->map) values.push_back(&e.value);
            else                          values.push_back(given);
            for (const cfg::ynode* v : values)
                if (std::find(f.options.begin(), f.options.end(), v->scalar) == f.options.end())
                    bad(*v, "template '" + def.name + "' field `" + f.name +
                            "` must be one of " +
                            [&] {
                                std::string s;
                                for (const auto& o : f.options) s += (s.empty() ? "" : ", ") + o;
                                return s;
                            }() + "; got '" + v->scalar + "'");
        }
        args.map.push_back({f.name, *given});
    }
    // A key the template does not declare is a typo, and accepting it silently
    // would leave the user's setting doing nothing at all.
    if (in.is_mapping())
        for (const auto& [k, v] : in.map) {
            const bool declared = std::any_of(def.fields.begin(), def.fields.end(),
                                              [&](const field& f) { return f.name == k; });
            if (!declared)
                bad(v, "template '" + def.name + "' has no field '" + k + "'");
        }

    const value input = cfg::node_to_value(args);
    value out;
    try {
        const auto m = sf::blobl::parse_mapping(def.mapping);
        const sf::blobl::interp in_terp(m);
        exec_ctx ctx;
        out = in_terp.run(input, ctx);
    } catch (const sf::spec_error&) {
        throw;
    } catch (const std::exception& e) {
        bad(usage, "template '" + def.name + "' failed to expand: " + e.what());
    }
    if (out.type() != vtype::object)
        bad(usage, "template '" + def.name + "' produced " +
                   std::string(out.type_name()) +
                   ", but a component config has to be a mapping");

    // Positions come from the USAGE, so an error inside the produced config
    // points at the line the user wrote rather than at nothing.
    cfg::ynode result = cfg::value_to_node(out, usage.pos);
    // A label on the usage survives onto the result, which is what makes
    // `label: foo` on a template usage behave as it does on any component.
    if (!label.empty()) result.map.insert(result.map.begin(), {"label", [&] {
        cfg::ynode l;
        l.type = cfg::ynode_type::scalar;
        l.scalar = label;
        l.quoted = true;
        l.pos = usage.pos;
        return l;
    }()});
    return result;
}

} // namespace sf::tmpl
