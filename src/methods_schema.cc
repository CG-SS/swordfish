// json_schema(): validate a value against a JSON Schema and return it unchanged
// when it conforms.
//
// Draft-07 vocabulary, minus `$ref`. A schema keyword this validator does not
// know is IGNORED, which is not laxity but the specification: JSON Schema
// defines unknown keywords as annotations. The keywords it does know but cannot
// evaluate -- `$ref` and its relatives, which need a document resolver -- are a
// named error rather than a silent pass, because a schema built around a $ref
// would otherwise validate nothing at all.
#include "swordfish/methods.hh"
#include "swordfish/regex.hh"
#include "swordfish/value.hh"

#include "methods_util.hh"

#include <cmath>
#include <string>
#include <vector>

namespace sf::m {

namespace {

struct fail_context {
    // Where in the instance the failure happened, as a dotted path, so the
    // message points at the offending field rather than at the document.
    std::string path;
    [[noreturn]] void bad(const std::string& why) const {
        throw eval_error((path.empty() ? std::string("value") : path) + " " + why);
    }
};

const char* schema_type_of(const value& v) {
    switch (v.type()) {
    case vtype::null: case vtype::deleted: case vtype::nothing: return "null";
    case vtype::boolean:   return "boolean";
    case vtype::i64:       return "integer";
    case vtype::f32: case vtype::f64: case vtype::raw_number: return "number";
    case vtype::timestamp: return "string";
    case vtype::string: case vtype::bytes: return "string";
    case vtype::array:     return "array";
    case vtype::object:    return "object";
    }
    return "null";
}

bool type_matches(std::string_view want, const value& v) {
    const std::string_view got = schema_type_of(v);
    if (want == got) return true;
    // An integer satisfies "number"; a whole-valued float satisfies "integer",
    // which is draft-06 and later behaviour.
    if (want == "number"  && got == "integer") return true;
    if (want == "integer" && v.is_float()) {
        const double d = v.as_f64();
        return std::isfinite(d) && d == std::trunc(d);
    }
    return false;
}

size_t utf8_length(std::string_view s) {
    size_t n = 0;
    for (unsigned char c : s) if ((c & 0xC0) != 0x80) ++n;
    return n;
}

void validate(const value& v, const value& schema, const fail_context& ctx);

void validate_each(const value& v, const value& list, const fail_context& ctx,
                   const char* what) {
    if (list.type() != vtype::array)
        throw eval_error(std::string("json_schema: ") + what + " must be an array");
    for (const auto& sub : list.arr()) validate(v, sub, ctx);
}

void validate(const value& v, const value& schema, const fail_context& ctx) {
    // A boolean schema: true accepts everything, false rejects everything.
    if (schema.type() == vtype::boolean) {
        if (!schema.as_bool()) ctx.bad("is rejected by a false schema");
        return;
    }
    if (schema.type() != vtype::object)
        throw eval_error("json_schema: a schema must be an object or a boolean");

    for (const auto& [key, arg] : schema.obj()) {
        if (key == "$ref" || key == "$dynamicRef" || key == "$recursiveRef")
            throw eval_error("json_schema: " + key + " is not supported");

        if (key == "type") {
            bool ok = false;
            if (arg.is_stringy()) ok = type_matches(arg.as_string(), v);
            else if (arg.type() == vtype::array) {
                for (const auto& t : arg.arr())
                    if (t.is_stringy() && type_matches(t.as_string(), v)) { ok = true; break; }
            } else {
                throw eval_error("json_schema: type must be a string or an array");
            }
            if (!ok) ctx.bad(std::string("has invalid type; expected ") +
                             (arg.is_stringy() ? arg.as_string() : arg.to_json()) +
                             ", given " + schema_type_of(v));
        } else if (key == "const") {
            if (!(v == arg)) ctx.bad("does not equal the required constant");
        } else if (key == "enum") {
            if (arg.type() != vtype::array)
                throw eval_error("json_schema: enum must be an array");
            bool ok = false;
            for (const auto& e : arg.arr()) if (v == e) { ok = true; break; }
            if (!ok) ctx.bad("is not one of the permitted values");
        } else if (key == "allOf") {
            validate_each(v, arg, ctx, "allOf");
        } else if (key == "anyOf") {
            if (arg.type() != vtype::array)
                throw eval_error("json_schema: anyOf must be an array");
            bool ok = false;
            for (const auto& sub : arg.arr()) {
                try { validate(v, sub, ctx); ok = true; break; }
                catch (const eval_error&) { /* try the next branch */ }
            }
            if (!ok) ctx.bad("matches none of the anyOf schemas");
        } else if (key == "oneOf") {
            if (arg.type() != vtype::array)
                throw eval_error("json_schema: oneOf must be an array");
            int matched = 0;
            for (const auto& sub : arg.arr()) {
                try { validate(v, sub, ctx); ++matched; }
                catch (const eval_error&) { /* not this one */ }
            }
            if (matched != 1)
                ctx.bad("matches " + std::to_string(matched) +
                        " of the oneOf schemas, expected exactly 1");
        } else if (key == "not") {
            bool matched = true;
            try { validate(v, arg, ctx); }
            catch (const eval_error&) { matched = false; }
            if (matched) ctx.bad("matches a schema it must not");
        } else if (key == "minimum") {
            if (v.is_number() && v.as_f64() < arg.as_f64()) ctx.bad("is below the minimum");
        } else if (key == "maximum") {
            if (v.is_number() && v.as_f64() > arg.as_f64()) ctx.bad("is above the maximum");
        } else if (key == "exclusiveMinimum") {
            if (v.is_number() && v.as_f64() <= arg.as_f64())
                ctx.bad("is not above the exclusive minimum");
        } else if (key == "exclusiveMaximum") {
            if (v.is_number() && v.as_f64() >= arg.as_f64())
                ctx.bad("is not below the exclusive maximum");
        } else if (key == "multipleOf") {
            if (v.is_number()) {
                const double d = arg.as_f64();
                if (d <= 0) throw eval_error("json_schema: multipleOf must be positive");
                const double q = v.as_f64() / d;
                if (q != std::trunc(q)) ctx.bad("is not a multiple of " + arg.to_json());
            }
        } else if (key == "minLength") {
            if (v.is_stringy() && utf8_length(v.as_string()) <
                static_cast<size_t>(arg.as_i64()))
                ctx.bad("is shorter than the minimum length");
        } else if (key == "maxLength") {
            if (v.is_stringy() && utf8_length(v.as_string()) >
                static_cast<size_t>(arg.as_i64()))
                ctx.bad("is longer than the maximum length");
        } else if (key == "pattern") {
            if (v.is_stringy() && !cached_re(want_string(arg)).match(v.as_string()))
                ctx.bad("does not match the required pattern");
        } else if (key == "minItems") {
            if (v.type() == vtype::array &&
                v.arr().size() < static_cast<size_t>(arg.as_i64()))
                ctx.bad("has fewer items than the minimum");
        } else if (key == "maxItems") {
            if (v.type() == vtype::array &&
                v.arr().size() > static_cast<size_t>(arg.as_i64()))
                ctx.bad("has more items than the maximum");
        } else if (key == "uniqueItems") {
            if (v.type() == vtype::array && arg.type() == vtype::boolean && arg.as_bool()) {
                const auto& a = v.arr();
                for (size_t i = 0; i < a.size(); ++i)
                    for (size_t j = i + 1; j < a.size(); ++j)
                        if (a[i] == a[j]) ctx.bad("has duplicate items");
            }
        } else if (key == "items") {
            if (v.type() != vtype::array) continue;
            if (arg.type() == vtype::array) {
                // Tuple form: the nth schema validates the nth item.
                for (size_t i = 0; i < v.arr().size() && i < arg.arr().size(); ++i)
                    validate(v.arr()[i], arg.arr()[i], {ctx.path + "." + std::to_string(i)});
            } else {
                for (size_t i = 0; i < v.arr().size(); ++i)
                    validate(v.arr()[i], arg, {ctx.path + "." + std::to_string(i)});
            }
        } else if (key == "additionalItems") {
            const value* tuple = schema.find("items");
            if (v.type() != vtype::array || !tuple || tuple->type() != vtype::array) continue;
            for (size_t i = tuple->arr().size(); i < v.arr().size(); ++i)
                validate(v.arr()[i], arg, {ctx.path + "." + std::to_string(i)});
        } else if (key == "contains") {
            if (v.type() != vtype::array) continue;
            bool ok = false;
            for (const auto& el : v.arr()) {
                try { validate(el, arg, ctx); ok = true; break; }
                catch (const eval_error&) { /* keep looking */ }
            }
            if (!ok) ctx.bad("contains no item matching the schema");
        } else if (key == "required") {
            if (v.type() != vtype::object) continue;
            if (arg.type() != vtype::array)
                throw eval_error("json_schema: required must be an array");
            for (const auto& r : arg.arr())
                if (!v.find(want_string(r)))
                    ctx.bad("is missing the required field " + want_string(r));
        } else if (key == "minProperties") {
            if (v.type() == vtype::object &&
                v.obj().size() < static_cast<size_t>(arg.as_i64()))
                ctx.bad("has fewer properties than the minimum");
        } else if (key == "maxProperties") {
            if (v.type() == vtype::object &&
                v.obj().size() > static_cast<size_t>(arg.as_i64()))
                ctx.bad("has more properties than the maximum");
        } else if (key == "properties") {
            if (v.type() != vtype::object) continue;
            if (arg.type() != vtype::object)
                throw eval_error("json_schema: properties must be an object");
            for (const auto& [name, sub] : arg.obj())
                if (const value* got = v.find(name))
                    validate(*got, sub, {ctx.path + "." + name});
        } else if (key == "patternProperties") {
            if (v.type() != vtype::object) continue;
            for (const auto& [pat, sub] : arg.obj()) {
                const re& r = cached_re(pat);
                for (const auto& [name, got] : v.obj())
                    if (r.match(name)) validate(got, sub, {ctx.path + "." + name});
            }
        } else if (key == "propertyNames") {
            if (v.type() != vtype::object) continue;
            for (const auto& [name, got] : v.obj()) {
                (void)got;
                validate(value(name), arg, {ctx.path + "." + name});
            }
        } else if (key == "additionalProperties") {
            if (v.type() != vtype::object) continue;
            const value* props = schema.find("properties");
            const value* pats  = schema.find("patternProperties");
            for (const auto& [name, got] : v.obj()) {
                if (props && props->type() == vtype::object && props->find(name)) continue;
                bool by_pattern = false;
                if (pats && pats->type() == vtype::object)
                    for (const auto& [pat, sub] : pats->obj()) {
                        (void)sub;
                        if (cached_re(pat).match(name)) { by_pattern = true; break; }
                    }
                if (by_pattern) continue;
                validate(got, arg, {ctx.path + "." + name});
            }
        } else if (key == "dependencies" || key == "dependentRequired" ||
                   key == "dependentSchemas") {
            if (v.type() != vtype::object || arg.type() != vtype::object) continue;
            for (const auto& [name, dep] : arg.obj()) {
                if (!v.find(name)) continue;
                if (dep.type() == vtype::array) {
                    for (const auto& r : dep.arr())
                        if (!v.find(want_string(r)))
                            ctx.bad("is missing " + want_string(r) + ", required by " + name);
                } else {
                    validate(v, dep, ctx);
                }
            }
        } else if (key == "if") {
            bool cond = true;
            try { validate(v, arg, ctx); }
            catch (const eval_error&) { cond = false; }
            const value* branch = schema.find(cond ? "then" : "else");
            if (branch) validate(v, *branch, ctx);
        }
        // `then`/`else` are handled with `if`; `format` is an annotation in
        // draft-07 and asserts nothing; everything else -- $schema, $id, title,
        // description, default, examples, definitions, $defs -- is metadata.
        // Unknown keywords are annotations by specification and are ignored.
    }
}

} // namespace

value json_schema(const value& v, const value& schema_text) {
    const value schema = parse_json(want_string(schema_text));
    validate(v, schema, {});
    return v;
}

} // namespace sf::m
