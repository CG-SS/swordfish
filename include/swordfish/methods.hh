// Every Bloblang method, declared in the namespace the emitter targets.
//
// Split out of value.hh once the catalogue passed a hundred entries: generated
// code reaches these through runtime.hh, and value.hh should describe the data
// model rather than the standard library built on top of it.
//
// A method is added in three places and no more -- here, its definition in one
// of the src/methods_*.cc files, and one line in src/blobl/method_registry.cc.
// Both backends then pick it up with no further edits.
#pragma once

#include "swordfish/value.hh"

#include <functional>
#include <string>
#include <vector>

namespace sf {

// ---- methods (the sf::m:: namespace the emitter targets) -------------------
namespace m {

// The per-element callback of a lambda-taking method. The interpreter passes a
// closure over its AST walker; generated code passes a real C++ lambda. Keeping
// the iteration on this side of the boundary is what stops the two backends
// from disagreeing about, say, whether `all()` on an empty array is true.
using lambda = std::function<value(const value&)>;

value to_string(const value& v);
value length(const value& v);
value uppercase(const value& v);
value lowercase(const value& v);
value format(const value& fmt, const std::vector<value>& args);
value number(const value& v);
value to_bytes(const value& v);
value floor_(const value& v);
value exists(const value& v, const value& path);

// map_each / filter. The element callback is supplied by the caller: the
// interpreter passes a closure over its AST walker, generated code passes a real
// C++ lambda. The iteration and the object/array shaping live here, once.
value map_each(const value& v, const std::function<value(const value&)>& fn);
value filter(const value& v, const std::function<value(const value&)>& fn);

// ---- coercion ---------------------------------------------------------------
value to_bool(const value& v);
value type_of(const value& v);
value not_null(const value& v);
value not_empty(const value& v);

// ---- strings ----------------------------------------------------------------
value capitalize(const value& v);
value trim(const value& v, const value& cutset);
value trim_prefix(const value& v, const value& prefix);
value trim_suffix(const value& v, const value& suffix);
value has_prefix(const value& v, const value& p);
value has_suffix(const value& v, const value& p);
value split(const value& v, const value& delim, const value& empty_as_null);
value join(const value& v, const value& delim);
value replace_all(const value& v, const value& from, const value& to);
value reverse(const value& v);
value quote(const value& v);
value index_of(const value& v, const value& needle);
value repeat(const value& v, const value& count);
value slice(const value& v, const value& low, const value& high);
value contains(const value& v, const value& needle);

// ---- encoding ---------------------------------------------------------------
value encode(const value& v, const value& scheme);
value decode(const value& v, const value& scheme);

// ---- numbers ----------------------------------------------------------------
value abs_(const value& v);
value ceil_(const value& v);
value round_(const value& v);
value min_(const value& v);
value max_(const value& v);
value sum(const value& v);

// ---- structured -------------------------------------------------------------
value keys(const value& v);
value values(const value& v);
value index(const value& v, const value& i);
value get_path(const value& v, const value& path);
value append(const value& v, const std::vector<value>& items);
value merge(const value& v, const value& with);
value unique(const value& v);
value unique_by(const value& v, const lambda& fn);
value sort_(const value& v);
value sort_with(const value& v, const lambda& fn);
value flatten(const value& v);

// ---- parsing ----------------------------------------------------------------
value parse_json_m(const value& v, const value& use_number);
value format_json(const value& v, const value& indent, const value& no_indent,
                  const value& escape_html);

// ---- strings (methods_string.cc) --------------------------------------------
value escape_html(const value& v);
value unescape_html(const value& v);
value escape_url_query(const value& v);
value unescape_url_query(const value& v);
value escape_url_path(const value& v);
value unescape_url_path(const value& v);
value filepath_join(const value& v);
value filepath_split(const value& v);
value unquote(const value& v);
value replace_all_many(const value& v, const value& items);
value slug(const value& v, const value& lang);
value strip_html(const value& v, const value& preserve);

// ---- numbers (methods_number.cc) --------------------------------------------
value cos_(const value& v);
value sin_(const value& v);
value tan_(const value& v);
value log_(const value& v);
value log10_(const value& v);
value pow_(const value& v, const value& exponent);
value bitwise_and(const value& v, const value& other);
value bitwise_or(const value& v, const value& other);
value bitwise_xor(const value& v, const value& other);
value int8_(const value& v);
value int16_(const value& v);
value int32_(const value& v);
value int64_(const value& v);
value uint8_(const value& v);
value uint16_(const value& v);
value uint32_(const value& v);
value uint64_(const value& v);
value float32_(const value& v);
value float64_(const value& v);

// ---- objects and arrays (methods_structured.cc) -----------------------------
value array_of(const value& v);
value assign(const value& v, const value& with);
value squash(const value& v);
value collapse(const value& v, const value& include_empty);
value enumerated(const value& v);
value key_values(const value& v);
value explode(const value& v, const value& path);
value with(const value& v, const std::vector<value>& paths);
value without(const value& v, const std::vector<value>& paths);
value concat(const value& v, const std::vector<value>& others);
value zip(const value& v, const std::vector<value>& others);
value find(const value& v, const value& needle);
value find_all(const value& v, const value& needle);
value diff(const value& v, const value& other);
value patch(const value& v, const value& changelog);
value not_(const value& v);

// Lambda-taking. Both backends build the callback and call these.
value all(const value& v, const lambda& fn);
value any(const value& v, const lambda& fn);
value find_by(const value& v, const lambda& fn);
value find_all_by(const value& v, const lambda& fn);
value sort_by(const value& v, const lambda& fn);
value map_each_key(const value& v, const lambda& fn);
value fold(const value& v, const value& initial, const lambda& fn);

// ---- timestamps (methods_time.cc) --------------------------------------------
value to_timestamp(const value& v);
value ts_format(const value& v, const value& layout, const value& tz);
value ts_strftime(const value& v, const value& directives, const value& tz);
value ts_parse(const value& v, const value& layout);
value ts_strptime(const value& v, const value& directives);
value ts_tz(const value& v, const value& tz);
value ts_unix(const value& v);
value ts_unix_milli(const value& v);
value ts_unix_micro(const value& v);
value ts_unix_nano(const value& v);
value ts_sub(const value& v, const value& other);
value ts_round(const value& v, const value& duration);
value ts_add_iso8601(const value& v, const value& spec);
value ts_sub_iso8601(const value& v, const value& spec);
value parse_duration(const value& v);
value parse_duration_iso8601(const value& v);

// ---- other formats (methods_parse.cc) ----------------------------------------
// One RFC 4180 record, read from `s` starting at `i`. Returns false at end of
// input. Exposed because the `csv` SCANNER needs exactly this and a second copy
// of Go's quoting rules -- including its `lazy_quotes` mode -- would drift from
// this one the first time either was corrected.
//
// `at_end` says whether `s` is all the input there will ever be. The scanner
// feeds ONE READ BUFFER at a time, so a record that runs off the end of `s` is
// ordinarily just a record that has not finished arriving; with `at_end` false
// this rewinds `i` and returns false, asking the caller for more bytes. Only
// when `at_end` is true is running out inside a quoted field an error.
bool read_csv_record(std::string_view s, size_t& i, char delim, bool lazy,
                     std::vector<std::string>& out, bool at_end = true);

value parse_csv(const value& v, const value& header, const value& delimiter,
                const value& lazy_quotes);
value parse_form_url_encoded(const value& v);
value parse_logfmt(const value& v);
value parse_url(const value& v);
value parse_yaml(const value& v);
value format_yaml(const value& v);
value parse_xml(const value& v, const value& cast);
value format_xml(const value& v, const value& indent, const value& no_indent,
                 const value& root_tag);
value json_path(const value& v, const value& expression);

// ---- JSON Schema (methods_schema.cc) -------------------------------------------
value json_schema(const value& v, const value& schema);

// ---- MessagePack (methods_msgpack.cc) -----------------------------------------
value format_msgpack(const value& v);
value parse_msgpack(const value& v);

// ---- dynamic mappings (methods_dynamic.cc) ------------------------------------
value bloblang(const value& v, const value& mapping_text);

// ---- crypto (methods_crypto.cc) -----------------------------------------------
value hash(const value& v, const value& algorithm, const value& key,
           const value& polynomial);
value uuid_v5(const value& v, const value& ns);
value encrypt_aes(const value& v, const value& scheme, const value& key, const value& iv);
value decrypt_aes(const value& v, const value& scheme, const value& key, const value& iv);
value compare_argon2(const value& v, const value& hashed);
value compare_bcrypt(const value& v, const value& hashed);

// ---- compression (methods_compress.cc) ----------------------------------------
value compress(const value& v, const value& algorithm, const value& level);
value decompress(const value& v, const value& algorithm);

// ---- JSON Web Tokens (methods_jwt.cc) -----------------------------------------
// Three families and three digest widths make the eighteen documented method
// names; the registry maps each name onto one (family, bits) pair.
enum class jwt_family { hmac, rsa, ecdsa };
value sign_jwt(const value& claims, jwt_family fam, int bits, const value& secret,
               const value& headers);
value parse_jwt(const value& token, jwt_family fam, int bits, const value& secret);

// The eighteen names, each a two-line forward to the pair above. Declared
// through a macro because the alternative is thirty-six lines that differ only
// in three characters, and a typo in one of them would be invisible.
#define SF_JWT_PAIR(name)                                                      \
    value sign_jwt_##name(const value& v, const value& secret,                 \
                          const value& headers);                               \
    value parse_jwt_##name(const value& v, const value& secret);
SF_JWT_PAIR(hs256)
SF_JWT_PAIR(hs384)
SF_JWT_PAIR(hs512)
SF_JWT_PAIR(rs256)
SF_JWT_PAIR(rs384)
SF_JWT_PAIR(rs512)
SF_JWT_PAIR(es256)
SF_JWT_PAIR(es384)
SF_JWT_PAIR(es512)
#undef SF_JWT_PAIR

// XXH64 with seed 0, the same digest `hash("xxhash64")` returns. Exposed
// because the `memory` cache selects its shard with it, exactly as the
// reference does (xxhash.ChecksumString64), so which shard a key lands in --
// and therefore when the entry expires -- agrees bit for bit.
uint64_t xxhash64(std::string_view in);
}

} // namespace sf
