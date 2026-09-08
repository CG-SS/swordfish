// Every Bloblang method and function, declared once.
#include "swordfish/blobl/method_registry.hh"

#include <algorithm>

namespace sf::blobl {

namespace {

// Marks a method the backends implement themselves: lambdas, hoisted regexes,
// and anything needing arguments evaluated lazily.
constexpr bool SPECIAL = true;

const std::vector<method_def>& methods() {
    static const std::vector<method_def> t = {
        // ---- coercion -------------------------------------------------------
        {{"string",    {}},                    SF_M(to_string)},
        {{"bytes",     {}},                    SF_M(to_bytes)},
        // Special because the fallback covers a failing TARGET, which means
        // the target cannot be evaluated before the method runs.
        {{"number",    {{"default", false}}},  nullptr, {}, 0, SPECIAL},
        {{"bool",      {{"default", false}}},  nullptr, {}, 0, SPECIAL},
        {{"type",      {}},                    SF_M(type_of)},
        {{"not_null",  {}},                    SF_M(not_null)},
        {{"not_empty", {}},                    SF_M(not_empty)},

        // ---- strings --------------------------------------------------------
        {{"uppercase",   {}},                       SF_M(uppercase)},
        {{"lowercase",   {}},                       SF_M(lowercase)},
        {{"capitalize",  {}},                       SF_M(capitalize)},
        {{"trim",        {{"cutset", false}}},      SF_M(trim)},
        {{"trim_prefix", {{"prefix", true}}},       SF_M(trim_prefix)},
        {{"trim_suffix", {{"suffix", true}}},       SF_M(trim_suffix)},
        {{"has_prefix",  {{"value", true}}},        SF_M(has_prefix)},
        {{"has_suffix",  {{"value", true}}},        SF_M(has_suffix)},
        {{"split",       {{"delimiter", true}, {"empty_as_null", false}}}, SF_M(split)},
        {{"join",        {{"delimiter", false}}},   SF_M(join)},
        {{"replace_all", {{"old", true}, {"new", true}}}, SF_M(replace_all)},
        {{"reverse",     {}},                       SF_M(reverse)},
        {{"quote",       {}},                       SF_M(quote)},
        {{"index_of",    {{"value", true}}},        SF_M(index_of)},
        {{"repeat",      {{"count", true}}},        SF_M(repeat)},
        {{"slice",       {{"low", true}, {"high", false}}}, SF_M(slice)},
        {{"contains",    {{"value", true}}},        SF_M(contains)},
        {{"length",      {}},                       SF_M(length)},
        {{"format",      {}, /*variadic=*/true},    SF_M(format)},
        // `replace` and `replace_all` are the same operation under two names;
        // the reference keeps both, so both are registered against one function.
        {{"replace",     {{"old", true}, {"new", true}}},  SF_M(replace_all)},
        {{"replace_many",     {{"values", true}}}, SF_M(replace_all_many)},
        {{"replace_all_many", {{"values", true}}}, SF_M(replace_all_many)},
        {{"escape_html",        {}}, SF_M(escape_html)},
        {{"unescape_html",      {}}, SF_M(unescape_html)},
        {{"escape_url_query",   {}}, SF_M(escape_url_query)},
        {{"unescape_url_query", {}}, SF_M(unescape_url_query)},
        {{"escape_url_path",    {}}, SF_M(escape_url_path)},
        {{"unescape_url_path",  {}}, SF_M(unescape_url_path)},
        {{"filepath_join",      {}}, SF_M(filepath_join)},
        {{"filepath_split",     {}}, SF_M(filepath_split)},
        {{"unquote",            {}}, SF_M(unquote)},
        {{"slug",       {{"lang", false}}},     SF_M(slug)},
        {{"strip_html", {{"preserve", false}}}, SF_M(strip_html)},

        // ---- encoding -------------------------------------------------------
        {{"encode", {{"scheme", true}}}, SF_M(encode)},
        {{"hash",   {{"algorithm", true}, {"key", false}, {"polynomial", false}}},
                                         SF_M(hash)},
        {{"uuid_v5", {{"ns", false}}},   SF_M(uuid_v5)},
        {{"encrypt_aes", {{"scheme", true}, {"key", true}, {"iv", true}}},
                                         SF_M(encrypt_aes)},
        {{"decrypt_aes", {{"scheme", true}, {"key", true}, {"iv", true}}},
                                         SF_M(decrypt_aes)},
        {{"compare_argon2", {{"hashed_secret", true}}}, SF_M(compare_argon2)},
        {{"compare_bcrypt", {{"hashed_secret", true}}}, SF_M(compare_bcrypt)},
        {{"compress",   {{"algorithm", true}, {"level", false}}}, SF_M(compress)},
        {{"decompress", {{"algorithm", true}}},                   SF_M(decompress)},

        // ---- JSON Web Tokens -------------------------------------------------
        {{"sign_jwt_hs256",  {{"signing_secret", true}, {"headers", false}}}, SF_M(sign_jwt_hs256)},
        {{"parse_jwt_hs256", {{"signing_secret", true}}}, SF_M(parse_jwt_hs256)},
        {{"sign_jwt_hs384",  {{"signing_secret", true}, {"headers", false}}}, SF_M(sign_jwt_hs384)},
        {{"parse_jwt_hs384", {{"signing_secret", true}}}, SF_M(parse_jwt_hs384)},
        {{"sign_jwt_hs512",  {{"signing_secret", true}, {"headers", false}}}, SF_M(sign_jwt_hs512)},
        {{"parse_jwt_hs512", {{"signing_secret", true}}}, SF_M(parse_jwt_hs512)},
        {{"sign_jwt_rs256",  {{"signing_secret", true}, {"headers", false}}}, SF_M(sign_jwt_rs256)},
        {{"parse_jwt_rs256", {{"signing_secret", true}}}, SF_M(parse_jwt_rs256)},
        {{"sign_jwt_rs384",  {{"signing_secret", true}, {"headers", false}}}, SF_M(sign_jwt_rs384)},
        {{"parse_jwt_rs384", {{"signing_secret", true}}}, SF_M(parse_jwt_rs384)},
        {{"sign_jwt_rs512",  {{"signing_secret", true}, {"headers", false}}}, SF_M(sign_jwt_rs512)},
        {{"parse_jwt_rs512", {{"signing_secret", true}}}, SF_M(parse_jwt_rs512)},
        {{"sign_jwt_es256",  {{"signing_secret", true}, {"headers", false}}}, SF_M(sign_jwt_es256)},
        {{"parse_jwt_es256", {{"signing_secret", true}}}, SF_M(parse_jwt_es256)},
        {{"sign_jwt_es384",  {{"signing_secret", true}, {"headers", false}}}, SF_M(sign_jwt_es384)},
        {{"parse_jwt_es384", {{"signing_secret", true}}}, SF_M(parse_jwt_es384)},
        {{"sign_jwt_es512",  {{"signing_secret", true}, {"headers", false}}}, SF_M(sign_jwt_es512)},
        {{"parse_jwt_es512", {{"signing_secret", true}}}, SF_M(parse_jwt_es512)},

        {{"decode", {{"scheme", true}}}, SF_M(decode)},

        // ---- numbers --------------------------------------------------------
        {{"abs",   {}}, SF_M(abs_)},
        {{"ceil",  {}}, SF_M(ceil_)},
        {{"floor", {}}, SF_M(floor_)},
        {{"round", {}}, SF_M(round_)},
        {{"min",   {}}, SF_M(min_)},
        {{"max",   {}}, SF_M(max_)},
        {{"sum",   {}}, SF_M(sum)},
        {{"cos",   {}}, SF_M(cos_)},
        {{"sin",   {}}, SF_M(sin_)},
        {{"tan",   {}}, SF_M(tan_)},
        {{"log",   {}}, SF_M(log_)},
        {{"log10", {}}, SF_M(log10_)},
        {{"pow",   {{"exponent", true}}}, SF_M(pow_)},
        {{"bitwise_and", {{"value", true}}}, SF_M(bitwise_and)},
        {{"bitwise_or",  {{"value", true}}}, SF_M(bitwise_or)},
        {{"bitwise_xor", {{"value", true}}}, SF_M(bitwise_xor)},
        {{"int8",    {}}, SF_M(int8_)},
        {{"int16",   {}}, SF_M(int16_)},
        {{"int32",   {}}, SF_M(int32_)},
        {{"int64",   {}}, SF_M(int64_)},
        {{"uint8",   {}}, SF_M(uint8_)},
        {{"uint16",  {}}, SF_M(uint16_)},
        {{"uint32",  {}}, SF_M(uint32_)},
        {{"uint64",  {}}, SF_M(uint64_)},
        {{"float32", {}}, SF_M(float32_)},
        {{"float64", {}}, SF_M(float64_)},

        // ---- timestamps ------------------------------------------------------
        // The format_timestamp*/parse_timestamp* names are the older spelling of
        // the ts_* ones; the reference keeps both, pointing at one function.
        {{"timestamp",     {{"default", false}}}, SF_M(to_timestamp)},
        {{"ts_format",     {{"format", false}, {"tz", false}}}, SF_M(ts_format)},
        {{"ts_strftime",   {{"format", true}, {"tz", false}}},  SF_M(ts_strftime)},
        {{"ts_parse",      {{"format", true}}},   SF_M(ts_parse)},
        {{"ts_strptime",   {{"format", true}}},   SF_M(ts_strptime)},
        {{"ts_tz",         {{"tz", true}}},       SF_M(ts_tz)},
        {{"ts_unix",       {}},                   SF_M(ts_unix)},
        {{"ts_unix_milli", {}},                   SF_M(ts_unix_milli)},
        {{"ts_unix_micro", {}},                   SF_M(ts_unix_micro)},
        {{"ts_unix_nano",  {}},                   SF_M(ts_unix_nano)},
        {{"ts_sub",        {{"t2", true}}},       SF_M(ts_sub)},
        {{"ts_round",      {{"duration", true}}}, SF_M(ts_round)},
        {{"ts_add_iso8601",{{"duration", true}}}, SF_M(ts_add_iso8601)},
        {{"ts_sub_iso8601",{{"duration", true}}}, SF_M(ts_sub_iso8601)},
        {{"parse_duration",         {}},          SF_M(parse_duration)},
        {{"parse_duration_iso8601", {}},          SF_M(parse_duration_iso8601)},
        {{"format_timestamp",            {{"format", false}, {"tz", false}}}, SF_M(ts_format)},
        {{"format_timestamp_strftime",   {{"format", true}, {"tz", false}}},  SF_M(ts_strftime)},
        {{"format_timestamp_unix",       {}},     SF_M(ts_unix)},
        {{"format_timestamp_unix_milli", {}},     SF_M(ts_unix_milli)},
        {{"format_timestamp_unix_micro", {}},     SF_M(ts_unix_micro)},
        {{"format_timestamp_unix_nano",  {}},     SF_M(ts_unix_nano)},
        {{"parse_timestamp",             {{"format", true}}}, SF_M(ts_parse)},
        {{"parse_timestamp_strptime",    {{"format", true}}}, SF_M(ts_strptime)},

        // ---- structured -----------------------------------------------------
        {{"exists",     {{"path", true}}},  SF_M(exists)},
        {{"keys",       {}},                SF_M(keys)},
        {{"values",     {}},                SF_M(values)},
        {{"index",      {{"index", true}}}, SF_M(index)},
        {{"get",        {{"path", true}}},  SF_M(get_path)},
        {{"append",     {}, /*variadic=*/true}, SF_M(append)},
        {{"merge",      {{"with", true}}},  SF_M(merge)},
        {{"unique",     {{"emit", false}}},    SF_M(unique)},
        {{"sort",       {{"compare", false}}}, SF_M(sort_)},
        {{"flatten",    {}},                SF_M(flatten)},
        {{"array",      {}},                SF_M(array_of)},
        {{"not",        {}},                SF_M(not_)},
        {{"assign",     {{"with", true}}},  SF_M(assign)},
        {{"squash",     {}},                SF_M(squash)},
        {{"collapse",   {{"include_empty", false}}}, SF_M(collapse)},
        {{"enumerated", {}},                SF_M(enumerated)},
        {{"key_values", {}},                SF_M(key_values)},
        {{"explode",    {{"path", true}}},  SF_M(explode)},
        {{"with",       {}, /*variadic=*/true},    SF_M(with)},
        {{"without",    {}, /*variadic=*/true},    SF_M(without)},
        {{"concat",     {}, /*variadic=*/true},    SF_M(concat)},
        {{"zip",        {}, /*variadic=*/true},    SF_M(zip)},
        {{"find",       {{"value", true}}}, SF_M(find)},
        {{"find_all",   {{"value", true}}}, SF_M(find_all)},
        {{"diff",       {{"other", true}}}, SF_M(diff)},
        {{"patch",      {{"changelog", true}}}, SF_M(patch)},

        // ---- parsing and formatting ----------------------------------------
        {{"parse_json",  {{"use_number", false}}}, SF_M(parse_json_m)},
        {{"parse_csv", {{"parse_header_row", false}, {"delimiter", false},
                        {"lazy_quotes", false}}},          SF_M(parse_csv)},
        {{"parse_form_url_encoded", {}}, SF_M(parse_form_url_encoded)},
        {{"parse_logfmt", {}},           SF_M(parse_logfmt)},
        {{"parse_url",    {}},           SF_M(parse_url)},
        {{"parse_yaml",   {}},           SF_M(parse_yaml)},
        {{"format_yaml",  {}},           SF_M(format_yaml)},
        {{"parse_xml",    {{"cast", false}}}, SF_M(parse_xml)},
        {{"format_xml",   {{"indent", false}, {"no_indent", false},
                           {"root_tag", false}}},  SF_M(format_xml)},
        {{"json_path",    {{"expression", true}}}, SF_M(json_path)},
        {{"json_schema",  {{"schema", true}}},     SF_M(json_schema)},
        {{"format_msgpack", {}}, SF_M(format_msgpack)},
        {{"parse_msgpack",  {}}, SF_M(parse_msgpack)},
        {{"bloblang", {{"mapping", true}}}, SF_M(bloblang)},
        {{"format_json", {{"indent", false}, {"no_indent", false},
                          {"escape_html", false}}},
                              SF_M(format_json)},

        // ---- handled by the backends themselves ------------------------------
        {{"or",       {{"fallback", true}}},  nullptr, {}, 0, SPECIAL},
        // Evaluates its target once per message of the batch; SPECIAL because
        // the target must not be evaluated in the caller's context first.
        {{"from_all", {}},                      nullptr, {}, 0, SPECIAL},
        {{"from",     {{"index", true}}},       nullptr, {}, 0, SPECIAL},
        {{"catch",    {{"fallback", true}}}, nullptr, {}, 0, SPECIAL},
        {{"apply",    {{"mapping", true}}},  nullptr, {}, 0, SPECIAL},
        {{"map_each",     {{"query", true}}}, nullptr, {}, 0, SPECIAL},
        {{"filter",       {{"query", true}}}, nullptr, {}, 0, SPECIAL},
        {{"all",          {{"test", true}}},  nullptr, {}, 0, SPECIAL},
        {{"any",          {{"test", true}}},  nullptr, {}, 0, SPECIAL},
        {{"find_by",      {{"query", true}}}, nullptr, {}, 0, SPECIAL},
        {{"find_all_by",  {{"query", true}}}, nullptr, {}, 0, SPECIAL},
        {{"sort_by",      {{"query", true}}}, nullptr, {}, 0, SPECIAL},
        {{"map_each_key", {{"query", true}}}, nullptr, {}, 0, SPECIAL},
        {{"fold", {{"initial", true}, {"query", true}}}, nullptr, {}, 0, SPECIAL},
        {{"re_match",             {{"pattern", true}}},                 nullptr, {}, 0, SPECIAL},
        {{"re_replace_all",       {{"pattern", true}, {"value", true}}}, nullptr, {}, 0, SPECIAL},
        {{"re_find_all",          {{"pattern", true}}},                 nullptr, {}, 0, SPECIAL},
        {{"re_find_all_submatch", {{"pattern", true}}},                 nullptr, {}, 0, SPECIAL},
        {{"re_replace",           {{"pattern", true}, {"value", true}}}, nullptr, {}, 0, SPECIAL},
        {{"re_find_object",       {{"pattern", true}}},                 nullptr, {}, 0, SPECIAL},
        {{"re_find_all_object",   {{"pattern", true}}},                 nullptr, {}, 0, SPECIAL},
    };
    return t;
}

const std::vector<function_def>& functions() {
    static const std::vector<function_def> t = {
        // Context-taking, so hand-written in both backends.
        {{"counter",  {{"min", false}, {"max", false}, {"set", false}}}, nullptr, {}, 0, SPECIAL},
        // The key is OPTIONAL: meta() with no argument is every entry as an
        // object, the same as root_meta() below. Marking it required rejected
        // the no-key form at arity-check time, before either backend saw it.
        {{"metadata", {{"key", false}}},  nullptr, {}, 0, SPECIAL},
        {{"meta",     {{"key", false}}},  nullptr, {}, 0, SPECIAL},
        {{"content",  {}},               nullptr, {}, 0, SPECIAL},
        {{"json",     {{"path", false}}}, nullptr, {}, 0, SPECIAL},
        {{"error",    {}},               nullptr, {}, 0, SPECIAL},
        {{"errored",  {}},               nullptr, {}, 0, SPECIAL},
        {{"batch_index", {}},            nullptr, {}, 0, SPECIAL},
        {{"batch_size",  {}},            nullptr, {}, 0, SPECIAL},
        {{"random_int", {{"seed", false}, {"min", false}, {"max", false}}},
                                         nullptr, {}, 0, SPECIAL},
        {{"count",       {{"name", true}}},  nullptr, {}, 0, SPECIAL},
        {{"root_meta",   {{"key", false}}},  nullptr, {}, 0, SPECIAL},
        {{"error_source_name",  {}},     nullptr, {}, 0, SPECIAL},
        {{"error_source_label", {}},     nullptr, {}, 0, SPECIAL},
        {{"error_source_path",  {}},     nullptr, {}, 0, SPECIAL},

        // Pure: table-driven, same as the methods.
        {{"deleted",  {}},               SF_F(deleted)},
        {{"nothing",  {}},               SF_F(nothing)},
        {{"throw",    {{"why", true}}},  SF_F(throw_)},
        {{"range",    {{"start", true}, {"stop", true}, {"step", false}}}, SF_F(range)},
        {{"hostname", {}},               SF_F(hostname)},
        {{"env",      {{"name", true}, {"no_cache", false}}}, SF_F(env)},
        {{"now",                  {}}, SF_F(now)},
        {{"timestamp_unix",       {}}, SF_F(timestamp_unix)},
        {{"timestamp_unix_milli", {}}, SF_F(timestamp_unix_milli)},
        {{"timestamp_unix_micro", {}}, SF_F(timestamp_unix_micro)},
        {{"timestamp_unix_nano",  {}}, SF_F(timestamp_unix_nano)},
        {{"pi",       {}}, SF_F(pi)},
        {{"uuid_v4",  {}}, SF_F(uuid_v4)},
        {{"uuid_v7",  {{"time", false}}}, SF_F(uuid_v7)},
        {{"ulid",     {{"encoding", false}, {"random_source", false}}}, SF_F(ulid)},
        {{"ksuid",    {}}, SF_F(ksuid)},
        {{"nanoid",   {{"length", false}, {"alphabet", false}}}, SF_F(nanoid)},
        {{"snowflake_id", {{"node_id", false}}}, SF_F(snowflake_id)},
        {{"file",     {{"path", true}, {"no_cache", false}}}, SF_F(read_file)},
        {{"bytes",    {{"length", true}}}, SF_F(zero_bytes)},
    };
    return t;
}

} // namespace

const lambda_method* find_lambda_method(std::string_view name) {
    static const lambda_method t[] = {
        {"map_each", 0}, {"filter", 0}, {"all", 0}, {"any", 0},
        {"find_by", 0}, {"find_all_by", 0}, {"sort_by", 0}, {"map_each_key", 0},
        {"fold", 1},
        {"sort", 0, /*query_optional=*/true},
        {"unique", 0, /*query_optional=*/true},
    };
    for (const auto& e : t) if (e.name == name) return &e;
    return nullptr;
}

value call_lambda_method(std::string_view name, const value& target,
                         const value& initial, const m::lambda& fn) {
    if (name == "map_each")     return m::map_each(target, fn);
    if (name == "filter")       return m::filter(target, fn);
    if (name == "all")          return m::all(target, fn);
    if (name == "any")          return m::any(target, fn);
    if (name == "find_by")      return m::find_by(target, fn);
    if (name == "find_all_by")  return m::find_all_by(target, fn);
    if (name == "sort_by")      return m::sort_by(target, fn);
    if (name == "map_each_key") return m::map_each_key(target, fn);
    if (name == "fold")         return m::fold(target, initial, fn);
    if (name == "sort")         return m::sort_with(target, fn);
    if (name == "unique")       return m::unique_by(target, fn);
    throw eval_error("unknown lambda method: " + std::string(name));
}

const method_def* find_method(std::string_view name) {
    for (const auto& m : methods()) if (m.sig.name == name) return &m;
    return nullptr;
}
const function_def* find_function(std::string_view name) {
    for (const auto& f : functions()) if (f.sig.name == name) return &f;
    return nullptr;
}

} // namespace sf::blobl
