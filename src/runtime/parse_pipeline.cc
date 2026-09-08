// YAML -> pipeline_spec. The single parse both execution modes consume.
#include "swordfish/codecs.hh"
#include <cctype>
#include "swordfish/runtime/pipeline_spec.hh"
#include "swordfish/runtime/scanner.hh"
#include "swordfish/blobl/parse.hh"
#include "swordfish/config/spec.hh"
#include "swordfish/components/builtin_docs.hh"
#include "swordfish/config/template.hh"

#include <algorithm>
#include <initializer_list>
#include <map>

namespace sf {

using namespace sf::cfg;

namespace {

[[noreturn]] void unsupported(const ynode& at, const std::string& kind,
                              const std::string& name) {
    throw spec_error(at.pos,
        kind + " '" + name + "' is not implemented by swordfish");
}

[[noreturn]] void bad(const ynode& at, const std::string& msg) {
    throw spec_error(at.pos, msg);
}

// The Bloblang fields a registry entry exposes, checked at load time. Shared by
// processors, inputs and outputs, because a syntax error means the same thing in
// all three.
void validate_bloblang_fields(
        const std::function<std::vector<const bloblang*>(const void*)>& hook,
        const std::shared_ptr<const void>& cfg, const ynode& at) {
    if (!hook || !cfg) return;
    for (const bloblang* b : hook(cfg.get())) {
        if (b->source.empty()) continue;
        try {
            if (b->is_query) (void)blobl::parse_query(b->source);
            else             (void)blobl::parse_mapping(b->source);
        } catch (const blobl::parse_error& e) {
            throw spec_error({at.pos.line + e.where.line - 1, e.where.col}, e.what());
        }
    }
}

// Bloblang inside a component config is parsed at load time so a syntax error is
// reported as a config error rather than surfacing on the first message.
void validate_bloblang(const component_config& cc, const ynode& at) {
    auto check = [&](const bloblang& b) {
        if (b.source.empty()) return;
        try {
            if (b.is_query) (void)blobl::parse_query(b.source);
            else            (void)blobl::parse_mapping(b.source);
        } catch (const blobl::parse_error& e) {
            throw spec_error({at.pos.line + e.where.line - 1, e.where.col}, e.what());
        }
    };
    for (const auto& [chk, kids] : cc.cases) check(chk);
    // ...and every Bloblang field of the component's own config, which the
    // registry exposes generically.
    if (const processor_def* def = find_processor(cc.kind); def && def->bloblangs && cc.cfg)
        for (const cfg::bloblang* b : def->bloblangs(cc.cfg.get())) check(*b);
}

std::string scalar_or(const ynode* n, std::string def) {
    return (n && n->is_scalar()) ? n->scalar : std::move(def);
}

// Scalars that are not strings. These used to be `std::stoull(n->scalar)` and
// `n->scalar == "true"`, which turned a typo into either a std::invalid_argument
// escaping as an unhandled exception or a silently false flag -- neither of them
// the named config error every other field gets.
uint64_t uint_or(const ynode* n, uint64_t def, const std::string& what) {
    if (!n || !n->is_scalar()) return def;
    // The sign is rejected BEFORE stoull sees it: stoull accepts a leading '-'
    // and wraps, so `copies: -1` became 18446744073709551615 rather than a
    // config error -- a whole-number field silently reading as the largest
    // number there is.
    const std::string& t = n->scalar;
    if (!t.empty() && (t[0] == '-' || t[0] == '+'))
        bad(*n, what + " must be a whole number, got '" + t + "'");
    size_t used = 0;
    unsigned long long v = 0;
    try { v = std::stoull(t, &used); }
    catch (const std::exception&) { bad(*n, what + " must be a whole number, got '" + t + "'"); }
    if (used != t.size())
        bad(*n, what + " must be a whole number, got '" + t + "'");
    return v;
}

// `false` in any of YAML's spellings. Used where a switched-OFF block is
// accepted and a switched-on one is refused, so a capitalised `False` must not
// read as "on" and get refused as unimplemented.
bool is_false_scalar(const std::string& raw) {
    std::string t = raw;
    for (char& c : t) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    return t == "false";
}

bool bool_or(const ynode* n, bool def, const std::string& what) {
    if (!n || !n->is_scalar()) return def;
    // YAML 1.1 spells a boolean several ways and the reference's reader accepts
    // them, so `True` and `TRUE` -- which a Python- or Go-shaped config writes
    // without thinking -- must not be a swordfish-only error.
    std::string t = n->scalar;
    for (char& c : t) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    if (t == "true")  return true;
    if (t == "false") return false;
    bad(*n, what + " must be true or false, got '" + n->scalar + "'");
}

// Rejects any key a component does not have. The composite inputs and outputs
// are parsed by hand rather than through spec_of -- a nested component list has
// no field type, the same reason a switch PROCESSOR's cases are hand-parsed --
// so the key check spec_of would have given is written out instead of skipped.
//
// It deliberately does NOT distinguish a field the reference has and swordfish
// does not from a misspelling: those are different mistakes and lead the reader
// to different places, so the first is caught by `unimplemented_fields` BEFORE
// this runs. Calling only_fields alone would report an unbuilt field as a typo.
template <class Range>
void only_fields_in(const ynode& body, const std::string& what, const Range& allowed) {
    for (const auto& [k, v] : body.map)
        if (std::find(std::begin(allowed), std::end(allowed), k) == std::end(allowed)) {
            std::string names;
            for (auto a : allowed) names += (names.empty() ? "" : ", ") + std::string(a);
            bad(v, what + " has no field '" + k + "'; expected " + names);
        }
}

void only_fields(const ynode& body, const std::string& what,
                 std::initializer_list<std::string_view> allowed) {
    only_fields_in(body, what, allowed);
}

// The allowed fields of a BUILT-IN, taken from the one table that also
// documents it. Written this way so a field the parser accepts is a field
// `swordfish list` shows: the built-ins are not in the component registry, so
// nothing described them at all until this existed, and a second hand-written
// list would have drifted the way `list` and `lint` once did over `kafka`.
void only_builtin_fields(const ynode& body, const std::string& what,
                         builtin_class cls, std::string_view kind) {
    const auto allowed = builtin_field_names(cls, kind);
    // A kind absent from the table would give an empty allowed-list, which
    // rejects every field. Refusing to check is the safe answer, and the gate
    // asserts the table covers every built-in the parser handles.
    if (allowed.empty()) return;
    only_fields_in(body, what, allowed);
}

// `codec` on the line-oriented outputs. `lines` is what swordfish writes, so
// that value is accepted and every other one is refused BY NAME rather than
// ignored -- writing `delim:;` framing as newlines would change the bytes on the
// wire for a config that asked for something else.
void check_line_codec(const ynode& body, const std::string& what) {
    if (const ynode* c = body.find("codec"); c && c->scalar != "lines")
        bad(*c, what + " `codec: " + c->scalar + "` is not implemented by swordfish; "
                "it writes `lines` only");
}

// Fields the REFERENCE has that swordfish does not. Refused by their own name,
// because "swordfish has not built `delete_on_finish`" and "you misspelt
// `paths`" send the reader to different places -- and because only_fields on its
// own would call the first one a typo.
void unimplemented_fields(const ynode& body, const std::string& what,
                          std::initializer_list<std::string_view> unbuilt) {
    for (auto f : unbuilt)
        if (const ynode* u = body.find(std::string(f)))
            bad(*u, what + " `" + std::string(f) + "` is not implemented by swordfish");
}


// Generic: the registry decides what a kind means, so this function never needs
// to change when a processor is added.


} // namespace

// A template usage is expanded HERE -- at the point a component kind is looked
// up -- rather than by a pre-pass over the document. Every nested input goes
// through parse_input, every nested processor through parse_processor, so this
// covers a template inside a `broker`, inside a `switch` case, inside a resource
// block: positions a document walk would have to know about one at a time, and
// would eventually miss one.
//
// The expansion's result is fed straight back into the same parser, so a
// template that produces another template's usage works, and the produced config
// is validated exactly as a hand-written one is.
const ynode* template_body(const ynode& node, std::string& kind_out) {
    for (const auto& e : node.map) {
        if (e.key == "label" || e.key == "processors") continue;
        kind_out = e.key;
        return &e.value;
    }
    return nullptr;
}

std::string label_of(const ynode& node) {
    const ynode* l = node.find("label");
    return l ? l->scalar : std::string();
}

// Returns the expanded node when `node` names a template of this type, or
// nullopt when it does not.
std::optional<ynode> maybe_expand(const ynode& node, tmpl::comp_type type) {
    if (!node.is_mapping()) return std::nullopt;
    std::string kind;
    if (template_body(node, kind) == nullptr) return std::nullopt;
    const tmpl::definition* def = tmpl::find(type, kind);
    if (def == nullptr) return std::nullopt;
    return tmpl::expand(*def, node, label_of(node));
}

component_config parse_processor(const ynode& node) {
    if (const auto expanded = maybe_expand(node, tmpl::comp_type::processor)) {
        // The guard is held ACROSS the recursive parse: that is where the next
        // expansion happens, so a counter that lived only inside expand() never
        // rose above one and a self-referential template blew the stack.
        const tmpl::expansion_guard depth(node.pos);
        return parse_processor(*expanded);
    }

    // `label` sits beside the kind rather than inside it, so it has to come off
    // before the "exactly one key" rule applies.
    std::string label;
    const ynode* kind_node = nullptr;
    std::string kind;
    if (node.is_mapping()) {
        for (const auto& e : node.map) {
            if (e.key == "label") { label = e.value.scalar; continue; }
            if (kind_node) bad(node, "a processor must name exactly one kind, got '" +
                                     kind + "' and '" + e.key + "'");
            kind = e.key;
            kind_node = &e.value;
        }
    }
    if (!kind_node) bad(node, "a processor must be a single-key object");
    const ynode& body = *kind_node;

    // `resource: <label>` names another component rather than configuring one,
    // so it carries a bare scalar and has no registry entry. Recorded here and
    // resolved against processor_resources once the whole document is parsed.
    if (kind == "resource") {
        component_config cc;
        cc.kind = kind;
        cc.label = std::move(label);
        cc.label_ref = body.scalar;
        cc.where = body.pos;
        if (cc.label_ref.empty()) bad(body, "`resource` needs the label of a processor");
        return cc;
    }

    const processor_def* def = find_processor(kind);
    if (!def) unsupported(node, "processor", kind);

    component_config cc;
    cc.kind  = kind;
    cc.label = std::move(label);
    cc.where = body.pos;

    // Recorded before the body is decoded: resolution runs on the whole
    // document afterwards and needs to know which cache was named.
    if (kind == "dedupe")
        if (const ynode* c = body.find("cache")) cc.cache_name = c->scalar;
    // The `rate_limit` PROCESSOR spells its label `resource`; every other
    // component spells it `rate_limit`. Both are recorded the same way and
    // resolved by the same code below.
    if (kind == "rate_limit") {
        if (const ynode* r = body.find("resource")) {
            cc.rate_limit_name = r->scalar;
            // An EMPTY label on this processor is a no-op processor, which is
            // never what someone writing `rate_limit:` meant. On the optional
            // `rate_limit:` field of an input or output an empty string does
            // mean "unthrottled", which is the reference's default -- so only
            // this one is refused.
            if (cc.rate_limit_name.empty())
                bad(*r, "`rate_limit.resource` is empty; it must name a "
                        "rate_limit_resources entry");
        }
    } else if (const ynode* r = body.find("rate_limit")) {
        cc.rate_limit_name = r->scalar;
    }

    lints ls;
    if (def->shape == processor_def::body::list) {
        // try/catch/for_each take a bare list; switch takes a list of cases.
        if (!body.is_sequence()) bad(body, kind + " expects a list");
        // `group_by` takes the same {check, processors} case list as `switch`
        // and routes by the same rule, so it parses through the same branch.
        if (kind == "switch" || kind == "group_by") {
            for (const auto& c : body.seq) {
                // Switch cases are parsed by hand rather than through spec_of,
                // because a case holds a nested processor list and the config
                // framework has no field type for one. The key validation
                // spec_of would have given is done explicitly, so a typo is
                // still a named error rather than a silently ignored field.
                for (const auto& [k, v] : c.map)
                    if (k != "check" && k != "processors")
                        bad(v, "a switch case has no field '" + k + "'; expected "
                               "'check' or 'processors'");
                const ynode* chk = c.find("check");
                const ynode* prc = c.find("processors");
                if (!chk) bad(c, "a switch case needs a `check`");
                if (!prc) bad(c, "a switch case needs `processors`");
                cc.cases.emplace_back(bloblang{chk->scalar, true}, parse_processors(*prc));
            }
        } else {
            cc.children = parse_processors(body);
        }
        ynode empty;
        empty.type = ynode_type::mapping;
        cc.cfg = def->parse(empty, kind, ls);
    } else {
        // A scalar body is shorthand for the component's primary field, the way
        // `mapping: root = this` and `cpp: |` are written.
        if (body.is_scalar()) {
            ynode wrapped;
            wrapped.type = ynode_type::mapping;
            wrapped.pos  = body.pos;
            const char* primary = (kind == "cpp") ? "body" : "mapping";
            wrapped.map.emplace_back(primary, body);
            cc.cfg = def->parse(wrapped, kind, ls);
        } else {
            // `branch` carries a nested processor list ALONGSIDE its two
            // mappings, which no field type can express. The list is lifted out
            // by hand and the rest goes through spec_of, so `request_map` and
            // `result_map` still get the usual validation and a typo in either
            // is a named error.
            if (kind == "workflow") {
                ynode rest = body;
                for (size_t i = 0; i < rest.map.size();) {
                    const auto& e = rest.map[i];
                    if (e.key == "branches") {
                        if (!e.value.is_mapping())
                            bad(e.value, "workflow `branches` must be a mapping of name to branch");
                        for (const auto& br : e.value.map) {
                            // Each entry is a branch config; parsed through the
                            // ordinary branch path so its two mappings get the
                            // same validation they would standalone.
                            ynode wrapped;
                            wrapped.type = ynode_type::mapping;
                            wrapped.pos  = br.value.pos;
                            wrapped.map.emplace_back("branch", br.value);
                            cc.branches.push_back({br.key, parse_processor(wrapped)});
                        }
                        rest.map.erase(rest.map.begin() + static_cast<long>(i));
                        continue;
                    }
                    if (e.key == "order") {
                        if (!e.value.is_sequence())
                            bad(e.value, "workflow `order` must be a list of lists");
                        for (const auto& g : e.value.seq) {
                            if (!g.is_sequence())
                                bad(g, "each entry of workflow `order` must itself be a list");
                            std::vector<std::string> group;
                            for (const auto& n : g.seq) group.push_back(n.scalar);
                            cc.order.push_back(std::move(group));
                        }
                        rest.map.erase(rest.map.begin() + static_cast<long>(i));
                        continue;
                    }
                    ++i;
                }
                cc.cfg = def->parse(rest, kind, ls);
            } else if (kind == "branch") {
                ynode rest = body;
                for (size_t i = 0; i < rest.map.size(); ++i)
                    if (rest.map[i].key == "processors") {
                        if (!rest.map[i].value.is_sequence())
                            bad(rest.map[i].value, "branch `processors` must be a list");
                        cc.children = parse_processors(rest.map[i].value);
                        rest.map.erase(rest.map.begin() + static_cast<long>(i));
                        break;
                    }
                cc.cfg = def->parse(rest, kind, ls);
            } else {
                cc.cfg = def->parse(body, kind, ls);
            }
        }
    }
    for (const auto& l : ls)
        if (l.level == lint_level::error)
            throw spec_error(l.where.line ? l.where : body.pos, l.path + ": " + l.message);

    // Bloblang fields are validated here rather than in the codec, so a syntax
    // error is a config error with a line number.
    validate_bloblang(cc, body);
    return cc;
}

std::vector<component_config> parse_processors(const ynode& seq) {
    std::vector<component_config> out;
    // A non-sequence is an ERROR, not an empty list. Returning {} silently made
    // the commonest YAML slip -- dropping the leading `-`, so
    // `processors: { mapping: ... }` is a map rather than a list -- run a
    // pipeline with NO processors at all: lint passed, the message came out
    // untransformed, and the reference refuses the same file. The same applied
    // inside a `switch` case, which then ran empty.
    //
    // A NULL node is refused too, and that carve-out was a mistake: the
    // reference rejects `processors:` with nothing under it ("expected array
    // value, got !!null"), which is what a fully commented-out list or a
    // template that rendered empty produces -- exactly the case where running
    // with no processors is least likely to be what was meant.
    if (!seq.is_sequence())
        bad(seq, "`processors` must be a list; each entry needs a leading `-`");
    for (const auto& p : seq.seq) out.push_back(parse_processor(p));
    return out;
}

namespace {

// One `cache_resources` entry. `memory`, `lru` and `file` are implemented; the
// rest of the reference's fourteen kinds are refused by name.
//
// Parsed by hand for the reason `scanner` is: the kind is the KEY, which the
// spec_of field model cannot express.
cache_spec parse_cache(const ynode& def, const std::string& label) {
    // A kind the reference HAS but swordfish does not is refused as
    // unimplemented; anything else is refused as a typo. They are different
    // mistakes and lead the reader to different places, so `only_fields` alone
    // -- which would call `redis` an unknown field -- is not enough.
    static constexpr std::string_view reference_kinds[] = {
        "aws_dynamodb", "aws_s3", "couchbase", "file", "gcp_cloud_storage", "lru",
        "memcached", "memory", "mongodb", "multilevel", "nats_kv", "noop", "redis",
        "redpanda", "ristretto", "sql", "ttlru"};
    for (const auto& e : def.map) {
        if (e.key == "label" || e.key == "memory" || e.key == "lru" || e.key == "file")
            continue;
        if (std::find(std::begin(reference_kinds), std::end(reference_kinds), e.key)
                != std::end(reference_kinds))
            bad(e.value, "cache '" + e.key + "' is not implemented by swordfish; it "
                         "implements memory, lru, file");
    }
    only_fields(def, "a cache_resources entry", {"label", "memory", "lru", "file"});
    // EXACTLY ONE kind. The if-chain below takes the first it finds, so an entry
    // naming two -- `{label: c, memory: {}, lru: {cap: 5}}` -- silently built a
    // `memory` cache and threw the `lru` block away, `cap` and all. The
    // reference refuses it ("unable to infer cache type, multiple candidates"),
    // and a config whose second half is discarded without a word is the kind of
    // silent approximation this file exists to prevent.
    {
        std::vector<std::string> named;
        for (const auto& e : def.map)
            if (e.key == "memory" || e.key == "lru" || e.key == "file")
                named.push_back(e.key);
        if (named.empty())
            bad(def, "cache '" + label + "' names no kind; it needs one of "
                     "memory, lru, file");
        if (named.size() > 1) {
            std::string all;
            for (const auto& n : named) all += (all.empty() ? "'" : "' and '") + n;
            bad(def, "cache '" + label + "' names more than one kind (" + all +
                     "'); a cache_resources entry takes exactly one");
        }
    }
    const auto read_duration = [&](const ynode* n, const char* what) {
        duration d;
        lints ls;
        cfg::codec<duration>::parse(*n, d, what, ls);
        if (!ls.empty())
            throw spec_error(n->pos, "cache '" + label + "': " + what +
                                     " is not a duration");
        return std::chrono::duration_cast<std::chrono::milliseconds>(d.ns);
    };
    // `init_values` is a table of key/VALUE pairs, and only the keys are kept:
    // the cache interface stores no values because `dedupe`, its only consumer,
    // asks solely whether a key is present. A `cache` processor would need the
    // values and would need that interface widened first -- so the values are
    // dropped deliberately, not by oversight.
    const auto read_init = [&](const ynode* n) {
        std::vector<std::string> keys;
        if (!n) return keys;
        if (n->is_sequence())
            bad(*n, "`init_values` is a table of key/value pairs, not a list");
        for (const auto& e : n->map) keys.push_back(e.key);
        return keys;
    };

    cache_spec spec;
    // Carried into the spec so make_cache can share one instance between every
    // component naming this label, which is what a resource IS.
    spec.label = label;
    if (const ynode* mem = def.find("memory")) {
        spec.kind = "memory";
        only_fields(*mem, "a `memory` cache",
                    {"default_ttl", "compaction_interval", "init_values", "shards"});
        // `shards` is IMPLEMENTED, not inert. The comment that used to sit here
        // said it only stripes Benthos' map to cut lock contention that
        // swordfish does not have, and that "nothing observable differs". Both
        // halves were false: each shard carries its own last-compaction time and
        // a write sweeps only its own shard, so the field decides when entries
        // expire. On the reference, one config with `shards: 1` emitted `A B A`
        // and with `shards: 2` emitted `A B`.
        spec.shards = static_cast<int64_t>(
            uint_or(mem->find("shards"), 1, "a `memory` cache `shards`"));
        if (spec.shards < 1) spec.shards = 1;   // the reference treats <= 1 as one
        if (const ynode* t = mem->find("default_ttl")) {
            // An EMPTY string is not zero. The reference parses this field as a
            // duration unconditionally and rejects "" --
            // `failed to parse 'default_ttl' as a duration string: time: invalid
            // duration ""` -- where this mapped it to zero and carried on.
            if (t->scalar.empty())
                bad(*t, "a `memory` cache `default_ttl` cannot be empty; it is a "
                        "duration such as `0s` or `5m`. The reference refuses the "
                        "same value rather than reading it as zero");
            spec.default_ttl = read_duration(t, "default_ttl");
        }
        if (const ynode* c = mem->find("compaction_interval")) {
            // "This field can be set to an empty string in order to disable
            // compactions/expiry entirely" -- which is not the same as a zero
            // interval, so the two are kept apart.
            if (c->scalar.empty()) spec.compaction_disabled = true;
            else spec.compaction_interval = read_duration(c, "compaction_interval");
        }
        spec.init_keys = read_init(mem->find("init_values"));
    } else if (const ynode* lru = def.find("lru")) {
        spec.kind = "lru";
        only_fields(*lru, "an `lru` cache",
                    {"cap", "init_values", "algorithm", "two_queues_recent_ratio",
                     "two_queues_ghost_ratio", "optimistic"});
        spec.cap = static_cast<int64_t>(uint_or(lru->find("cap"), 1000, "lru `cap`"));
        if (spec.cap <= 0) bad(*lru, "an `lru` cache `cap` must be greater than zero");
        spec.init_keys = read_init(lru->find("init_values"));
        // `arc` and `two_queues` are genuinely different eviction policies, not
        // tuning: which key survives an eviction differs, so they are refused
        // rather than served by the standard one.
        const std::string alg = scalar_or(lru->find("algorithm"), "standard");
        if (alg != "standard")
            bad(*lru, "an `lru` cache `algorithm: " + alg + "` is not implemented by "
                      "swordfish; only `standard` is, and the others evict different "
                      "keys rather than merely being tuned differently");
        for (auto f : {"two_queues_recent_ratio", "two_queues_ghost_ratio"})
            if (const ynode* u = lru->find(f))
                bad(*u, std::string("an `lru` cache `") + f + "` only applies to "
                        "`algorithm: two_queues`, which swordfish does not implement");
        // `optimistic` turns off Benthos' locking, at the cost of a non-atomic
        // ADD. Under Seastar a cache is shard-local and `add` cannot be
        // preempted, so there is no lock to drop and the operation is already
        // atomic: accepting it changes nothing either way.
        (void)bool_or(lru->find("optimistic"), false, "lru `optimistic`");
    } else if (const ynode* file = def.find("file")) {
        spec.kind = "file";
        only_fields(*file, "a `file` cache", {"directory"});
        spec.directory = scalar_or(file->find("directory"), "");
        if (spec.directory.empty())
            bad(*file, "a `file` cache requires a `directory`");
    } else {
        throw spec_error(def.pos, "cache '" + label +
            "' is not a `memory`, `lru` or `file` cache; swordfish implements no "
            "other kind");
    }
    return spec;
}

// A `rate_limit:` field -- on a processor, an input or an output -- names an
// entry in the document-level `rate_limit_resources` block. Resolved here for
// the same reason `dedupe`'s cache is: the registry only ever sees one
// component's own config, never the document around it. Unlike a processor
// resource, a limit MAY be named more than once; sharing the permits between
// components is the whole point of it being a resource.
void resolve_rate_limit(component_config& cc, const ynode& root) {
    if (cc.rate_limit_name.empty() || cc.rate_limit_resolved) return;
    const ynode* rs = root.find("rate_limit_resources");
    const ynode* def = nullptr;
    if (rs && rs->is_sequence())
        for (const auto& e : rs->seq)
            if (const auto* l = e.find("label"); l && l->scalar == cc.rate_limit_name) {
                def = &e; break;
            }
    if (!def)
        throw spec_error(cc.where, "rate limit '" + cc.rate_limit_name +
            "' is not declared in rate_limit_resources");
    // Every kind the reference has is listed, so an unimplemented one is refused
    // by its own name rather than as an unknown field -- the difference between
    // "swordfish does not do redis rate limits" and "you made a typo".
    only_fields(*def, "a rate_limit_resources entry", {"label", "local", "redis"});
    const ynode* local = def->find("local");
    if (!local)
        throw spec_error(cc.where, "rate limit '" + cc.rate_limit_name +
            "' is not a `local` rate limit; swordfish implements no other kind yet");
    // The reference rejects an unrecognised field here, and a config that lints
    // clean under swordfish and fails under the reference is the divergence that
    // costs a user the most: it is found after deployment rather than before.
    only_fields(*local, "a `local` rate limit", {"count", "interval"});
    cc.rate_limit_count =
        static_cast<int64_t>(uint_or(local->find("count"), 1000, "rate_limit `count`"));
    cc.rate_limit_interval = std::chrono::seconds{1};
    if (const ynode* iv = local->find("interval"); iv && !iv->scalar.empty()) {
        duration d;
        lints ls;
        cfg::codec<duration>::parse(*iv, d, "interval", ls);
        if (!ls.empty())
            throw spec_error(iv->pos, "rate limit '" + cc.rate_limit_name +
                "': interval is not a duration");
        // NOT duration_cast'd to milliseconds: that truncated toward zero.
        cc.rate_limit_interval = d.ns;
    }
    cc.rate_limit_resolved = true;
}

// Inputs and outputs carry their own component_config and their own trees, so
// each needs its own walk. Both exist only to reach every nested component: a
// `broker` of `http_client`s is three components that each named a limit.
void resolve_input_rate_limits(input_spec& in, const ynode& root) {
    if (in.comp) resolve_rate_limit(*in.comp, root);
    for (auto& c : in.children) resolve_input_rate_limits(c, root);
}

// Every nested processor list a component_config can hold. Returning them all
// from one place is what stops a walk from covering two of the three slots, as
// resolve_resources_impl did.
std::vector<std::vector<component_config>*> nested_lists(component_config& cc) {
    std::vector<std::vector<component_config>*> out{&cc.children};
    for (auto& [check, branch] : cc.cases)  out.push_back(&branch);
    for (auto& b : cc.branches)             out.push_back(&b.config.children);
    return out;
}

// Replaces every `resource: <label>` in a processor tree with the definition
// from `processor_resources`.
//
// Benthos resources are SHARED instances: two references name one object, which
// matters for anything stateful. Inlining gives each reference its own, so a
// label referenced more than once is refused by name rather than quietly given
// two independent copies -- that difference is invisible until a stateful
// resource produces subtly wrong results. A single reference is observationally
// identical to sharing, which is the case the corpus and most configs use.
void resolve_resources_impl(std::vector<component_config>& procs, const ynode& root,
                            std::map<std::string, int>& seen) {
    for (auto& cc : procs) {
        // The SAME entry is resolved until it settles. A `resource:` is replaced
        // in place by what it names, and what replaces it may itself be a
        // `dedupe`, carry a `rate_limit:`, or be another `resource:` -- none of
        // which was resolved, because the loop moved straight on to the nested
        // lists. `pipeline.processors: [{resource: dd}]` naming a dedupe in
        // `processor_resources` therefore linted clean and then aborted at
        // start-up with "cache 'c' was not resolved against cache_resources", on
        // a config the reference runs. The `seen` map below still terminates a
        // cycle, since a label reached twice is an error.
      for (;;) {
        // `dedupe` names a cache in `cache_resources`. Resolved here, with the
        // rest of the document-level lookups, because the registry only ever
        // sees one component's own config.
        if (cc.kind == "dedupe" && !cc.cache_resolved) {
            const ynode* cs = root.find("cache_resources");
            const ynode* def = nullptr;
            if (cs && cs->is_sequence())
                for (const auto& e : cs->seq)
                    if (const auto* l = e.find("label"); l && l->scalar == cc.cache_name) {
                        def = &e; break;
                    }
            if (!def)
                throw spec_error(cc.where, "cache '" + cc.cache_name +
                    "' is not declared in cache_resources");
            cc.cache = parse_cache(*def, cc.cache_name);
            cc.cache_resolved = true;
        }
        resolve_rate_limit(cc, root);
        if (cc.kind != "resource") break;
        {
            const std::string label = cc.label_ref;
            const ynode* rs = root.find("processor_resources");
            const ynode* def = nullptr;
            if (rs && rs->is_sequence())
                for (const auto& e : rs->seq)
                    if (const auto* l = e.find("label"); l && l->scalar == label) { def = &e; break; }
            if (!def)
                throw spec_error(cc.where, "resource '" + label +
                    "' is not declared in processor_resources");
            if (++seen[label] > 1)
                throw spec_error(cc.where, "resource '" + label +
                    "' is referenced more than once; swordfish inlines resources "
                    "rather than sharing one instance, which would differ for a "
                    "stateful processor");
            cc = parse_processor(*def);
        }
      }
        // component_config keeps nested processor trees in THREE slots --
        // `children`, `cases` and `branches` -- and this walk covered two. A
        // `dedupe`, `rate_limit:` or `resource:` inside a workflow branch was
        // therefore never resolved, and surfaced later as the registry's
        // "internal error: ... reached component construction unresolved" on a
        // config the reference runs. One loop over all three, so a fourth slot
        // is a compile error here rather than a silent gap.
        for (auto* nested : nested_lists(cc)) resolve_resources_impl(*nested, root, seen);
    }
}

// Every processor list an INPUT owns, walked the way the output side walks its
// equivalents. Both lists are built by both backends, so a list this misses is
// one whose `resource:` is never inlined and whose `dedupe` never finds its
// cache -- the config lints clean, and `swordfish run` aborts before it moves a
// message on a document the reference runs.
//
// `processors` was added to input_spec when inputs learned to take one, and the
// walk was not extended with it: the field went straight from "refused at parse"
// to "built but unresolved". A field reachable by the builder and not by this
// function is the shape of the bug, so both are listed here explicitly rather
// than one standing in for the other.
void resolve_input_resources(input_spec& in, const ynode& root,
                             std::map<std::string, int>& seen) {
    resolve_resources_impl(in.processors, root, seen);
    resolve_resources_impl(in.batching_processors, root, seen);
    for (auto& c : in.children) resolve_input_resources(c, root, seen);
}

// An output's own `processors:` can name a resource too, at any depth of a
// broker or switch tree. It shares `seen` with the pipeline's own processors,
// because the once-only rule is about the DOCUMENT: a resource referenced by a
// pipeline processor and again by an output is referenced twice.
void resolve_output_resources_impl(output_spec& out, const ynode& root,
                                   std::map<std::string, int>& seen) {
    if (out.comp) resolve_rate_limit(*out.comp, root);
    resolve_resources_impl(out.processors, root, seen);
    resolve_resources_impl(out.batching_processors, root, seen);
    for (auto& c : out.children) resolve_output_resources_impl(c, root, seen);
}

} // namespace

void resolve_resources(std::vector<component_config>& procs, const cfg::ynode& root) {
    std::map<std::string, int> seen;
    resolve_resources_impl(procs, root, seen);
}


namespace {

// Lifts every `scanner:` out of a component's body, at any depth, keyed by its
// dotted config path -- "scanner" for `socket`, "stream.scanner" for
// `http_client`.
//
// Generic rather than a list of known paths, because the rule is generic: in
// the reference a field named `scanner` is ALWAYS the scanner component, whose
// key is its kind. spec_of cannot express a field whose name is the value, which
// is the same reason `batching` and nested processor lists are lifted out before
// the body is decoded.
void lift_scanners(ynode& body, const std::string& prefix,
                   std::map<std::string, scanner_spec>& out);

// `codec`, the field `scanner` replaced. Deprecated in the reference and still
// ACCEPTED there -- its own corpus uses `codec: lines` -- so refusing it made
// swordfish reject real Benthos configs. It is a small chained language, and it
// chains exactly the way the scanners nest, so it translates rather than needing
// a second implementation.
scanner_spec scanner_from_codec(const ynode& at, const std::string& codec);

// Defined below, beside the outputs that were its first user.
void parse_batching(const ynode& node, output_spec& spec);

// `scanner: { csv: { ... } }`. A component, so it is a single-key object whose
// key is the kind and whose body is that kind's options -- and it NESTS, since
// `skip_bom`, `decompress` and `switch` each wrap another.
//
// Parsed by hand rather than through spec_of for the reason `batching`, `branch`
// and the composite outputs are: the field name is the component's kind, which
// the field model cannot express.
scanner_spec parse_scanner(const ynode& node) {
    auto sk = node.single_key();
    if (!sk)
        bad(node, "`scanner` must be a single-key object naming a scanner, as in "
                  "`scanner: { lines: {} }`; Redpanda Connect rejects a bare string "
                  "here too, at start-up rather than at lint");
    scanner_spec spec;
    spec.kind = sk->first;
    const ynode& body = *sk->second;

    // The kind is checked BEFORE its body, so an unimplemented scanner is named
    // as such rather than producing a pile of "no field" errors for options it
    // was never going to read.
    try {
        scanner_spec probe;
        probe.kind = spec.kind;
        // `switch` and `chunker` need more than a kind to construct; their own
        // validation below covers them, so only the name is being tested here.
        if (spec.kind != "switch" && spec.kind != "chunker") (void)make_scanner(probe);
    } catch (const std::exception& e) {
        const std::string msg = e.what();
        // A construction failure that is about the NAME is a config error; one
        // about the options is handled where those options are read.
        if (msg.find("is not implemented") != std::string::npos ||
            msg.find("is not a scanner") != std::string::npos)
            bad(node, msg);
    }

    const auto child_of = [&](const char* field, const ynode& b) {
        if (const ynode* c = b.find(field)) return parse_scanner(*c);
        scanner_spec d;                 // the reference's default `into`
        d.kind = "to_the_end";
        return d;
    };

    if (spec.kind == "lines") {
        only_fields(body, "the `lines` scanner",
                    {"custom_delimiter", "max_buffer_size", "omit_empty"});
        spec.custom_delimiter = scalar_or(body.find("custom_delimiter"), "");
        spec.max_buffer_size  = static_cast<int64_t>(
            uint_or(body.find("max_buffer_size"), 65536, "lines `max_buffer_size`"));
        spec.omit_empty       = bool_or(body.find("omit_empty"), false, "lines `omit_empty`");
    } else if (spec.kind == "csv") {
        only_fields(body, "the `csv` scanner",
                    {"custom_delimiter", "parse_header_row", "lazy_quotes",
                     "continue_on_error"});
        spec.custom_delimiter  = scalar_or(body.find("custom_delimiter"), "");
        if (spec.custom_delimiter.size() > 1)
            bad(body, "csv `custom_delimiter` must be a single character");
        spec.parse_header_row  = bool_or(body.find("parse_header_row"), true,
                                         "csv `parse_header_row`");
        spec.lazy_quotes       = bool_or(body.find("lazy_quotes"), false,
                                         "csv `lazy_quotes`");
        spec.continue_on_error = bool_or(body.find("continue_on_error"), false,
                                         "csv `continue_on_error`");
    } else if (spec.kind == "chunker") {
        only_fields(body, "the `chunker` scanner", {"size"});
        spec.size = static_cast<int64_t>(uint_or(body.find("size"), 0, "chunker `size`"));
        if (spec.size <= 0) bad(body, "the `chunker` scanner requires a `size` above zero");
    } else if (spec.kind == "re_match") {
        only_fields(body, "the `re_match` scanner", {"pattern", "max_buffer_size"});
        spec.pattern = scalar_or(body.find("pattern"), "");
        if (spec.pattern.empty())
            bad(body, "the `re_match` scanner requires a `pattern`");
        spec.max_buffer_size = static_cast<int64_t>(
            uint_or(body.find("max_buffer_size"), 65536, "re_match `max_buffer_size`"));
        // Compiled here so a bad pattern is a config error with a line number
        // rather than a failure on the first byte read.
        try { (void)make_scanner(spec); }
        catch (const std::exception& e) { bad(body, e.what()); }
    } else if (spec.kind == "avro") {
        only_fields(body, "the `avro` scanner", {"raw_json"});
        spec.raw_json = bool_or(body.find("raw_json"), false, "avro `raw_json`");
    } else if (spec.kind == "skip_bom") {
        only_fields(body, "the `skip_bom` scanner", {"into"});
        spec.children.push_back(child_of("into", body));
    } else if (spec.kind == "decompress") {
        only_fields(body, "the `decompress` scanner", {"algorithm", "into"});
        spec.algorithm = scalar_or(body.find("algorithm"), "");
        if (spec.algorithm.empty())
            bad(body, "the `decompress` scanner requires an `algorithm`");
        // The shared list, not a third copy of it: this one and the two in
        // processor_defs.cc drifted apart once already, and the message is built
        // from the same array so it cannot go stale either.
        const auto& algs = ::sf::codec::decompress_algorithms;
        if (std::find(std::begin(algs), std::end(algs), spec.algorithm) == std::end(algs)) {
            std::string names;
            for (auto a : algs) names += (names.empty() ? "" : ", ") + std::string(a);
            bad(body, "decompress `algorithm` '" + spec.algorithm +
                      "' is not one of " + names);
        }
        spec.children.push_back(child_of("into", body));
    } else if (spec.kind == "switch") {
        if (!body.is_sequence() || body.seq.empty())
            bad(body, "the `switch` scanner takes a non-empty list of candidates");
        for (const auto& c : body.seq) {
            only_fields(c, "a `switch` scanner candidate", {"re_match_name", "scanner"});
            const ynode* sc = c.find("scanner");
            if (!sc) bad(c, "a `switch` scanner candidate needs a `scanner`");
            spec.children.push_back(parse_scanner(*sc));
            spec.child_matches.push_back(scalar_or(c.find("re_match_name"), ""));
        }
    } else {
        // Everything left takes no options, so anything written under one would
        // be silently dropped.
        only_fields(body, "the `" + spec.kind + "` scanner", {});
    }
    return spec;
}

// Every path lift_scanners took out of a body must be one the component
// actually reads. `lift_scanners` removes the key BEFORE spec_of sees it, so
// spec_of can never report it as unrecognised: without this check a `scanner:`
// on a component that has none, or at the wrong path on one that does, was
// accepted and silently replaced by the default `lines`.
void check_scanner_paths(const ynode& body, const std::string& what,
                         const std::map<std::string, scanner_spec>& lifted,
                         const std::vector<std::string>& accepted) {
    for (const auto& [path, unused] : lifted) {
        (void)unused;
        if (std::find(accepted.begin(), accepted.end(), path) != accepted.end()) continue;
        if (accepted.empty())
            bad(body, what + " has no `scanner` field");
        std::string names;
        for (const auto& a : accepted) names += (names.empty() ? "" : ", ") + a;
        bad(body, what + " has no `scanner` at '" + path + "'; it reads one at " + names);
    }
}

void lift_scanners(ynode& body, const std::string& prefix,
                   std::map<std::string, scanner_spec>& out) {
    for (size_t i = 0; i < body.map.size();) {
        auto& e = body.map[i];
        const std::string path = prefix.empty() ? e.key : prefix + "." + e.key;
        if (e.key == "scanner") {
            out[path] = parse_scanner(e.value);
            body.map.erase(body.map.begin() + static_cast<long>(i));
            continue;
        }
        if (!e.value.map.empty()) lift_scanners(e.value, path, out);
        ++i;
    }
}

scanner_spec scanner_from_codec(const ynode& at, const std::string& codec) {
    const auto arg_after = [&](const std::string& prefix) {
        return codec.substr(prefix.size());
    };
    scanner_spec spec;
    // The chaining codecs wrap the rest of the string, which is what
    // `skip_bom.into` and `decompress.into` are.
    for (const auto& [prefix, kind, alg] : std::initializer_list<
             std::tuple<std::string, std::string, std::string>>{
                 {"skipbom/", "skip_bom",   ""},
                 {"gzip/",    "decompress", "gzip"},
                 {"pgzip/",   "decompress", "pgzip"}}) {
        if (codec.rfind(prefix, 0) == 0) {
            spec.kind = kind;
            spec.algorithm = alg;
            spec.children.push_back(scanner_from_codec(at, arg_after(prefix)));
            return spec;
        }
    }
    if (codec == "lines")     { spec.kind = "lines";      return spec; }
    if (codec == "all-bytes") { spec.kind = "to_the_end"; return spec; }
    if (codec == "tar")       { spec.kind = "tar";        return spec; }
    if (codec == "csv")       { spec.kind = "csv";        return spec; }
    if (codec.rfind("csv:", 0) == 0) {
        spec.kind = "csv";
        spec.custom_delimiter = arg_after("csv:");
        if (spec.custom_delimiter.size() != 1)
            bad(at, "the `csv:x` codec's delimiter must be a single character");
        return spec;
    }
    if (codec.rfind("delim:", 0) == 0) {
        spec.kind = "lines";
        spec.custom_delimiter = arg_after("delim:");
        if (spec.custom_delimiter.empty())
            bad(at, "the `delim:x` codec needs a delimiter");
        return spec;
    }
    if (codec.rfind("chunker:", 0) == 0) {
        spec.kind = "chunker";
        // The WHOLE argument has to be the number. std::stoll stops at the first
        // character it cannot use and reports success for what it read, so
        // `chunker:64k` and `chunker:12abc` were accepted as 64 and 12 -- a
        // silent reinterpretation of a size the author wrote deliberately.
        const std::string arg = arg_after("chunker:");
        size_t used = 0;
        try { spec.size = std::stoll(arg, &used); }
        catch (...) { bad(at, "the `chunker:x` codec needs a byte count, got '" + arg + "'"); }
        if (used != arg.size())
            bad(at, "the `chunker:x` codec needs a byte count, got '" + arg + "'");
        if (spec.size <= 0) bad(at, "the `chunker:x` codec needs a byte count above zero");
        return spec;
    }
    if (codec.rfind("regex:", 0) == 0) {
        spec.kind = "re_match";
        spec.pattern = arg_after("regex:");
        if (spec.pattern.empty()) bad(at, "the `regex:x` codec needs a pattern");
        return spec;
    }
    // The reference's remaining codecs have no scanner here. Named, so a config
    // using one is told what is missing rather than silently framed as lines.
    bad(at, "the `codec: " + codec + "` form is not implemented by swordfish; it "
            "implements lines, delim:x, all-bytes, tar, csv, csv:x, chunker:x, "
            "regex:x, and the skipbom/, gzip/ and pgzip/ prefixes");
}

// One input, recursively: `broker` and `sequence` hold others. Extracted from
// parse_pipeline so a nested input gets exactly the same treatment as a
// top-level one -- the built-ins' rules included.
//
// Internal, like parse_output below: they are used only by parse_pipeline, and
// as external symbols with no declaration in any header they would collide with
// any other `sf::parse_input` the project grew.

input_spec parse_input(const ynode& node) {
    if (const auto expanded = maybe_expand(node, tmpl::comp_type::input)) {
        // The guard is held ACROSS the recursive parse: that is where the next
        // expansion happens, so a counter that lived only inside expand() never
        // rose above one and a self-referential template blew the stack.
        const tmpl::expansion_guard depth(node.pos);
        return parse_input(*expanded);
    }

    input_spec spec;

    // `label` and `processors` sit BESIDE the kind, so they come off before the
    // "exactly one key" rule applies -- the same shape parse_output has had all
    // along. Applying single_key() to the whole node meant an input that was
    // merely LABELLED, which is how most real Benthos configs are written, was
    // refused with a message about the object's shape rather than about the
    // label; `processors:` on an input, an ordinary reference feature, went the
    // same way.
    const ynode* kind_node = nullptr;
    for (const auto& e : node.map) {
        // Accepted and not otherwise used, exactly as an output's is: a label
        // names the component for the reference's metrics paths, and swordfish's
        // metrics are not per-component.
        if (e.key == "label") continue;
        if (e.key == "processors") {
            if (!e.value.is_sequence()) bad(e.value, "input `processors` must be a list");
            spec.processors = parse_processors(e.value);
            continue;
        }
        if (kind_node)
            bad(node, "an input must name exactly one kind, got '" + spec.kind +
                      "' and '" + e.key + "'");
        spec.kind = e.key;
        kind_node = &e.value;
    }
    if (!kind_node) bad(node, "`input` must be a single-key object");
    const ynode& ib = *kind_node;

    // `auto_replay_nacks` belongs to EVERY input, as it does in the reference --
    // 51 of its 83 inputs carry the field, `kafka` among them, defaulting to
    // true. The comment that used to sit here said the opposite ("`kafka`
    // redelivers from its committed offset instead, and carries no such
    // field"), and it was wrong on both halves: the field is documented on the
    // reference's `kafka` input, and a Kafka consumer does NOT redeliver a
    // nacked record within a run -- poll() has already advanced past it.
    spec.auto_replay_nacks =
        bool_or(ib.find("auto_replay_nacks"), true,
                "input." + spec.kind + " `auto_replay_nacks`");

    if (spec.kind == "generate") {
        // The built-in inputs are hand-parsed rather than declared through
        // spec_of, so nothing validated their keys: a misspelt `mapping` left
        // the default `root = {}` running and the user's mapping never ran at
        // all. The reference names every one of these.
        unimplemented_fields(ib, "input.generate", {"batch_size"});
        only_builtin_fields(ib, "input.generate", builtin_class::input, "generate");
        // A non-scalar `mapping` used to fall back to the DEFAULT through
        // scalar_or, so `mapping: [a, b]` ran `root = {}` and the config's own
        // mapping never ran -- the silent approximation the key validation above
        // exists to stop, one field further in.
        if (const ynode* mn = ib.find("mapping")) {
            if (!mn->is_scalar()) bad(*mn, "input.generate `mapping` must be a string");
            spec.mapping = mn->scalar;
        } else {
            spec.mapping = "root = {}";
        }
        (void)blobl::parse_mapping(spec.mapping);
        spec.count = uint_or(ib.find("count"), 0, "input.generate `count`");
        // ONE SECOND, which is the reference's default ("Default(\"1s\")" in
        // input_generate.go). Defaulting to 0s here made a `generate` with no
        // interval free-run: three messages took 274ms against the reference's
        // 2450ms, and a config written for the reference behaved differently
        // rather than failing. An explicit `0s` -- and the empty string, which
        // the reference documents as "as fast as downstream services can
        // process them" -- still free-runs.
        const ynode* iv = ib.find("interval");
        if (iv && !iv->is_scalar()) bad(*iv, "input.generate `interval` must be a duration");
        const std::string interval = iv ? iv->scalar : std::string("1s");
        if (interval.empty()) {
            spec.interval = std::chrono::milliseconds::zero();
        } else if (auto d = parse_duration(interval)) {
            spec.interval = std::chrono::duration_cast<std::chrono::milliseconds>(d->ns);
        } else if (interval[0] == '@' || interval.rfind("TZ=", 0) == 0 ||
                   interval.find(' ') != std::string::npos) {
            // The reference takes "either a duration string or a cron
            // expression". Swordfish has no cron, and the rule is that an
            // unimplemented feature is refused BY NAME rather than approximated
            // -- so this says which of the two it could not do, instead of
            // calling a valid cron expression a malformed duration.
            bad(iv ? *iv : ib,
                "input.generate `interval` looks like a cron expression ('" +
                interval + "'); swordfish implements only the duration form");
        } else {
            // Silently ignoring an unparseable interval left the input
            // free-running, which is the one outcome the author cannot have
            // meant by writing one.
            bad(iv ? *iv : ib, "input.generate `interval` is not a duration: '" +
                               interval + "'");
        }
    } else if (spec.kind == "file" || spec.kind == "stdin") {
        if (spec.kind == "file") {
            unimplemented_fields(ib, "input.file", {"delete_on_finish"});
            // `path`, the swordfish-only singular alias, is GONE: the reference
            // lints it as unrecognised, so a config written here failed there --
            // the same trap as `poll_period` and `reconnect_period`.
            only_fields(ib, "input.file",
                        {"paths", "scanner", "codec", "auto_replay_nacks"});
        } else {
            only_builtin_fields(ib, "input.stdin", builtin_class::input, "stdin");
        }
        // `codec` is what `scanner` replaced. Still accepted by the reference --
        // its own test corpus uses `codec: lines` -- so refusing it rejected real
        // Benthos configs. Both at once is refused, because the two would have to
        // agree and nothing says which wins.
        if (const ynode* cd = ib.find("codec")) {
            if (ib.find("scanner"))
                bad(*cd, "input." + spec.kind + " sets both `scanner` and the "
                         "deprecated `codec`; use `scanner` alone");
            spec.scanner = scanner_from_codec(*cd, cd->scalar);
        }
        // Globs are expanded when the input opens, not here, so a pattern
        // matching nothing is a runtime condition rather than a config error.
        if (const ynode* ps = ib.find("paths")) {
            if (!ps->is_sequence()) bad(*ps, "input.file `paths` must be a list");
            for (const auto& e : ps->seq) spec.paths.push_back(e.scalar);
        }
        if (spec.kind == "file" && spec.paths.empty())
            bad(ib, "input.file requires `paths`");
        // The reference's `scanner` field is a COMPONENT, so it is a single-key
        // object -- `scanner: { csv: {} }`. Validated here rather than left to
        // the runtime so the error carries a line number.
        //
        // Both shapes used to be accepted and then ignored: a scalar
        // (`scanner: to_the_end`) never matched single_key() and silently left
        // the default, and an object naming any scanner but `to_the_end` fell
        // through to `lines` in both backends. Asking for `csv` and getting raw
        // lines changes the SHAPE of the data, which is the worst kind of
        // silent approximation this project can ship.
        if (const ynode* s = ib.find("scanner")) spec.scanner = parse_scanner(*s);
    } else if (spec.kind == "broker" || spec.kind == "sequence") {
        // The only two built-in inputs that validated nothing. `auto_replay_nacks`
        // written here looked accepted, did nothing, and -- until the emitter was
        // fixed -- made the compiled binary behave differently from the
        // interpreted one. The reference rejects all three of these by name.
        if (spec.kind == "broker")
            only_builtin_fields(ib, "input.broker", builtin_class::input, "broker");
        else
            only_builtin_fields(ib, "input.sequence", builtin_class::input, "sequence");
        const ynode* list = ib.find("inputs");
        if (!list || !list->is_sequence())
            bad(ib, "input." + spec.kind + " requires a list of `inputs`");
        for (const auto& child : list->seq) spec.children.push_back(parse_input(child));
        if (spec.children.empty()) bad(ib, "input." + spec.kind + " needs at least one input");
        if (spec.kind == "broker") {
            spec.copies = uint_or(ib.find("copies"), 1, "input.broker `copies`");
            // The same policy parser the outputs use, so the four triggers and
            // `processors` cannot mean different things on the two sides.
            if (const ynode* b = ib.find("batching"); b && !b->map.empty()) {
                output_spec carrier;
                parse_batching(*b, carrier);
                spec.batching            = carrier.batching;
                spec.batching_processors = std::move(carrier.batching_processors);
                // The condition was INVERTED. It refused exactly the case where
                // no batcher is built at all -- `batching: { count: 0 }`, which
                // the reference runs happily as "no batching" -- and passed the
                // case that does build one with nothing to fire it, where every
                // message is held until the source ends. The reference refuses
                // that shape on the output side by name ("batch policy must have
                // at least one active trigger"); on the input side it accepts it
                // and then emits nothing at all, which is worse, so swordfish
                // says so here instead.
                if (spec.batching.is_noop() && !spec.batching_processors.empty())
                    bad(*b, "input.broker `batching` sets no trigger; give it a "
                            "`count`, `byte_size`, `period` or `check`");
            }
        } else {
            if (const ynode* j = ib.find("sharded_join"))
                if (const ynode* t = j->find("type"); t && t->scalar != "none")
                    bad(*j, "input.sequence `sharded_join` is not implemented by swordfish");
        }
    } else if (const input_def* def = find_input(spec.kind)) {
        cfg::lints ls;
        component_config cc;
        cc.kind = spec.kind;
        cc.where = ib.pos;
        // Recorded before the body is decoded, as `dedupe`'s cache is: the label
        // is resolved against the whole document once parsing has finished.
        if (const ynode* r = ib.find("rate_limit")) cc.rate_limit_name = r->scalar;
        // Scanners come OUT of the body first, for the reason lift_scanners
        // gives; what spec_of then decodes has no `scanner` key left in it.
        ynode ib_rest = ib;
        // Lifted out before spec_of decodes the body, exactly as scanners are:
        // it is an ENGINE field that every input carries, not one a connector
        // declares, so no component's spec knows about it and `only_fields`
        // would refuse it.
        for (size_t i = 0; i < ib_rest.map.size(); ++i)
            if (ib_rest.map[i].key == "auto_replay_nacks") {
                ib_rest.map.erase(ib_rest.map.begin() + static_cast<long>(i));
                break;
            }
        lift_scanners(ib_rest, "", cc.scanners);
        check_scanner_paths(ib, "input." + spec.kind, cc.scanners, def->scanner_paths);
        cc.cfg = def->parse(ib_rest, "input." + spec.kind, ls);
        validate_bloblang_fields(def->bloblangs, cc.cfg, ib);
        for (const auto& l : ls)
            if (l.level == lint_level::error)
                // The PATH matters: "required field is missing" on its own does
                // not say which field, and this is the one of the three parse
                // paths that used to drop it.
                throw spec_error(l.where.line ? l.where : ib.pos,
                                 l.path + ": " + l.message);
        spec.comp = std::move(cc);
    } else {
        unsupported(node, "input", spec.kind);
    }
    return spec;
}

// The `check` on a switch case and the message on a `reject` are evaluated per
// message, so a syntax error in either belongs at config time with a line
// number rather than on the first batch that reaches the output.
void validate_query(const std::string& src, const ynode& at) {
    try { (void)blobl::parse_query(src); }
    catch (const blobl::parse_error& e) {
        throw spec_error({at.pos.line + e.where.line - 1, e.where.col}, e.what());
    }
}

// `batching:`, wherever it appears. Every output carries it INSIDE its body, as
// the reference does, and so does `input.broker`; one function for all of them,
// so the placements cannot drift apart.
void parse_batching(const ynode& node, output_spec& spec) {
    // The policy's own `processors` are lifted out first -- a nested processor
    // list has no field type -- and the rest goes through spec_of, so a typo in
    // `count` or `period` is still a named error.
    ynode rest = node;
    for (size_t i = 0; i < rest.map.size(); ++i)
        if (rest.map[i].key == "processors") {
            if (!rest.map[i].value.is_sequence())
                bad(rest.map[i].value, "`batching.processors` must be a list");
            spec.batching_processors = parse_processors(rest.map[i].value);
            rest.map.erase(rest.map.begin() + static_cast<long>(i));
            break;
        }
    cfg::lints ls;
    cfg::parse_into<batch_policy_config>(rest, spec.batching, "batching", ls);
    for (const auto& l : ls)
        if (l.level == lint_level::error)
            throw spec_error(l.where.line ? l.where : node.pos, l.path + ": " + l.message);
    if (!spec.batching.check.source.empty()) validate_query(spec.batching.check.source, node);
}

// One output, recursively: `broker`, `switch` and `fallback` hold others.
// Mirrors parse_input, and for the same reason -- a `file` nested three deep
// gets exactly the same treatment as a top-level one, rather than each nesting
// level restating the rules.
output_spec parse_output(const ynode& node) {
    if (const auto expanded = maybe_expand(node, tmpl::comp_type::output)) {
        // The guard is held ACROSS the recursive parse: that is where the next
        // expansion happens, so a counter that lived only inside expand() never
        // rose above one and a self-referential template blew the stack.
        const tmpl::expansion_guard depth(node.pos);
        return parse_output(*expanded);
    }

    output_spec spec;
    if (!node.is_mapping()) bad(node, "`output` must be a single-key object");

    // `label` and `processors` sit BESIDE the kind, so they come off before the
    // "exactly one key" rule applies -- the shape a processor's `label` has.
    // `processors` on an output applies only to what that output receives, which
    // is what lets a broker child reshape its own copy.
    const ynode* kind_node = nullptr;
    // A `batching` in the SIBLING position, which the reference refuses. Held so
    // the error can say where it belongs rather than reading as an unknown key.
    const ynode* beside_batching = nullptr;
    for (const auto& e : node.map) {
        if (e.key == "label") continue;
        if (e.key == "processors") {
            if (!e.value.is_sequence()) bad(e.value, "output `processors` must be a list");
            spec.processors = parse_processors(e.value);
            continue;
        }
        if (e.key == "batching") { beside_batching = &e.value; continue; }
        if (kind_node)
            bad(node, "an output must name exactly one kind, got '" + spec.kind +
                      "' and '" + e.key + "'");
        spec.kind = e.key;
        kind_node = &e.value;
    }
    if (!kind_node) bad(node, "`output` must be a single-key object");

    // `batching` sits INSIDE the component's body -- `output: { kafka: { topic:
    // t, batching: {...} } }` -- as it does in the reference. It used to be read
    // from BESIDE the kind, which was the opposite of the reference on both
    // counts: the reference accepts only the body and refuses the sibling
    // position outright. So every real Redpanda Connect config with output
    // batching failed to lint here, and every config that linted here was
    // refused there. `broker` alone had it right, and additionally accepted the
    // sibling form the reference rejects.
    //
    // Lifted out of the body before spec_of decodes it, exactly as `scanner` and
    // the nested processor lists are, because a policy is not a field spec_of
    // can express.
    if (beside_batching)
        bad(*beside_batching,
            "`batching` belongs inside the '" + spec.kind + "' body, not beside "
            "it; swordfish refuses it here");
    ynode ob_no_batching = *kind_node;
    ynode batching_body;
    bool has_batching = false;
    if (ob_no_batching.is_mapping()) {
        for (size_t i = 0; i < ob_no_batching.map.size(); ++i) {
            if (ob_no_batching.map[i].key != "batching") continue;
            batching_body = ob_no_batching.map[i].value;
            has_batching = true;
            ob_no_batching.map.erase(ob_no_batching.map.begin() + static_cast<long>(i));
            break;
        }
    }
    const ynode& ob = has_batching ? ob_no_batching : *kind_node;

    if (spec.kind == "file") {
        // `codec` chooses the framing the reference writes; swordfish writes
        // newline-delimited messages and nothing else, so anything but the
        // default `lines` is refused rather than written in a shape the config
        // did not ask for.
        check_line_codec(ob, "output.file");
        only_builtin_fields(ob, "output.file", builtin_class::output, "file");
        spec.path = scalar_or(ob.find("path"), "");
        if (spec.path.empty()) bad(ob, "output.file requires `path`");
        // The path INTERPOLATES, as the reference's does. Validated here, with a
        // line number, rather than failing at the first message -- the same
        // treatment `output.reject`'s message gets.
        validate_query(cfg::interpolation_to_query(spec.path), ob);
    } else if (spec.kind == "stdout") {
        check_line_codec(ob, "output.stdout");
        only_builtin_fields(ob, "output.stdout", builtin_class::output, "stdout");
    } else if (spec.kind == "drop") {
        only_fields(ob, "output.drop", {});
    } else if (spec.kind == "reject") {
        // A bare string, as in the reference; there is no object form to accept.
        // An empty one is refused there too: the text is the whole point of the
        // component, and a rejection with no reason tells an operator nothing.
        if (!ob.is_scalar() || ob.scalar.empty())
            bad(ob, "output.reject requires an error message, which is what gives "
                    "the rejection its context");
        spec.message = ob.scalar;
        validate_query(cfg::interpolation_to_query(spec.message), ob);
    } else if (spec.kind == "broker") {
        only_builtin_fields(ob, "output.broker", builtin_class::output, "broker");
        const ynode* list = ob.find("outputs");
        if (!list || !list->is_sequence())
            bad(ob, "output.broker requires a list of `outputs`");
        for (const auto& child : list->seq) spec.children.push_back(parse_output(child));
        if (spec.children.empty()) bad(ob, "output.broker needs at least one output");
        // Read through the node rather than scalar_or(): a non-scalar
        // `pattern` would otherwise fall back to the default and run a fan_out
        // for a config that asked for something else.
        if (const ynode* pn = ob.find("pattern")) {
            static constexpr std::string_view patterns[] = {
                "fan_out", "fan_out_fail_fast", "fan_out_sequential",
                "fan_out_sequential_fail_fast", "round_robin", "greedy"};
            if (!pn->is_scalar() ||
                std::find(std::begin(patterns), std::end(patterns), pn->scalar)
                    == std::end(patterns)) {
                std::string names;
                for (auto p : patterns) names += (names.empty() ? "" : ", ") + std::string(p);
                bad(*pn, "output.broker `pattern` must be one of " + names);
            }
            spec.pattern = pn->scalar;
        }
        spec.copies = uint_or(ob.find("copies"), 1, "output.broker `copies`");
    } else if (spec.kind == "switch") {
        only_builtin_fields(ob, "output.switch", builtin_class::output, "switch");
        const ynode* cs = ob.find("cases");
        if (!cs || !cs->is_sequence())
            bad(ob, "output.switch requires a list of `cases`");
        for (const auto& c : cs->seq) {
            only_fields(c, "a switch output case", {"check", "output", "continue"});
            const ynode* o = c.find("output");
            if (!o) bad(c, "a switch output case needs an `output`");
            output_spec child = parse_output(*o);
            // An absent or empty `check` always passes, which is how the
            // reference writes a catch-all final case.
            child.check = bloblang{scalar_or(c.find("check"), ""), true};
            if (!child.check.source.empty())
                validate_query(child.check.source, *c.find("check"));
            child.continue_ = bool_or(c.find("continue"), false,
                                      "a switch output case's `continue`");
            spec.children.push_back(std::move(child));
        }
        // The reference refuses fewer than two: a one-case switch is either the
        // output itself or a `drop_on`, and writing it as a switch hides which
        // was meant.
        if (spec.children.size() < 2)
            bad(ob, "output.switch needs at least two cases");
        spec.strict_mode = bool_or(ob.find("strict_mode"), false,
                                   "output.switch `strict_mode`");
        spec.retry_until_success = bool_or(ob.find("retry_until_success"), false,
                                           "output.switch `retry_until_success`");
        // The reference lints exactly this: a `reject` case under
        // retry_until_success retries a deliberate failure for ever.
        if (spec.retry_until_success)
            for (const auto& child : spec.children)
                if (child.kind == "reject")
                    bad(ob, "a `switch` output with a `reject` case must set "
                            "`retry_until_success: false`, or the rejection retries "
                            "indefinitely");
    } else if (spec.kind == "fallback") {
        // A bare LIST of outputs, as in the reference -- there is no `outputs`
        // key under it.
        if (!ob.is_sequence()) bad(ob, "output.fallback must be a list of outputs");
        for (const auto& child : ob.seq) spec.children.push_back(parse_output(child));
        if (spec.children.empty()) bad(ob, "output.fallback needs at least one output");
    } else if (const output_def* def = find_output(spec.kind)) {
        cfg::lints ls;
        component_config cc;
        cc.kind = spec.kind;
        cc.where = ob.pos;
        if (const ynode* r = ob.find("rate_limit")) cc.rate_limit_name = r->scalar;
        ynode ob_rest = ob;
        lift_scanners(ob_rest, "", cc.scanners);
        check_scanner_paths(ob, "output." + spec.kind, cc.scanners, def->scanner_paths);
        cc.cfg = def->parse(ob_rest, "output." + spec.kind, ls);
        validate_bloblang_fields(def->bloblangs, cc.cfg, ob);
        for (const auto& l : ls)
            if (l.level == cfg::lint_level::error)
                throw spec_error(l.where.line ? l.where : ob.pos, l.path + ": " + l.message);
        spec.comp = std::move(cc);
    } else {
        // Plain `else`: `stdout` and `drop` were both handled by their own
        // branches above, so the guard that used to be here could never be
        // false. Keeping it suggested there was a fourth case it protected.
        unsupported(ob, "output", spec.kind);
    }

    if (has_batching) {
        // Which outputs accept a policy is the reference's list, not ours: it
        // refuses `batching` on `stdout` by name, and a config that lints here
        // and not there is the compatibility direction that matters least but
        // costs nothing to preserve. `broker` is the one built-in that takes
        // one; the rest declare it on their registry entry.
        const output_def* def = find_output(spec.kind);
        const bool allowed = spec.kind == "broker" || (def && def->batching);
        if (!allowed)
            bad(batching_body, "`batching` is invalid when the output type is '" +
                               spec.kind + "'");
        parse_batching(batching_body, spec);
        // The reference's own rule, which swordfish did not have: a policy that
        // BUILDS a batcher -- one with processors, or with any trigger set --
        // must have at least one active trigger, or every message is held for
        // ever. `redpanda-connect` refuses it at init with "batch policy must
        // have at least one active trigger"; swordfish hung instead, silently.
        // A wholly empty `batching: { count: 0 }` builds no batcher and stays
        // acceptable, exactly as it is there.
        if (spec.batching.is_noop() && !spec.batching_processors.empty())
            bad(batching_body, "`batching` has `processors` but no trigger; give it "
                               "a `count`, `byte_size`, `period` or `check`");
    }
    return spec;
}

// The document-level blocks that are not a component: `shutdown_timeout`,
// `shutdown_delay`, `error_handling`, and the three observability blocks.
//
// Every one of these used to be listed in the root schema and read by nobody.
// A config asking for `error_handling.strict: true` ran non-strict, and one
// asking for `shutdown_delay: 30s` exited at once while the reference waited --
// silently, in both cases. Three are implemented here and the rest are refused
// by name, which is the project's rule (§ 4.5) and the only honest options.
void parse_engine_config(const ynode& root, stream_config& cfg) {
    const auto read_duration = [&](const ynode* n, const char* what,
                                   std::chrono::milliseconds& into) {
        if (!n || n->scalar.empty()) return;
        duration d;
        lints ls;
        cfg::codec<duration>::parse(*n, d, what, ls);
        if (!ls.empty())
            throw spec_error(n->pos, std::string(what) + " is not a duration");
        into = std::chrono::duration_cast<std::chrono::milliseconds>(d.ns);
    };
    read_duration(root.find("shutdown_timeout"), "shutdown_timeout", cfg.shutdown_timeout);
    read_duration(root.find("shutdown_delay"),   "shutdown_delay",   cfg.shutdown_delay);

    if (const ynode* eh = root.find("error_handling")) {
        only_fields(*eh, "`error_handling`", {"strict"});
        cfg.strict_errors = bool_or(eh->find("strict"), false, "error_handling `strict`");
    }

    // `buffer`, `metrics` and `tracer` name a COMPONENT each, and the single key
    // is checked by name so an unimplemented one says so rather than being
    // dropped. `buffer` takes `none` and `memory`; the other two take only their
    // no-op member, plus `prometheus`, which is what the `http` block already
    // serves.
    const auto single_component = [&](const char* field, const char* what,
                                      std::initializer_list<std::string_view> ok) {
        const ynode* n = root.find(field);
        if (!n) return;
        // A non-mapping body is an ERROR, not something to skip. Returning on
        // `map.empty()` treated `buffer: memory` -- the string form, which is
        // easy to write and which the reference rejects -- as "no buffer at
        // all", so the config ran unbuffered without a word. The same node is
        // decoded again by parse_buffer, which bailed out the same way.
        if (n->map.empty())
            throw spec_error(n->pos, std::string("`") + field +
                             "` must be a single-key object naming a " + what);
        auto sk = n->single_key();
        if (!sk)
            throw spec_error(n->pos, std::string("`") + field +
                             "` must be a single-key object naming a " + what);
        if (std::find(ok.begin(), ok.end(), sk->first) == ok.end()) {
            std::string names;
            for (auto o : ok) names += (names.empty() ? "" : ", ") + std::string(o);
            throw spec_error(sk->second->pos,
                             std::string(what) + " '" + sk->first +
                             "' is not implemented by swordfish; it implements " + names);
        }
    };
    // `prometheus` is not an approximation: the `http` block already serves
    // /metrics in Prometheus text format, which is the same thing this asks for.
    single_component("metrics", "metrics backend", {"none", "prometheus"});
    single_component("tracer",  "tracer",          {"none"});
    // The KIND is not the whole story: `prometheus` has options in the reference
    // and swordfish honours none of them, because it serves /metrics from the
    // `http` block rather than running its own exporter. Accepted-and-ignored is
    // exactly what the rule forbids -- `push_url` asks for PUSH delivery to a
    // gateway, and a config that asked for it got a pull endpoint and no warning
    // -- so each is refused by its own name, and anything else is a typo.
    if (const ynode* m = root.find("metrics")) {
        if (const ynode* p = m->find("prometheus")) {
            for (const auto& e : p->map) {
                static constexpr std::string_view reference_fields[] = {
                    "push_url", "push_interval", "push_job_name",
                    "use_histogram_timing", "histogram_buckets",
                    "add_process_metrics", "add_go_metrics", "file_output_path"};
                if (std::find(std::begin(reference_fields), std::end(reference_fields),
                              e.key) != std::end(reference_fields))
                    bad(e.value, "the `metrics.prometheus` option '" + e.key +
                                 "' is not implemented by swordfish, which serves "
                                 "/metrics from the `http` block");
            }
            only_fields(*p, "`metrics.prometheus`", {});
        }
    }
    // `buffer` is parsed in full by parse_buffer(); only the KIND is checked
    // here, so an unimplemented one is named before its body is read.
    single_component("buffer",  "buffer",          {"none", "memory"});


    // `logger` is a settings object rather than a component. Swordfish logs
    // through Seastar, whose level is set by --default-log-level, so the block
    // is refused rather than half-honoured: accepting `level: DEBUG` and then
    // logging at INFO is the failure this whole pass is closing.
    if (const ynode* lg = root.find("logger"); lg && !lg->map.empty())
        throw spec_error(lg->pos,
            "the `logger` block is not implemented by swordfish; logging is "
            "configured with seastar's own --default-log-level and --logger-log-level");
}

// The `buffer` block. Only `memory` and `none` are implemented; the kind itself
// was already checked by parse_engine_config, so what is left is the body.
//
// INSIDE the anonymous namespace, with parse_input and parse_output. It had
// external linkage and no header declaration -- exactly the hazard this file
// warns about for the parse helpers above -- so a second definition of the name
// anywhere else in the program would have been a silent ODR violation rather
// than a link error. Its only caller is parse_pipeline, in this file.
buffer_spec parse_buffer(const ynode& root) {
    buffer_spec spec;
    const ynode* b = root.find("buffer");
    if (!b || b->map.empty()) return spec;
    auto sk = b->single_key();
    if (!sk) return spec;
    spec.kind = sk->first;
    if (spec.kind != "memory") return spec;

    const ynode& body = *sk->second;
    only_fields(body, "the `memory` buffer", {"limit", "batch_policy"});
    spec.limit = static_cast<int64_t>(
        uint_or(body.find("limit"), 524288000, "buffer.memory `limit`"));
    if (spec.limit <= 0)
        bad(body, "buffer.memory `limit` must be greater than zero");

    if (const ynode* bp = body.find("batch_policy")) {
        spec.batch_enabled = bool_or(bp->find("enabled"), false,
                                     "buffer.memory `batch_policy.enabled`");
        // `enabled` is lifted out before the rest goes through the same policy
        // parser the outputs use, so the four triggers and `processors` cannot
        // drift between the two places a policy can appear.
        ynode rest = *bp;
        for (size_t i = 0; i < rest.map.size();)
            if (rest.map[i].key == "enabled")
                rest.map.erase(rest.map.begin() + static_cast<long>(i));
            else
                ++i;
        output_spec carrier;
        parse_batching(rest, carrier);
        spec.batching            = carrier.batching;
        spec.batching_processors = std::move(carrier.batching_processors);
        // A policy switched on with nothing that can trigger it would hold every
        // message until the source ended -- reported rather than discovered.
        if (spec.batch_enabled && spec.batching.is_noop())
            bad(*bp, "buffer.memory `batch_policy` is enabled but sets no trigger; "
                     "give it a `count`, `byte_size`, `period` or `check`");
    }
    return spec;
}

// Every resource block, validated whether or not anything references it.
//
// Resolution only ever parsed the entries something NAMED, so an unreferenced
// `cache_resources` entry with a misspelt field, a `local` rate limit with a
// zero count, or a `processor_resources` entry whose Bloblang does not parse all
// lint-ed clean here and were refused by the reference -- which validates the
// whole document. A resource block is usually shared config, so a broken one
// that nothing happens to use today is exactly the one a later edit reaches.
void check_unreferenced_resources(const ynode& root) {
    if (const ynode* cs = root.find("cache_resources"); cs && cs->is_sequence())
        for (const auto& e : cs->seq) {
            const ynode* l = e.find("label");
            (void)parse_cache(e, l ? l->scalar : std::string());
        }
    if (const ynode* rs = root.find("rate_limit_resources"); rs && rs->is_sequence())
        for (const auto& e : rs->seq) {
            // The same checks resolve_rate_limit runs, minus the lookup: this is
            // the entry, so there is nothing to find.
            only_fields(e, "a rate_limit_resources entry", {"label", "local", "redis"});
            const ynode* local = e.find("local");
            if (!local)
                throw spec_error(e.pos, "a rate_limit_resources entry is not a `local` "
                                        "rate limit; swordfish implements no other kind yet");
            only_fields(*local, "a `local` rate limit", {"count", "interval"});
            (void)uint_or(local->find("count"), 1000, "rate_limit `count`");
        }
    if (const ynode* ps = root.find("processor_resources"); ps && ps->is_sequence())
        for (const auto& e : ps->seq) (void)parse_processor(e);
    if (const ynode* is = root.find("input_resources"); is && is->is_sequence())
        for (const auto& e : is->seq) (void)parse_input(e);
    if (const ynode* os = root.find("output_resources"); os && os->is_sequence())
        for (const auto& e : os->seq) (void)parse_output(e);
}

} // namespace

cfg::ynode with_trivial_io(const cfg::ynode& root, std::string_view keep) {
    using namespace sf::cfg;
    ynode out;
    out.type = ynode_type::mapping;
    out.pos = root.pos;
    for (const auto& e : root.map) {
        if (e.key == "input" || e.key == "pipeline" || e.key == "output") {
            if (e.key != keep) continue;          // replaced below
        }
        out.map.push_back(e);
    }
    // A source that produces exactly one empty document and an output that
    // discards it: enough to satisfy parse_pipeline, cheap enough that building
    // it costs nothing, and incapable of moving data if one were ever run.
    if (keep != "input")
        out.map.push_back({"input",
                           parse_yaml("generate: { count: 1, interval: \"\", mapping: 'root = {}' }")});
    if (keep != "output") out.map.push_back({"output", parse_yaml("drop: {}")});
    return out;
}

pipeline_spec parse_pipeline(const ynode& root) {
    pipeline_spec spec;

    // The root schema, checked HERE rather than only in `sfconfig lint`. It used
    // to live in config_main.cc alone, so `swordfish run` and `swordfish build`
    // accepted an unrecognised top-level key in silence: a config that misspelt
    // `pipeline` as `pipline` ran with no processors at all, and `build`
    // compiled that empty pipeline into a binary somebody then shipped. lint and
    // the reference both refused the same file. One list, in the path all four
    // commands share.
    //
    // From benthos-main/internal/config/schema.go.
    static constexpr std::string_view root_fields[] = {
        "http", "input", "buffer", "pipeline", "output",
        "input_resources", "cache_resources", "rate_limit_resources",
        "processor_resources", "output_resources",
        "logger", "metrics", "tracer", "shutdown_delay", "shutdown_timeout",
        "error_handling", "tests"};
    for (const auto& e : root.map)
        if (std::find(std::begin(root_fields), std::end(root_fields), e.key)
                == std::end(root_fields))
            throw spec_error(e.value.pos, "field '" + e.key + "' is not recognised");

    const ynode* in = root.find("input");
    if (!in) throw spec_error(root.pos, "config has no `input`");
    spec.input = parse_input(*in);

    // The `http` block. Absent means disabled, which is NOT the reference's
    // default -- it enables the server on 4195 unless told otherwise. Swordfish
    // compiles to a binary a user redistributes, so opening a port has to be
    // something they asked for rather than something they inherit.
    if (const ynode* h = root.find("http")) {
        // `enabled` defaults to true for a block that is PRESENT, which is what
        // the bool_or below defaults to -- there is no separate assignment here,
        // because one that is always overwritten reads as a second rule.
        // Nothing validated this block, so `cert_file`, `key_file`, `cors` and
        // `basic_auth` were all accepted and read by nobody -- a config asking
        // for TLS and authentication on the observability port got a plaintext,
        // unauthenticated one and no warning. That is the same rule the
        // connectors are already held to ("a config asking for TLS fails to lint
        // rather than connecting in the clear"), applied here at last.
        //
        // The DISABLED forms are accepted, because they are what swordfish
        // does: `enabled: false` on cors or basic_auth, and an empty cert path,
        // ask for nothing that is missing.
        const auto refuse_unless_off = [&](const char* field, const char* what) {
            const ynode* n = h->find(field);
            if (!n) return;
            if (n->is_scalar()) {
                if (n->scalar.empty() || is_false_scalar(n->scalar)) return;
            } else if (const ynode* en = n->find("enabled");
                       !en || is_false_scalar(en->scalar)) {
                return;   // present but switched off, which is what we do
            }
            bad(*n, std::string("the `http` block's ") + what +
                    " is not implemented by swordfish");
        };
        refuse_unless_off("cert_file",       "`cert_file` (TLS on the observability server)");
        refuse_unless_off("key_file",        "`key_file` (TLS on the observability server)");
        refuse_unless_off("cors",            "`cors`");
        refuse_unless_off("basic_auth",      "`basic_auth`");
        refuse_unless_off("debug_endpoints", "`debug_endpoints`");
        only_fields(*h, "the `http` block",
                    {"enabled", "address", "root_path", "debug_endpoints",
                     "cert_file", "key_file", "cors", "basic_auth"});
        // Through bool_or, not `== "true"`. The literal comparison made every
        // other spelling read as FALSE, silently: `enabled: True` lints clean
        // and then answers nothing on /ping or /metrics, where the reference
        // serves both -- so a health check or a Prometheus scrape went dark with
        // no diagnostic anywhere. A value that is not a boolean at all is now a
        // named error rather than an off switch.
        spec.http.enabled = bool_or(h->find("enabled"), true, "the `http` block's `enabled`");
        if (const ynode* a = h->find("address")) {
            spec.http.address = a->scalar;
            // Validated HERE as well as when the server starts, so a bad address
            // is a lint error rather than something a shipped binary dies of.
            try { (void)parse_host_port(spec.http.address); }
            catch (const std::exception& e) { bad(*a, e.what()); }
        }
        if (const ynode* rp = h->find("root_path")) spec.http.root_path = rp->scalar;
    }

    if (const ynode* pl = root.find("pipeline")) {
        // `threads` is the reference's processor-parallelism knob. Swordfish
        // runs a pipeline per SHARD instead, so honouring it would mean either
        // ignoring `--smp` or multiplying by it; refused rather than accepted
        // and dropped, which is what happened before.
        // Named as UNIMPLEMENTED, not as a typo. only_fields alone reported
        // "`pipeline` has no field 'threads'", which reads as a misspelling and
        // contradicts the comment above it -- the field exists in the reference
        // and swordfish deliberately does not honour it.
        unimplemented_fields(*pl, "`pipeline`", {"threads"});
        only_fields(*pl, "`pipeline`", {"processors"});
        if (const ynode* ps = pl->find("processors"))
            spec.processors = parse_processors(*ps);
    }

    parse_engine_config(root, spec.config);
    spec.buffer = parse_buffer(root);
    // Last, so a fault in the pipeline itself is reported before a fault in a
    // block nothing uses.
    check_unreferenced_resources(root);
    if (const ynode* out = root.find("output")) spec.output = parse_output(*out);
    // One `seen` map across the whole document: the once-only rule for a
    // resource reference spans the pipeline and the outputs together. Resolution
    // used to sit under the two ifs above by indentation only, where it ran on
    // an empty list whenever there was no `pipeline` block.
    {
        std::map<std::string, int> seen;
        resolve_input_rate_limits(spec.input, root);
        resolve_input_resources(spec.input, root, seen);
        resolve_resources_impl(spec.processors, root, seen);
        resolve_resources_impl(spec.buffer.batching_processors, root, seen);
        resolve_output_resources_impl(spec.output, root, seen);
    }
    return spec;
}

} // namespace sf
