#include <iterator>
#include <fstream>
#include <filesystem>
#include <algorithm>
#include "swordfish/testing/unit_test.hh"

#include "swordfish/blobl/interp.hh"
#include "swordfish/blobl/parse.hh"
#include "swordfish/value.hh"
#include "swordfish/regex.hh"

#include <cstdlib>
#include <sstream>
#include <stdexcept>

namespace sf::testing {

std::string case_failure::str() const {
    // Benthos formats these as `name [line N]: reason`, and dropping the line
    // when it is unknown. Matching it means a user's editor tooling that greps
    // test output keeps working.
    if (line == 0) return name + ": " + reason;
    return name + " [line " + std::to_string(line) + "]: " + reason;
}

const cfg::ynode* json_pointer(const cfg::ynode& root, std::string_view ptr) {
    if (ptr.empty() || ptr == "/") return &root;
    if (ptr.front() != '/') return nullptr;      // a bare label, not a pointer
    const cfg::ynode* cur = &root;
    size_t i = 1;
    while (i <= ptr.size() && cur) {
        const size_t slash = ptr.find('/', i);
        std::string tok(ptr.substr(i, slash == std::string_view::npos
                                          ? std::string_view::npos : slash - i));
        // RFC 6901 escapes, in this order: ~1 is '/', ~0 is '~'.
        for (size_t p = tok.find("~1"); p != std::string::npos; p = tok.find("~1", p + 1))
            tok.replace(p, 2, "/");
        for (size_t p = tok.find("~0"); p != std::string::npos; p = tok.find("~0", p + 1))
            tok.replace(p, 2, "~");
        if (cur->is_sequence()) {
            if (tok.find_first_not_of("0123456789") != std::string::npos) return nullptr;
            const size_t idx = std::stoul(tok);
            if (idx >= cur->seq.size()) return nullptr;
            cur = &cur->seq[idx];
        } else {
            cur = cur->find(tok);
        }
        if (slash == std::string_view::npos) break;
        i = slash + 1;
    }
    return cur;
}

namespace {

std::string scalar_of(const cfg::ynode* n) { return n ? n->scalar : std::string(); }

// scalar_to_value / node_to_value moved to config/yaml.cc when templates needed
// them too: two answers to "what does this YAML scalar mean" is exactly the pair
// that drifts, and a test comparing against one while the config parser used the
// other would disagree about the number 5.
// Benthos renders these compactly before comparing, so a test written with
// pretty YAML matches a processor that emits compact JSON.
std::string compact_json(const cfg::ynode& n) {
    return node_to_value(n).to_json();
}

// A key nobody consumed used to be DROPPED, and in a test file that is the worst
// possible outcome: a case whose only assertion is one swordfish does not
// implement asserts NOTHING, and the run reports success. `file_equals` did
// exactly that. So every key is accounted for -- an unknown one is a typo, and a
// known-but-unbuilt one is named as unimplemented, the same distinction the
// config parser draws.
void only_keys(const cfg::ynode& n, const char* what,
               std::initializer_list<std::string_view> known,
               std::initializer_list<std::string_view> unbuilt) {
    for (const auto& [k, v] : n.map) {
        if (std::find(known.begin(), known.end(), k) != known.end()) continue;
        if (std::find(unbuilt.begin(), unbuilt.end(), k) != unbuilt.end())
            throw std::runtime_error(std::string(what) + " '" + k +
                                     "' is not implemented by swordfish");
        std::string names;
        for (auto a : known) names += (names.empty() ? "" : ", ") + std::string(a);
        throw std::runtime_error(std::string(what) + " '" + k +
                                 "' is not recognised; expected " + names);
    }
}

input_message parse_input(const cfg::ynode& n) {
    input_message im;
    im.where = n.pos;
    only_keys(n, "test input field",
              {"content", "json_content", "file_content", "metadata"},
              {"batch"});
    if (const auto* c = n.find("content"))      im.content = c->scalar;
    if (const auto* j = n.find("json_content")) im.json_content = compact_json(*j);
    if (const auto* f = n.find("file_content")) im.file_content = f->scalar;
    if (const auto* m = n.find("metadata"))
        for (const auto& [k, v] : m->map) im.metadata[k] = v.scalar;
    return im;
}

output_conditions parse_conditions(const cfg::ynode& n) {
    output_conditions oc;
    oc.where = n.pos;
    only_keys(n, "output condition",
              {"content_equals", "content_matches", "json_equals", "json_contains",
               "bloblang", "file_equals", "metadata_equals"},
              {"file_json_equals"});
    if (const auto* v = n.find("content_equals"))  oc.content_equals  = v->scalar;
    if (const auto* v = n.find("content_matches")) oc.content_matches = v->scalar;
    if (const auto* v = n.find("json_equals"))     oc.json_equals     = compact_json(*v);
    if (const auto* v = n.find("json_contains"))   oc.json_contains   = compact_json(*v);
    if (const auto* v = n.find("bloblang"))        oc.bloblang        = v->scalar;
    if (const auto* v = n.find("file_equals"))    oc.file_equals     = v->scalar;
    if (const auto* v = n.find("metadata_equals"))
        for (const auto& [k, e] : v->map)
            // A metadata value may be structured, not just a scalar. Reading
            // `.scalar` on a nested mapping gives "" and fails every such
            // assertion; rendering it as compact JSON compares like for like,
            // because that is how a structured metadata value prints.
            oc.metadata_equals[k] = e.is_scalar() ? e.scalar : compact_json(e);
    return oc;
}

} // namespace

std::vector<test_case> parse_cases(const cfg::ynode& root) {
    std::vector<test_case> out;
    const cfg::ynode* tests = root.find("tests");
    if (!tests || !tests->is_sequence()) return out;

    for (const auto& t : tests->seq) {
        test_case tc;
        tc.where = t.pos;
        tc.name = scalar_of(t.find("name"));
        if (const auto* e = t.find("environment"))
            for (const auto& [k, v] : e->map) tc.environment[k] = v.scalar;
        if (const auto* tp = t.find("target_processors")) tc.target_processors = tp->scalar;
        if (const auto* tm = t.find("target_mapping"))    tc.target_mapping    = tm->scalar;
        // Mocks replace a named component with a canned response.
        if (const auto* m = t.find("mocks"))
            for (const auto& e : m->map) tc.mocks.emplace_back(e.key, e.value);

        // `input_batch` is one batch; `input_batches` is a list of them. Both
        // exist in the wild and mean the same thing to the runner.
        if (const auto* ib = t.find("input_batch"); ib && ib->is_sequence()) {
            std::vector<input_message> b;
            for (const auto& m : ib->seq) b.push_back(parse_input(m));
            tc.input_batches.push_back(std::move(b));
        }
        if (const auto* ibs = t.find("input_batches"); ibs && ibs->is_sequence())
            for (const auto& batch : ibs->seq) {
                std::vector<input_message> b;
                for (const auto& m : batch.seq) b.push_back(parse_input(m));
                tc.input_batches.push_back(std::move(b));
            }
        if (const auto* obs = t.find("output_batches"); obs && obs->is_sequence())
            for (const auto& batch : obs->seq) {
                std::vector<output_conditions> b;
                for (const auto& c : batch.seq) b.push_back(parse_conditions(c));
                tc.output_batches.push_back(std::move(b));
            }
        out.push_back(std::move(tc));
    }
    return out;
}

namespace {

// Does `sub` appear within `sup`, recursively? Objects match on the keys `sub`
// names and ignore the rest; arrays must match element-for-element in order.
// This is what Benthos's json_contains means, and it is NOT plain equality.
bool json_contains_value(const value& sup, const value& sub) {
    if (sub.type() == vtype::object) {
        if (sup.type() != vtype::object) return false;
        for (const auto& [k, v] : sub.obj()) {
            const value* got = sup.find(k);
            if (!got || !json_contains_value(*got, v)) return false;
        }
        return true;
    }
    if (sub.type() == vtype::array) {
        if (sup.type() != vtype::array || sup.arr().size() != sub.arr().size()) return false;
        for (size_t i = 0; i < sub.arr().size(); ++i)
            if (!json_contains_value(sup.arr()[i], sub.arr()[i])) return false;
        return true;
    }
    return sup.to_json() == sub.to_json();
}

void fail(std::vector<case_failure>& out, const std::string& name, int line,
          std::string reason) {
    out.push_back(case_failure{name, line, std::move(reason)});
}

} // namespace

namespace {

// Splits a JSON pointer into its (unescaped) segments.
std::vector<std::string> pointer_segments(std::string_view ptr) {
    std::vector<std::string> out;
    if (ptr.empty() || ptr.front() != '/') return out;
    size_t i = 1;
    for (;;) {
        const size_t slash = ptr.find('/', i);
        std::string tok(ptr.substr(i, slash == std::string_view::npos
                                          ? std::string_view::npos : slash - i));
        for (size_t p = tok.find("~1"); p != std::string::npos; p = tok.find("~1", p + 1))
            tok.replace(p, 2, "/");
        for (size_t p = tok.find("~0"); p != std::string::npos; p = tok.find("~0", p + 1))
            tok.replace(p, 2, "~");
        out.push_back(std::move(tok));
        if (slash == std::string_view::npos) break;
        i = slash + 1;
    }
    return out;
}

// Mutable walk. `-` as the LAST segment is RFC 6901's append token, which the
// corpus uses to add a processor to the end of a pipeline; `append_to` is set
// to the array in that case and the return is null.
cfg::ynode* walk_mut(cfg::ynode& root, const std::vector<std::string>& segs,
                     cfg::ynode** append_to) {
    *append_to = nullptr;
    cfg::ynode* cur = &root;
    for (size_t k = 0; k < segs.size() && cur; ++k) {
        const std::string& tok = segs[k];
        const bool last = (k + 1 == segs.size());
        if (last && tok == "-" && cur->is_sequence()) { *append_to = cur; return nullptr; }
        if (cur->is_sequence()) {
            if (tok.find_first_not_of("0123456789") != std::string::npos) return nullptr;
            const size_t idx = std::stoul(tok);
            if (idx >= cur->seq.size()) return nullptr;
            cur = &cur->seq[idx];
        } else if (cur->is_mapping()) {
            cfg::ynode* next = nullptr;
            for (auto& e : cur->map) if (e.key == tok) { next = &e.value; break; }
            cur = next;
        } else {
            return nullptr;
        }
    }
    return cur;
}

// The node carrying `label: <name>`, searched anywhere in the tree.
cfg::ynode* find_labelled_mut(cfg::ynode& n, const std::string& label) {
    if (n.is_mapping()) {
        for (const auto& e : n.map)
            if (e.key == "label" && e.value.scalar == label) return &n;
        for (auto& e : n.map)
            if (auto* hit = find_labelled_mut(e.value, label)) return hit;
    } else if (n.is_sequence()) {
        for (auto& e : n.seq)
            if (auto* hit = find_labelled_mut(e, label)) return hit;
    }
    return nullptr;
}

std::string label_of(const cfg::ynode& n) {
    if (const auto* l = n.find("label")) return l->scalar;
    return {};
}

void substitute(cfg::ynode& target, const cfg::ynode& mock) {
    // The target's label survives the substitution unless the mock supplies its
    // own. Benthos does this deliberately: dropping it would leave the mocked
    // component anonymous, so a second mock naming the same label could not
    // find it, and metrics paths would change under the test.
    const std::string keep = label_of(mock).empty() ? label_of(target) : std::string();
    target = mock;
    if (!keep.empty() && label_of(target).empty())
        target.map.push_back(cfg::ynode::entry{"label", [&]{
            cfg::ynode l; l.type = cfg::ynode_type::scalar; l.scalar = keep; return l; }()});
}

} // namespace

void apply_mocks(cfg::ynode& doc,
                 const std::vector<std::pair<std::string, cfg::ynode>>& mocks) {
    // Pointers first, then labels -- the order Benthos uses, because a pointer
    // can introduce the very node a later label needs to resolve against.
    for (int pass = 0; pass < 2; ++pass) {
        for (const auto& [key, mock] : mocks) {
            const bool is_ptr = !key.empty() && key.front() == '/';
            if (is_ptr != (pass == 0)) continue;
            if (is_ptr) {
                cfg::ynode* append_to = nullptr;
                cfg::ynode* t = walk_mut(doc, pointer_segments(key), &append_to);
                if (append_to) { append_to->seq.push_back(mock); continue; }
                if (!t) throw std::runtime_error("mock path '" + key + "' matched nothing");
                substitute(*t, mock);
            } else {
                cfg::ynode* t = find_labelled_mut(doc, key);
                if (!t)
                    throw std::runtime_error("mock for label '" + key +
                        "' could not be applied as the label was not found in the test target file");
                substitute(*t, mock);
            }
        }
    }
}

void check_conditions(const output_conditions& c, const message& m,
                      const std::string& base_dir,
                      const std::string& case_name, size_t batch_i, size_t msg_i,
                      std::vector<case_failure>& out) {
    // Benthos names the position in every failure, because a batch of twenty
    // messages with one mismatch is otherwise a hunt.
    const std::string at = "batch " + std::to_string(batch_i) +
                           " message " + std::to_string(msg_i) + ": ";
    const int line = static_cast<int>(c.where.line);
    const std::string body = m.as_bytes();

    if (c.content_equals && *c.content_equals != body)
        fail(out, case_name, line, at + "content_equals: expected '" +
             *c.content_equals + "' but got '" + body + "'");

    if (c.file_equals) {
        const std::filesystem::path p =
            std::filesystem::path(base_dir) / *c.file_equals;
        std::ifstream f(p, std::ios::binary);
        if (!f)
            fail(out, case_name, line, at + "file_equals: cannot open " + p.string());
        else {
            const std::string want((std::istreambuf_iterator<char>(f)),
                                   std::istreambuf_iterator<char>());
            if (want != body)
                fail(out, case_name, line, at + "file_equals: expected '" + want +
                     "' but got '" + body + "'");
        }
    }

    if (c.content_matches) {
        try {
            if (!re(*c.content_matches).match(body))
                fail(out, case_name, line, at + "content_matches: '" + body +
                     "' does not match /" + *c.content_matches + "/");
        } catch (const std::exception& e) {
            fail(out, case_name, line, at + "content_matches: bad pattern: " + e.what());
        }
    }

    // The JSON assertions parse BOTH sides and compare the parsed forms, so key
    // order and whitespace do not decide a test.
    if (c.json_equals || c.json_contains) {
        value got;
        bool parsed = true;
        try { got = parse_json(body); }
        catch (const std::exception& e) {
            parsed = false;
            fail(out, case_name, line, at + "expected JSON but the message is not: " + e.what());
        }
        if (parsed && c.json_equals) {
            const value want = parse_json(*c.json_equals);
            if (got.to_json() != want.to_json())
                fail(out, case_name, line, at + "json_equals: expected " +
                     want.to_json() + " but got " + got.to_json());
        }
        if (parsed && c.json_contains) {
            const value want = parse_json(*c.json_contains);
            if (!json_contains_value(got, want))
                fail(out, case_name, line, at + "json_contains: " + got.to_json() +
                     " does not contain " + want.to_json());
        }
    }

    for (const auto& [k, want] : c.metadata_equals) {
        const value* got = m.meta().find(k);
        if (!got)
            fail(out, case_name, line, at + "metadata_equals: key '" + k + "' is not set");
        else if (got->is_stringy() ? got->as_string() != want : got->to_json() != want)
            fail(out, case_name, line, at + "metadata_equals: '" + k + "' expected '" +
                 want + "' but got '" +
                 (got->is_stringy() ? got->as_string() : got->to_json()) + "'");
    }

    if (c.bloblang) {
        try {
            const blobl::mapping prog = blobl::parse_query(*c.bloblang);
            const blobl::interp in_(prog);
            exec_ctx ctx;
            value parsed_body;
            bool structured = true;
            try { parsed_body = m.as_structured(); } catch (const eval_error&) { structured = false; }
            ctx.this_v = structured ? &parsed_body : nullptr;
            ctx.msg = &m;
            // Reads see the message's own metadata: there is no separate output
            // message being built here, unlike a mapping processor.
            ctx.meta_in = &m.meta();
            const value r = in_.run_ctx(ctx);
            if (!(r.type() == vtype::boolean ? r.as_bool() : truthy(r)))
                fail(out, case_name, line, at + "bloblang: '" + *c.bloblang +
                     "' returned " + r.to_json() + ", not true");
        } catch (const std::exception& e) {
            fail(out, case_name, line, at + "bloblang: '" + *c.bloblang + "' failed: " + e.what());
        }
    }
}

} // namespace sf::testing
