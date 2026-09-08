// The processor set beyond `mapping`. The combinators are the interesting ones:
// try/catch/switch/for_each own nested processor lists, which is what makes the
// pipeline a tree rather than a flat chain.
#include "swordfish/codecs.hh"
#include "swordfish/runtime/cache.hh"

#include <algorithm>

#include <seastar/core/smp.hh>
#include "swordfish/runtime/components.hh"


#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>
#include <seastar/util/log.hh>

namespace sf {

static seastar::logger plog("sf.processor");

namespace {

// Runs a nested processor list over a batch, threading results through, exactly
// as the top-level pipeline does. Shared by every combinator.
seastar::future<std::vector<batch>> run_chain(
        const std::vector<processor_ptr>& chain, batch b, seastar::abort_source& as) {
    std::vector<batch> batches;
    batches.push_back(std::move(b));
    for (const auto& p : chain) {
        std::vector<batch> next;
        for (auto& cur : batches) {
            if (cur.empty()) continue;
            auto out = co_await p->process(std::move(cur), as);
            for (auto& ob : out) if (!ob.empty()) next.push_back(std::move(ob));
        }
        batches = std::move(next);
        if (batches.empty()) break;
    }
    co_return batches;
}

class noop_processor final : public processor {
public:
    seastar::future<std::vector<batch>> process(batch b, seastar::abort_source&) override {
        std::vector<batch> out;
        out.push_back(std::move(b));
        return seastar::make_ready_future<std::vector<batch>>(std::move(out));
    }
    std::string name() const override { return "noop"; }
};

// `unarchive`: one message in, many out. Semantics taken from
// benthos-main/internal/impl/pure/processor_unarchive.go (Apache-2.0).
//
// tar, zip and binary are deliberately NOT here. They are named as
// unimplemented at config time rather than silently accepted, which is the
// project's rule for anything not built yet: a config that lints clean and then
// does the wrong thing is worse than one that refuses.
class unarchive_processor final : public processor {
public:
    explicit unarchive_processor(std::string format) : _format(std::move(format)) {}

    seastar::future<std::vector<batch>> process(batch b, seastar::abort_source&) override {
        batch out;
        for (auto& msg : b) {
            try {
                expand(msg, out);
            } catch (const std::exception& e) {
                // The message travels on carrying the error, as every other
                // processor does; the batch is not abandoned.
                message failed = msg.shallow_copy();
                failed.set_error(e.what());
                out.push_back(std::move(failed));
            }
        }
        std::vector<batch> res;
        if (!out.empty()) res.push_back(std::move(out));
        co_return res;
    }
    std::string name() const override { return "unarchive"; }

private:
    // Each part keeps the original's metadata, which is what `part.Copy()`
    // does in the reference.
    static message derive(const message& from, std::string body) {
        message m = from.shallow_copy();
        m.set_mapped(value(std::move(body)));
        return m;
    }

    void expand(const message& msg, batch& out) const {
        if (_format == "lines") {
            const std::string s = msg.as_bytes();
            size_t i = 0;
            for (;;) {
                const size_t nl = s.find('\n', i);
                out.push_back(derive(msg, s.substr(i, nl == std::string::npos
                                                          ? std::string::npos : nl - i)));
                if (nl == std::string::npos) break;
                i = nl + 1;
            }
            return;
        }
        if (_format == "json_array") {
            const value doc = msg.as_structured();
            if (doc.type() != vtype::array)
                throw eval_error("failed to parse message into JSON array: got " +
                                 std::string(doc.type_name()));
            for (const auto& e : doc.arr()) out.push_back(derive(msg, e.to_json()));
            return;
        }
        if (_format == "json_map") {
            const value doc = msg.as_structured();
            if (doc.type() != vtype::object)
                throw eval_error("failed to parse message into JSON map: got " +
                                 std::string(doc.type_name()));
            for (const auto& [k, v] : doc.obj()) {
                message m = derive(msg, v.to_json());
                // The reference records which key a part came from.
                m.meta().set("archive_key", value(k));
                out.push_back(std::move(m));
            }
            return;
        }
        if (_format == "json_documents") {
            // A stream of concatenated documents, not an array: `{"a":1}{"b":2}`.
            const std::string s = msg.as_bytes();
            size_t i = 0;
            while (i < s.size()) {
                while (i < s.size() && std::isspace(static_cast<unsigned char>(s[i]))) ++i;
                if (i >= s.size()) break;
                const size_t end = json_document_end(s, i);
                out.push_back(derive(msg, parse_json(s.substr(i, end - i)).to_json()));
                i = end;
            }
            return;
        }
        throw eval_error("unarchive format '" + _format + "' is not implemented by swordfish");
    }

    // Where the document starting at `from` ends. Brace/bracket depth, with
    // strings and their escapes skipped -- a `}` inside a string does not close
    // anything.
    static size_t json_document_end(const std::string& s, size_t from) {
        int depth = 0;
        bool in_str = false, esc = false;
        for (size_t i = from; i < s.size(); ++i) {
            const char c = s[i];
            if (in_str) {
                if (esc)            esc = false;
                else if (c == '\\') esc = true;
                else if (c == '"')  in_str = false;
                continue;
            }
            if (c == '"') { in_str = true; continue; }
            if (c == '{' || c == '[') ++depth;
            else if (c == '}' || c == ']') {
                if (--depth == 0) return i + 1;
            }
            // A bare scalar document (a number or `true`) ends at whitespace.
            else if (depth == 0 && std::isspace(static_cast<unsigned char>(c))) return i;
        }
        return s.size();
    }

    std::string _format;
};

// ---- batch-shaping processors ------------------------------------------------
// Semantics from benthos-main/internal/impl/pure (Apache-2.0), with the config
// schemas taken from `redpanda-connect create` so the field names and defaults
// are the reference's rather than plausible-looking guesses.

// `select_parts`: keep only the listed indices, in the order listed. A negative
// index counts back from the end; one out of range is skipped, not an error.
class select_parts_processor final : public processor {
public:
    explicit select_parts_processor(std::vector<int64_t> parts) : _parts(std::move(parts)) {}
    seastar::future<std::vector<batch>> process(batch b, seastar::abort_source&) override {
        batch out;
        const int64_t n = static_cast<int64_t>(b.size());
        for (int64_t idx : _parts) {
            const int64_t i = idx < 0 ? n + idx : idx;
            if (i < 0 || i >= n) continue;             // out of range: skipped
            out.push_back(b[static_cast<size_t>(i)].shallow_copy());
        }
        std::vector<batch> res;
        // An empty selection drops the batch rather than forwarding an empty
        // one, which is what every filtering processor here does.
        if (!out.empty()) res.push_back(std::move(out));
        co_return res;
    }
    std::string name() const override { return "select_parts"; }
private:
    std::vector<int64_t> _parts;
};

// `insert_part`: add a message at an index. -1 appends, and an index past the
// end clamps rather than erroring.
class insert_part_processor final : public processor {
public:
    insert_part_processor(int64_t index, transform_fn content)
        : _index(index), _content(std::move(content)) {}
    seastar::future<std::vector<batch>> process(batch b, seastar::abort_source&) override {
        const int64_t n = static_cast<int64_t>(b.size());
        int64_t i = _index;
        if (i < 0) i = std::max<int64_t>(n + i + 1, 0);
        else if (i > n) i = n;

        exec_ctx ctx;
        const ctx_scope guard_{ctx};
        ctx.all = &b;
        ctx.batch_size = n;
        ctx.batch_index = 0;
        // The content is interpolated against the FIRST message, as the
        // reference does (`p.part.Bytes(0, msg)`).
        value parsed;
        if (!b.empty()) {
            bool structured = true;
            try { parsed = b[0].as_structured(); } catch (const eval_error&) { structured = false; }
            ctx.this_v = structured ? &parsed : nullptr;
            ctx.msg = &b[0];
            ctx.meta_in = &b[0].meta();
        }
        // A failing interpolation LOGS and leaves the batch alone, which is what
        // the reference does ("Content interpolation error: ..." and the batch
        // goes on unchanged). Letting it throw made the batch a casualty of the
        // layer that caught it: under a buffer's batch policy the whole batch was
        // dropped, and those messages had already been acked to the source, so
        // four messages the reference delivered were gone with a single line in
        // the log.
        message inserted;
        try {
            inserted.set_mapped(_content(ctx));
        } catch (const std::exception& e) {
            plog.error("Content interpolation error: {}", e.what());
            std::vector<batch> out;
            out.push_back(std::move(b));
            co_return out;
        }

        batch out;
        out.reserve(b.size() + 1);
        for (int64_t k = 0; k < i; ++k) out.push_back(std::move(b[static_cast<size_t>(k)]));
        out.push_back(std::move(inserted));
        for (int64_t k = i; k < n; ++k) out.push_back(std::move(b[static_cast<size_t>(k)]));
        std::vector<batch> res;
        res.push_back(std::move(out));
        co_return res;
    }
    std::string name() const override { return "insert_part"; }
private:
    int64_t      _index;
    transform_fn _content;
};

// `bounds_check`: drop the whole batch when its shape or any message's size is
// outside the configured bounds. A rejected batch is dropped silently -- it is
// a filter, not an error.
class bounds_check_processor final : public processor {
public:
    bounds_check_processor(int64_t max_part, int64_t min_part, int64_t max_parts, int64_t min_parts)
        : _max_part(max_part), _min_part(min_part), _max_parts(max_parts), _min_parts(min_parts) {}
    seastar::future<std::vector<batch>> process(batch b, seastar::abort_source&) override {
        std::vector<batch> res;
        const int64_t n = static_cast<int64_t>(b.size());
        if (n < _min_parts || n > _max_parts) co_return res;
        for (const auto& m : b) {
            const int64_t sz = static_cast<int64_t>(m.as_bytes().size());
            if (sz < _min_part || sz > _max_part) co_return res;   // one bad message drops all
        }
        res.push_back(std::move(b));
        co_return res;
    }
    std::string name() const override { return "bounds_check"; }
private:
    int64_t _max_part, _min_part, _max_parts, _min_parts;
};

// `split`: break one batch into several. `size` counts messages; `byte_size`,
// when set, caps the accumulated bytes instead and takes precedence.
class split_processor final : public processor {
public:
    split_processor(int64_t size, int64_t byte_size) : _size(size), _byte_size(byte_size) {}
    seastar::future<std::vector<batch>> process(batch b, seastar::abort_source&) override {
        std::vector<batch> res;
        batch cur;
        int64_t bytes = 0;
        for (auto& m : b) {
            const int64_t sz = static_cast<int64_t>(m.as_bytes().size());
            const bool full = _byte_size > 0
                ? (!cur.empty() && bytes + sz > _byte_size)
                : (static_cast<int64_t>(cur.size()) >= _size);
            if (full) { res.push_back(std::move(cur)); cur = batch{}; bytes = 0; }
            bytes += sz;
            cur.push_back(std::move(m));
        }
        if (!cur.empty()) res.push_back(std::move(cur));
        co_return res;
    }
    std::string name() const override { return "split"; }
private:
    int64_t _size, _byte_size;
};

// `compress` / `decompress`: per message, through the codecs the Kafka record
// batches already use. `pgzip` is Go's PARALLEL gzip -- the same wire format,
// so it maps to gzip rather than being rejected: a config naming it works here
// and produces bytes the reference reads back.
class codec_processor final : public processor {
public:
    codec_processor(std::string algorithm, int level, bool compressing)
        : _alg(std::move(algorithm)), _level(level), _compressing(compressing) {}

    seastar::future<std::vector<batch>> process(batch b, seastar::abort_source&) override {
        batch out;
        out.reserve(b.size());
        for (auto& msg : b) {
            try {
                message m = msg.shallow_copy();
                m.set_bytes(apply(msg.as_bytes()));
                out.push_back(std::move(m));
            } catch (const std::exception& e) {
                // Travels with the message, as every other processor does.
                message m = msg.shallow_copy();
                m.set_error(std::string(_compressing ? "compress: " : "decompress: ") + e.what());
                out.push_back(std::move(m));
            }
        }
        std::vector<batch> res;
        if (!out.empty()) res.push_back(std::move(out));
        co_return res;
    }
    std::string name() const override { return _compressing ? "compress" : "decompress"; }

private:
    std::string apply(const std::string& in) const {
        if (_alg == "gzip"   || _alg == "pgzip")
            return _compressing ? codec::gzip_compress(in, _level)   : codec::gzip_decompress(in);
        if (_alg == "zlib")
            return _compressing ? codec::zlib_compress(in, _level)   : codec::zlib_decompress(in);
        if (_alg == "flate")
            return _compressing ? codec::flate_compress(in, _level)  : codec::flate_decompress(in);
        if (_alg == "snappy")
            return _compressing ? codec::snappy_compress(in)         : codec::snappy_decompress(in);
        if (_alg == "lz4")
            return _compressing ? codec::lz4_compress(in)            : codec::lz4_decompress(in);
        if (_alg == "zstd")
            return _compressing ? codec::zstd_compress(in, _level)
                                : codec::zstd_decompress(in);
        // Decode only: there is no bzip2 encoder here, and none in the reference
        // either -- it accepts `compress: bzip2` at lint and then fails.
        if (_alg == "bzip2" && !_compressing) return codec::bzip2_decompress(in);
        throw eval_error("algorithm '" + _alg + "' is not implemented by swordfish");
    }
    std::string _alg;
    int         _level;
    bool        _compressing;
};

// `archive`: the inverse of `unarchive` -- a whole batch becomes one message.
class archive_processor final : public processor {
public:
    explicit archive_processor(std::string format) : _format(std::move(format)) {}

    seastar::future<std::vector<batch>> process(batch b, seastar::abort_source&) override {
        std::vector<batch> res;
        if (b.empty()) co_return res;                 // nothing to archive

        std::string body;
        if (_format == "json_array") {
            std::vector<value> items;
            items.reserve(b.size());
            for (const auto& m : b) {
                // A message that is not JSON becomes a string element rather
                // than failing the batch, matching how the reference treats a
                // part it cannot parse structurally.
                try { items.push_back(m.as_structured()); }
                catch (const eval_error&) { items.push_back(value(m.as_bytes())); }
            }
            body = value::array(std::move(items)).to_json();
        } else if (_format == "lines" || _format == "concatenate") {
            const char* sep = _format == "lines" ? "\n" : "";
            for (size_t i = 0; i < b.size(); ++i) {
                if (i) body += sep;
                body += b[i].as_bytes();
            }
        } else {
            // Named, not silently passed through: tar, zip and binary need
            // container formats this does not implement yet.
            message m = b[0].shallow_copy();
            m.set_error("archive format '" + _format + "' is not implemented by swordfish");
            batch one;
            one.push_back(std::move(m));
            res.push_back(std::move(one));
            co_return res;
        }

        // Metadata comes from the FIRST message, which is what the reference
        // does: the archive is one message and has to carry one message's
        // metadata.
        message m = b[0].shallow_copy();
        m.set_bytes(std::move(body));
        batch one;
        one.push_back(std::move(m));
        res.push_back(std::move(one));
        co_return res;
    }
    std::string name() const override { return "archive"; }
private:
    std::string _format;
};

// `branch`: the enrichment pattern. Each message is mapped to a REQUEST, the
// child processors run on those requests, and each result is mapped back ONTO
// the original message. Semantics from
// benthos-main/internal/impl/pure/processor_branch.go (Apache-2.0).
//
// The two mappings differ in where `root` starts, and getting that wrong would
// silently discard data:
//   request_map  root starts EMPTY   -- it builds a fresh request
//   result_map   root starts at the ORIGINAL message -- it merges into it
// The second is the `mutation` semantics `root_init` exists for.
class branch_processor final : public processor {
public:
    branch_processor(transform_fn request, std::vector<processor_ptr> children,
                     transform_fn result)
        : _request(std::move(request)), _children(std::move(children)),
          _result(std::move(result)) {}

    // Which messages of the LAST process() call the request mapping declined
    // with deleted(). `workflow` needs it to record a branch as skipped rather
    // than succeeded: the reference distinguishes the two, and a replay that
    // re-runs only the unfinished branches depends on the distinction. Valid
    // until the next process() call, which is all the one caller needs.
    const std::vector<bool>& skipped() const noexcept { return _skipped; }

    seastar::future<std::vector<batch>> process(batch b, seastar::abort_source& as) override {
        // Requests, and which original each came from. A message whose
        // request_map yields `deleted()` is SKIPPED -- it passes through
        // untouched rather than being dropped, which is how a branch is made
        // conditional.
        batch requests;
        std::vector<size_t> origin;
        std::vector<value> originals(b.size());
        std::vector<bool>  parsed_ok(b.size(), false);
        _skipped.assign(b.size(), false);

        for (size_t i = 0; i < b.size(); ++i) {
            try { originals[i] = b[i].as_structured(); parsed_ok[i] = true; }
            catch (const eval_error&) {}

            if (!_request) {
                requests.push_back(b[i].shallow_copy());
                origin.push_back(i);
                continue;
            }
            exec_ctx ctx;
            const ctx_scope guard_{ctx};
            ctx.all = &b;
            ctx.batch_size = static_cast<int64_t>(b.size());
            ctx.batch_index = static_cast<int64_t>(i);
            ctx.this_v = parsed_ok[i] ? &originals[i] : nullptr;
            ctx.msg = &b[i];
            ctx.meta_in = &b[i].meta();
            message req = b[i].shallow_copy();
            ctx.meta = &req.meta();
            try {
                const value r = _request(ctx);
                if (r.is_deleted()) { _skipped[i] = true; continue; }   // conditional branch
                req.set_mapped(r);
                requests.push_back(std::move(req));
                origin.push_back(i);
            } catch (const std::exception& e) {
                b[i].set_error(std::string("request mapping failed: ") + e.what());
            }
        }

        std::vector<batch> out;
        if (requests.empty()) { out.push_back(std::move(b)); co_return out; }

        batch results;
        try {
            auto produced = co_await run_chain(_children, std::move(requests), as);
            for (auto& rb : produced)
                for (auto& m : rb) results.push_back(std::move(m));
        } catch (const std::exception& e) {
            for (auto& m : b) m.set_error(std::string("branch failed: ") + e.what());
            out.push_back(std::move(b));
            co_return out;
        }

        // The child processors must not change the count: a branch overlays
        // result i onto original i, so a filtered or split batch has no
        // meaningful mapping back. Benthos treats this as an error on every
        // message rather than guessing an alignment.
        if (results.size() != origin.size()) {
            const std::string err = "branch resulted in " + std::to_string(results.size()) +
                " messages from " + std::to_string(origin.size()) +
                "; processors in a branch must not change the batch size";
            for (auto& m : b) m.set_error(err);
            out.push_back(std::move(b));
            co_return out;
        }

        for (size_t k = 0; k < results.size(); ++k) {
            const size_t i = origin[k];
            if (!_result) { b[i] = std::move(results[k]); continue; }
            value res_parsed;
            bool res_ok = true;
            try { res_parsed = results[k].as_structured(); }
            catch (const eval_error&) { res_ok = false; }

            exec_ctx ctx;
            const ctx_scope guard_{ctx};
            ctx.all = &b;
            ctx.batch_size = static_cast<int64_t>(b.size());
            ctx.batch_index = static_cast<int64_t>(i);
            // `this` is the branch RESULT; `root` starts at the ORIGINAL.
            ctx.this_v = res_ok ? &res_parsed : nullptr;
            ctx.msg = &results[k];
            ctx.meta_in = &results[k].meta();
            ctx.meta = &b[i].meta();
            ctx.root_init = parsed_ok[i] ? &originals[i] : nullptr;
            try {
                b[i].set_mapped(_result(ctx));
            } catch (const std::exception& e) {
                b[i].set_error(std::string("result mapping failed: ") + e.what());
            }
        }
        out.push_back(std::move(b));
        co_return out;
    }

    seastar::future<> close() override {
        for (auto& p : _children)
            try { co_await p->close(); }
            catch (const std::exception& e) { plog.warn("close failed: {}", e.what()); }
    }
    std::string name() const override { return "branch"; }

private:
    transform_fn               _request;
    std::vector<processor_ptr> _children;
    transform_fn               _result;
    std::vector<bool>          _skipped;
};

// `workflow`: named branches run in dependency order, each merging its result
// into the message, so a later branch can read what an earlier one wrote.
//
// `order` is REQUIRED here. Benthos can infer the order from the branches'
// request and result mappings when it is omitted; that inference is not built,
// and guessing an order would be the worst possible failure -- branches running
// in the wrong sequence produce plausible, wrong data with no error. A config
// without `order` is therefore rejected by name at build time.
class workflow_processor final : public processor {
public:
    struct step { std::string name; processor_ptr branch; };

    workflow_processor(std::vector<std::vector<step>> order, std::vector<std::string> meta_path)
        : _order(std::move(order)), _meta_path(std::move(meta_path)) {}

    seastar::future<std::vector<batch>> process(batch b, seastar::abort_source& as) override {
        // Per message, which branches ran cleanly. Recorded at meta_path so a
        // downstream processor can see what happened, which is what makes a
        // partially-failed workflow debuggable.
        // The shape is the reference's, and each part of it is load-bearing:
        // `succeeded` and `skipped` are SORTED arrays of names, `failed` is an
        // OBJECT of name -> error message, and an empty one is omitted rather
        // than written as []. A replay that re-runs only the unfinished
        // branches reads exactly these three, so a branch recorded under the
        // wrong key would be re-run or skipped wrongly on the next pass.
        std::vector<std::vector<std::string>> succeeded(b.size());
        std::vector<std::vector<std::string>> skipped(b.size());
        std::vector<std::vector<std::pair<std::string, std::string>>> failed(b.size());

        for (auto& group : _order) {
            // Sequential within a group. Benthos runs a group in parallel, but
            // the branches in one group are by definition independent, so the
            // observable result is the same; concurrency here would only add
            // scheduling, and each branch already owns the whole batch.
            for (auto& st : group) {
                std::vector<bool> had_error(b.size());
                for (size_t i = 0; i < b.size(); ++i) had_error[i] = b[i].has_error();

                auto produced = co_await st.branch->process(std::move(b), as);
                b = produced.empty() ? batch{} : std::move(produced[0]);
                for (auto k = produced.begin() + (produced.empty() ? 0 : 1);
                     k != produced.end(); ++k)
                    for (auto& m : *k) b.push_back(std::move(m));

                // A `branch` is the only processor with a notion of declining a
                // message, and `workflow` is the only caller that asks -- so the
                // question is put to the concrete type rather than widening the
                // processor interface for one relationship. Every step IS a
                // branch: parse_processor builds them that way.
                const auto* bp = dynamic_cast<const branch_processor*>(st.branch.get());
                const std::vector<bool> none;
                const std::vector<bool>& was_skipped = bp ? bp->skipped() : none;

                for (size_t i = 0; i < b.size() && i < had_error.size(); ++i) {
                    if (i < was_skipped.size() && was_skipped[i])
                        skipped[i].push_back(st.name);
                    else if (b[i].has_error() && !had_error[i])
                        failed[i].emplace_back(st.name, b[i].error());
                    else
                        succeeded[i].push_back(st.name);
                }
            }
        }

        if (!_meta_path.empty()) {
            for (size_t i = 0; i < b.size(); ++i) {
                value doc;
                try { doc = b[i].as_structured(); }
                catch (const eval_error&) { continue; }   // nothing to record onto
                value rec = value::object();
                const auto sorted_array = [](std::vector<std::string> names) {
                    std::sort(names.begin(), names.end());
                    std::vector<value> vs;
                    vs.reserve(names.size());
                    for (auto& n : names) vs.push_back(value(std::move(n)));
                    return value::array(std::move(vs));
                };
                if (!succeeded[i].empty())
                    rec.set("succeeded", sorted_array(std::move(succeeded[i])));
                if (!skipped[i].empty())
                    rec.set("skipped", sorted_array(std::move(skipped[i])));
                if (!failed[i].empty()) {
                    value f = value::object();
                    for (auto& [name, err] : failed[i]) f.set(name, value(err));
                    rec.set("failed", std::move(f));
                }
                b[i].set_structured(assign_path(std::move(doc), _meta_path, 0, std::move(rec)));
            }
        }

        std::vector<batch> out;
        if (!b.empty()) out.push_back(std::move(b));
        co_return out;
    }

    seastar::future<> close() override {
        for (auto& g : _order)
            for (auto& st : g)
                try { co_await st.branch->close(); }
                catch (const std::exception& e) { plog.warn("close failed: {}", e.what()); }
    }
    std::string name() const override { return "workflow"; }

private:
    std::vector<std::vector<step>> _order;
    std::vector<std::string>       _meta_path;
};

// `dedupe`: drops a message whose key has been seen before.
// benthos-main/internal/impl/pure/processor_dedupe.go (Apache-2.0): the key is
// added to a cache, and a key that was already present means drop.
class dedupe_processor final : public processor {
public:
    dedupe_processor(cache_ptr c, transform_fn key, bool drop_on_err)
        : _cache(std::move(c)), _key(std::move(key)), _drop_on_err(drop_on_err) {}

    seastar::future<std::vector<batch>> process(batch b, seastar::abort_source&) override {
        batch kept;
        // `from(i)` reads another message of the batch, so the batch must stay
        // whole while keys are computed -- kept messages are SHALLOW-COPIED
        // rather than moved out. Moving them left `from(0)` reading a
        // moved-from message once the first one had been kept.
        _ctx.all = &b;
        const ctx_scope guard_{_ctx};   // see runtime.hh
        _ctx.batch_size = static_cast<int64_t>(b.size());
        _ctx.batch_index = 0;
        for (auto& msg : b) {
            const auto bump = [this] { ++_ctx.batch_index; };
            std::string key;
            bool failed = false;
            try {
                value parsed;
                bool structured = true;
                try { parsed = msg.as_structured(); } catch (const eval_error&) { structured = false; }
                _ctx.vars.clear();
                _ctx.this_v = structured ? &parsed : nullptr;
                _ctx.msg = &msg;
                _ctx.meta_in = &msg.meta();
                key = _key(_ctx).to_display_string();
                _ctx.meta_in = nullptr;
            } catch (const std::exception& e) {
                failed = true;
                if (!_drop_on_err) {
                    message m = msg.shallow_copy();
                    m.set_error(std::string("key interpolation error: ") + e.what());
                    kept.push_back(std::move(m));
                

                }
            }
            if (!failed) {
                // The cache call is GUARDED, and `drop_on_err` governs it -- that
                // is what the field means in the reference ("whether messages
                // should be dropped when the cache returns a general error such
                // as a network issue"). Unguarded, a cache that refused the key
                // threw out of process() entirely, so the stream layer marked the
                // message and FORWARDED it where the reference discards it: a
                // cache outage quietly turned a deduplicating pipeline into a
                // non-deduplicating one.
                bool novel = false;
                try {
                    novel = co_await _cache->add(key);
                } catch (const std::exception& e) {
                    failed = true;
                    if (!_drop_on_err) {
                        message m = msg.shallow_copy();
                        m.set_error(std::string("cache error: ") + e.what());
                        kept.push_back(std::move(m));
                    }
                }
                if (!failed && novel) kept.push_back(msg.shallow_copy());
            }
            bump();
        }
        // An empty batch is dropped entirely rather than forwarded empty, which
        // is what the reference does when every message was a duplicate.
        std::vector<batch> out;
        if (!kept.empty()) out.push_back(std::move(kept));
        co_return out;
    }
    std::string name() const override { return "dedupe"; }

private:
    cache_ptr    _cache;
    transform_fn _key;
    bool         _drop_on_err;
    exec_ctx     _ctx;
};

class log_processor final : public processor {
public:
    log_processor(std::string level, transform_fn t)
        : _level(std::move(level)), _t(std::move(t)) {}

    seastar::future<std::vector<batch>> process(batch b, seastar::abort_source&) override {
        for (const auto& msg : b) {
            std::string line;
            try {
                // A message that is not JSON is not an error here: `content()`
                // and `meta()` do not need a parsed document, and the
                // reference's own example logs raw content. Parsing eagerly and
                // letting the failure escape turned every non-JSON message into
                // "<log mapping failed>". This is the shape `switch` and
                // `dedupe` already use.
                exec_ctx ctx;
                value parsed;
                bool structured = true;
                try { parsed = msg.as_structured(); }
                catch (const eval_error&) { structured = false; }
                ctx.this_v  = structured ? &parsed : nullptr;
                ctx.msg     = &msg;
                ctx.meta_in = &msg.meta();
                line = _t(ctx).to_display_string();
            } catch (const std::exception& e) {
                line = std::string("<log mapping failed: ") + e.what() + ">";
            }
            if      (_level == "ERROR") plog.error("{}", line);
            else if (_level == "WARN")  plog.warn("{}", line);
            else if (_level == "DEBUG") plog.debug("{}", line);
            else                        plog.info("{}", line);
        }
        std::vector<batch> out;
        out.push_back(std::move(b));
        return seastar::make_ready_future<std::vector<batch>>(std::move(out));
    }
    std::string name() const override { return "log"; }
private:
    std::string  _level;
    transform_fn _t;
};

// `rate_limit`: hold the batch until the named limit permits it.
//
// Waiting is the entire behaviour, so the abort source matters more than usual:
// a limit of one per minute would otherwise hold a shutdown for a minute. An
// aborted wait passes the batch through rather than dropping it -- the messages
// have been accepted and dropping them at shutdown would be silent loss.
class rate_limit_processor final : public processor {
public:
    explicit rate_limit_processor(rate_limit_ptr limit) : _limit(std::move(limit)) {}

    seastar::future<std::vector<batch>> process(batch b, seastar::abort_source& as) override {
        // The return value is deliberately ignored: an abandoned wait means
        // shutdown, and the batch is forwarded rather than dropped -- the same
        // choice `sleep` makes, and for the same reason.
        (void)co_await rate_limit_wait(_limit, as);
        std::vector<batch> out;
        out.push_back(std::move(b));
        co_return out;
    }
    std::string name() const override { return "rate_limit"; }

private:
    rate_limit_ptr _limit;
};

class sleep_processor final : public processor {
public:
    explicit sleep_processor(std::chrono::milliseconds d) : _d(d) {}
    seastar::future<std::vector<batch>> process(batch b, seastar::abort_source& as) override {
        try { co_await seastar::sleep_abortable(_d, as); }
        // Intentionally empty: an aborted sleep means shutdown, and the batch is
        // still forwarded below rather than dropped. Swallowing it here is what
        // makes `sleep` interruptible without losing a message.
        catch (const seastar::sleep_aborted&) {}
        std::vector<batch> out;
        out.push_back(std::move(b));
        co_return out;
    }
    std::string name() const override { return "sleep"; }
private:
    std::chrono::milliseconds _d;
};

// `try`: run the children in order, stopping at the first failure. Messages that
// fail carry the error onward rather than being dropped.
class try_processor final : public processor {
public:
    try_processor(std::vector<processor_ptr> children, std::string label, std::string path)
        : _children(std::move(children)), _label(std::move(label)), _path(std::move(path)) {}

    seastar::future<std::vector<batch>> process(batch b, seastar::abort_source& as) override {
        batch backup;
        backup.reserve(b.size());
        for (const auto& m : b) backup.push_back(m.shallow_copy());
        try {
            co_return co_await run_chain(_children, std::move(b), as);
        } catch (const std::exception& e) {
            auto info = std::make_shared<const error_source_info>(
                error_source_info{"try", _label, _path});
            for (auto& m : backup) m.set_error(e.what(), info);
            std::vector<batch> out;
            out.push_back(std::move(backup));
            co_return out;
        }
    }
    seastar::future<> close() override {
        // Guarded per child: one throwing close must not strand the others.
        for (auto& c : _children)
            try { co_await c->close(); }
            catch (const std::exception& e) { plog.warn("close failed: {}", e.what()); }
    }
    std::string name() const override { return "try"; }
private:
    std::vector<processor_ptr> _children;
    std::string                _label;
    std::string                _path;
};

// `catch`: children run only on messages that are already carrying an error, and
// the error is cleared afterwards. Messages without an error pass straight through.
class catch_processor final : public processor {
public:
    explicit catch_processor(std::vector<processor_ptr> children) : _children(std::move(children)) {}

    seastar::future<std::vector<batch>> process(batch b, seastar::abort_source& as) override {
        batch failed, ok;
        for (auto& m : b) (m.has_error() ? failed : ok).push_back(std::move(m));

        std::vector<batch> out;
        if (!ok.empty()) out.push_back(std::move(ok));
        if (!failed.empty()) {
            for (auto& m : failed) m.clear_error();
            auto recovered = co_await run_chain(_children, std::move(failed), as);
            for (auto& rb : recovered) if (!rb.empty()) out.push_back(std::move(rb));
        }
        co_return out;
    }
    seastar::future<> close() override {
        // Guarded per child: one throwing close must not strand the others.
        for (auto& c : _children)
            try { co_await c->close(); }
            catch (const std::exception& e) { plog.warn("close failed: {}", e.what()); }
    }
    std::string name() const override { return "catch"; }
private:
    std::vector<processor_ptr> _children;
};

// `switch`: the first case whose check passes handles the message. Messages are
// regrouped per case, so each case sees a batch rather than single messages.
class switch_processor final : public processor {
public:
    struct case_ {
        transform_fn               check;
        std::vector<processor_ptr> children;
    };
    // `one_batch_in_order` is what separates `switch` from `group_by`, and they
    // are NOT the same processor however alike their config looks.
    //
    //   switch   (processor_switch.go:170-245) collects every case's output into
    //            ONE batch and calls SwitchReorderFromGroup, so the batch leaves
    //            in the order it arrived.
    //   group_by (processor_group_by.go:127-176) returns N SEPARATE batches, one
    //            per group in group order, with the unmatched messages last.
    //
    // The routing above is genuinely shared; only the assembly differs. `switch`
    // was registered as a clone of `group_by`'s implementation, so it re-ordered
    // and re-batched every message it touched.
    switch_processor(std::vector<case_> cases, bool one_batch_in_order)
        : _cases(std::move(cases)), _one_batch(one_batch_in_order) {}

    seastar::future<std::vector<batch>> process(batch b, seastar::abort_source& as) override {
        std::vector<batch> buckets(_cases.size());
        // Where each bucketed message sat in the incoming batch, so `switch` can
        // put the result back in that order. Parallel to `buckets`.
        std::vector<std::vector<size_t>> origin(_cases.size());
        std::vector<size_t> unmatched_origin;
        batch unmatched;
        size_t index = 0;
        for (auto& m : b) {
            const size_t here = index++;
            // Parse once per message, not once per case. A message that cannot
            // be parsed matches nothing and falls through untouched.
            exec_ctx ctx;
            value parsed;
            bool structured = true;
            try { parsed = m.as_structured(); }
            catch (const eval_error&) { structured = false; }
            ctx.this_v = structured ? &parsed : nullptr;
            // Routing only reads metadata -- there is no output message being
            // built here -- so the read source and the write target are the
            // same object.
            ctx.meta = &m.meta();
            ctx.meta_in = &m.meta();

            bool placed = false;
            for (size_t i = 0; i < _cases.size() && !placed; ++i) {
                try {
                    if (truthy(_cases[i].check(ctx))) {
                        buckets[i].push_back(std::move(m));
                        origin[i].push_back(here);
                        placed = true;
                    }
                } catch (const std::exception&) { /* a failing check does not match */ }
            }
            if (!placed) { unmatched.push_back(std::move(m)); unmatched_origin.push_back(here); }
        }

        std::vector<batch> out;
        // For `switch`: every result message paired with where it should sit.
        std::vector<std::pair<size_t, message>> ordered;

        const auto collect = [&](std::vector<batch> res, const std::vector<size_t>& from) {
            if (!_one_batch) {
                for (auto& rb : res) if (!rb.empty()) out.push_back(std::move(rb));
                return;
            }
            size_t produced = 0;
            for (auto& rb : res) produced += rb.size();
            size_t k = 0;
            for (auto& rb : res)
                for (auto& m : rb) {
                    // 1:1 while the chain neither split nor filtered, which is
                    // the case the ordering contract is about. When the count
                    // changed there is no message this one came from, so it
                    // takes the position of the LAST input that fed the chain
                    // and a stable sort keeps such messages adjacent, in the
                    // order the chain produced them. The reference reaches the
                    // same arrangement by a different route: it tags each part
                    // and sorts untagged ones by their current position.
                    const size_t at = (produced == from.size() && k < from.size())
                                          ? from[k]
                                          : (from.empty() ? 0 : from.back());
                    ordered.emplace_back(at, std::move(m));
                    ++k;
                }
        };

        for (size_t i = 0; i < _cases.size(); ++i) {
            if (buckets[i].empty()) continue;
            auto res = co_await run_chain(_cases[i].children, std::move(buckets[i]), as);
            collect(std::move(res), origin[i]);
        }
        // Messages matching no case pass through untouched, as Benthos does.
        if (!unmatched.empty()) {
            if (_one_batch) {
                for (size_t k = 0; k < unmatched.size(); ++k)
                    ordered.emplace_back(unmatched_origin[k], std::move(unmatched[k]));
            } else {
                out.push_back(std::move(unmatched));
            }
        }

        if (_one_batch) {
            std::stable_sort(ordered.begin(), ordered.end(),
                             [](const auto& l, const auto& r) { return l.first < r.first; });
            batch single;
            single.reserve(ordered.size());
            for (auto& [_, m] : ordered) single.push_back(std::move(m));
            if (!single.empty()) out.push_back(std::move(single));
        }
        co_return out;
    }
    seastar::future<> close() override {
        for (auto& c : _cases)
            for (auto& p : c.children)
                try { co_await p->close(); }
                catch (const std::exception& e) { plog.warn("close failed: {}", e.what()); }
    }
    std::string name() const override { return _one_batch ? "switch" : "group_by"; }
private:
    std::vector<case_> _cases;
    bool               _one_batch;
};

// `for_each`: children see one message at a time, so batch-aware processors
// inside cannot see across the batch. Results are recombined into one batch.
class for_each_processor final : public processor {
public:
    explicit for_each_processor(std::vector<processor_ptr> children)
        : _children(std::move(children)) {}

    seastar::future<std::vector<batch>> process(batch b, seastar::abort_source& as) override {
        batch combined;
        for (auto& m : b) {
            batch one;
            one.push_back(std::move(m));
            auto res = co_await run_chain(_children, std::move(one), as);
            for (auto& rb : res)
                for (auto& rm : rb) combined.push_back(std::move(rm));
        }
        std::vector<batch> out;
        if (!combined.empty()) out.push_back(std::move(combined));
        co_return out;
    }
    seastar::future<> close() override {
        // Guarded per child: one throwing close must not strand the others.
        for (auto& c : _children)
            try { co_await c->close(); }
            catch (const std::exception& e) { plog.warn("close failed: {}", e.what()); }
    }
    std::string name() const override { return "for_each"; }
private:
    std::vector<processor_ptr> _children;
};

} // namespace

processor_ptr make_noop_processor() { return std::make_unique<noop_processor>(); }
processor_ptr make_dedupe_processor(cache_ptr c, transform_fn key, bool drop_on_err) {
    // The cache is per shard, so a multi-shard pipeline would deduplicate
    // within each core and pass duplicates between them -- wrong in a way that
    // only appears under load and never as an error. Refused by name instead;
    // the project's rule is that an unimplemented behaviour is a named failure,
    // not a silent approximation.
    if (seastar::this_smp().shard_count() > 1)
        throw std::runtime_error(
            "dedupe needs a cache shared across shards, which swordfish does not have yet; "
            "run with --smp 1, or shard the input by the dedupe key");
    return std::make_unique<dedupe_processor>(std::move(c), std::move(key), drop_on_err);
}

processor_ptr make_workflow_processor(
        std::vector<std::vector<std::pair<std::string, processor_ptr>>> order,
        std::vector<std::string> meta_path) {
    std::vector<std::vector<workflow_processor::step>> groups;
    groups.reserve(order.size());
    for (auto& g : order) {
        std::vector<workflow_processor::step> steps;
        steps.reserve(g.size());
        for (auto& [nm, br] : g) steps.push_back({std::move(nm), std::move(br)});
        groups.push_back(std::move(steps));
    }
    return std::make_unique<workflow_processor>(std::move(groups), std::move(meta_path));
}

processor_ptr make_branch_processor(transform_fn request, std::vector<processor_ptr> children,
                                   transform_fn result) {
    return std::make_unique<branch_processor>(std::move(request), std::move(children),
                                              std::move(result));
}

processor_ptr make_codec_processor(std::string algorithm, int level, bool compressing) {
    return std::make_unique<codec_processor>(std::move(algorithm), level, compressing);
}
processor_ptr make_archive_processor(std::string format) {
    return std::make_unique<archive_processor>(std::move(format));
}

processor_ptr make_select_parts_processor(std::vector<int64_t> parts) {
    return std::make_unique<select_parts_processor>(std::move(parts));
}
processor_ptr make_insert_part_processor(int64_t index, transform_fn content) {
    return std::make_unique<insert_part_processor>(index, std::move(content));
}
processor_ptr make_bounds_check_processor(int64_t max_part, int64_t min_part,
                                          int64_t max_parts, int64_t min_parts) {
    return std::make_unique<bounds_check_processor>(max_part, min_part, max_parts, min_parts);
}
processor_ptr make_split_processor(int64_t size, int64_t byte_size) {
    return std::make_unique<split_processor>(size, byte_size);
}

processor_ptr make_unarchive_processor(std::string format) {
    return std::make_unique<unarchive_processor>(std::move(format));
}
processor_ptr make_rate_limit_processor(rate_limit_ptr limit) {
    return std::make_unique<rate_limit_processor>(std::move(limit));
}

processor_ptr make_log_processor(std::string level, transform_fn t) {
    return std::make_unique<log_processor>(std::move(level), std::move(t));
}
processor_ptr make_sleep_processor(std::chrono::milliseconds d) {
    return std::make_unique<sleep_processor>(d);
}
processor_ptr make_try_processor(std::vector<processor_ptr> c,
                                 std::string label, std::string path) {
    return std::make_unique<try_processor>(std::move(c), std::move(label), std::move(path));
}
processor_ptr make_catch_processor(std::vector<processor_ptr> c) {
    return std::make_unique<catch_processor>(std::move(c));
}
processor_ptr make_for_each_processor(std::vector<processor_ptr> c) {
    return std::make_unique<for_each_processor>(std::move(c));
}
namespace {
std::vector<switch_processor::case_> to_cases(
        std::vector<std::pair<transform_fn, std::vector<processor_ptr>>> cases) {
    std::vector<switch_processor::case_> cs;
    for (auto& [chk, ch] : cases)
        cs.push_back(switch_processor::case_{std::move(chk), std::move(ch)});
    return cs;
}
} // namespace

processor_ptr make_group_by_processor(
        std::vector<std::pair<transform_fn, std::vector<processor_ptr>>> cases) {
    return std::make_unique<switch_processor>(to_cases(std::move(cases)), false);
}

processor_ptr make_switch_processor(
        std::vector<std::pair<transform_fn, std::vector<processor_ptr>>> cases) {
    return std::make_unique<switch_processor>(to_cases(std::move(cases)), true);
}

} // namespace sf
