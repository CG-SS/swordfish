// File and stdin/stdout components, on Seastar's own I/O.
#include <map>
#include "swordfish/runtime/components.hh"
#include "swordfish/runtime/transaction.hh"

#include <seastar/core/when_any.hh>
#include "swordfish/runtime/scanner.hh"

#include <seastar/core/condition-variable.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sleep.hh>

#include <algorithm>
#include <chrono>
#include <deque>
#include <fnmatch.h>
#include <filesystem>

#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/log.hh>

#include <unistd.h>

namespace sf {

static seastar::logger iolog("sf.io");

namespace {

// One read() worth of stdin. Large enough that the thread hop per chunk is
// amortised, small enough not to stall the shard while it is copied back.
constexpr size_t read_chunk_bytes = 64 * 1024;

// Shared framing loop: pull chunks, scan them into messages, hand them out one
// batch at a time. `file` and `stdin` differ only in where the chunks come from.
class scanned_input : public input {
public:
    explicit scanned_input(scanner_ptr sc, size_t batch_size)
        : _scanner(std::move(sc)), _batch_size(batch_size) {}

    seastar::future<std::optional<std::pair<batch, ack_fn>>>
    read_batch(seastar::abort_source& as) override {
        while (_ready.empty()) {
            if (_eof) co_return std::nullopt;
            auto chunk = co_await next_chunk(as);
            if (chunk.empty()) {
                _eof = true;
                for (auto& m : _scanner->finish()) _ready.push_back(std::move(m));
                break;
            }
            for (auto& m : _scanner->feed(chunk)) _ready.push_back(std::move(m));
        }
        if (_ready.empty()) co_return std::nullopt;

        batch b;
        const size_t n = std::min(_batch_size, _ready.size());
        b.reserve(n);
        for (size_t i = 0; i < n; ++i) b.push_back(std::move(_ready[i]));
        _ready.erase(_ready.begin(), _ready.begin() + n);
        co_return std::make_pair(std::move(b), noop_ack());
    }

protected:
    virtual seastar::future<std::string> next_chunk(seastar::abort_source&) = 0;

private:
    scanner_ptr          _scanner;
    size_t               _batch_size;
    std::vector<message> _ready;
    bool                 _eof = false;
};

class file_input final : public scanned_input {
public:
    file_input(std::string path, scanner_ptr sc, size_t batch_size)
        : scanned_input(std::move(sc), batch_size), _path(std::move(path)) {}

    seastar::future<> connect(seastar::abort_source&) override {
        auto f = co_await seastar::open_file_dma(_path, seastar::open_flags::ro);
        // emplace() move-CONSTRUCTS. Seastar deprecates move-assigning a stream
        // because the default operator would not deal with an existing buffer or
        // an in-flight read.
        _in.emplace(seastar::make_file_input_stream(std::move(f)));
    }

    seastar::future<> close() override {
        if (_in) { auto in = std::move(*_in); _in.reset(); co_await in.close(); }
    }

protected:
    seastar::future<std::string> next_chunk(seastar::abort_source&) override {
        if (!_in) co_return std::string{};
        auto buf = co_await _in->read();
        co_return std::string(buf.get(), buf.size());
    }

private:
    std::string                                _path;
    std::optional<seastar::input_stream<char>> _in;
};

// stdin is not a reactor-managed descriptor and may be a terminal, a pipe or a
// file. Seastar has no portable async stdin, so reads happen on a Seastar thread
// -- the documented way to run blocking work without stalling a shard.
class stdin_input final : public scanned_input {
public:
    stdin_input(scanner_ptr sc, size_t batch_size)
        : scanned_input(std::move(sc), batch_size) {}

protected:
    seastar::future<std::string> next_chunk(seastar::abort_source& as) override {
        if (as.abort_requested()) return seastar::make_ready_future<std::string>(std::string{});
        return seastar::async([] {
            std::string buf(read_chunk_bytes, '\0');
            ssize_t n = ::read(0, buf.data(), buf.size());
            if (n <= 0) return std::string{};
            buf.resize(static_cast<size_t>(n));
            return buf;
        });
    }
};

// The `path` is INTERPOLATED, as the reference's is: it "supports interpolation
// functions", and splitting a stream into a file per key is the main reason to
// use this output at all. It was taken literally, so
// `path: 'out/${! json("id") }.txt'` wrote one file with that name in it --
// three messages into `out/a.txt`, `out/b.txt`, `out/c.txt` from the reference
// against a single `out/${! json("id") }.txt` here -- and no error either way.
//
// A dynamic path therefore keeps a stream per resolved name, opened on first use
// and held until close(). A static one still opens once in connect(), which is
// what makes an unwritable path fail at start-up rather than on the first
// message.
class file_output final : public output {
public:
    // `path_fn` is empty for a path with no interpolation in it.
    file_output(std::string path, transform_fn path_fn)
        : _path(std::move(path)), _path_fn(std::move(path_fn)) {}

    seastar::future<> connect(seastar::abort_source&) override {
        if (_path_fn) co_return;             // resolved per message instead
        _streams.emplace(_path, co_await open_stream(_path));
    }

    seastar::future<> write_batch(batch b, seastar::abort_source&) override {
        for (auto& m : b) {
            std::string path = _path;
            if (_path_fn) {
                exec_ctx ctx;
                value parsed;
                bool structured = true;
                try { parsed = m.as_structured(); } catch (...) { structured = false; }
                ctx.this_v  = structured ? &parsed : nullptr;
                ctx.msg     = &m;
                ctx.meta_in = &m.meta();
                path = _path_fn(ctx).to_display_string();
            }
            auto it = _streams.find(path);
            if (it == _streams.end()) {
                if (!_path_fn) co_return;    // connect() never opened it
                it = _streams.emplace(path, co_await open_stream(path)).first;
            }
            // One write per message rather than two: the newline is appended to
            // the payload so the stream sees a single buffer.
            std::string line;
            append_line(line, m.as_bytes());
            co_await it->second.write(std::move(line));
        }
    }

    seastar::future<> close() override {
        if (_closed) co_return;
        _closed = true;
        // In place, not via a move. A FILE stream is not batch-flushed and so
        // survives being moved, but a network one does not: output_stream's
        // move constructor leaves its `_in_poller` intrusive hook on the source
        // object, which then asserts when destroyed. Written the same way here
        // so the pattern nobody can safely copy is not on display.
        //
        // Guarded per stream: one file that fails to flush must not strand the
        // rest, which with a dynamic path may be hundreds.
        for (auto& [name, st] : _streams)
            try { co_await st.close(); }     // close() flushes first
            catch (const std::exception& e) {
                iolog.warn("file output '{}' failed to close: {}", name, e.what());
            }
        _streams.clear();
    }

private:
    static seastar::future<seastar::output_stream<char>> open_stream(const std::string& path) {
        auto f = co_await seastar::open_file_dma(
            path, seastar::open_flags::wo | seastar::open_flags::create
                | seastar::open_flags::truncate);
        co_return co_await seastar::make_file_output_stream(std::move(f));
    }

    std::string                                          _path;
    transform_fn                                         _path_fn;
    std::map<std::string, seastar::output_stream<char>>  _streams;
    bool                                                 _closed = false;
};


// ---- the `memory` buffer -------------------------------------------------------
//
// A fill loop reads the inner input as fast as the limit allows, ACKS THE SOURCE
// AT ONCE and parks the payload; read_batch() serves from the parked messages.
// That inversion -- acknowledge on arrival rather than on delivery -- is the
// whole of what a buffer is, and is why it weakens delivery guarantees.

// ---- input-side batching -------------------------------------------------------
//
// A fill fiber reads the inner input one batch ahead; read_batch() draws
// messages out of what it has parked until the policy triggers, then hands over
// one assembled batch. Reading ahead is bounded to a single batch, so back
// pressure still reaches the source -- this is not a buffer and does not
// pretend to be one.
//
// The acks are the interesting part. A source batch may straddle a trigger and
// so contribute to two assembled batches; it must not be acked until BOTH have
// landed, and it must be nacked if either fails. One ack_group per source batch
// does exactly that: a child per assembled batch it fed, sealed once its last
// message has been drawn.
class batched_input final : public input {
    struct parked {
        batch                                  b;
        seastar::lw_shared_ptr<ack_group>      group;
        // Which assembled batch last took a child from this source, so one
        // child is charged per assembly rather than one per message.
        size_t                                 charged_for = 0;
        // How many of `b` have been drawn. A CURSOR rather than erase-from-front:
        // `b` is a std::vector, so taking the front of it shifts everything after
        // it, and a 10k-message source batch -- an ordinary Kafka poll -- cost
        // ~50M message moves inside one reactor task, well past seastar's
        // blocked-reactor threshold.
        size_t                                 taken = 0;
    };

public:
    batched_input(input_ptr inner, batch_policy policy, std::vector<processor_ptr> procs)
        : _inner(std::move(inner)), _policy(std::move(policy)), _procs(std::move(procs)) {}

    seastar::future<> connect(seastar::abort_source& as) override {
        co_await _inner->connect(as);
        // Kept so flush() can hand the REAL abort source to the batching
        // processors. It used to pass `_dummy_abort`, which nothing ever aborts,
        // so a `sleep` or `rate_limit` in `input.broker.batching.processors` ran
        // to its full duration during shutdown and sailed straight past
        // `shutdown_timeout`. The same processor under `output.batching` is
        // interruptible, so the two sides disagreed about what a signal means.
        _as = &as;
        _abort_sub = as.subscribe([this]() noexcept { _filled.broken(); _drawn.broken(); });
        (void)seastar::with_gate(_filling, [this, &as] { return fill_loop(as); });
    }

    seastar::future<std::optional<std::pair<batch, ack_fn>>>
    read_batch(seastar::abort_source& as) override {
        bool timed_out = false;
        for (;;) {
            bool trigger = false;
            // Whether this pass actually took anything out of `_pending`, which
            // is the only thing the fill fiber is waiting to hear about. It used
            // to be signalled unconditionally, and reader and filler shared ONE
            // condition variable, so when `_pending` was empty -- the ordinary
            // idle state, with the filler parked inside the source's read rather
            // than on the cv -- the signal found no waiter, set the flag, and
            // the reader's own wait() consumed it and went round again. An idle
            // pipeline burned about 70% of a core.
            bool drew = false;
            while (!_pending.empty() && !trigger) {
                auto& p = _pending.front();
                if (p.charged_for != _assembly) {
                    p.charged_for = _assembly;
                    _children.push_back(p.group->add_child());
                }
                while (p.taken < p.b.size() && !trigger) {
                    message m = std::move(p.b[p.taken++]);
                    drew = true;
                    trigger = _policy.add(std::move(m));
                }
                if (p.taken == p.b.size()) {
                    auto g = p.group;
                    _pending.pop_front();
                    drew = true;
                    // Sealed only once its last message has been drawn: until
                    // then another assembled batch might still take a child.
                    co_await g->seal();
                }
            }
            if (drew) _drawn.signal();
            if (trigger) {
                auto out = co_await flush();
                if (out) co_return out;
                continue;
            }
            if (_source_done && _pending.empty()) {
                // The leftover, flushed before end of input is reported. Note
                // that this is only REACHED when the source actually reports
                // exhaustion; an auto-replaying source will not do so while the
                // held batch's ack is outstanding, so a policy with no `period`
                // still hangs -- as it does on the output side and as it does in
                // the reference. See make_batched_input's comment.
                if (!_policy.empty()) {
                    auto out = co_await flush();
                    if (out) co_return out;
                }
                co_return std::nullopt;
            }
            if (as.abort_requested()) co_return std::nullopt;
            try {
                const auto until = _policy.until_next();
                if (!_policy.empty() && until.count() > 0)
                    co_await _filled.wait(std::chrono::steady_clock::now() + until);
                else
                    co_await _filled.wait();
            } catch (const seastar::condition_variable_timed_out&) {
                timed_out = true;                 // no co_await inside a handler
            } catch (...) {
                co_return std::nullopt;           // broken: shutting down
            }
            if (timed_out) {
                timed_out = false;
                if (!_policy.empty()) {
                    auto out = co_await flush();
                    if (out) co_return out;
                }
            }
        }
    }

    seastar::future<> close() override {
        _filled.broken();
        _drawn.broken();
        co_await _filling.close();
        co_await abandon();
        co_await _inner->close();
    }

    connection_status status() const override { return _inner->status(); }

private:
    // Everything drawn but not yet flushed, NACKED rather than destroyed.
    //
    // `_children` are live children of ack_groups charged for the assembly in
    // progress, and `_pending` holds whole source batches whose groups were
    // never sealed. Both were simply dropped in close(), so the source's ack_fn
    // was neither acked nor nacked: an http_server handler waiting on one was
    // never released and its client's connection hung until the socket timed out.
    seastar::future<> abandon() {
        auto e = std::make_exception_ptr(
            std::runtime_error("input batching: shut down with a partial batch"));
        auto children = std::move(_children);
        _children.clear();
        for (const auto& c : children)
            try { co_await c(e); }
            catch (const std::exception& ex) {
                iolog.warn("input batching: nacking a partial batch failed: {}", ex.what());
            }
        auto pending = std::move(_pending);
        _pending.clear();
        for (auto& p : pending) {
            // A child is taken before sealing, so a group that never got one
            // still resolves rather than waiting for a child that cannot come.
            try {
                auto c = p.group->add_child();
                co_await p.group->seal();
                co_await c(e);
            } catch (const std::exception& ex) {
                iolog.warn("input batching: nacking a parked batch failed: {}", ex.what());
            }
        }
    }

    // Takes what accumulated, runs the policy's processors over it, and pairs it
    // with an ack that resolves every source batch that fed it.
    seastar::future<std::optional<std::pair<batch, ack_fn>>> flush() {
        auto children = std::move(_children);
        _children.clear();
        ++_assembly;

        batch b = _policy.take();
        std::vector<batch> batches;
        if (!b.empty()) batches.push_back(std::move(b));
        std::exception_ptr err;
        for (const auto& p : _procs) {
            std::vector<batch> next;
            for (auto& cur : batches) {
                if (cur.empty()) continue;
                try {
                    auto out = co_await p->process(std::move(cur),
                                                   _as ? *_as : _dummy_abort);
                    for (auto& ob : out) if (!ob.empty()) next.push_back(std::move(ob));
                } catch (...) { err = std::current_exception(); }
            }
            if (err) break;
            batches = std::move(next);
        }

        auto resolve = [children = seastar::make_lw_shared<std::vector<ack_fn>>(
                            std::move(children))](std::exception_ptr e) -> seastar::future<> {
            for (const auto& c : *children) co_await c(e);
        };
        if (err) {
            // The processors failed, so nothing was delivered and every source
            // that fed this batch is nacked rather than left waiting -- which is
            // right for a TRANSIENT failure and, with `auto_replay_nacks` on by
            // default, a loop for a deterministic one. Nacking is still the
            // at-least-once answer, so the loop is made VISIBLE rather than
            // silent: without this line the pipeline never emits anything and
            // never terminates, with nothing in the log to say why.
            iolog.warn("input batching: the batching processors failed, so the "
                       "sources are nacked and will be offered again: {}", err);
            co_await resolve(err);
            co_return std::nullopt;
        }
        if (batches.empty()) {
            // Filtered away entirely, which is a successful outcome: the sources
            // are acked, and the caller loops rather than reporting end of input.
            co_await resolve(nullptr);
            co_return std::nullopt;
        }
        // A processor that split the batch is not expressible as one ack, so the
        // extra batches are folded back in rather than silently dropped.
        batch first = std::move(batches.front());
        for (size_t i = 1; i < batches.size(); ++i)
            for (auto& m : batches[i]) first.push_back(std::move(m));
        co_return std::make_pair(std::move(first), ack_fn(std::move(resolve)));
    }

    seastar::future<> fill_loop(seastar::abort_source& as) {
        for (;;) {
            // One batch read ahead at most: the source is not drained faster
            // than the pipeline consumes it.
            while (!_pending.empty() && !as.abort_requested()) {
                try { co_await _drawn.wait(); }
                catch (...) { co_return; }
            }
            if (as.abort_requested()) break;
            std::optional<std::pair<batch, ack_fn>> r;
            try {
                r = co_await _inner->read_batch(as);
            } catch (...) {
                iolog.warn("input batching: the source failed: {}",
                           std::current_exception());
                break;
            }
            if (!r) break;
            if (r->first.empty()) { co_await r->second(nullptr); continue; }
            _pending.push_back(parked{std::move(r->first),
                                      ack_group::make(std::move(r->second)), 0});
            _filled.signal();
        }
        _source_done = true;
        _filled.broadcast();
    }

    input_ptr                  _inner;
    batch_policy               _policy;
    std::vector<processor_ptr> _procs;
    std::deque<parked>         _pending;
    std::vector<ack_fn>        _children;
    size_t                     _assembly = 1;
    bool                       _source_done = false;
    // Two, not one: the reader waits for the filler to park a batch, the
    // filler waits for the reader to draw one out. Sharing a single cv let
    // the reader consume the signal it had just posted for the filler.
    seastar::condition_variable _filled;   // filler -> reader
    seastar::condition_variable _drawn;    // reader -> filler
    seastar::gate              _filling;
    seastar::abort_source*     _as = nullptr;    // the stream's, set at connect()
    seastar::abort_source      _dummy_abort;     // only until connect() runs
    seastar::optimized_optional<seastar::abort_source::subscription> _abort_sub;
};

// ---- input: processors beside the kind -------------------------------------
// `input: { generate: {...}, processors: [...] }`. The reference gives every
// input one of these, and it runs on what the input produced before the batch
// reaches the pipeline layer.
//
// A decorator rather than a fourth layer, for the reason the buffer is one: what
// it changes is the SHAPE of what the input hands over, not the wiring. It has
// to hold a queue because processors SPLIT -- `split` or `unarchive` turn one
// read into several batches -- and read_batch owes the caller exactly one. The
// source is acked once, when every batch it produced has been resolved, which is
// what ack_group expresses.
class processed_input final : public input {
public:
    processed_input(input_ptr inner, std::vector<processor_ptr> procs)
        : _inner(std::move(inner)), _procs(std::move(procs)) {}

    seastar::future<> connect(seastar::abort_source& as) override {
        return _inner->connect(as);
    }

    seastar::future<std::optional<std::pair<batch, ack_fn>>>
    read_batch(seastar::abort_source& as) override {
        for (;;) {
            if (!_ready.empty()) {
                auto r = std::move(_ready.front());
                _ready.pop_front();
                co_return std::move(r);
            }
            auto r = co_await _inner->read_batch(as);
            if (!r) co_return std::nullopt;
            if (r->first.empty()) { co_await r->second(nullptr); continue; }

            std::vector<batch> batches;
            batches.push_back(std::move(r->first));
            std::exception_ptr err;
            for (const auto& p : _procs) {
                std::vector<batch> next;
                for (auto& cur : batches) {
                    if (cur.empty()) continue;
                    try {
                        auto out = co_await p->process(std::move(cur), as);
                        for (auto& ob : out) if (!ob.empty()) next.push_back(std::move(ob));
                    } catch (...) { err = std::current_exception(); }
                }
                if (err) break;
                batches = std::move(next);
                if (batches.empty()) break;
            }
            if (err) {
                // Nothing was delivered, so the source is nacked and may send it
                // again -- the same choice the input-batching flush makes.
                iolog.warn("input processors failed: {}", err);
                co_await r->second(err);
                continue;
            }
            if (batches.empty()) {
                // Filtered away entirely. A success, not an end of input: the
                // source is acked and the loop asks for the next batch.
                co_await r->second(nullptr);
                continue;
            }
            auto group = ack_group::make(std::move(r->second));
            std::vector<std::pair<batch, ack_fn>> out;
            out.reserve(batches.size());
            for (auto& b : batches) out.emplace_back(std::move(b), group->add_child());
            co_await group->seal();
            for (size_t i = 1; i < out.size(); ++i) _ready.push_back(std::move(out[i]));
            co_return std::move(out.front());
        }
    }

    seastar::future<> close() override {
        for (auto& p : _procs)
            try { co_await p->close(); }
            catch (const std::exception& e) { iolog.warn("input processor close failed: {}", e.what()); }
        co_await _inner->close();
    }

    connection_status status() const override { return _inner->status(); }

private:
    input_ptr                                    _inner;
    std::vector<processor_ptr>                   _procs;
    std::deque<std::pair<batch, ack_fn>>         _ready;
};

class buffered_input final : public input {
public:
    buffered_input(input_ptr inner, int64_t limit, batch_policy policy,
                   std::vector<processor_ptr> procs, bool batching)
        : _inner(std::move(inner)), _limit(limit), _policy(std::move(policy)),
          _procs(std::move(procs)), _batching(batching) {}

    seastar::future<> connect(seastar::abort_source& as) override {
        co_await _inner->connect(as);
        // The real abort source, kept for flush(): the batch policy's processors
        // used to get `_dummy_abort`, which nothing ever aborts, so a `sleep` or
        // `rate_limit` among them ran to its full duration during shutdown and
        // went straight past `shutdown_timeout`.
        _as = &as;
        _abort_sub = as.subscribe([this]() noexcept {
            _cv.broken();
        });
        (void)seastar::with_gate(_filling, [this, &as] { return fill_loop(as); });
    }

    seastar::future<std::optional<std::pair<batch, ack_fn>>>
    read_batch(seastar::abort_source& as) override {
        bool timed_out = false;
        for (;;) {
            // Already through the policy and its processors; hand it straight on.
            if (!_ready.empty()) {
                auto b = std::move(_ready.front());
                _ready.pop_front();
                co_return std::make_pair(std::move(b), noop_ack());
            }
            if (!_parked.empty()) {
                if (!_batching) {
                    // No policy: one written batch out per read, unchanged.
                    auto b = std::move(_parked.front());
                    _parked.pop_front();
                    _bytes -= size_of(b);
                    _cv.signal();
                    co_return std::make_pair(std::move(b), noop_ack());
                }
                // With a policy, messages are drawn from the parked batches
                // until it triggers, so one output batch may span several
                // written ones -- which is what the reference does.
                bool trigger = false;
                // `_front_taken` is a CURSOR into the front batch, for the reason
                // batched_input keeps one: a batch is a std::vector, so drawing
                // its front shifts everything behind it and a large source batch
                // cost O(n^2) message moves in a single reactor task.
                while (!_parked.empty() && !trigger) {
                    auto& front = _parked.front();
                    while (_front_taken < front.size() && !trigger) {
                        message m = std::move(front[_front_taken++]);
                        _bytes -= static_cast<int64_t>(m.as_bytes().size());
                        trigger = _policy.add(std::move(m));
                    }
                    if (_front_taken == front.size()) {
                        _parked.pop_front();
                        _front_taken = 0;
                    }
                }
                _cv.signal();
                if (trigger) {
                    auto out = co_await flush();
                    if (out) co_return out;
                    continue;
                }
            }
            if (_source_done && _parked.empty() && _ready.empty()) {
                // Everything written has been drawn; a partial batch still
                // counts, which is the "best attempt at flushing all remaining
                // messages" the reference makes on shutdown.
                if (_batching && !_policy.empty()) {
                    auto out = co_await flush();
                    if (out) co_return out;
                }
                co_return std::nullopt;
            }
            if (as.abort_requested()) co_return std::nullopt;
            try {
                // A period trigger is the only one that fires with nothing
                // arriving, so the wait is bounded by it when one is set.
                const auto until = _batching ? _policy.until_next()
                                             : std::chrono::milliseconds{0};
                if (_batching && !_policy.empty() && until.count() > 0) {
                    co_await _cv.wait(std::chrono::steady_clock::now() + until);
                } else {
                    co_await _cv.wait();
                }
            } catch (const seastar::condition_variable_timed_out&) {
                // co_await is not permitted inside a handler, so the timeout is
                // recorded and the flush happens after it.
                timed_out = true;
            } catch (...) {
                co_return std::nullopt;      // broken: shutting down
            }
            if (timed_out) {
                timed_out = false;
                if (_batching && !_policy.empty()) {
                    auto out = co_await flush();
                    if (out) co_return out;
                }
            }
        }
    }

    seastar::future<> close() override {
        _cv.broken();
        co_await _filling.close();
        co_await _inner->close();
    }

    connection_status status() const override { return _inner->status(); }

private:
    static int64_t size_of(const batch& b) {
        int64_t n = 0;
        for (const auto& m : b) n += static_cast<int64_t>(m.as_bytes().size());
        return n;
    }

    // Runs the batch policy's processors over what accumulated. Returns nothing
    // when they filtered everything away, which is a success and not an end of
    // input -- so the caller loops rather than stopping.
    seastar::future<std::optional<std::pair<batch, ack_fn>>> flush() {
        batch b = _policy.take();
        if (b.empty()) co_return std::nullopt;
        std::vector<batch> batches;
        batches.push_back(std::move(b));
        // Guarded, as the input-batching flush is. A batch processor that
        // PROPAGATES rather than marks its failure used to escape all the way to
        // stream::input_loop's outer catch, which logs "input layer failed" and
        // ends the input layer for the life of the process -- taking with it
        // every message still parked in the buffer, all of which had already
        // been acked to the source and so could never be replayed. This batch is
        // lost either way; the rest of the buffer need not be.
        std::exception_ptr err;
        for (const auto& p : _procs) {
            std::vector<batch> next;
            for (auto& cur : batches) {
                if (cur.empty()) continue;
                try {
                    auto out = co_await p->process(std::move(cur),
                                                   _as ? *_as : _dummy_abort);
                    for (auto& ob : out) if (!ob.empty()) next.push_back(std::move(ob));
                } catch (...) { err = std::current_exception(); }
            }
            if (err) break;
            batches = std::move(next);
        }
        if (err) {
            // There is no source left to nack: a buffered message was acked on
            // arrival, which is the trade the buffer exists to make. So the
            // batch is dropped, loudly, and the buffer keeps running.
            iolog.error("buffer: dropping a batch whose batching processors failed: {}",
                        err);
            co_return std::nullopt;
        }
        if (batches.empty()) co_return std::nullopt;
        // The processors may split; anything past the first is queued for the
        // next read. It goes into _ready, NOT back into _parked: _parked is the
        // queue the policy draws FROM, so re-parking sent processor output back
        // through the policy and through the batching processors a second time
        // -- an `archive` would archive its own output -- and left `_bytes`
        // counting messages that had already been subtracted from it.
        batch first = std::move(batches.front());
        for (size_t i = 1; i < batches.size(); ++i)
            _ready.push_back(std::move(batches[i]));
        co_return std::make_pair(std::move(first), noop_ack());
    }

    seastar::future<> fill_loop(seastar::abort_source& as) {
        for (;;) {
            std::optional<std::pair<batch, ack_fn>> r;
            try {
                r = co_await _inner->read_batch(as);
            } catch (...) {
                iolog.warn("buffer: the source failed: {}", std::current_exception());
                break;
            }
            if (!r) break;
            const int64_t extra = size_of(r->first);
            if (extra > _limit) {
                // A batch bigger than the whole buffer can never fit, so waiting
                // for room would block for ever. It is ACKED rather than nacked:
                // a nack would be replayed by `auto_replay_nacks` into the same
                // impossible wait, which livelocks -- the first version did
                // exactly that, retrying one oversize message 75 times in ten
                // seconds. The reference drops it and runs on; swordfish does
                // the same and says so, loudly, once.
                iolog.error("buffer: dropping a batch of {} bytes that cannot fit a "
                            "limit of {}; raise `buffer.memory.limit`", extra, _limit);
                try { co_await r->second(nullptr); }
                catch (...) { iolog.warn("buffer: ack failed: {}",
                                         std::current_exception()); }
                continue;
            }
            // ACKED HERE, before it is parked and long before it is delivered.
            try { co_await r->second(nullptr); }
            catch (...) { iolog.warn("buffer: ack failed: {}", std::current_exception()); }

            while (_bytes + extra > _limit && !as.abort_requested()) {
                try { co_await _cv.wait(); }
                catch (...) { co_return; }
            }
            if (as.abort_requested()) break;
            _bytes += extra;
            _parked.push_back(std::move(r->first));
            _cv.signal();
        }
        _source_done = true;
        _cv.broadcast();
    }

    input_ptr                  _inner;
    int64_t                    _limit;
    batch_policy               _policy;
    std::vector<processor_ptr> _procs;
    bool                       _batching;
    std::deque<batch>          _parked;   // written, not yet through the policy
    size_t                     _front_taken = 0;  // cursor into _parked.front()
    std::deque<batch>          _ready;    // already through it: a split result
    int64_t                    _bytes = 0;
    bool                       _source_done = false;
    seastar::condition_variable _cv;
    seastar::gate              _filling;
    seastar::abort_source*     _as = nullptr;    // the stream's, set at connect()
    seastar::abort_source      _dummy_abort;     // only until connect() runs
    seastar::optimized_optional<seastar::abort_source::subscription> _abort_sub;
};

} // namespace

input_ptr make_file_input(std::string path, scanner_ptr sc, size_t batch_size) {
    return std::make_unique<file_input>(std::move(path), std::move(sc), batch_size);
}

input_ptr make_files_input(const std::vector<std::string>& paths,
                           const scanner_spec& scanner, size_t batch_size) {
    // Globs are expanded HERE, when the input is constructed on the shard that
    // will read it, rather than at parse time: a pattern matching files created
    // after start-up still resolves.
    std::vector<std::string> expanded;
    for (const auto& pat : paths) {
        if (pat.find_first_of("*?[") == std::string::npos) { expanded.push_back(pat); continue; }
        const std::filesystem::path pp(pat);
        const auto dir = pp.has_parent_path() ? pp.parent_path() : std::filesystem::path(".");
        std::error_code ec;
        std::vector<std::string> hits;
        for (std::filesystem::directory_iterator it(dir, ec), end; !ec && it != end;
             it.increment(ec))
            if (it->is_regular_file(ec) &&
                fnmatch(pp.filename().c_str(), it->path().filename().c_str(), 0) == 0)
                hits.push_back(it->path().string());
        // Sorted, so a glob reads in a stable order rather than whatever the
        // directory happens to return -- otherwise the same config produces a
        // different message order on each run.
        std::sort(hits.begin(), hits.end());
        for (auto& h : hits) expanded.push_back(std::move(h));
    }
    // The FILENAME is handed to make_scanner, which is what `switch` matches its
    // `re_match_name` against -- so each file in a glob may get a different
    // scanner, which is the whole point of that scanner existing.
    if (expanded.size() == 1)
        return make_file_input(expanded[0], make_scanner(scanner, expanded[0]), batch_size);
    // Several files read one after another, which is what `sequence` does;
    // reusing it keeps one implementation of "read these in order". An empty
    // expansion goes through here too and yields a source that ends at once,
    // which is what a glob matching nothing means.
    std::vector<input_ptr> kids;
    kids.reserve(expanded.size());
    for (const auto& f : expanded)
        kids.push_back(make_file_input(f, make_scanner(scanner, f), batch_size));
    return make_sequence_input(std::move(kids));
}
input_ptr make_processed_input(input_ptr inner, std::vector<processor_ptr> procs) {
    if (procs.empty()) return inner;
    return std::make_unique<processed_input>(std::move(inner), std::move(procs));
}

input_ptr make_batched_input(input_ptr inner, batch_policy policy,
                             std::vector<processor_ptr> batch_procs) {
    return std::make_unique<batched_input>(std::move(inner), std::move(policy),
                                           std::move(batch_procs));
}

input_ptr make_buffered_input(input_ptr inner, int64_t limit_bytes, bool batching,
                              batch_policy policy,
                              std::vector<processor_ptr> batch_procs) {
    return std::make_unique<buffered_input>(std::move(inner), limit_bytes,
                                            std::move(policy), std::move(batch_procs),
                                            batching);
}

input_ptr make_stdin_input(scanner_ptr sc, size_t batch_size) {
    return std::make_unique<stdin_input>(std::move(sc), batch_size);
}
output_ptr make_file_output(std::string path, transform_fn path_fn) {
    return std::make_unique<file_output>(std::move(path), std::move(path_fn));
}

} // namespace sf

namespace sf {

// `broker`: read from several inputs at once. `copies` instantiates each child
// N times, which is how Benthos scales a single source.
//
// Fan-in uses when_any rather than polling the children in turn: a round-robin
// read would block on an idle child while another already had data, which for a
// broker over two streaming sources serialises them.
class broker_input final : public input {
public:
    explicit broker_input(std::vector<input_ptr> children)
        : _children(std::move(children)),
          _pending(_children.size()),
          _done(_children.size(), false) {}

    seastar::future<> connect(seastar::abort_source& as) override {
        for (auto& c : _children) co_await c->connect(as);
    }

    seastar::future<std::optional<std::pair<batch, ack_fn>>>
    read_batch(seastar::abort_source& as) override {
        for (;;) {
            // Anything already in hand comes FIRST. when_any() can report several
            // children ready at once -- with `interval: 0s` they all are -- and
            // the extras are parked in _ready. Draining them here rather than
            // only on the path below is what stops them being thrown away when
            // the last child ends: `if (live.empty()) return nullopt` used to
            // discard whatever _ready still held, so a broker of two finite
            // `generate` inputs at count 2 delivered THREE messages and reported
            // success. Every child's messages had been read and acked; one was
            // simply dropped between the read and the return.
            if (!_ready.empty()) {
                auto v = std::move(_ready.front());
                _ready.erase(_ready.begin());
                co_return v;
            }
            // Give every live child an outstanding read, then wait for the
            // first to answer. A child that has ended keeps no pending future.
            std::vector<size_t> live;
            for (size_t i = 0; i < _children.size(); ++i) {
                if (_done[i]) continue;
                if (!_pending[i]) _pending[i] = _children[i]->read_batch(as);
                live.push_back(i);
            }
            if (live.empty()) co_return std::nullopt;      // every child exhausted

            std::vector<seastar::future<std::optional<std::pair<batch, ack_fn>>>> fs;
            fs.reserve(live.size());
            for (size_t i : live) fs.push_back(std::move(*_pending[i]));
            for (size_t i : live) _pending[i].reset();

            auto res = co_await seastar::when_any(fs.begin(), fs.end());
            // Put the futures that did NOT complete back, so their reads are
            // not restarted -- restarting would drop whatever they had in
            // flight, which for an at-least-once source means lost messages.
            size_t k = 0;
            std::optional<std::pair<batch, ack_fn>> got;
            bool got_set = false, failed = false;
            std::exception_ptr err;
            for (auto& f : res.futures) {
                const size_t idx = live[k++];
                if (!f.available()) { _pending[idx] = std::move(f); continue; }
                if (f.failed()) { if (!failed) { failed = true; err = f.get_exception(); } continue; }
                auto v = f.get();
                if (!v) { _done[idx] = true; continue; }   // that child ended
                if (!got_set) { got = std::move(v); got_set = true; }
                else          { _ready.push_back(std::move(*v)); }
            }
            if (got_set) co_return got;
            if (failed) std::rethrow_exception(err);
            // Everything that completed was an end-of-input; loop and re-check.
        }
    }

    seastar::future<> close() override {
        for (auto& c : _children)
            try { co_await c->close(); }
            catch (const std::exception& e) { iolog.warn("broker child close failed: {}", e.what()); }
    }

private:
    std::vector<input_ptr> _children;
    std::vector<std::optional<seastar::future<std::optional<std::pair<batch, ack_fn>>>>> _pending;
    std::vector<bool>      _done;
    std::vector<std::pair<batch, ack_fn>> _ready;
};

// `sequence`: read each input to exhaustion, in order.
class sequence_input final : public input {
public:
    explicit sequence_input(std::vector<input_ptr> children) : _children(std::move(children)) {}

    seastar::future<> connect(seastar::abort_source& as) override {
        // Only the first: a later input is connected when the run reaches it,
        // so a `sequence` of files does not hold every descriptor open at once.
        if (!_children.empty()) co_await _children[0]->connect(as);
    }

    seastar::future<std::optional<std::pair<batch, ack_fn>>>
    read_batch(seastar::abort_source& as) override {
        while (_at < _children.size()) {
            auto v = co_await _children[_at]->read_batch(as);
            if (v) co_return v;
            // Exhausted: close it before opening the next, so a long sequence
            // does not accumulate open sources.
            try { co_await _children[_at]->close(); }
            catch (const std::exception& e) { iolog.warn("sequence close failed: {}", e.what()); }
            if (++_at < _children.size()) co_await _children[_at]->connect(as);
        }
        co_return std::nullopt;
    }

    seastar::future<> close() override {
        for (size_t i = _at; i < _children.size(); ++i)
            try { co_await _children[i]->close(); }
            catch (const std::exception& e) { iolog.warn("sequence close failed: {}", e.what()); }
    }

private:
    std::vector<input_ptr> _children;
    size_t                 _at = 0;
};

// `auto_replay_nacks`: hold each batch until it is acked, and offer a nacked one
// again instead of dropping it. Modelled on
// benthos-main/internal/component/input/async_preserver.go and its autoretry
// list, including the two free attempts before a backoff starts -- which is what
// stops a permanently failing destination from becoming a busy loop.
//
// Retries take PRIORITY over new reads, so a failing destination applies back
// pressure to the source rather than accumulating an unbounded backlog.
class auto_retry_input final : public input {
    struct pending {
        batch  payload;
        ack_fn ack;                 // the source's own, called on success
        int    attempts = 0;
        // MICROseconds, not milliseconds: the reference's backoff starts at 1ms
        // and grows by 1.1x, and 1ms * 11 / 10 is 1ms in integer arithmetic --
        // a backoff that never backs off, which is a 1ms poll loop against a
        // permanently failing destination.
        std::chrono::microseconds backoff{retry_initial};
    };
    // Shared with every ack this input hands out, because an ack can outlive the
    // input: a write still in flight when the stream tears down resolves
    // afterwards, and a captured `this` would be dangling by then. The Kafka
    // input holds its consumer the same way and for the same reason.
    // Matching benthos-main/internal/autoretry: 1ms, x1.1, capped at 1s, with
    // the first two retries free of any wait.
    static constexpr std::chrono::microseconds retry_initial{1000};
    static constexpr std::chrono::microseconds retry_cap{1000000};

    struct state {
        std::vector<seastar::lw_shared_ptr<pending>> queue;
        size_t                     adopted = 0;   // handed out, not yet resolved
        seastar::condition_variable activity;
        // ONE read of the inner input, kept in flight. Without this the
        // "retries take priority" rule held only BETWEEN reads: a source whose
        // read_batch blocks -- Kafka polls its broker until records arrive --
        // left this coroutine parked inside the inner read while a nacked batch
        // sat in the queue behind it, and nothing ever went back to look.
        // Measured against a live broker: one record, an output that always
        // rejects, and after eight seconds `in=1 out=0 acks=0 nacks=1`. It was
        // read once, nacked once, and never replayed. The three built-ins this
        // wrapper was written for return promptly or end, which is why it
        // worked there and only there.
        //
        // The reference keeps its read in flight for the same reason and calls
        // it `pendingRead` (benthos internal/autoretry/auto_retry_list.go),
        // noting "Send is a lower priority than retry".
        bool                       reading = false;
        bool                       source_done = false;
        std::optional<std::pair<batch, ack_fn>> ready;   // a completed read
        std::exception_ptr         read_err;
        seastar::gate              reader;
    };

public:
    explicit auto_retry_input(input_ptr inner)
        : _inner(std::move(inner)), _st(seastar::make_lw_shared<state>()) {}

    seastar::future<> connect(seastar::abort_source& as) override {
        return _inner->connect(as);
    }

    seastar::future<std::optional<std::pair<batch, ack_fn>>>
    read_batch(seastar::abort_source& as) override {
        // Broken on abort so a wait for an outstanding ack cannot outlive the
        // drain. Subscribed once, lazily: read_batch is where the abort source
        // first becomes available.
        if (!_abort_sub && !as.abort_requested())
            _abort_sub = as.subscribe([st = _st]() noexcept { st->activity.broken(); });

        for (;;) {
            if (as.abort_requested()) co_return std::nullopt;

            // 1. A retry outranks everything, including a read that has already
            //    come back.
            if (!_st->queue.empty()) {
                auto p = std::move(_st->queue.front());
                _st->queue.erase(_st->queue.begin());
                if (++p->attempts > 2) {
                    try { co_await seastar::sleep_abortable(p->backoff, as); }
                    catch (const seastar::sleep_aborted&) { co_return std::nullopt; }
                    p->backoff = std::min(retry_cap, p->backoff * 11 / 10);
                }
                co_return std::make_pair(copy_of(p->payload), wrap(p));
            }

            // 2. A read that finished while we were busy.
            if (_st->ready) {
                auto r = std::move(*_st->ready);
                _st->ready.reset();
                auto p = seastar::make_lw_shared<pending>(
                    pending{std::move(r.first), std::move(r.second), 0, retry_initial});
                ++_st->adopted;
                co_return std::make_pair(copy_of(p->payload), wrap(p));
            }
            if (_st->read_err) {
                auto e = _st->read_err;
                _st->read_err = nullptr;
                std::rethrow_exception(e);
            }

            // 3. Exhausted only when nothing is still out: an in-flight batch
            //    may come back as a retry, and reporting end-of-input here
            //    would lose it.
            if (_st->source_done && _st->adopted == 0) co_return std::nullopt;

            // 4. Keep exactly one read in flight, then wait for whichever
            //    happens first -- it completing, or a nack arriving.
            start_read(as);
            // RE-CHECKED before committing to the wait. The read can complete
            // synchronously -- `generate` with no interval hands back an
            // already-ready future -- and a broadcast with no waiter registered
            // is simply lost, so waiting unconditionally here deadlocked the
            // pipeline on the very first message. Between this check and the
            // wait below there is no suspension point, so nothing can slip in.
            if (!_st->queue.empty() || _st->ready || _st->read_err ||
                (_st->source_done && _st->adopted == 0))
                continue;
            try { co_await _st->activity.wait(); }
            catch (const seastar::broken_condition_variable&) { co_return std::nullopt; }
        }
    }

    seastar::future<> close() override {
        // Anything still owed is dropped rather than nacked, which is what the
        // reference does too (its nackAllPending is written and unused). Said
        // out loud, because it is the one path where this wrapper loses a
        // message rather than replaying it.
        if (const size_t owed = _st->queue.size() + _st->adopted; owed > 0)
            iolog.warn("auto_replay_nacks: {} batch(es) were still awaiting delivery "
                       "at shutdown and were not replayed", owed);
        // The in-flight read holds a reference to the inner input, so it has to
        // be done before the inner is closed underneath it.
        _st->activity.broken();
        co_await _st->reader.close();
        // A read that landed after the last read_batch call owns an ack_fn
        // nobody will ever resolve; dropping it silently would leave the source
        // waiting, so it is nacked.
        if (_st->ready) {
            auto r = std::move(*_st->ready);
            _st->ready.reset();
            if (r.second)
                co_await r.second(std::make_exception_ptr(
                    std::runtime_error("shut down before the batch was delivered")));
        }
        co_await _inner->close();
    }
    connection_status status() const override { return _inner->status(); }

private:

    // Starts the single in-flight read, if there is not one already and the
    // source has not ended. The fiber captures only the SHARED state, never
    // `this`: an input can be torn down while a read is outstanding, and the
    // gate is what close() waits on.
    void start_read(seastar::abort_source& as) {
        if (_st->reading || _st->source_done || _st->reader.is_closed()) return;
        _st->reading = true;
        auto st = _st;
        (void)seastar::with_gate(_st->reader, [this, st, &as] {
            return _inner->read_batch(as).then_wrapped(
                [st](seastar::future<std::optional<std::pair<batch, ack_fn>>> f) {
                    st->reading = false;
                    try {
                        auto r = f.get();
                        if (r) st->ready = std::move(*r);
                        else   st->source_done = true;
                    } catch (...) {
                        st->read_err = std::current_exception();
                    }
                    st->activity.broadcast();
                });
        });
    }

    // The pipeline mutates what it is given, so what is held back has to be a
    // copy. Shallow: a refcount per message, not a payload.
    static batch copy_of(const batch& b) {
        batch out;
        out.reserve(b.size());
        for (const auto& m : b) out.push_back(m.shallow_copy());
        return out;
    }

    ack_fn wrap(seastar::lw_shared_ptr<pending> p) {
        return [st = _st, p](std::exception_ptr e) mutable -> seastar::future<> {
            if (e) {
                // Still adopted: it goes back on the queue and stays counted, so
                // the input is not "exhausted" while it is owed a delivery.
                st->queue.push_back(std::move(p));
                st->activity.broadcast();
                return seastar::make_ready_future<>();
            }
            --st->adopted;
            st->activity.broadcast();
            // The source learns of the success ONLY here, after however many
            // attempts it took -- which is what makes a nack a retry rather
            // than a loss.
            if (!p->ack) return seastar::make_ready_future<>();
            auto f = p->ack(nullptr);
            // `p` owns the ack_fn, and an ack_fn may be a coroutine lambda whose
            // captures live in the callable itself. Held until the future it
            // returned has resolved, rather than trusting every caller to keep
            // the transaction alive across the await.
            return f.finally([p] {});
        };
    }

    input_ptr                             _inner;
    seastar::lw_shared_ptr<state>         _st;
    seastar::optimized_optional<seastar::abort_source::subscription> _abort_sub;
};

input_ptr make_auto_retry_input(input_ptr inner) {
    return std::make_unique<auto_retry_input>(std::move(inner));
}

input_ptr make_broker_input(std::vector<input_ptr> children) {
    return std::make_unique<broker_input>(std::move(children));
}
input_ptr make_sequence_input(std::vector<input_ptr> children) {
    return std::make_unique<sequence_input>(std::move(children));
}

} // namespace sf
