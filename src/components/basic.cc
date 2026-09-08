// The first components: enough to run a pipeline end to end on Seastar.
// The full component set is much larger.
#include "swordfish/runtime/components.hh"


#include <seastar/core/coroutine.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/seastar.hh>

#include <seastar/core/alien.hh>

#include <iostream>
#include <unistd.h>
#include <cerrno>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <system_error>
#include <thread>

namespace sf {

// ---- input: generate -------------------------------------------------------
// Produces messages from a Bloblang mapping, optionally on an interval.
// Sharding: `count` is divided across shards so `-n` totals what the user asked.
class generate_input final : public input {
public:
    generate_input(transform_fn t, uint64_t count, std::chrono::milliseconds interval)
        : _t(std::move(t)), _remaining(count), _interval(interval) {}

    seastar::future<std::optional<std::pair<batch, ack_fn>>>
    read_batch(seastar::abort_source& as) override {
        if (_remaining == 0) co_return std::nullopt;
        // The interval sits BETWEEN messages, not before the first one. The
        // reference emits immediately and then ticks -- `count: 1, interval: 2s`
        // finishes in 413ms there and took 2302ms here -- so sleeping first
        // delayed every pipeline's first message by a whole interval. It was
        // invisible while `interval` defaulted to 0s and appeared the moment the
        // default was corrected to the reference's 1s.
        if (_started && _interval.count() > 0) {
            try { co_await seastar::sleep_abortable(_interval, as); }
            catch (const seastar::sleep_aborted&) { co_return std::nullopt; }
        }
        _started = true;
        --_remaining;
        value empty = value::object();
        _ctx.vars.clear();               // per-message variable scope
        _ctx.this_v = &empty;
        // The mapping runs against the batch BEING ACCUMULATED, so the first
        // message sees a size of zero -- benthos input_generate.go builds its
        // batch with `for i := 0; i < batchSize; i++ { MapPart(0, batch) ... }`
        // and appends afterwards. Only `vars` and `this` were bound here, so
        // `batch_size()` fell through to exec_ctx's default of 1 and reported 1
        // where the reference reports 0. mapping_processor already binds all
        // three; this site did not.
        static const batch none;
        _ctx.all = &none;
        _ctx.batch_size = 0;
        _ctx.batch_index = 0;
        value v = _t(_ctx);
        batch b;
        // set_mapped, not from_value: a STRING root becomes the message content
        // verbatim, so `root = "abc"` generates `abc` rather than the JSON
        // `"abc"`. from_value stored it structured, and every consumer that
        // read the bytes back saw the quotes -- which made `generate` unusable
        // for feeding a processor that expects raw text.
        message m;
        m.set_mapped(std::move(v));
        b.push_back(std::move(m));
        co_return std::make_pair(std::move(b), noop_ack());
    }

private:
    transform_fn              _t;
    exec_ctx                  _ctx;
    uint64_t                  _remaining;
    std::chrono::milliseconds _interval;
    bool                      _started = false;
};

input_ptr make_generate_input(transform_fn t, uint64_t count,
                              std::chrono::milliseconds interval) {
    return std::make_unique<generate_input>(std::move(t), count, interval);
}

// ---- processor: mapping ----------------------------------------------------
// Runs a Bloblang mapping. In compiled mode this same component is constructed
// with a generated function pointer instead of an AST.
class mapping_processor final : public processor {
public:
    mapping_processor(transform_fn t, bool mutate, std::string label, std::string path)
        : _t(std::move(t)), _mutate(mutate),
          // Built ONCE and shared by every message this processor fails: a batch
          // of a thousand failures should not allocate a thousand copies.
          _source(std::make_shared<const error_source_info>(
              error_source_info{"mapping", std::move(label), std::move(path)})) {}

    seastar::future<std::vector<batch>> process(batch b, seastar::abort_source&) override {
        batch out;
        out.reserve(b.size());
        _ctx.all = &b;                   // for from_all(); b outlives the loop
        const ctx_scope guard_{_ctx};   // see runtime.hh
        _ctx.batch_size = static_cast<int64_t>(b.size());
        _ctx.batch_index = 0;
        for (auto& msg : b) {
            const auto bump = [this] { ++_ctx.batch_index; };
            // Parse eagerly but tolerate failure: a mapping that never touches
            // `this` still runs, and only one that does will error.
            value parsed;
            bool structured = true;
            try { parsed = msg.as_structured(); }
            catch (const eval_error&) { structured = false; }
            // Variables are scoped to ONE mapping execution. Reusing the
            // context across messages leaked `let $x` from one message into the
            // next -- cross-message contamination, not just a stale value.
            // `counters` deliberately survives: counter() is stateful by design.
            _ctx.vars.clear();
            _ctx.this_v = structured ? &parsed : nullptr;
            _ctx.msg = &msg;                 // content(), json(), error(), errored()
            message m2 = msg.shallow_copy();
            _ctx.meta = &m2.meta();          // `meta x = ...` writes through to it
            _ctx.meta_in = &msg.meta();      // meta() reads the INPUT's, not this
            // Only when the content parsed: for a mutation over unstructured
            // content there is no document to start from, and leaving root at
            // `nothing` keeps the content untouched, which is what a mutation
            // that assigns nothing should do anyway.
            _ctx.root_init = (_mutate && structured) ? &parsed : nullptr;
            try {
                value result = _t(_ctx);
                _ctx.meta = nullptr;
                _ctx.meta_in = nullptr;
                _ctx.root_init = nullptr;
                if (result.is_deleted()) { bump(); continue; }   // filtered
                // `nothing` leaves the original content in place; anything else
                // replaces it, with strings written verbatim (see set_mapped).
                m2.set_mapped(std::move(result));
                out.push_back(std::move(m2));
                bump();
            } catch (const eval_error& e) {
                // Errors travel with the message rather than killing the batch.
                _ctx.meta = nullptr;
                _ctx.meta_in = nullptr;
                _ctx.root_init = nullptr;
                m2.set_error(e.what(), source_info());
                out.push_back(std::move(m2));
                bump();
            }
        }
        std::vector<batch> res;
        if (!out.empty()) res.push_back(std::move(out));
        return seastar::make_ready_future<std::vector<batch>>(std::move(res));
    }

    std::string name() const override { return "mapping"; }

private:
    const std::shared_ptr<const error_source_info>& source_info() const { return _source; }

    transform_fn _t;
    bool         _mutate = false;
    std::shared_ptr<const error_source_info> _source;
    exec_ctx     _ctx;
};

processor_ptr make_mapping_processor(transform_fn t, bool mutate,
                                     std::string label, std::string path) {
    return std::make_unique<mapping_processor>(std::move(t), mutate,
                                               std::move(label), std::move(path));
}

// ---- output: stdout --------------------------------------------------------

// stdout is not a reactor-managed descriptor. It may be a terminal, a pipe or a
// regular file; the file description is shared with whatever started the
// process; and Seastar offers no async path to it. So ONE OS THREAD owns it and
// the shards hand it work.
//
// The thread is not an optimisation, it is the whole point. `::write(1, ...)`
// used to run directly on the shard, and a reader that stopped consuming put
// the reactor inside the kernel for as long as it liked -- measured with a slow
// FIFO reader: `/ready` never answered (curl timed out at 5s against an endpoint
// that had logged itself as listening), and a SIGTERM sent at t=0 was not seen
// for 3.0s, exactly until the reader drained. `shutdown_timeout` could not help,
// because the forcing timer is itself a reactor task: on a reader that stopped
// for good, SIGTERM left the process alive through a 25s poll and it needed
// SIGKILL. The reference came down 0.51s after the same signal.
//
// Making the descriptor non-blocking instead is the obvious alternative and is
// wrong: O_NONBLOCK lives on the file DESCRIPTION, which for a terminal is
// shared with the shell that started us, so setting it changes the behaviour of
// another process's stdout.
//
// One thread for the process, not one per shard, so writes from different
// shards cannot interleave mid-batch either.
class stdout_writer {
public:
    static stdout_writer& get() {
        static stdout_writer w;
        return w;
    }

    // Callable from any shard. The returned future resolves when the bytes are
    // out, or fails with the errno that stopped them.
    seastar::future<> write(std::string buf) {
        if (buf.empty()) return seastar::make_ready_future<>();
        auto req = std::make_unique<request>();
        req->buf   = std::move(buf);
        req->shard = seastar::this_shard_id();
        req->alien = &seastar::engine().alien();
        auto f = req->pr.get_future();
        {
            std::lock_guard<std::mutex> g(_m);
            if (!_running)
                return seastar::make_exception_future<>(std::runtime_error(
                    "stdout output was closed while a write was pending"));
            _q.push_back(req.release());
        }
        _cv.notify_one();
        return f;
    }

    // Reference-counted because every shard has its own stdout_output and they
    // share this thread. The last close() takes the thread down, and it happens
    // while the reactor is still up -- which matters, because the thread posts
    // its completions back onto a shard.
    void ref() {
        std::lock_guard<std::mutex> g(_m);
        if (_refs++ == 0) {
            _running = true;
            _t = std::thread([this] { loop(); });
        }
    }
    void unref() {
        std::thread t;
        {
            std::lock_guard<std::mutex> g(_m);
            if (_refs == 0 || --_refs != 0) return;
            _running = false;
            t = std::move(_t);
        }
        _cv.notify_all();
        if (t.joinable()) t.join();
    }

private:
    struct request {
        std::string        buf;
        unsigned           shard = 0;
        seastar::alien::instance* alien = nullptr;
        seastar::promise<> pr;
        int                err = 0;
    };

    void loop() {
        for (;;) {
            request* req = nullptr;
            {
                std::unique_lock<std::mutex> g(_m);
                _cv.wait(g, [this] { return !_q.empty() || !_running; });
                if (_q.empty()) {
                    if (!_running) return;
                    continue;
                }
                req = _q.front();
                _q.pop_front();
            }
            req->err = write_all(req->buf);
            // The promise belongs to its shard and must be touched only there.
            // This thread never looks at it.
            seastar::alien::run_on(*req->alien, req->shard, [req]() noexcept {
                std::unique_ptr<request> owned(req);
                if (owned->err)
                    owned->pr.set_exception(std::make_exception_ptr(std::system_error(
                        owned->err, std::generic_category(), "stdout write failed")));
                else
                    owned->pr.set_value();
            });
        }
    }

    // Returns 0, or the errno that stopped the write. Written to fd 1 directly
    // rather than through a seastar::file_desc, which takes OWNERSHIP and would
    // close the process's stdout in its destructor.
    static int write_all(const std::string& buf) {
        size_t off = 0;
        while (off < buf.size()) {
            const ssize_t n = ::write(1, buf.data() + off, buf.size() - off);
            if (n > 0) { off += static_cast<size_t>(n); continue; }
            if (n < 0 && errno == EINTR) continue;
            // A zero return on a blocking write of a non-empty buffer is not
            // progress; reporting it as one would spin here for ever.
            return n < 0 ? errno : EIO;
        }
        return 0;
    }

    std::mutex               _m;
    std::condition_variable  _cv;
    std::deque<request*>     _q;
    std::thread              _t;
    unsigned                 _refs = 0;
    bool                     _running = false;
};

class stdout_output final : public output {
public:
    stdout_output()  { stdout_writer::get().ref(); }
    ~stdout_output() override { if (_open) stdout_writer::get().unref(); }

    seastar::future<> write_batch(batch b, seastar::abort_source&) override {
        std::string buf;
        for (const auto& m : b) append_line(buf, m.as_bytes());
        // The failure PROPAGATES. It used to `break` out of the write loop and
        // return a ready future, so EPIPE, ENOSPC and a short write all reported
        // success: stream::do_write then counted an ack and every composite
        // above was disarmed -- `fallback` never reached its next tier,
        // `retry_output` never retried, `fan_out` never nacked. Measured with
        // 5000 messages into `fallback: [stdout, file]` piped to
        // `head -c 100`: swordfish exited 0 claiming `acks=5000` with 100 bytes
        // delivered and the fallback file empty. That is the acked-exactly-once
        // rule the project treats as the bug class the engine dies of.
        //
        // Benthos's own stdout output returns the write error
        // (internal/impl/io/output_stdout.go), so nacking is the upstream
        // behaviour, not an invention here. The reference BINARY dies with
        // SIGPIPE instead, but that is Go's default signal disposition for fd 1
        // firing before the component's error path is ever reached; Seastar
        // ignores SIGPIPE, so here the write returns EPIPE and the batch is
        // nacked.
        return stdout_writer::get().write(std::move(buf));
    }

    seastar::future<> close() override {
        if (_open) { _open = false; stdout_writer::get().unref(); }
        return seastar::make_ready_future<>();
    }

    // max_in_flight() is left at the base class default of 1: stdout is a single
    // shared descriptor, so concurrent writes would interleave.
private:
    bool _open = true;
};

output_ptr make_stdout_output() { return std::make_unique<stdout_output>(); }

// ---- output: drop ----------------------------------------------------------
class drop_output final : public output {
public:
    seastar::future<> write_batch(batch, seastar::abort_source&) override {
        return seastar::make_ready_future<>();
    }
    size_t max_in_flight() const override { return 64; }
};

output_ptr make_drop_output() { return std::make_unique<drop_output>(); }

} // namespace sf
