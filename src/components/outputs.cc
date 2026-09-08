// The outputs that hold other outputs, plus `reject`.
//
// Semantics taken from benthos-main/internal/impl/pure/output_*.go (Apache-2.0).
// One structural difference is worth stating once, because it explains why this
// file is a fraction of the size of its Go counterpart: there, an output is a
// goroutine reading a transaction channel, so a broker has to fan transactions
// out over channels and re-aggregate the acks itself. Here `write_batch`
// returns a future that resolves when the write is done, and `stream::do_write`
// owns the ack. A composite therefore only has to await its children and
// propagate the first failure -- the ack accounting is already correct.
#include <seastar/core/gate.hh>
#include "swordfish/runtime/batching.hh"
#include "swordfish/runtime/components.hh"

#include <seastar/core/coroutine.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/queue.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/timer.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/when_all.hh>
#include <seastar/util/log.hh>

#include <algorithm>

namespace sf {

static seastar::logger olog("sf.output");

namespace {

using namespace std::chrono_literals;

// Each child of a fan-out sees its OWN copy: a child whose processors set
// metadata must not have that visible to the next one. Shallow, so this costs a
// refcount per message rather than a payload copy.
batch shallow_copy_of(const batch& b) {
    batch out;
    out.reserve(b.size());
    for (const auto& m : b) out.push_back(m.shallow_copy());
    return out;
}

// Binds a context to one message of a batch, the way a processor does before
// running a mapping over it. `parsed` is an out-parameter rather than a local
// because ctx.this_v points into it and must outlive the call.
void bind(exec_ctx& ctx, batch& b, size_t i, value& parsed) {
    message& m = b[i];
    bool structured = true;
    try { parsed = m.as_structured(); } catch (const eval_error&) { structured = false; }
    ctx.this_v = structured ? &parsed : nullptr;
    ctx.msg = &m;
    // Routing and rendering only READ metadata -- there is no output message
    // being built -- so the read source and the write target are the same.
    ctx.meta = &m.meta();
    ctx.meta_in = &m.meta();
    ctx.all = &b;
    ctx.batch_index = static_cast<int64_t>(i);
    ctx.batch_size = static_cast<int64_t>(b.size());
}

std::string describe(std::exception_ptr e) {
    if (!e) return "unknown error";
    try { std::rethrow_exception(e); }
    catch (const std::exception& ex) { return ex.what(); }
    catch (...) { return "unknown error"; }
}

// Waits for every future, then rethrows the FIRST failure. Every future must be
// consumed even once one has failed, or Seastar reports the rest as ignored
// exceptional futures at exit.
seastar::future<> all_or_first_error(std::vector<seastar::future<>> fs) {
    auto res = co_await seastar::when_all(fs.begin(), fs.end());
    std::exception_ptr first;
    for (auto& f : res) {
        if (!f.failed()) { f.ignore_ready_future(); continue; }
        auto e = f.get_exception();
        if (!first) first = e;
    }
    if (first) std::rethrow_exception(first);
}

// ---- error_handling.strict --------------------------------------------------

}   // namespace

std::exception_ptr strict_rejection(const batch& b) {
    for (const auto& m : b) {
        if (!m.has_error()) continue;
        return std::make_exception_ptr(std::runtime_error(
            "error_handling.strict: rejected due to failed processing: " + m.error()));
    }
    return nullptr;
}

namespace {

// ---- reject -----------------------------------------------------------------

// Fails every write, with the reason rendered from the first message of the
// batch -- matching the reference, whose `errExpr.String(0, msg)` renders
// against part 0 however many parts the batch has.
class reject_output final : public output {
public:
    explicit reject_output(transform_fn message) : _message(std::move(message)) {}

    seastar::future<> write_batch(batch b, seastar::abort_source&) override {
        std::string text;
        try {
            if (b.empty()) {
                // No message to render against. `${! error() }` would throw
                // here, and a rejection with no reason is exactly what the
                // reference refuses to configure, so say why rather than
                // producing an empty one.
                text = "rejected: an empty batch carries no message to render "
                       "the rejection reason from";
            } else {
                exec_ctx ctx;
                value parsed;
                bind(ctx, b, 0, parsed);
                text = _message(ctx).to_display_string();
            }
        } catch (const std::exception& e) {
            return seastar::make_exception_future<>(std::runtime_error(
                std::string("reject message interpolation error: ") + e.what()));
        }
        return seastar::make_exception_future<>(std::runtime_error(std::move(text)));
    }

    connection_status status() const override {
        connection_status s;
        s.connected = true;
        s.label = "reject";
        return s;
    }

private:
    transform_fn _message;
};

// ---- retry ------------------------------------------------------------------

// Backoff matching the reference's default for a wrapped output: 500ms, x1.5,
// capped at 3s, no overall deadline. Deterministic rather than jittered --
// there is one retry loop per output instance, so there is no herd to spread.
constexpr auto retry_initial = 500ms;
constexpr auto retry_max     = 3000ms;

class retry_output final : public output {
public:
    explicit retry_output(output_ptr inner) : _inner(std::move(inner)) {}

    seastar::future<> connect(seastar::abort_source& as) override {
        return _inner->connect(as);
    }

    seastar::future<> write_batch(batch b, seastar::abort_source& as) override {
        auto delay = retry_initial;
        for (;;) {
            std::exception_ptr err;
            try {
                co_await _inner->write_batch(shallow_copy_of(b), as);
                co_return;
            } catch (...) {
                err = std::current_exception();
            }
            // Abort means shutdown, and retrying forever would hold the drain
            // open until the forceful stage killed it. The last error is the
            // one the source is nacked with.
            if (as.abort_requested()) std::rethrow_exception(err);
            olog.warn("output failed, retrying in {}ms: {}", delay.count(), describe(err));
            try {
                co_await seastar::sleep_abortable(delay, as);
            } catch (const seastar::sleep_aborted&) {
                std::rethrow_exception(err);
            }
            delay = std::min(retry_max, delay * 3 / 2);
        }
    }

    seastar::future<> drain() override { return _inner->drain(); }
    seastar::future<> close() override { return _inner->close(); }
    connection_status status() const override { return _inner->status(); }
    size_t max_in_flight() const override { return _inner->max_in_flight(); }

private:
    output_ptr _inner;
};

// ---- processors -------------------------------------------------------------

// An output's own `processors:`. Without `error_handling.strict` a throwing
// processor marks its messages and they continue, rather than failing the write
// and nacking the source -- the main pipeline's non-strict path. With it, the
// same rule the pipeline applies applies here: a processing error is terminal
// for the batch and it is rejected rather than written.
//
// That second half was missing entirely. `strict_errors` was read in exactly one
// place, the main pipeline, so `output.processors` ignored it: measured with
// `error_handling.strict: true` and an output processor of
// `root = throw("bad")`, swordfish printed both messages and reported
// `in=2 out=2 acks=2 nacks=0`, while the reference wrote nothing and rejected
// each one by name.
class processed_output final : public output {
public:
    processed_output(output_ptr inner, std::vector<processor_ptr> procs, bool strict)
        : _inner(std::move(inner)), _procs(std::move(procs)), _strict(strict) {}

    seastar::future<> connect(seastar::abort_source& as) override {
        return _inner->connect(as);
    }

    seastar::future<> write_batch(batch b, seastar::abort_source& as) override {
        std::vector<batch> batches;
        batches.push_back(std::move(b));
        for (const auto& p : _procs) {
            std::vector<batch> next;
            std::exception_ptr fatal;
            for (auto& cur : batches) {
                if (cur.empty()) continue;
                // No backup under strict: a throwing processor is terminal
                // there, so the copies would only be made to be thrown away.
                batch backup;
                if (!_strict) {
                    backup.reserve(cur.size());
                    for (const auto& m : cur) backup.push_back(m.shallow_copy());
                }
                try {
                    auto out = co_await p->process(std::move(cur), as);
                    for (auto& ob : out) if (!ob.empty()) next.push_back(std::move(ob));
                } catch (...) {
                    // catch(...) rather than catch(std::exception&), matching
                    // the main pipeline: a processor that throws something else
                    // must still leave its messages marked and moving, not fail
                    // the whole write.
                    if (_strict) { fatal = std::current_exception(); break; }
                    const std::string why = describe(std::current_exception());
                    for (auto& m : backup) m.set_error(why);
                    next.push_back(std::move(backup));
                }
            }
            if (fatal) std::rethrow_exception(fatal);
            batches = std::move(next);
            if (batches.empty()) break;
            // A processor that FAILS a message marks it rather than throwing --
            // that is the non-strict behaviour and it matches the reference --
            // so strict mode has to look at the marks, after each stage, exactly
            // as the pipeline does.
            if (_strict)
                for (const auto& nb : batches)
                    if (auto e = strict_rejection(nb)) std::rethrow_exception(e);
        }
        // Nothing left is a SUCCESS: the processors filtered everything, which
        // is a legitimate outcome and not a delivery failure.
        //
        // The sub-batches go out CONCURRENTLY, as the pipeline launches split
        // batches through an ack_group and as fan_out writes its children.
        // Awaiting each one before issuing the next did not merely serialise
        // them, it deadlocked against a `batching` policy underneath: a batched
        // output cannot resolve a write until its policy triggers, and the
        // messages that would trigger it are in the sub-batches this loop has
        // not issued yet. Measured with `split: {size: 1}` over
        // `batching: {count: 3}`: swordfish printed nothing and had to be
        // SIGKILLed where the reference printed six lines and exited 0.
        //
        // BOUNDED by what the inner output actually permits, because "issue them
        // all at once" would be a different bug. Seastar's output_stream says
        // "all methods must be called sequentially [...] no method may be
        // invoked before the previous method's returned future is resolved", and
        // file_output holds a std::map iterator across its write, so a terminal
        // output whose max_in_flight() is 1 must still receive one write at a
        // time. A `batching` output asks for `count`, which is exactly the
        // concurrency the deadlock above needs, so both cases fall out of the
        // same limit rather than a special case.
        seastar::semaphore room(std::max<size_t>(1, _inner->max_in_flight()));
        std::vector<seastar::future<>> fs;
        fs.reserve(batches.size());
        for (auto& out : batches) {
            if (out.empty()) continue;
            fs.push_back(seastar::with_semaphore(
                room, 1, [this, &as, ob = std::move(out)]() mutable {
                    return _inner->write_batch(std::move(ob), as);
                }));
        }
        // `room` is a local, so nothing may escape this frame holding units.
        if (!fs.empty()) co_await all_or_first_error(std::move(fs));
    }

    seastar::future<> drain() override { return _inner->drain(); }

    seastar::future<> close() override {
        for (auto& p : _procs)
            try { co_await p->close(); }
            catch (const std::exception& e) { olog.warn("processor close failed: {}", e.what()); }
        co_await _inner->close();
    }

    connection_status status() const override { return _inner->status(); }
    size_t max_in_flight() const override { return _inner->max_in_flight(); }

private:
    output_ptr                 _inner;
    std::vector<processor_ptr> _procs;
    bool                       _strict = false;
};

// ---- broker -----------------------------------------------------------------

// Shared by every pattern: connecting, closing and reporting over the children.
class multi_output : public output {
public:
    explicit multi_output(std::vector<output_ptr> children) : _children(std::move(children)) {}

    seastar::future<> connect(seastar::abort_source& as) override {
        for (auto& c : _children) co_await c->connect(as);
    }

    seastar::future<> drain() override {
        for (auto& c : _children) co_await c->drain();
    }

    seastar::future<> close() override {
        // Guarded per child: a throwing close must not skip the ones behind it,
        // which would leave sockets and files open for the life of the process.
        for (auto& c : _children)
            try { co_await c->close(); }
            catch (const std::exception& e) { olog.warn("child output close failed: {}", e.what()); }
    }

    connection_status status() const override {
        connection_status s;
        s.connected = true;
        for (const auto& c : _children) {
            const auto cs = c->status();
            if (!cs.connected) { s.connected = false; if (s.error.empty()) s.error = cs.error; }
        }
        return s;
    }

protected:
    std::vector<output_ptr> _children;
};

class fan_out_output final : public multi_output {
public:
    fan_out_output(std::vector<output_ptr> children, bool sequential)
        : multi_output(std::move(children)), _sequential(sequential) {}

    seastar::future<> write_batch(batch b, seastar::abort_source& as) override {
        if (_sequential) {
            // One child at a time, each only started once the previous has
            // confirmed receipt. A failure stops the sequence: the children
            // behind it never see the batch, which is what the reference does
            // by propagating the nack rather than continuing.
            for (auto& c : _children) co_await c->write_batch(shallow_copy_of(b), as);
            co_return;
        }
        std::vector<seastar::future<>> fs;
        fs.reserve(_children.size());
        for (auto& c : _children) fs.push_back(c->write_batch(shallow_copy_of(b), as));
        co_await all_or_first_error(std::move(fs));
    }

    // One, deliberately. A fan-out must deliver a batch to every child, so a
    // second concurrent batch would double the load on the slowest of them
    // without the pipeline ever getting ahead.
    // One for this output's OWN concurrency, but never less than what a child
    // needs. The stream sizes its in-flight semaphore once, from the OUTERMOST
    // output (stream.cc `_in_flight.emplace(_out->max_in_flight())`), so a
    // composite that returned a flat 1 starved a `batching` policy nested
    // beneath it: the batcher needs `count` writes in flight to reach its count,
    // only one was ever allowed, so the policy could never trigger. Twenty
    // messages at `count: 5` produced nothing at all and the process could not
    // be stopped, where redpanda-connect wrote all twenty and exited. The
    // requirement has to travel UP.
    size_t max_in_flight() const override {
        size_t need = 1;
        for (const auto& c : _children) need = std::max(need, c->max_in_flight());
        return need;
    }

private:
    bool _sequential;
};

class round_robin_output final : public multi_output {
public:
    using multi_output::multi_output;

    seastar::future<> write_batch(batch b, seastar::abort_source& as) override {
        // Not awaited before the index moves on, so several writes in flight
        // still land on distinct children.
        const size_t i = _next++ % _children.size();
        return _children[i]->write_batch(std::move(b), as);
    }

    size_t max_in_flight() const override { return _children.size(); }

private:
    size_t _next = 0;
};

// `greedy`: in the reference every child reads the SAME transaction channel, so
// a batch goes to whichever is ready first. A queue of free child indices is
// that, exactly: claim one, use it, put it back.
class greedy_output final : public multi_output {
public:
    explicit greedy_output(std::vector<output_ptr> children)
        : multi_output(std::move(children)), _free(_children.size()) {
        for (size_t i = 0; i < _children.size(); ++i) (void)_free.push(size_t{i});
    }

    seastar::future<> write_batch(batch b, seastar::abort_source& as) override {
        const size_t i = co_await _free.pop_eventually();
        std::exception_ptr err;
        try {
            co_await _children[i]->write_batch(std::move(b), as);
        } catch (...) {
            err = std::current_exception();
        }
        // Returned on both paths: a child kept out of the queue after a failure
        // would shrink the broker by one on every error until it deadlocked.
        (void)_free.push(size_t{i});
        if (err) std::rethrow_exception(err);
    }

    // Every child can be busy at once; with fewer in flight than children the
    // idle ones would never be claimed and `greedy` would degrade to a slower
    // round robin.
    size_t max_in_flight() const override { return _children.size(); }

private:
    seastar::queue<size_t> _free;
};

// ---- fallback ---------------------------------------------------------------

class fallback_output final : public multi_output {
public:
    using multi_output::multi_output;

    seastar::future<> write_batch(batch b, seastar::abort_source& as) override {
        std::exception_ptr last;
        for (size_t i = 0; i < _children.size(); ++i) {
            if (last) {
                // The next tier sees why the previous one failed, so a config
                // downstream can route or annotate on it.
                const value why{describe(last)};
                for (auto& m : b) m.meta().set("fallback_error", why);
            }
            try {
                co_await _children[i]->write_batch(shallow_copy_of(b), as);
                co_return;
            } catch (...) {
                last = std::current_exception();
            }
        }
        // Every tier failed, so the source is nacked with the last reason. An
        // empty child list cannot reach here -- parsing refuses one -- but the
        // guard keeps a null rethrow from being the way we find out otherwise.
        if (last) std::rethrow_exception(last);
        throw std::runtime_error("fallback output has no outputs to write to");
    }

    // One for this output's OWN concurrency, but never less than what a child
    // needs. The stream sizes its in-flight semaphore once, from the OUTERMOST
    // output (stream.cc `_in_flight.emplace(_out->max_in_flight())`), so a
    // composite that returned a flat 1 starved a `batching` policy nested
    // beneath it: the batcher needs `count` writes in flight to reach its count,
    // only one was ever allowed, so the policy could never trigger. Twenty
    // messages at `count: 5` produced nothing at all and the process could not
    // be stopped, where redpanda-connect wrote all twenty and exited. The
    // requirement has to travel UP.
    size_t max_in_flight() const override {
        size_t need = 1;
        for (const auto& c : _children) need = std::max(need, c->max_in_flight());
        return need;
    }
};

// ---- switch -----------------------------------------------------------------

class switch_output final : public output {
public:
    switch_output(std::vector<switch_output_case> cases, bool strict_mode)
        : _cases(std::move(cases)), _strict(strict_mode) {}

    seastar::future<> connect(seastar::abort_source& as) override {
        for (auto& c : _cases) co_await c.out->connect(as);
    }

    seastar::future<> write_batch(batch b, seastar::abort_source& as) override {
        std::vector<batch> buckets(_cases.size());
        for (size_t i = 0; i < b.size(); ++i) {
            exec_ctx ctx;
            value parsed;
            bind(ctx, b, i, parsed);

            bool routed = false;
            for (size_t j = 0; j < _cases.size(); ++j) {
                bool test = true;
                if (_cases[j].check) {
                    try {
                        test = truthy(_cases[j].check(ctx));
                    } catch (const std::exception& e) {
                        // A check that throws does not match. Logged rather
                        // than swallowed: with strict_mode off the message is
                        // then dropped, and a silently dropped message is the
                        // failure this component is most likely to hide.
                        olog.error("switch output case {} check failed: {}", j, e.what());
                        test = false;
                    }
                }
                if (!test) continue;
                routed = true;
                buckets[j].push_back(b[i].shallow_copy());
                if (!_cases[j].continue_) break;
            }
            if (!routed && _strict)
                throw std::runtime_error("no switch output conditions were met by message");
        }

        std::vector<seastar::future<>> fs;
        for (size_t j = 0; j < _cases.size(); ++j) {
            if (buckets[j].empty()) continue;
            fs.push_back(_cases[j].out->write_batch(std::move(buckets[j]), as));
        }
        // No case matched anything and strict_mode is off: the messages are
        // dropped and the write SUCCEEDS, which is what the reference means by
        // "effectively dropped".
        if (fs.empty()) co_return;
        co_await all_or_first_error(std::move(fs));
    }

    seastar::future<> drain() override {
        for (auto& c : _cases) co_await c.out->drain();
    }

    seastar::future<> close() override {
        for (auto& c : _cases)
            try { co_await c.out->close(); }
            catch (const std::exception& e) { olog.warn("switch output close failed: {}", e.what()); }
    }

    connection_status status() const override {
        connection_status s;
        s.connected = true;
        for (const auto& c : _cases) {
            const auto cs = c.out->status();
            if (!cs.connected) { s.connected = false; if (s.error.empty()) s.error = cs.error; }
        }
        return s;
    }

    // One: routing decides per batch which children are involved, so a second
    // concurrent batch could reorder two batches that both land on one case.
    // One for this output's OWN concurrency, but never less than what a child
    // needs. The stream sizes its in-flight semaphore once, from the OUTERMOST
    // output (stream.cc `_in_flight.emplace(_out->max_in_flight())`), so a
    // composite that returned a flat 1 starved a `batching` policy nested
    // beneath it: the batcher needs `count` writes in flight to reach its count,
    // only one was ever allowed, so the policy could never trigger. Twenty
    // messages at `count: 5` produced nothing at all and the process could not
    // be stopped, where redpanda-connect wrote all twenty and exited. The
    // requirement has to travel UP.
    size_t max_in_flight() const override {
        size_t need = 1;
        for (const auto& c : _cases) need = std::max(need, c.out->max_in_flight());
        return need;
    }

private:
    std::vector<switch_output_case> _cases;
    bool                            _strict;
};

// ---- batching ---------------------------------------------------------------
//
// An output's `batching:` policy. Messages accumulate until a trigger fires,
// the batching processors run over what accumulated, and the result is written
// to the inner output.
//
// The awkward part is the ACK, and it is the whole reason this is a wrapper
// rather than a queue. `write_batch` must not resolve until the messages it was
// given have actually been written, because `stream::do_write` acks the source
// the moment it does. So each call parks a promise, and a flush resolves every
// promise it was carrying with the outcome of the one write it produced. That
// is `ack_group` in reverse: many transactions in, one write out.
//
// The interaction with `max_in_flight` is the subtle constraint. The stream
// allows only that many unresolved `write_batch` calls at once, so a policy
// needing N messages to trigger needs N of them parked -- and if the limit is
// lower than the trigger, nothing further arrives and the batch never
// completes. `max_in_flight()` below is sized from `count` for that reason, and
// `flush_on_capacity` is the backstop for the triggers whose message count
// cannot be known in advance.
class batched_output final : public output {
public:
    batched_output(output_ptr inner, batch_policy policy,
                   std::vector<processor_ptr> procs, int64_t count_hint, bool strict)
        : _inner(std::move(inner)), _policy(std::move(policy)),
          _procs(std::move(procs)), _count_hint(count_hint), _strict(strict) {}

    seastar::future<> connect(seastar::abort_source& as) override {
        _as = &as;
        return _inner->connect(as);
    }

    seastar::future<> write_batch(batch b, seastar::abort_source& as) override {
        if (b.empty()) co_return;

        // KEPT, not discarded. This used to end `(void)as;` while flush() used
        // the connect-time source instead -- and those are two different things:
        // connect() is handed `_drain` ("soft stop: stop reading"), which
        // stop_gracefully() aborts at the START of shutdown, whereas do_write
        // passes `_stop_now` ("hard stop: abandon in flight"). So every flush
        // issued during the graceful window -- including drain()'s flush of the
        // last partial batch, which is the whole reason drain() exists -- handed
        // an ALREADY-ABORTED source to the batching processors and to the inner
        // output, during the phase whose purpose is to let outstanding work
        // finish. Demonstrated with a `sleep: 5s` processor and SIGTERM at
        // t=1.5s: the same sleep ran to completion under `output.processors`
        // (9.05s) and was cut short under `batching.processors` (0.51s).
        //
        // retry_output and http_client both stop retrying when their source is
        // aborted, so an inner output failing transiently during the drain would
        // give up rather than retry.
        _write_as = &as;

        auto waiter = seastar::make_lw_shared<seastar::promise<std::exception_ptr>>();
        _waiting.push_back(waiter);

        bool trigger = false;
        for (auto& m : b) trigger |= _policy.add(std::move(m));

        // The backstop. A `byte_size` or `check` policy has no message count
        // known in advance, so the in-flight limit can be reached before the
        // trigger is. Flushing early is a smaller batch than was asked for and
        // is said out loud, once: the alternative is a pipeline that stops.
        if (!trigger && _waiting.size() >= max_in_flight()) {
            if (!_warned_capacity) {
                _warned_capacity = true;
                olog.warn("batching flushed at {} messages because the output's "
                          "in-flight limit was reached before the policy triggered; "
                          "set `batching.count` or `batching.period` to control this",
                          _policy.size());
            }
            trigger = true;
        }

        // Arm the period timer whenever something is waiting and it is not
        // already armed. The timer is what makes `period` a real trigger rather
        // than a test that only runs when the next message happens to arrive.
        arm_timer();

        if (trigger) {
            if (!_flushes.is_closed()) {
                (void)seastar::with_gate(_flushes, [this] { return flush(); });
            } else {
                // The gate is CLOSED and this write still arrived. drain() closes
                // it and stream::output_loop calls drain() before it closes the
                // write gate, so a write already suspended further up -- inside a
                // `processors:` list, say -- can land here afterwards. Declining
                // the flush parked its waiter for ever: the messages were never
                // written, the write never resolved, and the pipeline hung with
                // them unacked. Flushed INLINE instead, outside the gate, because
                // the gate exists to let drain() wait for flushes it did not
                // start -- not to refuse work that must still happen.
                co_await flush();
            }
        }
        co_await waiter->get_future().then([](std::exception_ptr e) {
            if (e) std::rethrow_exception(e);
        });
    }

    // Where the last partial batch goes. Doing it in close() instead deadlocks:
    // the stream waits for every write_batch to resolve before it closes
    // anything, and the parked ones only resolve when this flush happens.
    seastar::future<> drain() override {
        _timer.cancel();
        co_await quiesce();
        if (!_policy.empty() || !_waiting.empty()) co_await flush();
        co_await _inner->drain();
    }

    seastar::future<> close() override {
        _timer.cancel();
        // A backstop for the forced shutdown path, where drain() may not have
        // run. These messages have been accepted from the source and not yet
        // acked, so discarding them would lose them silently.
        co_await quiesce();
        if (!_policy.empty() || !_waiting.empty()) co_await flush();
        for (auto& p : _procs)
            try { co_await p->close(); }
            catch (const std::exception& e) { olog.warn("batching processor close failed: {}", e.what()); }
        co_await _inner->close();
    }

    connection_status status() const override { return _inner->status(); }

    // Sized so a `count` policy can actually reach its count: each parked call
    // may carry as little as one message. Without a count the floor is a
    // compromise between how large a byte- or check-triggered batch can grow
    // and how many messages may sit unacked.
    size_t max_in_flight() const override {
        const size_t need = _count_hint > 0 ? static_cast<size_t>(_count_hint) : 256;
        return std::max(need, _inner->max_in_flight());
    }

private:
    // The source a flush should run under: the hard stop the writes are given,
    // falling back to the connect-time one before any write has arrived (the
    // timer cannot fire before then, but close() can still run).
    seastar::abort_source& flush_as() { return _write_as ? *_write_as : *_as; }

    void arm_timer() {
        const auto until = _policy.until_next();
        if (until.count() <= 0 || _timer.armed()) return;
        _timer.set_callback([this] {
            if (!_flushes.is_closed())
                (void)seastar::with_gate(_flushes, [this] { return flush(); });
        });
        _timer.arm(until);
    }

    // Runs the batching processors over what accumulated and writes the result.
    // Three outcomes, and they are NOT interchangeable: a batch is written and
    // its waiters take the write's verdict; an empty result means the
    // processors filtered everything, which is a success; a processor failure
    // means the data was never written, so the waiters are nacked and the
    // source can send it again.
    // Flushes run CONCURRENTLY. `if (_flushing) co_return;` used to serialise
    // them, and it did not merely delay the second one -- it DISCARDED it. The
    // "honoured now" arm_timer() that followed is a no-op for a policy with no
    // `period`, since until_next() returns 0 there, so with a count-only policy
    // in front of an output whose writes take real time, every trigger raised
    // while a write was in flight was lost: those messages were never written,
    // their waiters never resolved, the stream's write gate never closed, and
    // the process hung for ever with them unacked. Concurrent calls are the
    // normal case rather than an edge, since output_loop launches writes without
    // awaiting them.
    //
    // Serialising them at all was the wrong shape, not just the wrong recovery.
    // The reference forms a batch the moment the policy triggers and dispatches
    // it, so `count: 2` over twelve messages is six writes of two; coalescing
    // whatever accumulated during the previous write gave two writes, of two and
    // ten. The snapshot below -- `_waiting` moved out and `_policy.take()` --
    // is synchronous, with no co_await between the two, so each flush leaves
    // with its own batch and they cannot race. Concurrency stays bounded by the
    // stream's in-flight limit, which is what max_in_flight() is sized for.
    seastar::future<> flush() {
        _timer.cancel();

        auto waiters = std::move(_waiting);
        _waiting.clear();
        batch b = _policy.take();

        std::exception_ptr err;
        bool filtered = false;
        if (b.empty()) {
            filtered = true;
        } else {
            std::vector<batch> batches;
            batches.push_back(std::move(b));
            for (const auto& p : _procs) {
                std::vector<batch> next;
                for (auto& cur : batches) {
                    if (cur.empty()) continue;
                    try {
                        auto out = co_await p->process(std::move(cur), flush_as());
                        for (auto& ob : out) if (!ob.empty()) next.push_back(std::move(ob));
                    } catch (...) {
                        err = std::current_exception();
                    }
                }
                if (err) break;
                batches = std::move(next);
                if (batches.empty()) break;
            }
            // The batching processors are the last thing that can mark a
            // message before it is written, so `error_handling.strict` has to be
            // applied here too -- it was not, and a `batching.processors` list
            // that failed a message wrote it and acked it.
            if (!err && _strict)
                for (const auto& nb : batches)
                    if (auto e = strict_rejection(nb)) { err = e; break; }
            if (!err) {
                if (batches.empty()) {
                    filtered = true;
                } else {
                    for (auto& out : batches) {
                        if (out.empty()) continue;
                        try {
                            co_await _inner->write_batch(std::move(out), flush_as());
                        } catch (...) {
                            err = std::current_exception();
                            break;
                        }
                    }
                }
            }
        }
        (void)filtered;   // an empty flush resolves its waiters successfully

        for (auto& w : waiters) w->set_value(err);
        if (!_policy.empty()) arm_timer();
    }

    // Waits for the flushes already in flight, once. drain() and close() both
    // need it, and a gate cannot be closed twice.
    seastar::future<> quiesce() {
        if (_flushes_closed) co_return;
        _flushes_closed = true;
        co_await _flushes.close();
    }

    output_ptr                 _inner;
    batch_policy               _policy;
    std::vector<processor_ptr> _procs;
    int64_t                    _count_hint;
    bool                       _strict = false;
    seastar::abort_source*     _as = nullptr;        // connect-time: the DRAIN source
    seastar::abort_source*     _write_as = nullptr;  // what do_write passes: the HARD stop
    std::vector<seastar::lw_shared_ptr<seastar::promise<std::exception_ptr>>> _waiting;
    seastar::timer<seastar::lowres_clock> _timer;
    seastar::gate              _flushes;             // flushes still running
    bool                       _flushes_closed = false;
    bool                       _warned_capacity = false;
};

} // namespace

output_ptr make_reject_output(transform_fn message) {
    return std::make_unique<reject_output>(std::move(message));
}

output_ptr make_retry_output(output_ptr inner) {
    return std::make_unique<retry_output>(std::move(inner));
}

output_ptr make_batched_output(output_ptr inner, int64_t count, int64_t byte_size,
                              std::chrono::milliseconds period, transform_fn check,
                              std::vector<processor_ptr> procs, bool strict_errors) {
    // A policy with no trigger would hold every message until shutdown, so it
    // is not wrapped at all rather than wrapped and never fired.
    const bool no_trigger = count <= 0 && byte_size <= 0 && period.count() <= 0 && !check;
    if (no_trigger && procs.empty()) return inner;
    batch_policy policy(count, byte_size, period, std::move(check));
    return std::make_unique<batched_output>(std::move(inner), std::move(policy),
                                            std::move(procs), count, strict_errors);
}

output_ptr make_processed_output(output_ptr inner, std::vector<processor_ptr> procs,
                                 bool strict_errors) {
    if (procs.empty()) return inner;
    return std::make_unique<processed_output>(std::move(inner), std::move(procs),
                                              strict_errors);
}

output_ptr make_broker_output(std::vector<output_ptr> children, const std::string& pattern) {
    if (children.empty())
        throw std::runtime_error("output.broker has no outputs to write to");
    if (pattern == "round_robin")
        return std::make_unique<round_robin_output>(std::move(children));
    if (pattern == "greedy")
        return std::make_unique<greedy_output>(std::move(children));
    const bool sequential = pattern == "fan_out_sequential"
                         || pattern == "fan_out_sequential_fail_fast";
    return std::make_unique<fan_out_output>(std::move(children), sequential);
}

output_ptr make_fallback_output(std::vector<output_ptr> children) {
    if (children.empty())
        throw std::runtime_error("output.fallback has no outputs to write to");
    return std::make_unique<fallback_output>(std::move(children));
}

output_ptr make_switch_output(std::vector<switch_output_case> cases, bool strict_mode) {
    if (cases.empty())
        throw std::runtime_error("output.switch has no cases to route to");
    return std::make_unique<switch_output>(std::move(cases), strict_mode);
}

} // namespace sf
