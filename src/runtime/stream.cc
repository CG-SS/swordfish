#include "swordfish/runtime/stream.hh"
#include "swordfish/runtime/components.hh"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>
#include <seastar/util/later.hh>
#include <seastar/core/timer.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/with_timeout.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/signal.hh>
#include <seastar/core/smp.hh>
#include <seastar/util/log.hh>

namespace {
// Thrown into the inter-layer queues to unblock a waiting consumer when the
// producing layer has finished. seastar::queue::abort wants an exception_ptr.
struct layer_closed : std::exception {
    const char* what() const noexcept override { return "stream layer closed"; }
};
}


namespace sf {

static seastar::logger slog("sf.stream");

stream::stream(stream_spec spec)
    : _spec(std::move(spec))
    , _to_pipeline(_spec.config.queue_depth)
    , _to_output(_spec.config.queue_depth) {}

stream::~stream() = default;

seastar::future<> stream::start() {
    _in    = _spec.make_input();
    _procs = _spec.make_processors();
    _out   = _spec.make_output();

    // One counter block per component: the input, each processor, the output.
    // Sized from what was actually BUILT rather than from the spec's identity
    // list, so a spec with no identities (a hand-built one, or a binary
    // compiled before identities existed) still counts -- it just reports the
    // aggregate, because get_stats() has no names to attach.
    _comp.assign(_procs.size() + 2, comp_counters{});
    _out_index = _procs.size() + 1;

    co_await _in->connect(_drain);
    co_await _out->connect(_drain);

    // Each layer is a fiber held by the gate, so close() waits for all three.
    (void)seastar::with_gate(_gate, [this] { return input_loop(); });
    (void)seastar::with_gate(_gate, [this] { return pipeline_loop(); });
    (void)seastar::with_gate(_gate, [this] { return output_loop(); });
}

// ---- input -----------------------------------------------------------------

seastar::future<> stream::input_loop() {
    try {
        while (!_drain.abort_requested()) {
            std::optional<std::pair<batch, ack_fn>> r;
            try {
                r = co_await _in->read_batch(_drain);
            } catch (const seastar::abort_requested_exception&) {
                break;
            }
            if (!r) break;                      // end of input

            _stats.batches_in  += 1;
            _stats.messages_in += r->first.size();
            if (!_comp.empty()) {
                _comp[0].batch_received += 1;
                _comp[0].received       += r->first.size();
            }

            // The same care the pipeline layer's push takes, and for the same
            // reason: `push_eventually` on a queue of `std::optional<transaction>`
            // materialises the optional AT THE CALL SITE, so an aborted queue
            // returns an exception with the transaction already moved into a
            // temporary that is then destroyed -- ack never called, source never
            // told. It was not even caught here: the exception went straight to
            // the layer-wide catch below and the batch, already counted in
            // `messages_in`, vanished. not_full() reports the abort and push()
            // never consumes when it declines, so the value stays in `slot`.
            std::optional<transaction> slot(
                transaction{std::move(r->first), std::move(r->second), &_stop_now});
            std::exception_ptr push_err;
            try {
                co_await _to_pipeline.not_full();
                _to_pipeline.push(std::move(slot));
                slot.reset();
            } catch (...) {
                push_err = std::current_exception();
            }
            if (push_err) {
                if (slot && slot->ack) {
                    _stats.nacks += 1;
                    co_await slot->ack(push_err);
                }
                break;
            }
        }
    } catch (...) {
        slog.error("input layer failed: {}", std::current_exception());
    }
    _input_done = true;
    co_await _to_pipeline.push_eventually(std::nullopt);   // drain, do not discard
}

// ---- pipeline --------------------------------------------------------------

seastar::future<> stream::pipeline_loop() {
    try {
        while (true) {
            std::optional<transaction> slot;
            try {
                slot = co_await _to_pipeline.pop_eventually();
            } catch (...) {
                break;                          // hard stop
            }
            if (!slot) break;                   // end of stream
            transaction t = std::move(*slot);

            std::vector<batch> batches;
            batches.push_back(std::move(t.payload));
            std::exception_ptr fatal;

            size_t proc_index = 0;
            for (auto& p : _procs) {
                const size_t this_index = proc_index++;
                std::vector<batch> next;
                // `this_index + 1` because _comp[0] is the input. Counted per
                // BATCH handed to the processor, which is what the reference's
                // processor_batch_received counts.
                const size_t ci = this_index + 1;
                for (auto& b : batches) {
                    if (b.empty()) continue;
                    if (ci < _comp.size()) {
                        _comp[ci].batch_received += 1;
                        _comp[ci].received       += b.size();
                    }
                    // Shallow copies (refcount bumps) so a throwing processor
                    // does not take the batch down with it: Benthos marks the
                    // messages and lets them continue
                    // (internal/pipeline/processor.go).
                    batch backup;
                    if (!_spec.config.strict_errors) {
                        backup.reserve(b.size());
                        for (const auto& m : b) backup.push_back(m.shallow_copy());
                    }
                    try {
                        auto out = co_await p->process(std::move(b), _stop_now);
                        for (auto& ob : out) {
                            if (ob.empty()) continue;
                            if (ci < _comp.size()) {
                                _comp[ci].batch_sent += 1;
                                _comp[ci].sent       += ob.size();
                            }
                            next.push_back(std::move(ob));
                        }
                    } catch (...) {
                        _stats.proc_errors += 1;
                        if (ci < _comp.size()) _comp[ci].errors += 1;
                        auto e = std::current_exception();
                        if (_spec.config.strict_errors) { fatal = e; break; }
                        std::string msg;
                        try { std::rethrow_exception(e); }
                        catch (const std::exception& ex) { msg = ex.what(); }
                        catch (...) { msg = "unknown processor error"; }
                        // Recorded once per failure rather than per message:
                        // error_source_*() reads it, and a batch of a thousand
                        // messages should not allocate a thousand copies.
                        auto info = std::make_shared<const error_source_info>(
                            error_source_info{p->name(), {},
                                              "pipeline.processors." +
                                                  std::to_string(this_index)});
                        for (auto& m : backup) m.set_error(msg, info);
                        next.push_back(std::move(backup));
                    }
                }
                if (fatal) break;
                batches = std::move(next);
                if (batches.empty()) break;     // everything filtered

                // `error_handling.strict`: "a processing error is terminal for
                // the affected message: it skips the remaining processors in its
                // pipeline and is rejected (nacked) at the output rather than
                // written." A processor that FAILS a message marks it rather
                // than throwing -- that is the non-strict behaviour and it
                // matches the reference -- so strict mode has to look at the
                // marks, here, after each stage.
                //
                // The granularity differs from the reference and the difference
                // is stated rather than hidden: swordfish acks per TRANSACTION,
                // so one errored message nacks the batch it arrived in, and its
                // clean siblings are not written either. The reference nacks
                // just the errored message. For a source that produces one
                // message per batch -- `generate`, `http_server`, `socket`, and
                // any batch of one -- the two are identical; for a real batch
                // swordfish is the more conservative of the two, since
                // at-least-once means a replayed sibling is recoverable and a
                // dropped one is not.
                if (_spec.config.strict_errors) {
                    for (const auto& b : batches) {
                        for (const auto& m : b) {
                            if (!m.has_error()) continue;
                            _stats.proc_errors += 1;
                            fatal = std::make_exception_ptr(std::runtime_error(
                                "error_handling.strict: " + m.error()));
                            break;
                        }
                        if (fatal) break;
                    }
                    if (fatal) break;
                }
            }

            if (fatal) {
                _stats.nacks += 1;
                co_await t.ack(fatal);
                continue;
            }
            if (batches.empty()) {
                // Filtering is a successful outcome: ack the source.
                _stats.filtered += 1;
                _stats.acks     += 1;
                co_await t.ack(nullptr);
                continue;
            }

            // One transaction in, possibly several out: aggregate their acks so
            // the source is resolved exactly once.
            auto group = ack_group::make(std::move(t.ack));
            std::vector<transaction> children;
            children.reserve(batches.size());
            for (auto& b : batches)
                children.emplace_back(std::move(b), group->add_child(), &_stop_now);
            co_await group->seal();

            // If the queue aborts mid-push, the children still in hand must be
            // nacked -- they were counted by the ack_group, so silently dropping
            // them would leave the source waiting forever.
            //
            // push_eventually() cannot be used for that. The queue holds
            // `std::optional<transaction>`, so passing a bare transaction
            // materialises a temporary optional AT THE CALL SITE, and seastar's
            // `_ex` fast path then returns an exception future with the value
            // sitting in that temporary -- which is destroyed, taking the ack
            // with it, while `children[pushed]` is left moved-from. Nacking the
            // moved-from copy called an empty noncopyable_function, so
            // std::bad_function_call escaped to the layer catch: the source was
            // neither acked nor nacked, the children after it were never nacked
            // either, and `nacks` counted one that never happened.
            //
            // not_full() is the half of the pair that reports an abort, and
            // push() never consumes when it declines. Together they leave the
            // value in `in_hand`, ours to nack, whichever way the queue fails.
            size_t pushed = 0;
            std::exception_ptr push_err;
            std::optional<transaction> in_hand;
            try {
                for (; pushed < children.size(); ++pushed) {
                    in_hand.emplace(std::move(children[pushed]));
                    co_await _to_output.not_full();
                    _to_output.push(std::move(in_hand));
                    in_hand.reset();
                }
            } catch (...) {
                push_err = std::current_exception();
            }
            // co_await is not allowed inside a catch handler, so the nacks
            // happen here instead.
            if (push_err) {
                if (in_hand && in_hand->ack) {
                    _stats.nacks += 1;
                    co_await in_hand->ack(push_err);
                }
                in_hand.reset();
                for (size_t i = pushed + 1; i < children.size(); ++i) {
                    if (!children[i].ack) continue;
                    _stats.nacks += 1;
                    co_await children[i].ack(push_err);
                }
                break;
            }
        }
    } catch (...) {
        slog.error("pipeline layer failed: {}", std::current_exception());
    }
    _pipeline_done = true;
    co_await _to_output.push_eventually(std::nullopt);
}

// ---- output ----------------------------------------------------------------

seastar::future<> stream::output_loop() {
    _in_flight.emplace(_out->max_in_flight());
    try {
        while (true) {
            std::optional<transaction> slot;
            try {
                slot = co_await _to_output.pop_eventually();
            } catch (...) {
                break;                          // hard stop
            }
            if (!slot) break;                   // end of stream
            transaction t = std::move(*slot);
            // Take a unit, then launch the write WITHOUT awaiting it, holding
            // the unit until it finishes. Awaiting inline (as this used to) meant
            // only one write was ever outstanding and max_in_flight had no
            // effect whatsoever.
            auto units = co_await seastar::get_units(*_in_flight, 1);
            // The lambda must NOT itself be a coroutine. with_gate() destroys the
            // closure once the returned future suspends, and a coroutine lambda
            // keeps its captures in that closure -- so `t` (and its ack_fn) would
            // be destroyed while still in use. Delegating to a member coroutine
            // whose parameters are by value moves them into the coroutine frame,
            // which lives as long as the coroutine.
            (void)seastar::with_gate(_writes,
                [this, t = std::move(t), u = std::move(units)]() mutable {
                    return do_write(std::move(t), std::move(u));
                });
        }
    } catch (...) {
        slog.error("output layer failed: {}", std::current_exception());
    }
    // Told before the gate is awaited, and that order is required: an output
    // that holds batches back releases them here, and the writes it releases
    // are the ones the gate below is waiting for.
    if (_out) {
        try { co_await _out->drain(); }
        catch (...) { slog.warn("output drain failed: {}", std::current_exception()); }
    }
    // Outside the try, so it runs on every path: every launched write must
    // finish before this layer is done, or its ack would be lost and its
    // semaphore units would outlive the semaphore.
    // Swallowed deliberately -- a teardown failure must not mask whatever
    // caused the teardown -- but logged, so it is not invisible.
    try {
        co_await _writes.close();
    } catch (...) {
        slog.debug("closing in-flight writes failed: {}", std::current_exception());
    }
    _output_done = true;
    if (!_finished_set) { _finished_set = true; _finished.set_value(); }
}

seastar::future<> stream::do_write(transaction t, seastar::semaphore_units<> units) {
    const size_t n = t.payload.size();
    std::exception_ptr err;
    // `error_handling.strict` AT THE OUTPUT, which is where the reference puts
    // it -- the wrapper that owns the writer, after everything that could mark a
    // message (benthos internal/component/output/async_writer.go).
    //
    // The pipeline's own per-stage check does not cover this. A message marked
    // by an INPUT processor reaches here with no pipeline processor having run,
    // so nothing looked at the mark: measured with `strict: true` and an input
    // processor of `root = throw(...)`, swordfish wrote both messages and acked
    // them while the reference wrote nothing and rejected each. The two other
    // places an error can enter -- an output's own `processors:` and a
    // `batching.processors:` list -- run INSIDE write_batch below and are
    // checked there, in processed_output and batched_output.
    if (_spec.config.strict_errors) err = strict_rejection(t.payload);
    const size_t n_out = t.payload.size();
    if (!err) {
        try {
            co_await _out->write_batch(std::move(t.payload), _stop_now);
            if (_out_index < _comp.size()) {
                _comp[_out_index].batch_sent += 1;
                _comp[_out_index].sent       += n_out;
            }
        } catch (...) {
            err = std::current_exception();
            if (_out_index < _comp.size()) _comp[_out_index].errors += 1;
        }
    } else {
        _stats.proc_errors += 1;
        // A strict rejection is the OUTPUT refusing the batch, so it is that
        // component's error even though no write was attempted.
        if (_out_index < _comp.size()) _comp[_out_index].errors += 1;
    }
    if (_out_index < _comp.size()) {
        _comp[_out_index].batch_received += 1;
        _comp[_out_index].received       += n_out;
    }
    if (err) { _stats.nacks += 1; }
    else { _stats.acks += 1; _stats.batches_out += 1; _stats.messages_out += n; }
    co_await t.ack(err);
}

stream::stats stream::get_stats() const {
    stats s = _stats;
    // The identities are attached HERE, once per scrape, rather than carried on
    // the counters -- which are touched per batch. A spec with no identities
    // reports the aggregate only, which is what a hand-built spec and an older
    // compiled binary both do.
    const auto& ids = _spec.component_idents;
    if (!ids.empty() && ids.size() == _comp.size()) {
        s.components.reserve(_comp.size());
        for (size_t i = 0; i < _comp.size(); ++i) {
            component_stats cs;
            cs.id = ids[i];
            cs.c  = _comp[i];
            // `connection_up` is a gauge, and the only one here. It is read from
            // the component rather than counted, for the same reason /ready is:
            // the connection_status graph is the only thing that actually knows.
            if (cs.id.kind == comp_kind::input)
                cs.c.connection_up = (_in && _in->status().connected) ? 1 : 0;
            else if (cs.id.kind == comp_kind::output)
                cs.c.connection_up = (_out && _out->status().connected) ? 1 : 0;
            s.components.push_back(std::move(cs));
        }
    }
    // Read from the components. `/ready` used to be hardcoded true, so the whole
    // connection_status graph the connectors fill in was unreachable and a probe
    // passed for a pipeline whose input had never connected. Before start() has
    // built them there is nothing connected either, which is the right answer.
    s.not_ready_in  = (_in  && _in->status().connected)  ? 0 : 1;
    s.not_ready_out = (_out && _out->status().connected) ? 0 : 1;
    return s;
}

seastar::future<> stream::wait_until_drained() {
    return _finished.get_shared_future();
}

// ---- shutdown --------------------------------------------------------------

// gate::close() asserts if called twice, so both shutdown paths funnel through
// here. Without this, a graceful shutdown that TIMED OUT would escalate to
// stop_now(), close the gate a second time and abort the process -- crashing at
// exactly the moment the operator least wants it to.
seastar::future<> stream::close_gate_once() {
    if (!_gate_closed) {
        _gate_closed = true;
        _gate_close = seastar::shared_future<>(_gate.close());
    }
    // The SAME close, awaited again -- not a ready future. Returning a ready one
    // was the whole defect: with_timeout "doesn't cancel any tasks associated
    // with the original future", so a graceful close that timed out left the
    // gate closing in the background with the flag already set, and the forced
    // path then awaited nothing, closed the components and returned. sharded<>
    // destroyed the service next, and ~gate()'s "gate destroyed with outstanding
    // requests" assertion aborted the process -- exit 134 at exactly the moment
    // an operator least wants it.
    return _gate_close.get_future();
}

seastar::future<> stream::stop_gracefully(std::chrono::milliseconds budget) {
    _drain.request_abort();                     // stop reading; let the rest drain
    co_await seastar::with_timeout(
        seastar::lowres_clock::now() + budget, close_gate_once());
}

seastar::future<> stream::stop_now(std::chrono::milliseconds budget) {
    _drain.request_abort();
    _stop_now.request_abort();

    // Nack whatever is still queued before discarding it. At-least-once means
    // the source must learn these were not delivered, so it can redeliver.
    //
    // Drained REPEATEDLY, and only then aborted. One pass was not enough, and
    // the order cannot simply be reversed: seastar's queue::abort() DESTROYS
    // whatever it holds, so aborting first would drop those transactions without
    // telling anyone at all. Meanwhile pop() wakes a producer blocked in
    // not_full(), and its push lands after the loop has moved past that queue --
    // so a batch could be neither acked nor nacked and at-least-once was broken
    // on exactly the path that exists to preserve it. `_drain` is already
    // aborted, so each producer adds at most one more before it exits its own
    // loop; the pass counter is a backstop rather than the mechanism.
    auto e = std::make_exception_ptr(layer_closed{});

    // Collected SYNCHRONOUSLY -- no co_await inside the pop loop. Acking as we
    // popped let the layer consumers interleave: a push wakes output_loop's
    // pop_eventually, whose continuation is scheduled, and if this loop empties
    // the queue first that continuation calls pop() on an empty queue, which is
    // a SEASTAR_ASSERT and aborts the process. The acks are run afterwards,
    // where suspending is harmless.
    std::vector<ack_fn> to_nack;
    const auto collect = [&] {
        for (auto* q : {&_to_pipeline, &_to_output})
            while (!q->empty()) {
                auto slot = q->pop();
                if (slot && slot->ack) to_nack.push_back(std::move(slot->ack));
            }
    };

    // Twice, with the abort between. abort() DESTROYS what the queue holds, so
    // it cannot come first; but a single pass before it was not enough either,
    // because popping wakes a producer blocked in not_full() whose push then
    // lands after this loop has moved past that queue -- neither acked nor
    // nacked, which is at-least-once broken on the path that exists to keep it.
    // After the abort a blocked producer gets an exception instead and nacks its
    // own batch, so the second pass only has to catch one that was already past
    // not_full() and committed to pushing.
    collect();
    _to_pipeline.abort(e);
    _to_output.abort(e);
    co_await seastar::yield();
    collect();
    _to_pipeline.abort(e);
    _to_output.abort(e);

    for (const auto& a : to_nack) {
        _stats.nacks += 1;
        co_await a(e);
    }
    // Wait for the layer fibers for real, bounded by what is left of the budget.
    bool still_running = false;
    try {
        co_await seastar::with_timeout(seastar::lowres_clock::now() + budget,
                                       close_gate_once());
    } catch (const seastar::timed_out_error&) {
        still_running = true;
    } catch (...) {
        slog.debug("forced gate close failed: {}", std::current_exception());
    }
    if (still_running) {
        // Everything that could unblock a layer fiber has been aborted by now,
        // so one still running is in a wait that cannot be interrupted -- a
        // batching processor's `sleep`, say, which runs against its own abort
        // source. Waiting hangs the shutdown; RETURNING destroys the stream
        // underneath the fiber, which trips the gate assertion and takes the
        // explanation down with it. So: say what is happening, then wait.
        // Named, not just counted: "something did not stop" sends an operator
        // looking in three places. `_input_done` used to be written and never
        // read, which is exactly the state this needs.
        std::string outstanding;
        const auto add = [&outstanding](const char* what) {
            outstanding += (outstanding.empty() ? "" : ", ");
            outstanding += what;
        };
        if (!_input_done)    add("input");
        if (!_pipeline_done) add("pipeline");
        if (!_output_done)   add("output");
        if (outstanding.empty()) outstanding = "an unidentified layer";
        slog.error("the {} layer is still running {}ms after the forced shutdown "
                   "and cannot be interrupted; waiting for it rather than "
                   "destroying the stream underneath it",
                   outstanding, budget.count());
        try {
            co_await close_gate_once();
        } catch (...) {
            slog.debug("forced gate close failed: {}", std::current_exception());
        }
    }
    if (!_finished_set) { _finished_set = true; _finished.set_value(); }
}

seastar::future<> stream::stop() {
    // Idempotent, and SHARED: sharded<>::stop() invokes the service's stop()
    // itself, so a caller that also stops explicitly would otherwise close the
    // gate twice and trip an assertion inside Seastar. Returning a ready future
    // to the second caller is not enough -- the signal path's forced stop runs
    // from a discarded future, and sharded<>::stop() then destroyed the service
    // while it was still running.
    if (!_stopped) {
        _stopped = true;
        _stop_fut = seastar::shared_future<>(do_stop());
    }
    return _stop_fut.get_future();
}

seastar::future<> stream::do_stop() {

    // Three-quarters of the budget for graceful, the rest for forceful, matching
    // the ratchet in internal/stream/type.go.
    const auto total    = _spec.config.shutdown_timeout;
    const auto graceful = total - total / 4;

    // co_await is not permitted inside a catch handler, so the escalation is
    // recorded and acted on afterwards. `_forced` skips the graceful phase
    // outright: the signal path arrives here having already spent that budget on
    // a drain that did not finish, and repeating it doubled `shutdown_timeout`.
    bool needs_force = _forced;
    if (!needs_force) {
        try {
            co_await stop_gracefully(graceful);
        } catch (...) {
            needs_force = true;
        }
    }
    if (needs_force) {
        if (_forced)
            slog.info("forced shutdown requested; skipping the graceful phase");
        else
            slog.info("graceful shutdown did not complete within {}ms; forcing",
                      graceful.count());
        co_await stop_now(total - graceful);
    }
    // Each close is guarded so one failure cannot skip the rest. These run in
    // sequence on the shutdown path of every pipeline, and a throwing input
    // close would otherwise leave every processor and the output open --
    // holding sockets, files and in-flight batches that nothing will release.
    // The same shape, in the Kafka consumer's teardown, ended a chaos run with
    // a core dump.
    auto quietly = [](const char* what, auto&& f) -> seastar::future<> {
        try { co_await std::forward<decltype(f)>(f); }
        catch (const std::exception& e) { slog.warn("{} close failed: {}", what, e.what()); }
    };
    if (_in)  co_await quietly("input", _in->close());
    for (auto& p : _procs) co_await quietly("processor", p->close());
    if (_out) co_await quietly("output", _out->close());
}

} // namespace sf

namespace sf {

namespace {
// Per-shard holder so sharded<> can own the stream.
class generated_service {
public:
    explicit generated_service(stream_spec spec) : _stream(std::move(spec)) {}
    seastar::future<> start() { return _stream.start(); }
    seastar::future<> wait()  { return _stream.wait_until_drained(); }
    void              drain() { _stream.request_drain(); }
    seastar::future<> stop()  { return _stream.stop(); }
    // The signal path's escalation, as in run_main.cc: the graceful budget was
    // already spent draining, so this skips straight to the forceful half.
    seastar::future<> force_stop() { _stream.request_force(); return _stream.stop(); }
    stream::stats     stats() const { return _stream.get_stats(); }
private:
    stream _stream;
};
} // namespace

// cppcheck-suppress unusedFunction
//   Called only from the main() that `swordfish build` generates.
int run_generated(int argc, char** argv, std::function<stream_spec()> factory) {
    seastar::app_template::config ac;
    ac.name = "swordfish";
    seastar::app_template app(std::move(ac));
    return app.run(argc, argv, [factory = std::move(factory)] () -> seastar::future<int> {
        seastar::sharded<generated_service> streams;
        // Build the spec once and let sharded<> copy it to each shard. A
        // sharded_parameter would evaluate the factory ON each shard, and the
        // lambda would then be reaching into shard 0's coroutine frame -- which
        // segfaults as soon as smp::count > 1.
        // Kept, because the observability server needs the http block and the
        // factory must only be called once (see above).
        stream_spec spec_for_http = factory();
        const http_spec http_cfg = spec_for_http.http;
        // Read before the move, for the same reason http_cfg is.
        const auto delay_cfg = spec_for_http.config.shutdown_delay;
        const auto budget    = spec_for_http.config.shutdown_timeout;
        // `shutdown_timeout` applied to the SIGNAL path, which never had it: the
        // handler below only requested a drain and the wait that follows had
        // nothing behind it, so a pipeline blocked in a processor or a stuck
        // output stayed alive indefinitely. stream::stop() is where the
        // graceful/forceful ratchet lives and is idempotent, so escalating to it
        // here leaves the streams.stop() at the end unharmed. Mirrors
        // run_main.cc, because a compiled binary must shut down the way an
        // interpreted run does.
        co_await streams.start(std::move(spec_for_http));
        // sharded<> asserts if it is destroyed without stop(). A failing
        // input or output connect would otherwise abort the process with
        // "terminate called without an active exception" instead of reporting
        // the error, so every path below must reach streams.stop().
        std::exception_ptr err;
        std::unique_ptr<observe_server> obs;
        stream::stats total{};
        // Declared OUTSIDE the try: the teardown after the catch has to disarm
        // them whichever way the block exits.
        auto live  = seastar::make_lw_shared<bool>(true);
        auto force = seastar::make_lw_shared<seastar::timer<>>();
        // Nothing below captures this coroutine's frame BY REFERENCE: seastar
        // never unregisters a signal handler, so one delivered after the frame
        // is gone would dereference freed memory. `live` is cleared before the
        // streams are torn down and every callback checks it.
        auto* svc = &streams;                   // valid for exactly as long as *live
        force->set_callback([svc, live, budget] {
            if (!*live) return;
            slog.warn("shutdown did not complete within {}ms of the signal; forcing",
                      budget.count());
            (void)svc->invoke_on_all(&generated_service::force_stop);
        });
        try {
        // SIGINT/SIGTERM drain rather than kill: in-flight messages finish and
        // are acked, which is what at-least-once delivery requires on shutdown.
        for (int sig : {SIGINT, SIGTERM})
            seastar::handle_signal(sig, [svc, live, force, sig, budget] {
                if (!*live) return;
                slog.info("signal {} received, draining", sig);
                (void)svc->invoke_on_all([](auto& s) {
                    s.drain();
                    return seastar::make_ready_future<>();
                });
                if (!force->armed()) force->arm(budget);
            }, true);

        co_await streams.invoke_on_all(&generated_service::start);

        // A COMPILED pipeline serves the same endpoints as an interpreted one.
        // Observability that only worked under `swordfish run` would be absent
        // from exactly the artefact that gets deployed.
        obs = std::make_unique<observe_server>(http_cfg, [&streams]() {
            return gather_from(streams);
        });
        co_await obs->start();

        co_await streams.invoke_on_all(&generated_service::wait);

        co_await streams.invoke_on_all([&total](const generated_service& s) {
            total += s.stats();
            return seastar::make_ready_future<>();
        });
        } catch (...) {
            err = std::current_exception();
        }
        // From here the streams are being torn down, so neither the handlers nor
        // the forcing timer may touch them again.
        *live = false;
        force->cancel();
        // `shutdown_delay`: "a period of time to wait for metrics and traces to
        // be pulled or pushed from the process". So it runs after the stream has
        // finished and BEFORE the observability server is stopped -- stopping it
        // first would close the endpoint the delay exists to keep open.
        //
        // Applied on the error path too, because that is what the reference
        // does and because a failure is exactly when a last scrape is worth
        // having. It is a plain sleep: a signal does not cut it short.
        if (delay_cfg.count() > 0) {
            slog.info("shutdown_delay: holding for {}ms before closing",
                      delay_cfg.count());
            co_await seastar::sleep(delay_cfg);
        }
        if (obs) {
            try { co_await obs->stop(); }
            catch (const std::exception& e) { slog.warn("http server stop failed: {}", e.what()); }
        }
        co_await streams.stop();

        if (err) {
            try { std::rethrow_exception(err); }
            catch (const std::exception& e) { slog.error("pipeline failed: {}", e.what()); }
            co_return 1;
        }
        slog.info("in={} out={} acks={} nacks={} filtered={} proc_errors={}",
                  total.messages_in, total.messages_out, total.acks,
                  total.nacks, total.filtered, total.proc_errors);
        co_return 0;
    });
}

} // namespace sf
