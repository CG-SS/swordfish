// The stream: input -> [buffer] -> pipeline -> output, one complete instance per
// shard. Nothing crosses cores on the happy path.
#pragma once

#include "swordfish/runtime/component.hh"
#include "swordfish/runtime/component_stats.hh"
#include "swordfish/runtime/observe.hh"

#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/queue.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_future.hh>

#include <chrono>
#include <optional>
#include <functional>

namespace sf {

struct stream_config {
    // Queue depth 1 reproduces Go's unbuffered-channel backpressure, which is
    // what Benthos relies on. Higher trades tail latency for throughput.
    size_t                    queue_depth      = 1;
    // The reference's defaults, from benthos-main/internal/config/schema.go.
    std::chrono::milliseconds shutdown_timeout{20000};
    // "A period of time to wait for metrics and traces to be pulled or pushed
    // from the process" -- so it is a wait AFTER the stream has stopped and
    // before the process exits, not part of the drain.
    std::chrono::milliseconds shutdown_delay{0};
    // `error_handling.strict`: a processing error is terminal for the affected
    // message -- it skips the remaining processors and is nacked at the output
    // rather than written.
    bool                      strict_errors    = false;
};

// Built per shard. The factories run on the shard that will own the components,
// so nothing is constructed on one core and used on another.
struct stream_spec {
    std::function<input_ptr()>                    make_input;
    std::function<std::vector<processor_ptr>()>   make_processors;
    std::function<output_ptr()>                   make_output;
    stream_config                                 config;
    // Carried through from the `http` block so a COMPILED pipeline serves the
    // same endpoints as an interpreted one. Observability that only worked in
    // `swordfish run` would be missing from exactly the artefact that gets
    // deployed.
    http_spec                                     http;
    // The identity of each component, in metrics order: the input, then each
    // processor, then the output. Filled by build_stream_spec from the parsed
    // pipeline -- a component built by a factory does not know its own place in
    // the config, and that place is exactly what the reference keys every
    // metric series on.
    //
    // Empty is legal and means "report the aggregate only": a compiled binary
    // from before this existed, or a spec built by hand in a test.
    std::vector<comp_ident>                       component_idents;
};

// Entry point for a generated program. Everything a compiled pipeline needs at
// runtime lives in libswordfish_rt; main.cc supplies only the factory.
int run_generated(int argc, char** argv, std::function<stream_spec()> factory);

class stream {
public:
    explicit stream(stream_spec spec);
    ~stream();

    seastar::future<> start();

    // Resolves when the input is exhausted and every layer has finished of its
    // own accord. For a finite source that is natural completion; for an
    // unbounded one it never resolves, and the caller stops the stream instead.
    // Without this a caller can only force a stop, which truncates the pipeline.
    seastar::future<> wait_until_drained();

    // Stop reading and let in-flight work finish. This is what a SIGTERM should
    // trigger: an unbounded source otherwise never completes, so
    // wait_until_drained() would park forever and the process would ignore the
    // signal entirely.
    void request_drain() { if (!_drain.abort_requested()) _drain.request_abort(); }

    // Three-stage shutdown, matching internal/stream/type.go: stop reading and
    // let in-flight work drain; then abandon it; then wait for the layer fibers
    // and, if they will not stop, name which of them is still running rather
    // than destroying the stream underneath them.
    seastar::future<> stop_gracefully(std::chrono::milliseconds budget);
    seastar::future<> stop_now(std::chrono::milliseconds budget);
    seastar::future<> stop();
private:
    seastar::future<> do_stop();
public:
    // Skip stop()'s graceful phase. The signal path has already spent the
    // graceful budget on a drain that did not finish, so spending a second one
    // inside stop() would double `shutdown_timeout`.
    void request_force() noexcept { _forced = true; }          // the full ratchet

    struct stats {
        uint64_t batches_in    = 0;
        uint64_t batches_out   = 0;
        uint64_t messages_in   = 0;
        uint64_t messages_out  = 0;
        uint64_t acks          = 0;
        uint64_t nacks         = 0;
        uint64_t filtered      = 0;
        uint64_t proc_errors   = 0;
        // 1 per shard whose input (resp. output) reports itself disconnected, so
        // summing across shards gives "how many are not ready". Kept apart
        // because the reference's /ready names which SIDE is down, and a probe
        // that says "something is disconnected" sends an operator looking in two
        // places instead of one.
        uint64_t not_ready_in  = 0;
        uint64_t not_ready_out = 0;

        // Per component, in metrics order: the input, then each processor, then
        // the output. Every shard builds the same stream from the same spec, so
        // component i is the same component everywhere -- which is why these sum
        // by INDEX rather than by path, and why a shard that has not started yet
        // simply contributes an empty list.
        std::vector<component_stats> components;

        // Summing lives with the fields. It was open-coded in three places --
        // both entry points and the end-of-run summary -- so adding a counter
        // meant remembering all three, and a forgotten one would under-report
        // silently rather than fail to compile.
        stats& operator+=(const stats& o) {
            batches_in  += o.batches_in;   batches_out  += o.batches_out;
            messages_in += o.messages_in;  messages_out += o.messages_out;
            acks        += o.acks;         nacks        += o.nacks;
            filtered    += o.filtered;     proc_errors  += o.proc_errors;
            // The first shard to report decides the identities; the rest add
            // their counts. A mismatch in length would mean two shards built
            // different pipelines, which cannot happen from one spec.
            if (components.empty()) components = o.components;
            else for (size_t i = 0; i < components.size() && i < o.components.size(); ++i)
                components[i].c += o.components[i].c;
            not_ready_in  += o.not_ready_in;
            not_ready_out += o.not_ready_out;
            return *this;
        }
    };
    // BY VALUE, because `not_ready` is read from the components at the moment of
    // the call rather than counted as it goes: a readiness probe has to see the
    // connection state now, not what it was when a counter last moved.
    stats get_stats() const;

private:
    seastar::future<> close_gate_once();
    // Takes its arguments BY VALUE: as a coroutine, its frame owns them, which a
    // capturing lambda coroutine would not.
    seastar::future<> do_write(transaction t,
                               seastar::semaphore_units<> units);
    seastar::future<> input_loop();
    seastar::future<> pipeline_loop();
    seastar::future<> output_loop();

    stream_spec                    _spec;
    input_ptr                      _in;
    std::vector<processor_ptr>     _procs;
    output_ptr                     _out;

    // std::nullopt is an end-of-stream sentinel. Aborting the queue instead
    // would DISCARD whatever is still queued, and those transactions would never
    // be acked -- the ack property test caught exactly that.
    seastar::queue<std::optional<transaction>> _to_pipeline;
    seastar::queue<std::optional<transaction>> _to_output;

    seastar::gate                  _gate;
    seastar::abort_source          _drain;      // soft stop: stop reading
    seastar::abort_source          _stop_now;   // hard stop: abandon in flight
    // Which layer fibers have finished. `_input_done` was written and never
    // read; all three are read now, by the forced-shutdown path, so it can NAME
    // the layer that would not stop instead of saying only that something did
    // not -- which is what the comment above stop_now() has always promised.
    bool                           _input_done    = false;
    bool                           _pipeline_done = false;
    bool                           _output_done   = false;
    seastar::shared_promise<>      _finished;
    bool                           _finished_set = false;
    bool                           _stopped = false;   // stop() is idempotent
    bool                           _forced  = false;   // skip the graceful phase
    // The ONE stop, shared by every caller. Idempotence used to mean "the second
    // caller returns immediately", which is not the same thing: the signal
    // path's forced stop runs from a DISCARDED future, so sharded<>::stop()
    // returned at once and destroyed the service underneath it -- a segfault on
    // shard 0 with a jump to 0x5d00000001, every time a SIGTERM reached the
    // forcing path. A second caller now awaits the first.
    seastar::shared_future<>       _stop_fut;
    bool                           _gate_closed = false;
    // The ONE gate-close future, shared by every caller. A plain
    // `_gate_closed` flag was not enough: with_timeout does not cancel the
    // future it timed out on, so after a graceful close timed out the flag
    // was set and the forced path had nothing left to wait for.
    seastar::shared_future<>       _gate_close;
    seastar::gate                  _writes;            // in-flight output writes
    // The in-flight semaphore must OUTLIVE the writes that hold its units. As a
    // local inside output_loop it was destroyed while detached writes still held
    // units, which crashed intermittently.
    std::optional<seastar::semaphore> _in_flight;
    stats                          _stats;
    // Per-component counters, in metrics order: [0] the input, [1..N] the
    // processors, [N+1] the output. A plain vector indexed directly, because
    // these are touched on the message path -- a map keyed by path string would
    // put a hash and a compare between every batch and its counter.
    //
    // Sized in start(), once the processors have been built. Empty until then,
    // and every increment guards on that: get_stats() can be called by a scrape
    // before the stream has started.
    std::vector<comp_counters>     _comp;
    size_t                         _out_index = 0;   // _comp[_out_index] is the output
};

} // namespace sf
