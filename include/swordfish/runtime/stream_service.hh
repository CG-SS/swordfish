// One stream, replicated across shards.
//
// This was a local class in run_main.cc until `swordfish streams` needed the
// same thing N times over. Both entry points hold their streams identically --
// a `sharded<stream_service>` per stream, one independent
// input -> pipeline -> output per shard -- so a stream running under `streams`
// behaves exactly as the same config does under `run`. That is the property
// that makes streams mode worth having rather than a second runtime.
#pragma once

#include "swordfish/runtime/stream.hh"

#include <seastar/core/future.hh>

#include <utility>

namespace sf {

class stream_service {
public:
    explicit stream_service(stream_spec spec) : _stream(std::move(spec)) {}

    seastar::future<> start() { return _stream.start(); }
    seastar::future<> wait()  { return _stream.wait_until_drained(); }
    void              drain() { _stream.request_drain(); }
    seastar::future<> stop()  { return _stream.stop(); }

    // The signal path's escalation: the graceful budget was already spent
    // draining, so this goes straight to the forceful half rather than spending
    // a second `shutdown_timeout` on a drain that has demonstrably not finished.
    seastar::future<> force_stop() { _stream.request_force(); return _stream.stop(); }

    stream::stats stats() const { return _stream.get_stats(); }

private:
    stream _stream;
};

} // namespace sf
