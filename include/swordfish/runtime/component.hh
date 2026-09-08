// Component interfaces. Deliberately close to benthos-main/public/service so
// porting a connector is mostly mechanical.
#pragma once

#include "swordfish/runtime/transaction.hh"
#include "swordfish/runtime/transform.hh"

#include <seastar/core/future.hh>
#include <seastar/core/abort_source.hh>

#include <memory>
#include <optional>
#include <string>

namespace sf {

struct connection_status {
    bool        connected = false;
    std::string label;
    std::string error;
};

class input {
public:
    virtual ~input() = default;
    virtual seastar::future<> connect(seastar::abort_source&) {
        return seastar::make_ready_future<>();
    }
    // std::nullopt means end of input: the source is exhausted and the stream
    // should drain and stop. An exception means a read failure.
    virtual seastar::future<std::optional<std::pair<batch, ack_fn>>>
        read_batch(seastar::abort_source&) = 0;
    virtual seastar::future<> close() { return seastar::make_ready_future<>(); }
    virtual connection_status status() const {
        connection_status s;
        s.connected = true;
        return s;
    }
};

class processor {
public:
    virtual ~processor() = default;
    // Returns zero or more batches. Zero means every message was filtered,
    // which is a successful outcome, not an error.
    virtual seastar::future<std::vector<batch>> process(batch, seastar::abort_source&) = 0;
    virtual seastar::future<> close() { return seastar::make_ready_future<>(); }
    virtual std::string name() const = 0;
};

class output {
public:
    virtual ~output() = default;
    virtual seastar::future<> connect(seastar::abort_source&) {
        return seastar::make_ready_future<>();
    }
    virtual seastar::future<> write_batch(batch, seastar::abort_source&) = 0;

    // No further batch will be written: the source is exhausted and everything
    // it produced has been handed over. An output holding data back must
    // release it HERE rather than in close().
    //
    // The distinction is not cosmetic. `close()` runs after the stream has
    // drained, and the stream cannot drain while a write_batch is unresolved --
    // so an output that waits for close() to flush its last partial batch
    // deadlocks: the batch is waiting for a shutdown that is waiting for the
    // batch. A batching output found exactly that.
    virtual seastar::future<> drain() { return seastar::make_ready_future<>(); }

    virtual seastar::future<> close() { return seastar::make_ready_future<>(); }
    virtual connection_status status() const {
        connection_status s;
        s.connected = true;
        return s;
    }
    virtual size_t max_in_flight() const { return 1; }
};

using input_ptr     = std::unique_ptr<input>;
using processor_ptr = std::unique_ptr<processor>;
using output_ptr    = std::unique_ptr<output>;

} // namespace sf
