// A connection to one Kafka broker.
//
// Owns the socket, the correlation-id sequence, and the demultiplexing of
// responses back to the futures that are waiting for them. Requests are
// PIPELINED: several may be in flight, and the broker answers them in order per
// connection, but the correlation id is what actually matches a response to its
// request, so nothing here depends on that ordering.
//
// One connection belongs to one shard. There is no locking because there is no
// sharing -- the share-nothing rule that makes the rest of the runtime work
// applies here too.
#pragma once

#include "swordfish/kafka/protocol.hh"
#include "messages.hh"

#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/net/api.hh>

#include <chrono>
#include <map>
#include <memory>
#include <optional>
#include <string>

namespace sf::kafka {

// A broker-reported error. Distinct from protocol_error, which means the bytes
// did not make sense; this one means they made sense and said "no".
class broker_error : public std::runtime_error {
public:
    broker_error(int16_t err_code, const std::string& what)
        : std::runtime_error(what), code(err_code) {}
    int16_t code;
};

// The error codes we act on by name. The full list is long and mostly means
// "give up"; these are the ones with specific handling.
namespace err {
constexpr int16_t none                     = 0;
constexpr int16_t offset_out_of_range      = 1;
constexpr int16_t unknown_topic_or_part    = 3;
constexpr int16_t leader_not_available     = 5;
constexpr int16_t not_leader_or_follower   = 6;
constexpr int16_t request_timed_out        = 7;
constexpr int16_t coordinator_load_in_progress = 14;
constexpr int16_t coordinator_not_available = 15;
constexpr int16_t not_coordinator          = 16;
constexpr int16_t illegal_generation       = 22;
constexpr int16_t unknown_member_id        = 25;
constexpr int16_t rebalance_in_progress    = 27;
constexpr int16_t unsupported_version      = 35;
constexpr int16_t fenced_instance_id       = 82;
}

const char* error_name(int16_t code) noexcept;
// Name and number together: an unnamed code printed as bare "ERROR" is
// undiagnosable, and the number is what the protocol documentation lists.
std::string describe_error(int16_t code);
// True where retrying the same request after a metadata refresh is the right
// response, rather than failing the pipeline.
bool is_retriable(int16_t code) noexcept;

class connection {
public:
    connection() = default;
    ~connection();
    connection(connection&&) = delete;
    connection& operator=(connection&&) = delete;

    // Opens the socket and negotiates API versions. `client_id` is what shows
    // up in the broker's logs and metrics, so it is worth setting.
    //
    // The host may be a NAME: brokers advertise themselves by hostname far more
    // often than by address, and `seed_brokers` in a config is almost always
    // written that way.
    seastar::future<> connect(std::string host, uint16_t port, std::string client_id);
    // "host:port", the form seed_brokers uses.
    seastar::future<> connect(const std::string& hostport, std::string client_id);
    seastar::future<> close();

    // False once the read loop has ended, not merely once close() was called.
    // A connection whose reader is gone cannot complete anything: the socket is
    // still open, so the old `_out.has_value()` answered yes and callers happily
    // sent requests into it that could never be answered.
    bool connected() const noexcept { return _out.has_value() && !_dead; }

    // Kafka's request.timeout.ms. Must exceed the broker-side wait of the
    // longest request in use -- a Fetch legitimately parks for
    // `fetch_max_wait` -- so the consumer raises it rather than using this
    // default blindly.
    void set_request_timeout(std::chrono::milliseconds t) noexcept { _request_timeout = t; }

    // The highest version of `key` both ends support, or -1 when the broker does
    // not support the API at all. Valid after connect().
    int16_t negotiated(int16_t api_key) const noexcept;

    // Send a request and wait for its response. The version is chosen by
    // negotiation unless one is given explicitly.
    template <class Req, class Resp>
    seastar::future<Resp> send(const Req& req, int16_t version = -1);

    // Send a request the broker will NOT answer, and return once it is on the
    // wire. The only such request Kafka has is Produce with acks=0, and it is
    // not an optimisation: waiting for a response that is never coming parks
    // for the whole request timeout and then declares the connection dead.
    template <class Req>
    seastar::future<> send_oneway(const Req& req, int16_t version = -1);

    const std::string& peer() const noexcept { return _peer; }

private:
    seastar::future<> read_loop();
    // The version to actually send: `requested` (or the broker's max when it is
    // negative) clamped into the range this build implements, with a named error
    // when the two ranges do not overlap. Defined in the .cc; called from the
    // send templates below.
    int16_t pick_version(int16_t api_key, int16_t requested,
                         int16_t our_min, int16_t our_max) const;
    seastar::future<> write_frame(std::string frame);
    seastar::future<std::string> exchange(std::string frame, int32_t correlation_id);
    void fail_pending(std::exception_ptr e);

    std::optional<seastar::connected_socket>   _socket;
    std::optional<seastar::input_stream<char>> _in;
    std::optional<seastar::output_stream<char>> _out;
    std::string                                _client_id;
    std::string                                _peer;
    int32_t                                    _next_correlation = 1;
    std::map<int32_t, seastar::promise<std::string>> _pending;
    // api key -> the range the BROKER advertises. Both ends are needed: the max
    // to clamp down to, and the min to detect a broker whose oldest supported
    // version is newer than anything this build can encode.
    std::map<int16_t, std::pair<int16_t, int16_t>> _versions;
    // The read loop's future, awaited by close(). Deliberately NOT a gate: a
    // gate asserts in its destructor if anything is still in flight, which
    // turns "you forgot to close the connection" into a crash during exception
    // unwinding -- exactly when it is least debuggable.
    seastar::future<>                          _read_done = seastar::make_ready_future<>();
    bool                                       _closing = false;
    // Set when the read loop ends for ANY reason. Once it has, no pending or
    // future request can ever be completed, so every one of them must fail
    // immediately rather than wait forever.
    bool                                       _dead = false;
    std::chrono::milliseconds                  _request_timeout{30000};
    std::chrono::milliseconds                  _connect_timeout{10000};
};

// Defined here because they are templates; the machinery they call is not.
template <class Req>
seastar::future<> connection::send_oneway(const Req& req, int16_t version) {
    if (_dead)
        return seastar::make_exception_future<>(
            protocol_error(_peer + ": connection is no longer usable"));
    try {
        version = pick_version(Req::api_key, version, Req::min_version, Req::max_version);
    } catch (...) {
        return seastar::make_exception_future<>(std::current_exception());
    }
    // A correlation id is still consumed. The broker sends nothing back, but the
    // id must not be reused by a later request that DOES expect an answer.
    const int32_t corr = _next_correlation++;
    std::string frame = encode_request(req, version, corr,
                                       _client_id.empty() ? std::nullopt
                                                          : std::optional(_client_id));
    return write_frame(std::move(frame));
}

template <class Req, class Resp>
seastar::future<Resp> connection::send(const Req& req, int16_t version) {
    if (_dead)
        return seastar::make_exception_future<Resp>(
            protocol_error(_peer + ": connection is no longer usable"));
    try {
        version = pick_version(Req::api_key, version, Req::min_version, Req::max_version);
    } catch (...) {
        return seastar::make_exception_future<Resp>(std::current_exception());
    }
    const int32_t corr = _next_correlation++;
    std::string frame = encode_request(req, version, corr,
                                       _client_id.empty() ? std::nullopt
                                                          : std::optional(_client_id));
    return exchange(std::move(frame), corr).then([version](const std::string& body) {
        Resp resp;
        decode_response(body, version, resp);
        return resp;
    });
}

} // namespace sf::kafka
