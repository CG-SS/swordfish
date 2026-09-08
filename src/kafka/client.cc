#include "swordfish/kafka/client.hh"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/with_timeout.hh>

#include <seastar/core/coroutine.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/net/dns.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/util/backtrace.hh>
#include <seastar/util/log.hh>

#include <algorithm>

namespace sf::kafka {

static seastar::logger klog("sf.kafka");

const char* error_name(int16_t code) noexcept {
    switch (code) {
    case err::none:                      return "NONE";
    case err::offset_out_of_range:       return "OFFSET_OUT_OF_RANGE";
    case err::unknown_topic_or_part:     return "UNKNOWN_TOPIC_OR_PARTITION";
    case err::leader_not_available:      return "LEADER_NOT_AVAILABLE";
    case err::not_leader_or_follower:    return "NOT_LEADER_OR_FOLLOWER";
    case err::request_timed_out:         return "REQUEST_TIMED_OUT";
    case err::coordinator_load_in_progress: return "COORDINATOR_LOAD_IN_PROGRESS";
    case err::coordinator_not_available: return "COORDINATOR_NOT_AVAILABLE";
    case err::not_coordinator:           return "NOT_COORDINATOR";
    case err::illegal_generation:        return "ILLEGAL_GENERATION";
    case err::unknown_member_id:         return "UNKNOWN_MEMBER_ID";
    case err::rebalance_in_progress:     return "REBALANCE_IN_PROGRESS";
    case err::unsupported_version:       return "UNSUPPORTED_VERSION";
    case err::fenced_instance_id:        return "FENCED_INSTANCE_ID";
    default:                             return "ERROR";
    }
}

bool is_retriable(int16_t code) noexcept {
    switch (code) {
    case err::leader_not_available:
    case err::not_leader_or_follower:
    case err::request_timed_out:
    case err::coordinator_load_in_progress:
    case err::coordinator_not_available:
    case err::not_coordinator:
    case err::unknown_topic_or_part:      // transient right after topic creation
        return true;
    default:
        return false;
    }
}

std::string describe_error(int16_t code) {
    return std::string(error_name(code)) + " (" + std::to_string(code) + ")";
}

connection::~connection() {
    // Closing is the caller's job; this only keeps a forgotten close from
    // taking the process down with it.
    if (!_closing && _socket)
        klog.error("{}: connection destroyed without close() from {}", _peer,
                   seastar::current_backtrace());
}

seastar::future<> connection::connect(const std::string& hostport, std::string client_id) {
    const size_t colon = hostport.rfind(':');
    if (colon == std::string::npos)
        throw protocol_error("broker address needs a port: " + hostport);
    const std::string host = hostport.substr(0, colon);
    const int port = std::stoi(hostport.substr(colon + 1));
    if (port <= 0 || port > 65535)
        throw protocol_error("broker port out of range: " + hostport);
    return connect(host, static_cast<uint16_t>(port), std::move(client_id));
}

seastar::future<> connection::connect(std::string host, uint16_t port,
                                      std::string client_id) {
    _client_id = std::move(client_id);
    _peer = host + ":" + std::to_string(port);
    // Resolve first: socket_address does not take a name, and a broker is
    // almost always advertised as one. co_await cannot appear in a catch block,
    // so the numeric attempt sets a flag rather than resolving inside it.
    seastar::net::inet_address ip;
    bool numeric = false;
    try {
        ip = seastar::net::inet_address(host);
        numeric = true;
    } catch (const std::invalid_argument&) {
        // Intentionally empty: failing to parse the host as a literal address
        // just means it is a NAME, which the DNS lookup below handles. The
        // `numeric` flag, not the exception, carries the outcome.
    }
    if (!numeric) ip = co_await seastar::net::dns::resolve_name(host);
    const seastar::socket_address addr(ip, port);
    // Bounded for the same reason as the request above: a connect to a host
    // that drops packets rather than refusing them hangs until the kernel's own
    // retry budget runs out, which is minutes.
    _socket = co_await seastar::with_timeout(
        seastar::lowres_clock::now() + _connect_timeout, seastar::connect(addr));
    // Kafka is request/response over a long-lived connection; Nagle would add
    // up to 40ms to every small request for no benefit.
    _socket->set_nodelay(true);
    _in.emplace(_socket->input());
    _out.emplace(_socket->output());

    // The reader runs until the connection closes; close() awaits it.
    _read_done = read_loop().handle_exception([this](std::exception_ptr e) {
        if (!_closing) klog.debug("{}: read loop ended: {}", _peer, e);
    }).finally([this] {
        // The reader is what completes requests. Once it has stopped, nothing
        // ever will, so the connection has to advertise itself as unusable --
        // otherwise `connected()` keeps returning true, callers keep sending,
        // and each of those waits on a promise with nobody left to fulfil it.
        // That is a HANG, not an error: a chaos run left two consumers parked
        // in the reactor's idle loop for four hours after their broker was
        // killed.
        _dead = true;
        if (!_closing)
            fail_pending(std::make_exception_ptr(
                protocol_error(_peer + ": connection lost")));
    });

    // Version negotiation. v0 is sent first because a broker that does not
    // support our highest version answers UNSUPPORTED_VERSION -- and it answers
    // it in the v0 FORMAT, which is the only one every broker can produce.
    api_versions_request req;
    req.client_software_name = "swordfish";
    req.client_software_version = "0.1";
    // A broker that accepts TCP but fails this leaves us holding an open socket
    // and a running read loop. Without the cleanup the caller drops the
    // connection, its destructor runs with the read loop still in flight, and
    // Seastar aborts on the destroyed future -- a failed handshake taking the
    // process down with it.
    int16_t code = err::none;
    std::exception_ptr failure;
    try {
        auto resp = co_await send<api_versions_request, api_versions_response>(req, 0);
        code = resp.error_code;
        if (code == err::none)
            for (const auto& k : resp.api_keys)
                _versions[k.api_key] = {k.min_version, k.max_version};
    } catch (...) {
        failure = std::current_exception();
    }
    // Outside the handler: co_await is not allowed inside one.
    if (failure || code != err::none) {
        co_await close();
        if (failure) std::rethrow_exception(failure);
        throw broker_error(code, "ApiVersions failed: " + describe_error(code));
    }
    klog.debug("{}: negotiated {} apis", _peer, _versions.size());
}

int16_t connection::negotiated(int16_t api_key) const noexcept {
    const auto it = _versions.find(api_key);
    return it == _versions.end() ? int16_t(-1) : it->second.second;
}

// The broker's advertised maximum was used verbatim, and nothing consulted the
// versions the generated codecs actually implement -- so a broker advertising a
// version newer than this build knows had that version SENT to it, and the
// answer came back in a format the decoder could not read. Reproduced against a
// mock broker advertising Produce max_version 20 against a codec max of 13:
// swordfish sent v20 and died with `truncated response: wanted 1 byte(s), 0
// left`, naming neither the API nor the version. Every API this build uses
// happens to match Kafka 4.3.1 exactly, so nothing misbehaves today; the next
// version bump on the broker side is what turns it into that message.
int16_t connection::pick_version(int16_t api_key, int16_t requested,
                                 int16_t our_min, int16_t our_max) const {
    const auto it = _versions.find(api_key);
    if (it == _versions.end()) {
        // The handshake itself: ApiVersions is sent with an explicit v0 BEFORE
        // anything is known about the broker, precisely because v0 is the one
        // format every broker can answer in. There is nothing to negotiate
        // against yet, so an explicit version is honoured -- clamped to our own
        // range, which is all that can be checked here.
        if (requested >= 0)
            return std::min({requested, our_max});
        throw protocol_error("broker does not support api key " + std::to_string(api_key));
    }
    const auto [broker_min, broker_max] = it->second;

    // An explicit request is a CEILING, never a way past our own maximum: the
    // handshake asks for ApiVersions v0 on purpose, and clamping can only ever
    // lower it.
    int16_t v = requested < 0 ? broker_max : requested;
    v = std::min({v, broker_max, our_max});
    if (v < our_min || v < broker_min)
        throw protocol_error(
            "no common version for api key " + std::to_string(api_key) +
            ": broker supports v" + std::to_string(broker_min) + "-v" +
            std::to_string(broker_max) + ", this build implements v" +
            std::to_string(our_min) + "-v" + std::to_string(our_max));
    return v;
}

seastar::future<> connection::close() {
    if (_closing) co_return;
    _closing = true;
    if (_in) co_await _in->close();
    co_await std::move(_read_done);
    if (_out) {
        // The output stream is closed after the reader has stopped, or an
        // in-flight write would be cancelled underneath itself.
        auto out = std::move(*_out);
        _out.reset();
        co_await out.close();
    }
    _in.reset();
    _socket.reset();
    fail_pending(std::make_exception_ptr(protocol_error("connection closed")));
}

void connection::fail_pending(std::exception_ptr e) {
    auto pending = std::move(_pending);
    _pending.clear();
    for (auto& [id, p] : pending) p.set_exception(e);
}

seastar::future<> connection::write_frame(std::string frame) {
    if (!_out) throw protocol_error("not connected");
    // Flushed, not just written: a buffered request the broker never receives is
    // indistinguishable from one it ignored.
    co_await _out->write(std::move(frame));
    co_await _out->flush();
}

seastar::future<std::string> connection::exchange(std::string frame, int32_t correlation_id) {
    if (!_out) throw protocol_error("not connected");
    auto [it, inserted] = _pending.try_emplace(correlation_id);
    if (!inserted) throw protocol_error("duplicate correlation id");
    auto fut = it->second.get_future();

    // A coroutine rather than a continuation chain, and deliberately: the old
    // version ended with .handle_exception([this, ...]) to clean up the pending
    // entry, and that continuation could run AFTER the connection was
    // destroyed. close() resolves the waiting promises and returns, the owner
    // then drops the connection, and the scheduled handler would touch a freed
    // _pending. Here the cleanup happens while this coroutine is still running,
    // and the final await touches nothing afterwards.
    try {
        // Write and flush before waiting: the broker will not answer a request
        // it has not received, and a buffered request would deadlock us.
        co_await _out->write(std::move(frame));
        co_await _out->flush();
    } catch (...) {
        _pending.erase(correlation_id);
        throw;
    }

    // Bounded, always. The dead-connection check above catches the case where
    // the reader has already stopped, but a broker that accepts the request and
    // then never answers -- paused, partitioned, or wedged -- leaves no signal
    // at all, and an unbounded wait there is indistinguishable from a hang.
    // Kafka's own request.timeout.ms default is the same 30s.
    std::exception_ptr failure;
    std::string body;
    try {
        body = co_await seastar::with_timeout(
            seastar::lowres_clock::now() + _request_timeout, std::move(fut));
    } catch (...) {
        failure = std::current_exception();
    }
    if (!failure) co_return body;

    // Outside the handler: co_await is not allowed inside one, and this path
    // may yet grow one. A request that timed out leaves the connection in an
    // unknown state -- its response may still arrive and be matched against a
    // correlation id whose waiter is gone -- so the connection is retired
    // rather than reused, which is what every other Kafka client does too.
    _pending.erase(correlation_id);
    _dead = true;
    fail_pending(std::make_exception_ptr(
        protocol_error(_peer + ": request timed out after " +
                       std::to_string(_request_timeout.count()) + "ms")));
    std::rethrow_exception(failure);
}

seastar::future<> connection::read_loop() {
    while (!_closing) {
        // Every response is an int32 length followed by that many bytes.
        auto len_buf = co_await _in->read_exactly(4);
        if (len_buf.size() < 4) break;                 // peer closed
        reader lr(std::string_view(len_buf.get(), 4));
        const int32_t len = lr.i32();
        // cppcheck-suppress knownConditionTrueFalse
        //   Off the wire: a hostile or broken peer may send anything here.
        if (len < 0 || len > 100 * 1024 * 1024)
            throw protocol_error("response length out of range: " + std::to_string(len));
        auto body = co_await _in->read_exactly(static_cast<size_t>(len));
        if (body.size() < static_cast<size_t>(len)) break;

        // The correlation id is the first field of every response header, and
        // reading it here is what lets the reply reach the right waiter without
        // the read loop knowing which API it belongs to.
        std::string frame(body.get(), body.size());
        reader hr(frame);
        const int32_t corr = hr.i32();
        const auto it = _pending.find(corr);
        if (it == _pending.end()) {
            klog.warn("{}: response for unknown correlation id {}", _peer, corr);
            continue;
        }
        auto p = std::move(it->second);
        _pending.erase(it);
        p.set_value(std::move(frame));
    }
    if (!_closing)
        fail_pending(std::make_exception_ptr(protocol_error("connection closed by peer")));
}

} // namespace sf::kafka
