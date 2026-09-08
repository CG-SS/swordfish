// `socket` in and out and `socket_server` in, on Seastar's own TCP and
// unix-domain sockets.
//
// Behaviour derived from the public documentation in
// connect-main/docs/modules/components/pages/{inputs,outputs}/socket*.adoc and
// from the Apache-2.0 sources in benthos-main/internal/impl/io.
#include "swordfish/components/socket.hh"
#include "swordfish/components/registry.hh"
#include "swordfish/runtime/components.hh"
#include "swordfish/runtime/rendezvous.hh"
#include "swordfish/runtime/scanner.hh"

#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/net/api.hh>
#include <seastar/net/dns.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/util/log.hh>

#include <utility>

namespace sf::sock {

static seastar::logger slog("sf.socket");

namespace {

// A tcp address is host:port and needs resolving; a unix address is a path and
// needs none. Kept in one function so "which kind of address is this" is decided
// once rather than at each of the three call sites.
seastar::future<seastar::socket_address> address_for(const std::string& network,
                                                     const std::string& address) {
    if (network == "unix") co_return seastar::socket_address(seastar::unix_domain_addr(address));
    const size_t colon = address.rfind(':');
    if (colon == std::string::npos || colon + 1 == address.size())
        throw std::runtime_error("socket `address` must be host:port for network '" +
                                 network + "', got '" + address + "'");
    std::string host = address.substr(0, colon);
    if (host.size() >= 2 && host.front() == '[' && host.back() == ']')
        host = host.substr(1, host.size() - 2);
    if (host.empty()) host = "0.0.0.0";
    const std::string port_s = address.substr(colon + 1);
    if (port_s.find_first_not_of("0123456789") != std::string::npos)
        throw std::runtime_error("socket `address` has a non-numeric port: '" + port_s + "'");
    const unsigned long port = std::stoul(port_s);
    if (port == 0 || port > 65535)
        throw std::runtime_error("socket `address` has a port outside 1-65535: '" + port_s + "'");
    try {
        // `seastar::net::inet_address` does not resolve names, so a literal is
        // tried first and a hostname goes to the resolver.
        co_return seastar::socket_address(seastar::net::inet_address(host),
                                          static_cast<uint16_t>(port));
    } catch (const std::invalid_argument&) {
    }
    auto addr = co_await seastar::net::dns::resolve_name(host);
    co_return seastar::socket_address(addr, static_cast<uint16_t>(port));
}

// Reads a connection to exhaustion, framing it with a scanner and handing out one
// message at a time. Shared by both inputs: they differ only in where the
// connection comes from.
class framed_connection {
public:
    framed_connection(seastar::connected_socket s, scanner_ptr sc)
        : _socket(std::move(s)), _in(_socket.input()), _scanner(std::move(sc)) {}

    // std::nullopt once the peer has closed and the scanner has been flushed.
    seastar::future<std::optional<message>> next() {
        while (_ready.empty()) {
            if (_eof) co_return std::nullopt;
            auto buf = co_await _in.read();
            if (buf.empty()) {
                _eof = true;
                // The scanner's tail: a last line with no newline after it is
                // still a message, and dropping it would silently lose data at
                // exactly the end of every stream.
                for (auto& m : _scanner->finish()) _ready.push_back(std::move(m));
                break;
            }
            for (auto& m : _scanner->feed(std::string(buf.get(), buf.size())))
                _ready.push_back(std::move(m));
        }
        if (_ready.empty()) co_return std::nullopt;
        message m = std::move(_ready.front());
        _ready.erase(_ready.begin());
        co_return m;
    }

    seastar::future<> close() {
        try { co_await _in.close(); }
        catch (const std::exception& e) { slog.debug("socket close failed: {}", e.what()); }
    }

private:
    seastar::connected_socket        _socket;
    seastar::input_stream<char>      _in;
    scanner_ptr                      _scanner;
    std::vector<message>             _ready;
    bool                             _eof = false;
};

// ---- socket input -----------------------------------------------------------

class socket_input final : public input {
public:
    socket_input(input_config cfg, scanner_spec sc)
        : _cfg(std::move(cfg)), _scanner(std::move(sc)) {}

    seastar::future<> connect(seastar::abort_source&) override {
        const auto addr = co_await address_for(_cfg.network, _cfg.address);
        auto s = co_await seastar::connect(addr);
        _conn = std::make_unique<framed_connection>(std::move(s), make_scanner(_scanner));
        slog.info("socket connected to {} over {}", _cfg.address, _cfg.network);
    }

    seastar::future<std::optional<std::pair<batch, ack_fn>>>
    read_batch(seastar::abort_source& as) override {
        if (!_conn || as.abort_requested()) co_return std::nullopt;
        auto m = co_await _conn->next();
        if (!m) co_return std::nullopt;          // the peer closed: end of input
        batch b;
        b.push_back(std::move(*m));
        co_return std::make_pair(std::move(b), noop_ack());
    }

    seastar::future<> close() override {
        if (!_conn) co_return;
        auto c = std::move(_conn);
        co_await c->close();
    }

    connection_status status() const override {
        connection_status s;
        s.connected = _conn != nullptr;
        s.label = "socket";
        return s;
    }

private:
    input_config                       _cfg;
    scanner_spec                       _scanner;
    std::unique_ptr<framed_connection> _conn;
};

// ---- socket_server input ----------------------------------------------------

class socket_server_input final : public input {
    // Depth one, so an idle pipeline applies back pressure straight through to
    // the connections rather than buffering an unbounded backlog per client.
    //
    // A rendezvous rather than a seastar::queue: every connection is served by
    // its own fiber, so every connection is another PRODUCER, and
    // seastar::queue supports exactly one blocked producer -- a second would
    // overwrite the first's promise and break it. See runtime/rendezvous.hh.
    static constexpr size_t queue_depth = 1;

public:
    socket_server_input(server_input_config cfg, scanner_spec sc)
        : _cfg(std::move(cfg)), _scanner(std::move(sc)), _queue(queue_depth) {}

    seastar::future<> connect(seastar::abort_source& as) override {
        _abort_sub = as.subscribe([this]() noexcept { stop_accepting(); });
        const auto addr = co_await address_for(_cfg.network, _cfg.address);
        seastar::listen_options lo;
        lo.reuse_address = true;
        // A unix socket path left over from a previous run would make bind()
        // fail with EADDRINUSE even though nothing holds it. The reference
        // unlinks it too.
        if (_cfg.network == "unix") ::unlink(_cfg.address.c_str());
        _listener = seastar::listen(addr, lo);
        slog.info("socket_server listening on {} over {}", _cfg.address, _cfg.network);
        (void)seastar::with_gate(_conns, [this] { return accept_loop(); });
    }

    seastar::future<std::optional<std::pair<batch, ack_fn>>>
    read_batch(seastar::abort_source& as) override {
        if (as.abort_requested())
            return seastar::make_ready_future<std::optional<std::pair<batch, ack_fn>>>(
                std::nullopt);
        return _queue.pop()
            .then([](std::pair<batch, ack_fn> v) {
                return std::optional<std::pair<batch, ack_fn>>(std::move(v));
            })
            .handle_exception([](std::exception_ptr) {
                // A listening server has no natural end of input; the
                // rendezvous is closed only by shutdown.
                return std::optional<std::pair<batch, ack_fn>>(std::nullopt);
            });
    }

    seastar::future<> close() override {
        stop_accepting();
        co_await _conns.close();
        if (_cfg.network == "unix") ::unlink(_cfg.address.c_str());
    }

    connection_status status() const override {
        connection_status s;
        s.connected = _listening;
        s.label = "socket_server";
        return s;
    }

private:
    void stop_accepting() noexcept {
        if (!_listening) return;
        _listening = false;
        try { _listener.abort_accept(); } catch (...) {}
        (void)_queue.close(std::make_exception_ptr(seastar::abort_requested_exception()));
    }

    seastar::future<> accept_loop() {
        while (_listening) {
            seastar::accept_result ar;
            try {
                ar = co_await _listener.accept();
            } catch (...) {
                break;                            // abort_accept, or the socket died
            }
            // Each connection is read by its own fiber, so a slow client cannot
            // stop the others being served.
            (void)seastar::with_gate(_conns, [this, ar = std::move(ar)]() mutable {
                return serve(std::move(ar.connection));
            });
        }
    }

    seastar::future<> serve(seastar::connected_socket s) {
        framed_connection conn(std::move(s), make_scanner(_scanner));
        for (;;) {
            std::optional<message> m;
            try {
                m = co_await conn.next();
            } catch (const std::exception& e) {
                slog.warn("socket_server connection failed: {}", e.what());
                break;
            }
            if (!m) break;
            batch b;
            b.push_back(std::move(*m));
            try {
                co_await _queue.push(std::make_pair(std::move(b), noop_ack()));
            } catch (...) {
                break;                            // shutting down
            }
        }
        co_await conn.close();
    }

    server_input_config                      _cfg;
    scanner_spec                             _scanner;
    rendezvous<std::pair<batch, ack_fn>>     _queue;
    seastar::server_socket                   _listener;
    seastar::gate                            _conns;
    bool                                     _listening = true;
    seastar::optimized_optional<seastar::abort_source::subscription> _abort_sub;
};

// ---- socket output ----------------------------------------------------------

class socket_output final : public output {
public:
    explicit socket_output(output_config cfg) : _cfg(std::move(cfg)) {}

    seastar::future<> connect(seastar::abort_source&) override {
        const auto addr = co_await address_for(_cfg.network, _cfg.address);
        auto s = co_await seastar::connect(addr);
        _socket.emplace(std::move(s));
        _out.emplace(_socket->output());
    }

    seastar::future<> write_batch(batch b, seastar::abort_source&) override {
        if (!_out) co_return;
        // Newline-delimited, which is the `lines` framing the reference's socket
        // output writes and what its socket input expects back.
        for (const auto& m : b) {
            std::string line;
            append_line(line, m.as_bytes());
            co_await _out->write(line);
        }
        // A network output_stream batches its flushes, so without this the
        // bytes sit in the buffer until the poller gets to them.
        co_await _out->flush();
    }

    seastar::future<> close() override {
        if (!_out || _closed) co_return;
        _closed = true;
        // Closed IN PLACE, as the file output is. Moving it out first -- which
        // this used to do -- is safe only for a stream that is not
        // batch-flushed: seastar's output_stream move constructor does not
        // move its `_in_poller` intrusive hook, so a network stream registered
        // with the batch-flush poller stays linked on the SOURCE object and
        // trips a boost assertion the moment that source is destroyed. It cost
        // a core dump on the first socket round trip.
        try {
            co_await _out->close();       // close() flushes first
        } catch (const std::exception& e) {
            slog.debug("socket output close failed: {}", e.what());
        }
        _out.reset();
        _socket.reset();
    }

    connection_status status() const override {
        connection_status s;
        s.connected = _out.has_value();
        s.label = "socket";
        return s;
    }

private:
    output_config                               _cfg;
    std::optional<seastar::connected_socket>    _socket;
    std::optional<seastar::output_stream<char>> _out;
    bool                                        _closed = false;
};

} // namespace

input_ptr make_socket_input(const input_config& c, const scanner_spec& sc) {
    input_ptr in = std::make_unique<socket_input>(c, sc);
    // `auto_replay_nacks` is applied HERE rather than by each caller, so the
    // interpreted and compiled paths cannot disagree about whether a nack
    // replays or drops.
    // `auto_replay_nacks` is applied by build_input_kind, for every input
    // alike; wrapping here as well would replay a nack twice.
    return in;
}

input_ptr make_socket_server_input(const server_input_config& c, const scanner_spec& sc) {
    input_ptr in = std::make_unique<socket_server_input>(c, sc);
    // `auto_replay_nacks` is applied by build_input_kind, for every input
    // alike; wrapping here as well would replay a nack twice.
    return in;
}

output_ptr make_socket_output(const output_config& c) {
    return std::make_unique<socket_output>(c);
}

void register_components() {
    auto socket_in = make_input_def<input_config>(
        "socket",
        [](const input_config& c, const component_config& cc) {
            auto sc = scanner_for(cc, "scanner");
            return [c, sc] { return make_socket_input(c, sc); };
        },
        [](const input_config& c, const component_config& cc) {
            return "sf::sock::make_socket_input(" +
                   cfg::emit_cpp(c, "sf::sock::input_config") + ", " +
                   emit_scanner_spec(scanner_for(cc, "scanner")) + ")";
        },
        "swordfish/components/socket.hh");
    socket_in.scanner_paths = {"scanner"};
    register_input(std::move(socket_in));

    auto socket_srv = make_input_def<server_input_config>(
        "socket_server",
        [](const server_input_config& c, const component_config& cc) {
            auto sc = scanner_for(cc, "scanner");
            return [c, sc] { return make_socket_server_input(c, sc); };
        },
        [](const server_input_config& c, const component_config& cc) {
            return "sf::sock::make_socket_server_input(" +
                   cfg::emit_cpp(c, "sf::sock::server_input_config") + ", " +
                   emit_scanner_spec(scanner_for(cc, "scanner")) + ")";
        },
        "swordfish/components/socket.hh");
    socket_srv.scanner_paths = {"scanner"};
    register_input(std::move(socket_srv));

    register_output(make_output_def<output_config>(
        "socket",
        [](const output_config& c, const component_config&) { return [c] { return make_socket_output(c); }; },
        [](const output_config& c, const component_config&) {
            return "sf::sock::make_socket_output(" +
                   cfg::emit_cpp(c, "sf::sock::output_config") + ")";
        },
        "swordfish/components/socket.hh"));
}

} // namespace sf::sock
