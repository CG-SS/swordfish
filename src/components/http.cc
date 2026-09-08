// `http_server` input, `http_client` output and `sync_response`, on Seastar's
// own HTTP stack (seastar/http/{httpd,client}.hh).
//
// Behaviour derived from the public documentation in
// connect-main/docs/modules/components/pages/{inputs,outputs}/http_*.adoc and
// from the Apache-2.0 sources in benthos-main/internal/impl/io.
#include "swordfish/components/http.hh"
#include "swordfish/components/registry.hh"
#include "swordfish/runtime/components.hh"
#include "swordfish/runtime/rendezvous.hh"
#include "swordfish/runtime/sync_response.hh"
#include "swordfish/runtime/scanner.hh"
#include "swordfish/runtime/transform.hh"
#include "swordfish/blobl/parse.hh"

#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/queue.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/with_timeout.hh>
#include <seastar/core/when_all.hh>
#include <seastar/core/loop.hh>
// Seastar's connection_factory.hh brace-initialises a tls_options without
// naming every member, which -Wextra flags. It is a third-party header included
// by a first-party file, so the suppression has to be here: the project's rule
// is zero first-party warnings, and a warning nobody can fix trains people to
// ignore the list.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmissing-field-initializers"
#include <seastar/http/client.hh>
#pragma GCC diagnostic pop
#include <seastar/http/handlers.hh>
#include <seastar/http/routes.hh>
#include <seastar/http/httpd.hh>
#include <seastar/websocket/client.hh>
#include <seastar/websocket/common.hh>
#include <seastar/net/dns.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/util/log.hh>

#include <fmt/format.h>
#include <seastar/util/short_streams.hh>

#include <algorithm>
#include <cctype>
#include <random>

namespace sf::http {

static seastar::logger hlog("sf.http");

namespace {

// ---- shared helpers ---------------------------------------------------------

// host:port, where the port is required. Shared by the server input and by URL
// parsing, so "the address had no port" reads the same either way.
struct endpoint {
    std::string host;
    uint16_t    port = 0;
};

endpoint split_host_port(const std::string& addr, const std::string& what) {
    // Rightmost colon, so an IPv6 literal in brackets survives.
    const size_t colon = addr.rfind(':');
    if (colon == std::string::npos || colon + 1 == addr.size())
        throw std::runtime_error(what + " must be host:port, got '" + addr + "'");
    endpoint e;
    e.host = addr.substr(0, colon);
    if (e.host.size() >= 2 && e.host.front() == '[' && e.host.back() == ']')
        e.host = e.host.substr(1, e.host.size() - 2);
    const std::string p = addr.substr(colon + 1);
    if (p.find_first_not_of("0123456789") != std::string::npos)
        throw std::runtime_error(what + " has a non-numeric port: '" + p + "'");
    const unsigned long v = std::stoul(p);
    if (v == 0 || v > 65535)
        throw std::runtime_error(what + " has a port outside 1-65535: '" + p + "'");
    e.port = static_cast<uint16_t>(v);
    // An empty host means "all interfaces", which is what `:4195` asks for.
    if (e.host.empty()) e.host = "0.0.0.0";
    return e;
}

// `seastar::net::inet_address` does NOT resolve names -- constructing one from
// "localhost" throws. A hostname therefore has to go through the resolver, and
// forgetting that once cost this project a whole detour into "there is no
// broker reachable".
seastar::future<seastar::socket_address> resolve(endpoint e) {
    try {
        co_return seastar::socket_address(seastar::net::inet_address(e.host), e.port);
    } catch (const std::invalid_argument&) {
        // Not a literal address: ask the resolver.
    }
    auto addr = co_await seastar::net::dns::resolve_name(e.host);
    co_return seastar::socket_address(addr, e.port);
}

struct parsed_url {
    endpoint    ep;
    std::string path;      // includes the query string
    std::string host_hdr;  // the Host: header, port included when non-default
};

parsed_url parse_url(const std::string& url) {
    static constexpr std::string_view http_scheme = "http://";
    if (url.rfind("https://", 0) == 0)
        throw std::runtime_error(
            "http_client: https is not implemented by swordfish yet; the TLS "
            "configuration a secure client needs is not built, and sending in "
            "the clear instead would be worse than refusing");
    if (url.rfind(http_scheme, 0) != 0)
        throw std::runtime_error("http_client `url` must begin with http://, got '" + url + "'");

    const std::string rest = url.substr(http_scheme.size());
    const size_t slash = rest.find('/');
    const std::string authority = rest.substr(0, slash);
    parsed_url out;
    out.path = slash == std::string::npos ? "/" : rest.substr(slash);
    out.host_hdr = authority;
    // Does the authority carry a port? For an IPv6 literal the search has to
    // start after the closing bracket, or the colons INSIDE `[::1]` are mistaken
    // for one and the host is parsed as `[:` with a port of `1]`.
    const size_t after_host = authority.rfind(']');
    const size_t port_colon = authority.find(':', after_host == std::string::npos
                                                      ? 0 : after_host);
    // A URL may omit the port; host:port cannot, so the default is filled in
    // before splitting rather than being a second code path.
    out.ep = split_host_port(port_colon == std::string::npos ? authority + ":80" : authority,
                             "http_client `url` authority");
    return out;
}

// An interpolated header map, compiled once and evaluated per message.
//
// `headers` on http_client (both directions) and on sync_response are
// interpolated in the reference -- "this field supports interpolation
// functions" -- and were sent LITERALLY here, so a per-message correlation id or
// a computed Content-Type went out as the raw `${! ... }` text and the receiver
// misrouted or rejected it, with no error on either side.
//
// Interpretation happens here rather than through the registry's transform_for,
// because these components construct their own transforms in both backends: an
// emitted binary builds the same component from the same config, so it gets the
// same behaviour without the emitter having to carry a function per header.
using header_fns = std::vector<std::pair<std::string, transform_fn>>;

header_fns compile_headers(const std::map<std::string, cfg::interpolation>& hs) {
    header_fns out;
    out.reserve(hs.size());
    for (const auto& [k, v] : hs)
        out.emplace_back(k, interpreted_transform(
                                blobl::parse_query(cfg::interpolation_to_query(v.source))));
    return out;
}

// The exec_ctx an interpolation sees. `m` is null where there is no message --
// an http_client INPUT polls, so it has none -- and an empty message is bound
// rather than nothing at all, so `content()` yields "" instead of throwing and a
// literal header still works.
struct interp_scope {
    exec_ctx ctx;
    value    parsed;
    message  empty;

    explicit interp_scope(message* m) {
        message& src = m ? *m : empty;
        bool structured = true;
        try { parsed = src.as_structured(); } catch (...) { structured = false; }
        ctx.this_v  = structured ? &parsed : nullptr;
        ctx.msg     = &src;
        ctx.meta_in = &src.meta();
    }
};

std::string eval_header(const transform_fn& f, message* m) {
    interp_scope s(m);
    return f(s.ctx).to_display_string();
}

// A verb name as seastar's router spells it. A verb the router cannot express is
// refused BY NAME at construction rather than silently registering nothing and
// 404-ing every request that uses it.
seastar::httpd::operation_type verb_operation(const std::string& verb) {
    static const std::map<std::string, seastar::httpd::operation_type> ops{
        {"GET",     seastar::httpd::operation_type::GET},
        {"POST",    seastar::httpd::operation_type::POST},
        {"PUT",     seastar::httpd::operation_type::PUT},
        {"DELETE",  seastar::httpd::operation_type::DELETE},
        {"HEAD",    seastar::httpd::operation_type::HEAD},
        {"OPTIONS", seastar::httpd::operation_type::OPTIONS},
        {"TRACE",   seastar::httpd::operation_type::TRACE},
        {"CONNECT", seastar::httpd::operation_type::CONNECT},
        {"PATCH",   seastar::httpd::operation_type::PATCH}};
    const auto it = ops.find(verb);
    if (it == ops.end())
        throw std::runtime_error("http_server `allowed_verbs`: '" + verb +
                                 "' is not an HTTP method swordfish can serve");
    return it->second;
}

// Does `url` match the configured `path`, and what did its `{param}` segments
// capture?
//
// Three forms, all of which the reference documents and only the first of which
// swordfish had: an exact match (with an optional query string), a path ending
// in `/` which "will match against all extensions of that path", and
// `{param}` segments which "are added to ingested messages as metadata". The
// missing two were not refused either -- a documented path form simply 404'd
// every request it was written to serve, with both linters passing.
bool path_matches(std::string_view url, const std::string& path,
                  std::map<std::string, std::string>& captures) {
    // The query string is not part of the path, and is not matched against.
    if (const auto q = url.find('?'); q != std::string_view::npos) url = url.substr(0, q);

    // A trailing '/' matches that prefix and everything under it.
    if (!path.empty() && path.back() == '/' && path.find('{') == std::string::npos)
        return url.rfind(path, 0) == 0;

    if (path.find('{') == std::string::npos) return url == path;

    // Segment by segment, so `{id}` captures exactly one segment -- which is
    // what makes `/p/{id}` match `/p/42` and not `/p/42/extra`.
    const auto split = [](std::string_view v) {
        std::vector<std::string_view> out;
        size_t i = 0;
        while (i <= v.size()) {
            const size_t j = v.find('/', i);
            out.push_back(v.substr(i, j == std::string_view::npos ? j : j - i));
            if (j == std::string_view::npos) break;
            i = j + 1;
        }
        return out;
    };
    const auto want = split(path);
    const auto got  = split(url);
    if (want.size() != got.size()) return false;
    std::map<std::string, std::string> found;
    for (size_t i = 0; i < want.size(); ++i) {
        if (want[i].size() >= 2 && want[i].front() == '{' && want[i].back() == '}') {
            if (got[i].empty()) return false;          // a parameter must match something
            found.emplace(std::string(want[i].substr(1, want[i].size() - 2)),
                          std::string(got[i]));
            continue;
        }
        if (want[i] != got[i]) return false;
    }
    captures = std::move(found);
    return true;
}

std::string describe(std::exception_ptr e) {
    if (!e) return "unknown error";
    try { std::rethrow_exception(e); }
    catch (const std::exception& ex) { return ex.what(); }
    catch (...) { return "unknown error"; }
}

// A 60-hex-character boundary, the same shape mime/multipart generates. Random
// per response because a boundary that appears in a payload would break the
// framing, and a fixed one makes that a certainty rather than a chance.
std::string random_boundary() {
    static std::mt19937_64 rng(std::random_device{}());
    static constexpr char hex[] = "0123456789abcdef";
    std::string out;
    out.reserve(60);
    for (int i = 0; i < 60; ++i) out += hex[rng() & 0xf];
    return out;
}

// Which Content-Type a multipart section gets. Go's http.DetectContentType
// implements the whole WHATWG sniffing table; this distinguishes only text from
// binary, which is the distinction those two answers actually encode for
// pipeline payloads. Said plainly rather than left to be discovered: a caller
// relying on swordfish to sniff PNG from JPEG will not get it.
std::string sniff_content_type(std::string_view body) {
    for (unsigned char c : body)
        if (c < 0x09 || (c > 0x0d && c < 0x20) || c == 0x7f)
            return "application/octet-stream";
    return "text/plain; charset=utf-8";
}

std::string upper(std::string s) {
    for (char& c : s) c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    return s;
}

// Go's canonical MIME header form: `content-type` -> `Content-Type`. Worth doing
// rather than passing the wire casing through, because these names become
// metadata keys and a Bloblang mapping reads them by name -- a client that sent
// `content-type` and one that sent `Content-Type` must reach the same key or the
// mapping works for one caller and silently not the other.
std::string canonical_header(std::string_view k) {
    std::string out;
    out.reserve(k.size());
    bool start = true;
    for (char c : k) {
        const auto u = static_cast<unsigned char>(c);
        out += start ? static_cast<char>(std::toupper(u))
                     : static_cast<char>(std::tolower(u));
        start = (c == '-');
    }
    return out;
}

// `type/subtype; name=value` -- enough of RFC 2045 to find a multipart boundary.
// Values may be quoted; anything else about the parameter grammar this does not
// need to know.
std::pair<std::string, std::map<std::string, std::string>>
parse_media_type(std::string_view v) {
    std::map<std::string, std::string> params;
    auto semi = v.find(';');
    std::string media(v.substr(0, semi));
    while (!media.empty() && std::isspace(static_cast<unsigned char>(media.back())))
        media.pop_back();
    for (char& c : media) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    while (semi != std::string_view::npos) {
        auto rest = v.substr(semi + 1);
        auto next = rest.find(';');
        auto attr = rest.substr(0, next);
        auto eq = attr.find('=');
        if (eq != std::string_view::npos) {
            auto name = attr.substr(0, eq);
            auto val = attr.substr(eq + 1);
            auto trim = [](std::string_view t) {
                while (!t.empty() && std::isspace(static_cast<unsigned char>(t.front())))
                    t.remove_prefix(1);
                while (!t.empty() && std::isspace(static_cast<unsigned char>(t.back())))
                    t.remove_suffix(1);
                return t;
            };
            name = trim(name);
            val = trim(val);
            if (val.size() >= 2 && val.front() == '"' && val.back() == '"')
                val = val.substr(1, val.size() - 2);
            std::string lname(name);
            for (char& c : lname)
                c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
            params.emplace(std::move(lname), std::string(val));
        }
        semi = next == std::string_view::npos ? std::string_view::npos : semi + 1 + next;
    }
    return {std::move(media), std::move(params)};
}

// Splits an RFC 1341 multipart body into its parts' bodies. Each part's own
// headers are dropped, which is what the reference does -- mime/multipart reads
// the part's headers and then `io.ReadAll(p)`, and only the body becomes a
// message.
//
// A missing boundary PARAMETER throws -- the reference answers that 400. A body
// that simply never contains the delimiter does not: mime/multipart's NextPart
// returns io.EOF on the first call and the batch comes out empty, which the
// reference answers 200 with no messages emitted. The distinction was worth
// measuring rather than assuming, because the intuitive reading -- "this is not
// multipart, refuse it" -- is the one the reference does not take.
std::vector<std::string> split_multipart(std::string_view body, std::string_view boundary) {
    if (boundary.empty())
        throw std::runtime_error("multipart content-type with no boundary parameter");
    const std::string dash = "--" + std::string(boundary);
    std::vector<std::string> parts;

    // The preamble before the first delimiter is discarded. The first delimiter
    // may open the body directly (no leading CRLF), so it is searched for on its
    // own and every later one with the CRLF that terminates the part before it.
    std::size_t pos = 0;
    if (body.compare(0, dash.size(), dash) == 0) {
        pos = dash.size();
    } else {
        const auto first = body.find("\r\n" + dash);
        if (first == std::string_view::npos) return parts;  // no parts, not an error
        pos = first + 2 + dash.size();
    }

    for (;;) {
        // A delimiter followed by `--` closes the body; anything after it is the
        // epilogue and is discarded.
        if (body.compare(pos, 2, "--") == 0) break;
        // Transport padding, then CRLF. Running out of input here means no
        // complete delimiter line was ever read, which is an END, not an error:
        // the reference answers `--zzz` with no trailing CRLF 200 and zero
        // messages.
        auto eol = body.find("\r\n", pos);
        if (eol == std::string_view::npos) return parts;
        pos = eol + 2;

        // The part's headers, up to the blank line. A part with no headers opens
        // with the blank line itself. Running out of input immediately is EOF
        // and ends the body cleanly; content that is not a terminated header
        // block is malformed and is refused. The reference draws the line in
        // exactly that place -- `--zzz\r\n` is 200, `--zzz\r\n\r\n` is 400 --
        // and it was measured rather than reasoned about.
        std::size_t body_start;
        if (pos >= body.size()) {
            return parts;
        } else if (body.compare(pos, 2, "\r\n") == 0) {
            body_start = pos + 2;
        } else if (const auto blank = body.find("\r\n\r\n", pos);
                   blank != std::string_view::npos) {
            body_start = blank + 4;
        } else {
            throw std::runtime_error("multipart part has no header terminator");
        }

        // The search starts TWO BYTES BEFORE the body, on the CRLF that ended
        // the header block, because that CRLF is also the one a delimiter on the
        // very next line needs: a part whose body is empty is written
        // `...headers CRLF CRLF --boundary`, and searching from body_start skips
        // straight past it and swallows the delimiter into the body. That is not
        // a corner case -- it is how an empty part is encoded, and getting it
        // wrong made a body the reference rejects parse into a part containing
        // the boundary text.
        const auto next = body.find("\r\n" + dash, body_start - 2);
        if (next == std::string_view::npos)
            throw std::runtime_error("multipart part is not terminated by a delimiter");
        parts.emplace_back(next <= body_start ? std::string()
                                              : std::string(body.substr(body_start,
                                                                        next - body_start)));
        pos = next + 2 + dash.size();
    }
    return parts;
}

// `a=1; b=2` from a Cookie header. Duplicates keep the FIRST value, as Go's
// r.Cookies() ordering makes the first one win when it is written to a map.
std::map<std::string, std::string> parse_cookies(std::string_view v) {
    std::map<std::string, std::string> out;
    std::size_t pos = 0;
    while (pos <= v.size()) {
        auto semi = v.find(';', pos);
        auto item = v.substr(pos, semi == std::string_view::npos ? std::string_view::npos
                                                                 : semi - pos);
        while (!item.empty() && std::isspace(static_cast<unsigned char>(item.front())))
            item.remove_prefix(1);
        while (!item.empty() && std::isspace(static_cast<unsigned char>(item.back())))
            item.remove_suffix(1);
        auto eq = item.find('=');
        if (eq != std::string_view::npos && eq > 0) {
            auto val = item.substr(eq + 1);
            if (val.size() >= 2 && val.front() == '"' && val.back() == '"')
                val = val.substr(1, val.size() - 2);
            out.emplace(std::string(item.substr(0, eq)), std::string(val));
        }
        if (semi == std::string_view::npos) break;
        pos = semi + 1;
    }
    return out;
}

// ---- sync_response ----------------------------------------------------------

// Writes the batch into the store the input attached, for the input to send
// back. Against an input that attached none this is a no-op, which is what the
// reference documents: "For most inputs this mechanism is ignored entirely, in
// which case the sync response is dropped without penalty."
class sync_response_output final : public output {
public:
    seastar::future<> write_batch(batch b, seastar::abort_source&) override {
        if (b.empty()) return seastar::make_ready_future<>();
        // The store lives on the FIRST message, matching
        // internal/transaction/result_store.go, and the whole batch is added to
        // it -- so a pipeline that split one request into four answers with all
        // four.
        const auto store = b[0].response();
        if (!store) {
            if (!_warned) {
                _warned = true;
                hlog.info("sync_response: this input does not support synchronous "
                          "responses, so the messages routed here are discarded");
            }
            return seastar::make_ready_future<>();
        }
        store->add(std::move(b));
        return seastar::make_ready_future<>();
    }

    connection_status status() const override {
        connection_status s;
        s.connected = true;
        s.label = "sync_response";
        return s;
    }

private:
    bool _warned = false;
};

// ---- http_server input ------------------------------------------------------

// One request in flight: the batch it produced, the store its response goes in,
// and the promise the handler waits on until the pipeline resolves it.
struct in_flight {
    std::shared_ptr<response_store>      store;
    seastar::promise<std::exception_ptr> done;
    bool                                 resolved = false;

    // A promise may be set exactly once, and setting one twice aborts the
    // process. The ack should fire once -- ack_group guarantees it -- but a
    // handler that has already timed out and a shutdown that nacks the same
    // batch are two paths to the same promise, and the cost of being sure is
    // one bool.
    void resolve(std::exception_ptr e) {
        if (resolved) return;
        resolved = true;
        done.set_value(e);
    }
};

class http_server_input;

// Forwards every request to the input. Declared out of line because it needs the
// input's definition, which needs the handler's type in a route.
class endpoint_handler final : public seastar::httpd::handler_base {
public:
    explicit endpoint_handler(http_server_input& in) : _in(in) {}
    seastar::future<std::unique_ptr<seastar::http::reply>>
    handle(const seastar::sstring& path, std::unique_ptr<seastar::http::request> req,
           std::unique_ptr<seastar::http::reply> rep) override;
private:
    http_server_input& _in;
};

class http_server_input final : public input {
    // Depth one, so a request blocks as soon as the pipeline is behind. That IS
    // the backpressure: the reference uses an unbuffered channel here for the
    // same reason, and a deep queue would accept requests it has no capacity to
    // serve.
    //
    // A rendezvous rather than a seastar::queue because every concurrent request
    // is another PRODUCER, and seastar::queue supports one blocked producer --
    // see runtime/rendezvous.hh.
    static constexpr size_t queue_depth = 1;

public:
    http_server_input(server_input_config cfg, rate_limit_ptr limit)
        : _cfg(std::move(cfg)), _limit(std::move(limit)), _queue(queue_depth) {
        for (auto& v : _cfg.allowed_verbs) v = upper(v);
        _sync_headers = compile_headers(_cfg.sync_response.headers);
    }

    seastar::future<> connect(seastar::abort_source& as) override {
        _abort_sub = as.subscribe([this]() noexcept { release_all(); });
        const auto addr = co_await resolve(split_host_port(_cfg.address,
                                                           "http_server `address`"));
        _server = std::make_unique<seastar::httpd::http_server>("swordfish-http-server");
        // Streamed bodies, so a large POST is not buffered into an sstring
        // before the handler sees it.
        _server->set_content_streaming(true);
        // A hand-written handler rather than seastar's function_handler: that
        // one overwrites Content-Type with the type it was constructed with,
        // which would relabel every synchronous response and destroy a
        // multipart boundary.
        _server->_routes.add_default_handler(new endpoint_handler(*this));
        // The reference sends no Server header, and matching it costs nothing.
        _server->set_server_header(std::nullopt);
        // Every shard listens on the same address, which is what Seastar's own
        // http_server_control does (it invokes listen on all of them). The
        // kernel then spreads connections across shards and each request is
        // served entirely on the shard that accepted it -- no cross-shard
        // transaction machinery, which this runtime does not have.
        co_await _server->listen(addr);
        hlog.info("http_server listening on {}{}", _cfg.address, _cfg.path);
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
                // The rendezvous is closed only by shutdown. An HTTP endpoint
                // never reaches a natural end of input, so this is the only way
                // one is reported.
                return std::optional<std::pair<batch, ack_fn>>(std::nullopt);
            });
    }

    seastar::future<> close() override {
        // Order matters, and the obvious order is wrong. Stopping the server
        // first looks right -- no new request can arrive -- but http_server::stop()
        // calls shutdown() on every OPEN connection too, so a handler whose
        // pipeline work has already finished loses the reply it was about to
        // write. Measured: with a 3s processor and SIGTERM at t=1s, swordfish
        // reported `in=1 out=1 acks=1 nacks=0` and the client got an empty reply
        // -- the message was acked as delivered and the caller was told nothing.
        // The reference answers that request normally.
        //
        // So: release what is queued (nacking it, so no handler waits on an ack
        // that will never come), let the handlers finish, let their replies
        // flush, and only then stop. This is the same shape http_server_output's
        // close() already used.
        release_all();
        co_await _handlers.close();
        if (_server) {
            const auto deadline = seastar::lowres_clock::now() + drain_budget;
            while (_server->current_connections() > 0 &&
                   seastar::lowres_clock::now() < deadline)
                co_await seastar::sleep(std::chrono::milliseconds(2));
            try { co_await _server->stop(); }
            catch (const std::exception& e) { hlog.warn("http_server stop failed: {}", e.what()); }
        }
        _server.reset();
    }

    connection_status status() const override {
        connection_status s;
        s.connected = _server != nullptr;
        s.label = "http_server";
        return s;
    }

    seastar::future<std::unique_ptr<seastar::http::reply>>
    handle(std::unique_ptr<seastar::http::request> req,
           std::unique_ptr<seastar::http::reply> rep) {
        // Held by the gate so close() waits for in-flight requests rather than
        // tearing the queue out from under them.
        auto closed = [] {
            auto r = std::make_unique<seastar::http::reply>();
            r->_status = seastar::http::reply::status_type::service_unavailable;
            r->_content = "shutting down\n";
            return r;
        };
        try {
            // Caught synchronously as well as on the future: see the note on the
            // output's handler for why gate::enter() needs both.
            return seastar::with_gate(_handlers, [this, req = std::move(req),
                                                  rep = std::move(rep)]() mutable {
                return do_handle(std::move(req), std::move(rep));
            }).handle_exception_type([closed](const seastar::gate_closed_exception&) {
                return closed();
            });
        } catch (const seastar::gate_closed_exception&) {
            return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(closed());
        }
    }

    seastar::future<std::unique_ptr<seastar::http::reply>>
    do_handle(std::unique_ptr<seastar::http::request> req,
              std::unique_ptr<seastar::http::reply> rep) {
        const std::string verb = upper(std::string(req->_method));
        if (std::find(_cfg.allowed_verbs.begin(), _cfg.allowed_verbs.end(), verb)
                == _cfg.allowed_verbs.end()) {
            rep->_status = seastar::http::reply::status_type::method_not_allowed;
            rep->_content = "method not allowed\n";
            co_return std::move(rep);
        }
        // `path` selects an endpoint; anything else is a 404, as it is in the
        // reference. A default handler is used rather than a routed one because
        // the path is configuration, not a compile-time constant.
        std::map<std::string, std::string> path_params;
        if (!path_matches(std::string_view(req->_url), _cfg.path, path_params)) {
            rep->_status = seastar::http::reply::status_type::not_found;
            rep->_content = "not found\n";
            co_return std::move(rep);
        }

        // Checked after the method and path, as the reference checks it, so a
        // request to the wrong endpoint does not spend a permit. Answered rather
        // than delayed: holding a client's connection open to wait would consume
        // the very resource the limit protects. Retry-After is in seconds and is
        // rounded UP, because rounding down would invite the client back before
        // a permit exists.
        if (_limit) {
            const auto wait = _limit->access();
            if (wait.count() > 0) {
                rep->_status = seastar::http::reply::status_type::too_many_requests;
                // `access()` is NANOSECONDS now, and Retry-After is whole
                // seconds rounded UP -- rounding down would invite the client
                // back before a permit exists.
                const auto secs = std::chrono::duration_cast<std::chrono::seconds>(
                    wait + std::chrono::nanoseconds(999'999'999));
                rep->_headers["Retry-After"] = std::to_string(secs.count());
                rep->_content = "too many requests\n";
                co_return std::move(rep);
            }
        }

        std::string body;
        if (req->content_stream)
            body = co_await seastar::util::read_entire_stream_contiguous(*req->content_stream);

        // A multipart request is a BATCH, one message per part -- documented
        // behaviour that was not implemented: every part, its headers and its
        // boundaries arrived as a single opaque message, so a producer sending
        // three parts got one message containing the wire framing.
        std::vector<std::string> bodies;
        {
            const auto ct = req->get_header("Content-Type");
            const auto [media, params] =
                parse_media_type(ct.empty() ? std::string_view("application/octet-stream")
                                            : std::string_view(ct));
            if (media.starts_with("multipart/")) {
                try {
                    auto it = params.find("boundary");
                    bodies = split_multipart(
                        body, it == params.end() ? std::string_view() : std::string_view(it->second));
                } catch (const std::exception& ex) {
                    // The reference answers a malformed multipart request 400
                    // rather than ingesting it.
                    hlog.debug("http_server: rejecting malformed multipart request: {}", ex.what());
                    rep->_status = seastar::http::reply::status_type::bad_request;
                    rep->_content = "bad multipart request\n";
                    co_return std::move(rep);
                }
            } else {
                bodies.push_back(std::move(body));
            }
        }

        auto f = seastar::make_lw_shared<in_flight>();
        f->store = std::make_shared<response_store>();

        batch b;
        for (auto& part : bodies) {
            message m(std::move(part));
            m.meta().set("http_server_verb", value(verb));
            m.meta().set("http_server_request_path", value(std::string(req->_url)));
            m.meta().set("http_server_user_agent",
                         value(std::string(req->get_header("User-Agent"))));
            // The peer's IP, without the port -- SplitHostPort's host half.
            if (const auto& ca = req->get_client_address(); !ca.is_af_unix())
                m.meta().set("http_server_remote_ip",
                             value(fmt::format("{}", ca.addr())));
            // Every request header, then the query, then `{param}` captures,
            // then cookies -- the reference's order, and it is load-bearing: a
            // later source overwrites an earlier one under the same name.
            // Only the first three were set, so a mapping reading an
            // `Authorization` or `X-Request-Id` header saw nothing at all.
            for (const auto& [k, v] : req->_headers)
                m.meta().set(canonical_header(k), value(std::string(v)));
            for (const auto& [k, vs] : req->get_query_params())
                if (!vs.empty()) m.meta().set(std::string(k), value(std::string(vs.front())));
            // A `{param}` segment becomes metadata under its own name, which is
            // what the reference does: `path: /p/{id}` on a request to /p/42
            // sets `id` = "42".
            for (const auto& [k, v] : path_params) m.meta().set(k, value(v));
            for (const auto& [k, v] : parse_cookies(req->get_header("Cookie")))
                m.meta().set(k, value(v));
            m.set_response(f->store);
            b.push_back(std::move(m));
        }
        // Zero parts is answered 200 with an empty body and nothing is enqueued,
        // which is what the reference does -- an empty batch reaches the
        // pipeline there and produces no output. Emitting a synthetic empty
        // message instead would put a message into the stream that the reference
        // never produces.
        if (b.empty()) {
            rep->_status = seastar::http::reply::status_type::ok;
            rep->_content = "";
            co_return std::move(rep);
        }
        ack_fn ack = [f](std::exception_ptr e) {
            f->resolve(e);
            return seastar::make_ready_future<>();
        };

        // The ENQUEUE is bounded by the same `timeout` the response wait uses.
        // It had no deadline at all, and the queue is one deep: once the pipeline
        // stalled, every request past the first parked here for ever, so
        // connections accumulated and the server stopped accepting. `timeout` is
        // the field that is supposed to prevent exactly that.
        const auto deadline = std::chrono::steady_clock::now() +
            std::chrono::duration_cast<std::chrono::milliseconds>(_cfg.timeout.ns);
        bool enqueue_timed_out = false;
        try {
            co_await _queue.push(std::make_pair(std::move(b), std::move(ack)), deadline);
        } catch (const seastar::condition_variable_timed_out&) {
            enqueue_timed_out = true;
        } catch (...) {
            rep->_status = seastar::http::reply::status_type::service_unavailable;
            rep->_content = "shutting down\n";
            co_return std::move(rep);
        }
        if (enqueue_timed_out) {
            rep->_status = seastar::http::reply::status_type::request_timeout;
            rep->_content = "timed out waiting for the pipeline\n";
            co_return std::move(rep);
        }

        // Bounded: without this a stalled output would hold every connection
        // open until the client gave up, and the server would stop accepting.
        std::exception_ptr err;
        bool timed_out = false;
        try {
            err = co_await seastar::with_timeout(
                seastar::lowres_clock::now() + std::chrono::duration_cast<
                    std::chrono::milliseconds>(_cfg.timeout.ns),
                f->done.get_future());
        } catch (const seastar::timed_out_error&) {
            timed_out = true;
        } catch (...) {
            err = std::current_exception();
        }

        if (timed_out) {
            // 408, which is what the reference answers -- swordfish said 503,
            // and the two mean different things to a caller deciding whether to
            // retry this request or to back off the service.
            rep->_status = seastar::http::reply::status_type::request_timeout;
            rep->_content = "timed out waiting for the pipeline\n";
            co_return std::move(rep);
        }
        if (err) {
            rep->_status = seastar::http::reply::status_type::internal_server_error;
            rep->_content = describe(err) + "\n";
            co_return std::move(rep);
        }

        // A `sync_response` output somewhere in the pipeline will have filled
        // the store. Nothing there means the pipeline consumed the message,
        // which is a successful request with an empty body.
        std::vector<const message*> parts;
        for (const auto& rb : f->store->batches())
            for (const auto& msg : rb) parts.push_back(&msg);

        // Both the status and the headers are applied ONLY when the pipeline
        // actually produced a synchronous response. The reference does this
        // inside `if len(svcBatch) > 0`, and it matters: routing to `drop`
        // rather than `sync_response` used to answer `201 Created` with the
        // configured Content-Type over a zero-length body, so a caller parsed an
        // empty body as the JSON it had been promised. With nothing in the store
        // the answer is a bare 200.
        if (!parts.empty()) {
            rep->_status = static_cast<seastar::http::reply::status_type>(
                _cfg.sync_response.status);
            // Interpolated against the message being answered, as the reference
            // does. `hf` rather than `f`, which is the enclosing scope's future.
            for (const auto& [k, hf] : _sync_headers)
                rep->add_header(k, eval_header(hf, const_cast<message*>(parts[0])));
        }
        if (parts.size() == 1) {
            // Verbatim, with no separator added. A trailing newline here would
            // corrupt any caller that treats the body as bytes.
            rep->_content = parts[0]->as_bytes();
        } else if (parts.size() > 1) {
            // Several messages come back as multipart/form-data, which is what
            // the reference does (mime/multipart's Writer, whose
            // FormDataContentType names that type). Joining them with newlines
            // instead would silently merge messages a caller is meant to be
            // able to tell apart.
            const std::string boundary = random_boundary();
            std::string multipart;
            for (const auto* p : parts) {
                multipart += "--" + boundary + "\r\n";
                multipart += "Content-Type: " + sniff_content_type(p->as_bytes()) + "\r\n\r\n";
                multipart += p->as_bytes();
                multipart += "\r\n";
            }
            multipart += "--" + boundary + "--\r\n";
            rep->_headers.erase("Content-Type");
            rep->add_header("Content-Type", "multipart/form-data; boundary=" + boundary);
            rep->_content = std::move(multipart);
        }
        co_return std::move(rep);
    }

private:
    // Closing hands back whatever was still buffered. Those batches have a
    // request handler waiting on each of their acks, so they are nacked rather
    // than dropped -- a handler whose promise nobody resolves would sit until
    // its timeout and answer 503 for no reason.
    void release_all() noexcept {
        auto e = std::make_exception_ptr(seastar::abort_requested_exception());
        for (auto& entry : _queue.close(e)) (void)entry.second(e);
    }

    // Bounds the wait for replies to flush during close(). current_connections()
    // is the only progress signal httpd exposes, and a client parking an idle
    // keep-alive connection must not hold shutdown open for ever.
    static constexpr std::chrono::milliseconds drain_budget{2000};

    server_input_config                        _cfg;
    header_fns                                 _sync_headers;
    rate_limit_ptr                             _limit;
    rendezvous<std::pair<batch, ack_fn>>       _queue;
    std::unique_ptr<seastar::httpd::http_server> _server;
    seastar::gate                              _handlers;
    seastar::optimized_optional<seastar::abort_source::subscription> _abort_sub;
};

seastar::future<std::unique_ptr<seastar::http::reply>>
endpoint_handler::handle(const seastar::sstring&, std::unique_ptr<seastar::http::request> req,
                         std::unique_ptr<seastar::http::reply> rep) {
    return _in.handle(std::move(req), std::move(rep));
}

// ---- http_client input ------------------------------------------------------

// Two modes over one config. POLLING makes a request per read_batch and turns
// each response body into a message. STREAMING makes one long-lived request and
// frames the body as it arrives, which is what a server-sent-events or
// newline-delimited-JSON feed needs. The reference selects between them with
// `stream.enabled`, and so does this.
class http_client_input final : public input {
    static constexpr size_t queue_depth = 1;      // back pressure, as elsewhere

public:
    http_client_input(client_input_config cfg, rate_limit_ptr limit, scanner_spec sc)
        : _cfg(std::move(cfg)), _url(parse_url(_cfg.url)), _verb(upper(_cfg.verb)),
          _limit(std::move(limit)), _stream_scanner(std::move(sc)), _queue(queue_depth) {
        if (!_cfg.payload.source.empty())
            _payload = interpreted_transform(
                blobl::parse_query(cfg::interpolation_to_query(_cfg.payload.source)));
        _headers = compile_headers(_cfg.headers);
    }

    seastar::future<> connect(seastar::abort_source& as) override {
        const auto addr = co_await resolve(_url.ep);
        _client = std::make_unique<seastar::http::client>(addr);
        if (_cfg.stream.enabled) {
            _abort_sub = as.subscribe([this]() noexcept {
                (void)_queue.close(
                    std::make_exception_ptr(seastar::abort_requested_exception()));
            });
            (void)seastar::with_gate(_streaming, [this, &as] { return stream_loop(as); });
        }
    }

    seastar::future<std::optional<std::pair<batch, ack_fn>>>
    read_batch(seastar::abort_source& as) override {
        if (as.abort_requested()) co_return std::nullopt;
        if (_cfg.stream.enabled) {
            try {
                auto v = co_await _queue.pop();
                co_return std::optional<std::pair<batch, ack_fn>>(std::move(v));
            } catch (...) {
                co_return std::nullopt;           // closed: shutting down
            }
        }
        // Polling. A body that is empty and `drop_empty_bodies` is set is not a
        // message and not an end of input either, so the loop simply asks again.
        // With no rate limit configured this polls as fast as the target answers,
        // which is what the reference does and the reason its own documentation
        // example for this input configures one.
        //
        // A FAILED REQUEST DOES NOT END THE INPUT. request_once's retry ladder is
        // per-request; when it is exhausted the poll waits and asks again, which
        // is what the reference does. Letting the exception out killed the input
        // layer permanently -- and with nothing else running, the process, WITH
        // EXIT CODE 0, so a supervisor could not tell it from normal completion.
        // One refused TCP connection was enough. Measured on the same config:
        // redpanda-connect consumed 4556 messages once the peer appeared, by
        // which time swordfish had been dead for four seconds.
        auto backoff = std::chrono::duration_cast<std::chrono::milliseconds>(
            _cfg.retry_period.ns);
        // Kept apart from `backoff`: an endpoint that is UP and simply has
        // nothing to say is not the same condition as one that will not answer,
        // and a run of empty bodies must not make a later real failure start
        // from a long delay.
        auto empty_backoff = backoff;
        const auto cap = std::chrono::duration_cast<std::chrono::milliseconds>(
            _cfg.max_retry_backoff.ns);
        while (!as.abort_requested()) {
            // co_await is not permitted inside a catch handler, so the failure
            // is recorded and acted on below -- the shape stop() and the
            // pipeline push already use.
            std::string body;
            std::exception_ptr err;
            bool aborted = false;
            try {
                body = co_await request_once(as);
            } catch (const seastar::abort_requested_exception&) {
                aborted = true;
            } catch (...) {
                err = std::current_exception();
            }
            if (aborted || (err && as.abort_requested())) co_return std::nullopt;
            if (err) {
                hlog.error("http_client: {} is not answering, polling again in "
                           "{}ms: {}", _cfg.url, backoff.count(), describe(err));
                bool slept = true;
                try { co_await seastar::sleep_abortable(backoff, as); }
                catch (...) { slept = false; }
                if (!slept) co_return std::nullopt;
                backoff = std::min(cap, backoff * 2);
                continue;
            }
            // Reset on success: the next outage starts its own ladder rather
            // than inheriting a backoff from an outage hours ago.
            backoff = std::chrono::duration_cast<std::chrono::milliseconds>(
                _cfg.retry_period.ns);
            if (body.empty() && _cfg.drop_empty_bodies) {
                // A DELAY, not an immediate re-poll. The reference turns this
                // into component.ErrTimeout (input_http_client.go:260-266), so
                // its reader waits before asking again; re-issuing straight away
                // turned any long-poll or drain-style endpoint that answers 200
                // with an empty body into a several-thousand-request-per-second
                // hammer on one shard. Bounded by the same ladder a failure
                // uses, and reset the moment a body arrives.
                try { co_await seastar::sleep_abortable(empty_backoff, as); }
                catch (...) { co_return std::nullopt; }
                empty_backoff = std::min(cap, empty_backoff * 2);
                continue;
            }
            empty_backoff = std::chrono::duration_cast<std::chrono::milliseconds>(
                _cfg.retry_period.ns);
            batch b;
            b.push_back(message(std::move(body)));
            co_return std::make_pair(std::move(b), noop_ack());
        }
        co_return std::nullopt;
    }

    seastar::future<> close() override {
        (void)_queue.close(std::make_exception_ptr(seastar::abort_requested_exception()));
        co_await _streaming.close();
        if (!_client) co_return;
        auto c = std::move(_client);
        try { co_await c->close(); }
        catch (const std::exception& e) { hlog.warn("http_client close failed: {}", e.what()); }
    }

    connection_status status() const override {
        connection_status s;
        s.connected = _client != nullptr;
        s.label = "http_client";
        return s;
    }

private:
    seastar::http::request build_request() {
        auto req = seastar::http::request::make(_verb, _url.host_hdr, _url.path);
        // A polling input has NO message, so interpolation is evaluated against
        // an empty one rather than a bare exec_ctx: `content()` yields "" and a
        // literal header still works, where the bare context made every
        // interpolation throw and the input send zero requests.
        std::string content_type = "application/octet-stream";
        for (const auto& [k, f] : _headers) {
            const std::string v = eval_header(f, nullptr);
            req._headers[k] = v;
            if (k == "Content-Type") content_type = v;
        }
        if (_payload) {
            interp_scope s(nullptr);
            req.write_body(content_type,
                           seastar::sstring(_payload(s.ctx).to_display_string()));
        }
        return req;
    }

    // One polling request, with the same retry ladder the output uses. A request
    // that fails every attempt throws, which the stream layer reports rather
    // than treating as an end of input.
    seastar::future<std::string> request_once(seastar::abort_source& as) {
        auto delay = std::chrono::duration_cast<std::chrono::milliseconds>(
            _cfg.retry_period.ns);
        const auto cap = std::chrono::duration_cast<std::chrono::milliseconds>(
            _cfg.max_retry_backoff.ns);
        for (int attempt = 0; ; ++attempt) {
            // Before EVERY attempt, retries included: a retry is another request
            // to the same target, and the limit counts requests. An abandoned
            // wait means shutdown, which the retry loop below turns straight into
            // a rethrow because the abort source is set.
            if (!co_await rate_limit_wait(_limit, as))
                throw seastar::abort_requested_exception();
            std::exception_ptr err;
            auto body = std::make_shared<std::string>();
            auto status_code = std::make_shared<int>(0);
            try {
                auto handler = [body, status_code](const seastar::http::reply& rep,
                                              seastar::input_stream<char>&& in)
                        -> seastar::future<> {
                    *status_code = static_cast<int>(rep._status);
                    auto s = std::move(in);
                    *body = co_await seastar::util::read_entire_stream_contiguous(s);
                    co_await s.close();
                };
                co_await seastar::with_timeout(
                    seastar::lowres_clock::now() +
                        std::chrono::duration_cast<std::chrono::milliseconds>(_cfg.timeout.ns),
                    _client->make_request(build_request(), std::move(handler),
                                          std::nullopt, &as));
                if (*status_code < 200 || *status_code > 299)
                    throw std::runtime_error("http_client: " + _cfg.url + " answered " +
                                             std::to_string(*status_code));
                co_return std::move(*body);
            } catch (...) {
                err = std::current_exception();
            }
            if (attempt >= _cfg.retries || as.abort_requested())
                std::rethrow_exception(err);
            hlog.warn("http_client request failed (attempt {} of {}), retrying in {}ms: {}",
                      attempt + 1, _cfg.retries + 1, delay.count(), describe(err));
            try { co_await seastar::sleep_abortable(delay, as); }
            catch (const seastar::sleep_aborted&) { std::rethrow_exception(err); }
            delay = std::min(cap, delay * 2);
        }
    }

    seastar::future<> stream_loop(seastar::abort_source& as) {
        do {
            try {
                // Shared, not unique: the handler lambda is copied into
                // seastar's request machinery and must keep the scanner alive.
                std::shared_ptr<sf::scanner> scanner = make_scanner(_stream_scanner);
                // The body is consumed INSIDE the reply handler: that is the
                // only place seastar's client exposes the stream, and it is
                // where messages have to be pushed out from.
                auto handler = [this, scanner](const seastar::http::reply& rep,
                                               seastar::input_stream<char>&& in)
                        -> seastar::future<> {
                    if (static_cast<int>(rep._status) < 200 ||
                        static_cast<int>(rep._status) > 299)
                        throw std::runtime_error("http_client: " + _cfg.url + " answered " +
                                                 std::to_string(static_cast<int>(rep._status)));
                    auto s = std::move(in);
                    for (;;) {
                        auto buf = co_await s.read();
                        if (buf.empty()) {
                            for (auto& m : scanner->finish()) co_await emit(std::move(m));
                            break;
                        }
                        for (auto& m : scanner->feed(std::string(buf.get(), buf.size())))
                            co_await emit(std::move(m));
                    }
                    co_await s.close();
                };
                if (!co_await rate_limit_wait(_limit, as)) break;
                co_await _client->make_request(build_request(), std::move(handler),
                                               std::nullopt, &as);
            } catch (const std::exception& e) {
                if (as.abort_requested()) break;
                hlog.warn("http_client stream ended: {}", e.what());
            }
            if (!_cfg.stream.reconnect || as.abort_requested()) break;
            try {
                co_await seastar::sleep_abortable(
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        _cfg.retry_period.ns), as);
            } catch (const seastar::sleep_aborted&) { break; }
        } while (!as.abort_requested());
        // Nothing more will arrive, so the reader must be released rather than
        // left waiting on a rendezvous that no longer has a producer.
        (void)_queue.close(std::make_exception_ptr(seastar::abort_requested_exception()));
    }

    seastar::future<> emit(message m) {
        batch b;
        b.push_back(std::move(m));
        return _queue.push(std::make_pair(std::move(b), noop_ack()));
    }

    client_input_config                      _cfg;
    parsed_url                               _url;
    std::string                              _verb;
    transform_fn                             _payload;
    header_fns                               _headers;
    rate_limit_ptr                           _limit;
    scanner_spec                             _stream_scanner;
    std::unique_ptr<seastar::http::client>   _client;
    rendezvous<std::pair<batch, ack_fn>>     _queue;
    seastar::gate                            _streaming;
    seastar::optimized_optional<seastar::abort_source::subscription> _abort_sub;
};

// The wait between reconnect attempts. A constant rather than a field: the
// reference exposes no such setting, and a swordfish-only one is what
// `reconnect_period` was.
static constexpr std::chrono::milliseconds reconnect_wait{1000};

// ---- websocket --------------------------------------------------------------
//
// Both directions are CLIENTS, which is what the reference's `websocket` input
// and output are: each takes a `url` and dials out. The server side of the
// protocol lives on `http_server`'s `ws_path` there, and is not implemented
// here -- see the TODO on server_input_config::ws_path for why and for the
// options.
//
// Three things about seastar's websocket client shape the code below, and each
// of them was found the hard way.
//
// WRITING. The handler is given an output_stream over a 512-byte data sink
// constructed with trim_to_size, so a write larger than that is SPLIT and
// response_loop() sends each piece as its own complete BINARY frame -- a peer
// then receives several messages where the pipeline sent one, silently, for any
// payload over 512 bytes. `send_data()` frames a whole buffer in one call, so
// nothing here touches the handler's stream.
//
// FLUSHING. `connected_socket::output()` batches its flushes: `flush()` marks
// the buffer and returns, and the poller writes later. `send_data()` therefore
// resolves before the bytes are on the wire, and a teardown that calls
// `shutdown_output()` discards whatever is still pending -- which cost the LAST
// message of every run, acked as delivered and never sent. `output_stream::
// close()` is the one operation that waits for the batch (it awaits the
// internal promise), so `finish()` below closes the write buffer explicitly
// before shutting the socket down.
//
// READING. Each frame's payload is pushed whole into the connection's input
// queue, so one read() is one message. Seastar does not reassemble CONTINUATION
// frames -- it pushes them as separate buffers -- so a peer that fragments a
// message delivers it here as several. Noted rather than worked around:
// reassembly belongs in seastar, and neither implementation fragments what it
// sends.

namespace ws = seastar::experimental::websocket;

// Drives a websocket connection directly rather than through
// client_connection::process(), which additionally runs response_loop() -- the
// loop that turns writes on the handler's output_stream into frames. Nothing
// here writes through that stream, and leaving the loop out means nothing closes
// `_write_buf` behind the teardown's back.
//
// Deriving is the only way to reach the protected framing and buffers. The base
// lives in seastar's `experimental` namespace, so this is the part most likely
// to need attention when seastar moves.
class framed_client final : public ws::client_connection<false> {
public:
    using ws::client_connection<false>::client_connection;

    // One message, one frame, whatever its size.
    //
    // The OPCODE is a parameter rather than a constant: `open_message_type` was
    // accepted and ignored, so `text` produced a binary frame -- which some
    // servers reject outright, and which no amount of comparing message bodies
    // would have shown.
    seastar::future<> send_message(const std::string& body,
                                   ws::opcodes type = ws::opcodes::BINARY) {
        return send_data(type, seastar::temporary_buffer<char>(body.data(), body.size()));
    }

    // Decoded inbound messages: one read() per frame.
    seastar::input_stream<char>& messages() { return _input; }

    // The frame loop. Answers PING and CLOSE, and feeds messages(). Runs until
    // finish() stops it or the peer goes away.
    seastar::future<> pump() {
        try {
            while (!_done) co_await read_one();
        } catch (...) {
            // A read failing IS how a closed connection reports itself; the
            // caller sees it as the end of the stream.
        }
    }

    // Ordered teardown: CLOSE frame, then a REAL flush, then the socket. See
    // the FLUSHING note above for why the explicit close() is not redundant.
    seastar::future<> finish() {
        if (_half_close) co_return;
        _half_close = true;
        try {
            co_await send_data(ws::opcodes::CLOSE, seastar::temporary_buffer<char>(0));
            co_await _write_buf.close();
        } catch (const std::exception& e) {
            hlog.debug("websocket close frame failed: {}", e.what());
        }
        _done = true;
        // Unblocks pump(), which is otherwise parked in a read that will never
        // complete now that we are the ones closing.
        try { shutdown_input(); } catch (...) {}
        try {
            co_await seastar::when_all_succeed(_input.close(), _output.close())
                        .discard_result();
        } catch (...) {}
    }
};

// ws://host[:port]/path is an http:// URL with a different scheme, so it goes
// through the same parser once the scheme is swapped.
parsed_url parse_ws_url(const std::string& url) {
    static constexpr std::string_view ws_scheme = "ws://";
    if (url.rfind("wss://", 0) == 0)
        throw std::runtime_error(
            "websocket: wss is not implemented by swordfish yet; the TLS "
            "configuration a secure client needs is not built, and connecting "
            "in the clear instead would be worse than refusing");
    if (url.rfind(ws_scheme, 0) != 0)
        throw std::runtime_error("websocket `url` must begin with ws://, got '" + url + "'");
    return parse_url("http://" + url.substr(ws_scheme.size()));
}

// Resolve, connect, and complete the opening handshake. The frame loop is NOT
// started here: the caller owns it, because it also owns the gate it runs in.
seastar::future<std::unique_ptr<framed_client>> dial_ws(const parsed_url& u) {
    const auto addr = co_await resolve(u.ep);
    auto fd = co_await seastar::connect(addr);
    auto conn = std::make_unique<framed_client>(
        std::move(fd), u.path, u.host_hdr, seastar::sstring{}, ws::handler_t{});
    co_await conn->handshake();
    co_return conn;
}

class websocket_input final : public input {
    static constexpr size_t queue_depth = 1;

public:
    explicit websocket_input(websocket_input_config cfg)
        : _cfg(std::move(cfg)), _url(parse_ws_url(_cfg.url)), _queue(queue_depth) {}

    seastar::future<> connect(seastar::abort_source& as) override {
        _abort_sub = as.subscribe([this]() noexcept {
            (void)_queue.close(
                std::make_exception_ptr(seastar::abort_requested_exception()));
        });
        // Only STARTS the loop. Waiting for a first successful dial here would
        // make a pipeline fail to start against a peer that is merely slow to
        // come up, and the loop reconnects anyway.
        (void)seastar::with_gate(_reading, [this, &as] { return read_loop(as); });
        co_return;
    }

    seastar::future<std::optional<std::pair<batch, ack_fn>>>
    read_batch(seastar::abort_source& as) override {
        if (as.abort_requested()) co_return std::nullopt;
        try {
            auto v = co_await _queue.pop();
            co_return std::optional<std::pair<batch, ack_fn>>(std::move(v));
        } catch (...) {
            co_return std::nullopt;               // closed: shutting down
        }
    }

    seastar::future<> close() override {
        (void)_queue.close(std::make_exception_ptr(seastar::abort_requested_exception()));
        if (_conn) co_await _conn->finish();
        co_await _reading.close();
        _conn.reset();
    }

    connection_status status() const override {
        connection_status s;
        s.connected = _conn != nullptr;
        s.label = "websocket";
        return s;
    }

private:
    seastar::future<> read_loop(seastar::abort_source& as) {
        while (!as.abort_requested() && !_queue.closed()) {
            try {
                _conn = co_await dial_ws(_url);
                // CONSECUTIVE, which is what the field means -- "how many
                // consecutive reconnects to attempt". The counter was only ever
                // incremented, so it capped the input's TOTAL lifetime
                // disconnects instead of bounding a failing reconnect storm: a
                // healthy long-lived feed that reconnects normally reached the
                // limit and terminated the pipeline.
                _retries = 0;
                if (!_cfg.open_message.empty()) {
                    // One frame, before anything is read: a subscribe message
                    // has to arrive before the feed it subscribes to.
                    co_await _conn->send_message(
                        _cfg.open_message,
                        _cfg.open_message_type == "text" ? ws::opcodes::TEXT
                                                         : ws::opcodes::BINARY);
                }
                // The frame loop and the consumer run together; whichever ends
                // first ends the connection.
                co_await seastar::when_all(_conn->pump(), consume())
                            .then([](auto&& fs) {
                                std::apply([](auto&&... f) { (f.ignore_ready_future(), ...); },
                                           std::move(fs));
                            });
            } catch (const std::exception& e) {
                if (as.abort_requested() || _queue.closed()) break;
                hlog.warn("websocket {} disconnected: {}", _cfg.url, e.what());
            }
            if (_conn) { co_await _conn->finish(); _conn.reset(); }
            if (as.abort_requested() || _queue.closed()) break;
            // `connection.max_retries`: zero never reconnects, below zero is
            // unlimited, and reaching the limit terminates the input GRACEFULLY
            // -- the reference's word, so the stream drains rather than failing.
            if (_cfg.connection.max_retries >= 0 &&
                ++_retries > _cfg.connection.max_retries) {
                hlog.info("websocket {}: giving up after {} reconnect attempts",
                          _cfg.url, _cfg.connection.max_retries);
                break;
            }
            try {
                co_await seastar::sleep_abortable(reconnect_wait, as);
            } catch (const seastar::sleep_aborted&) { break; }
        }
        // Nothing more will arrive; release the reader rather than leaving it
        // on a rendezvous with no producer.
        (void)_queue.close(std::make_exception_ptr(seastar::abort_requested_exception()));
    }

    seastar::future<> consume() {
        for (;;) {
            auto buf = co_await _conn->messages().read();
            if (buf.empty()) co_return;           // the peer closed
            if (_cfg.max_message_size > 0 &&
                static_cast<int64_t>(buf.size()) > _cfg.max_message_size) {
                // The reference closes with status 1009 here. Closing and
                // reconnecting is the same outcome; the status code is not
                // reachable through seastar's close path.
                hlog.warn("websocket message of {} bytes exceeds max_message_size {}; "
                          "closing the connection", buf.size(), _cfg.max_message_size);
                co_return;
            }
            batch b;
            b.push_back(message(std::string(buf.get(), buf.size())));
            co_await _queue.push(std::make_pair(std::move(b), noop_ack()));
        }
    }

    websocket_input_config               _cfg;
    int64_t                              _retries = 0;
    parsed_url                           _url;
    rendezvous<std::pair<batch, ack_fn>> _queue;
    std::unique_ptr<framed_client>       _conn;
    seastar::gate                        _reading;
    seastar::optimized_optional<seastar::abort_source::subscription> _abort_sub;
};

class websocket_output final : public output {
public:
    explicit websocket_output(websocket_output_config cfg)
        : _cfg(std::move(cfg)), _url(parse_ws_url(_cfg.url)) {}

    seastar::future<> connect(seastar::abort_source&) override {
        co_await dial_once();
    }

    seastar::future<> write_batch(batch b, seastar::abort_source& as) override {
        for (const auto& m : b) {
            if (!_conn) co_await dial_once();
            // co_await is not permitted inside a catch handler, so the failure
            // is recorded and acted on after it -- the shape stream.cc uses on
            // its nack paths.
            std::exception_ptr err;
            try {
                co_await _conn->send_message(m.as_bytes());
            } catch (...) {
                err = std::current_exception();
            }
            if (!err) continue;
            // One reconnect, then the batch is nacked. Retrying for ever here
            // would hide a peer that is gone; the source's own replay decides
            // how persistent to be.
            hlog.warn("websocket send failed, reconnecting: {}", describe(err));
            co_await drop_connection();
            if (as.abort_requested()) std::rethrow_exception(err);
            try {
                co_await seastar::sleep_abortable(reconnect_wait, as);
            } catch (const seastar::sleep_aborted&) { std::rethrow_exception(err); }
            co_await dial_once();
            co_await _conn->send_message(m.as_bytes());
        }
    }

    seastar::future<> close() override { co_await drop_connection(); }

    connection_status status() const override {
        connection_status s;
        s.connected = _conn != nullptr;
        s.label = "websocket";
        return s;
    }

    // One at a time: frames on a connection are ordered, and two concurrent
    // send_data() calls would interleave two messages.
    size_t max_in_flight() const override { return 1; }

private:
    seastar::future<> dial_once() {
        _conn = co_await dial_ws(_url);
        // The frame loop answers PING and CLOSE for the life of the connection.
        // Not awaited: it returns only when the connection ends.
        (void)seastar::with_gate(_running, [this] { return _conn->pump(); });
    }

    seastar::future<> drop_connection() {
        if (!_conn) co_return;
        co_await _conn->finish();
        co_await _running.close();
        _conn.reset();
        // A gate cannot be reopened, so a reconnect needs a fresh one.
        _running = seastar::gate{};
    }

    websocket_output_config        _cfg;
    parsed_url                     _url;
    std::unique_ptr<framed_client> _conn;
    seastar::gate                  _running;
};

// ---- http_server output -----------------------------------------------------
//
// The inversion of every other output here: this one WAITS FOR A CONSUMER.
// `write_batch` offers the batch and does not resolve until a client has taken
// it, which is what makes an unconsumed endpoint apply back pressure to the
// pipeline instead of buffering. The reference does the same with an unbuffered
// channel.

// One batch offered to whichever handler takes it first, and the promise that
// tells `write_batch` how it went.
struct offered_batch {
    batch                                payload;
    seastar::promise<std::exception_ptr> delivered;
    bool                                 resolved = false;

    void resolve(std::exception_ptr e) {
        if (resolved) return;
        resolved = true;
        delivered.set_value(e);
    }
};
using offered_ptr = seastar::lw_shared_ptr<offered_batch>;

class http_server_output;

class served_endpoint final : public seastar::httpd::handler_base {
public:
    served_endpoint(http_server_output& out, bool streaming)
        : _out(out), _streaming(streaming) {}
    seastar::future<std::unique_ptr<seastar::http::reply>>
    handle(const seastar::sstring& path, std::unique_ptr<seastar::http::request> req,
           std::unique_ptr<seastar::http::reply> rep) override;
private:
    http_server_output& _out;
    bool                _streaming;
};

class http_server_output final : public output {
    // Depth one: a batch waits for a consumer rather than piling up behind one.
    static constexpr size_t queue_depth = 1;
    // How long close() will wait for in-flight replies to leave the socket.
    static constexpr std::chrono::milliseconds drain_budget{2000};

public:
    explicit http_server_output(server_output_config cfg)
        : _cfg(std::move(cfg)), _queue(queue_depth) {
        for (auto& v : _cfg.allowed_verbs) v = upper(v);
    }

    seastar::future<> connect(seastar::abort_source& as) override {
        _abort_sub = as.subscribe([this]() noexcept {
            release_all(std::make_exception_ptr(seastar::abort_requested_exception()));
        });
        const auto addr = co_await resolve(split_host_port(_cfg.address,
                                                           "http_server `address`"));
        _server = std::make_unique<seastar::httpd::http_server>("swordfish-http-output");
        _server->set_server_header(std::nullopt);
        // Two routed paths rather than a default handler: unlike the input,
        // which serves one configurable path, this serves two and they behave
        // differently, so the routing has to distinguish them.
        //
        // Registered for EVERY verb, with verb_allowed() as the only filter --
        // the same shape the INPUT gets from its default handler, and the reason
        // the two now answer alike. Both routes were GET-only, so `allowed_verbs`
        // could subtract GET and never add anything: `allowed_verbs: [POST]`
        // lints clean in both implementations and then 404'd every request,
        // where the reference answers 200. Registering only the CONFIGURED verbs
        // fixes that but still 404s a disallowed one, where the reference says
        // 405 -- the router cannot distinguish "no such path" from "not that
        // method", so the handler has to.
        //
        // The configured names are still validated, so a verb that is not an
        // HTTP method is refused by name rather than silently never matching.
        for (const auto& verb : _cfg.allowed_verbs) (void)verb_operation(verb);
        for (int op = 0; op < seastar::httpd::operation_type::NUM_OPERATION; ++op) {
            const auto o = static_cast<seastar::httpd::operation_type>(op);
            _server->_routes.add(o, seastar::httpd::url(_cfg.path),
                                 new served_endpoint(*this, /*streaming=*/false));
            _server->_routes.add(o, seastar::httpd::url(_cfg.stream_path),
                                 new served_endpoint(*this, /*streaming=*/true));
        }
        co_await _server->listen(addr);
        hlog.info("http_server output serving {}{} and {}{}",
                  _cfg.address, _cfg.path, _cfg.address, _cfg.stream_path);
    }

    seastar::future<> write_batch(batch b, seastar::abort_source& as) override {
        if (b.empty()) co_return;
        auto o = seastar::make_lw_shared<offered_batch>();
        o->payload = std::move(b);
        co_await _queue.push(offered_ptr(o));
        // Resolved by the handler that takes it, or by release_all() on
        // shutdown. Not bounded by a timeout here: an output with no client
        // attached is meant to block, and the abort source is what ends it.
        auto err = co_await o->delivered.get_future();
        if (err) std::rethrow_exception(err);
        (void)as;
    }

    seastar::future<> close() override {
        // The rendezvous FIRST, then the gate. The other order deadlocks: the
        // streaming handler sits in pop() waiting for the next batch, the gate
        // waits for that handler, and the release that would wake it is behind
        // the gate. Releasing first fails the pending pops, the handlers finish,
        // and the gate closes.
        release_all(std::make_exception_ptr(seastar::abort_requested_exception()));
        co_await _handlers.close();

        if (_server) {
            // http_server::stop() calls shutdown() on every open connection,
            // and it does so before awaiting its own task gate. A reply that is
            // assembled but not yet flushed is therefore discarded -- which for
            // the last batch of a bounded source means the client sees "empty
            // reply from server" for a message that was already acked. The
            // reference delivers that one, so this waits for the connections to
            // go away first. current_connections() is the only progress signal
            // httpd exposes; the budget bounds a client that parks an idle
            // keep-alive connection.
            const auto deadline = seastar::lowres_clock::now() + drain_budget;
            while (_server->current_connections() > 0 &&
                   seastar::lowres_clock::now() < deadline)
                co_await seastar::sleep(std::chrono::milliseconds(2));
            try { co_await _server->stop(); }
            catch (const std::exception& e) { hlog.warn("http_server output stop failed: {}", e.what()); }
        }
        _server.reset();
    }

    connection_status status() const override {
        connection_status s;
        s.connected = _server != nullptr;
        s.label = "http_server";
        return s;
    }

    // Several batches may be offered at once, so several waiting clients are
    // each served their own rather than queueing behind one. The reference gets
    // the same effect from several handlers reading one channel.
    size_t max_in_flight() const override { return 64; }

private:
    friend class served_endpoint;

    // Anything still queued has a `write_batch` waiting on it. Aborting the
    // queue DISCARDS its contents, so the promises have to be resolved first or
    // those writes would never return.
    void release_all(std::exception_ptr e) noexcept {
        for (auto& o : _queue.close(e)) o->resolve(e);
    }

    seastar::future<std::unique_ptr<seastar::http::reply>>
    handle(std::unique_ptr<seastar::http::request> req,
           std::unique_ptr<seastar::http::reply> rep, bool streaming) {
        auto closed = [] {
            auto r = std::make_unique<seastar::http::reply>();
            r->_status = seastar::http::reply::status_type::service_unavailable;
            r->_content = "Server closed\n";
            return r;
        };
        try {
            // gate::enter() throws SYNCHRONOUSLY, so this cannot be a
            // handle_exception_type on the returned future: the exception never
            // becomes one. Left uncaught it escaped as a C++ exception and
            // seastar's routes rendered the C++ type name into a 500 JSON body.
            return seastar::with_gate(_handlers, [this, streaming, req = std::move(req),
                                                  rep = std::move(rep)]() mutable {
                return streaming ? do_stream(std::move(req), std::move(rep))
                                 : do_get(std::move(req), std::move(rep));
            }).handle_exception_type([closed](const seastar::gate_closed_exception&) {
                return closed();
            });
        } catch (const seastar::gate_closed_exception&) {
            return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(closed());
        }
    }

    bool verb_allowed(const seastar::http::request& req) const {
        const std::string verb = upper(std::string(req._method));
        return std::find(_cfg.allowed_verbs.begin(), _cfg.allowed_verbs.end(), verb)
               != _cfg.allowed_verbs.end();
    }

    // One request, one batch. The body shape matches the reference exactly: a
    // single message verbatim, several as multipart/form-data with every part
    // labelled application/octet-stream -- note that this endpoint does NOT
    // sniff the type the way a synchronous response does.
    seastar::future<std::unique_ptr<seastar::http::reply>>
    do_get(std::unique_ptr<seastar::http::request> req,
           std::unique_ptr<seastar::http::reply> rep) {
        if (!verb_allowed(*req)) {
            rep->_status = seastar::http::reply::status_type::method_not_allowed;
            rep->_content = "Incorrect method\n";
            co_return std::move(rep);
        }
        offered_ptr o;
        try {
            // The rendezvous takes the deadline itself. Wrapping a pop in
            // seastar::with_timeout would leave the abandoned pop registered,
            // and it would then take a batch and drop it -- a lost message on
            // every timed-out request.
            o = co_await _queue.pop(
                seastar::lowres_clock::now() + std::chrono::duration_cast<
                    std::chrono::milliseconds>(_cfg.timeout.ns));
        } catch (const seastar::condition_variable_timed_out&) {
            rep->_status = seastar::http::reply::status_type::request_timeout;
            rep->_content = "Timed out waiting for message\n";
            co_return std::move(rep);
        } catch (...) {
            rep->_status = seastar::http::reply::status_type::service_unavailable;
            rep->_content = "Server closed\n";
            co_return std::move(rep);
        }

        std::string body;
        if (o->payload.size() == 1) {
            rep->add_header("Content-Type", "application/octet-stream");
            body = o->payload[0].as_bytes();
        } else {
            const std::string boundary = random_boundary();
            for (const auto& m : o->payload) {
                body += "--" + boundary + "\r\n";
                body += "Content-Type: application/octet-stream\r\n\r\n";
                body += m.as_bytes();
                body += "\r\n";
            }
            body += "--" + boundary + "--\r\n";
            rep->add_header("Content-Type", "multipart/form-data; boundary=" + boundary);
        }

        // Content-Length, as the reference sends. The batch is resolved here,
        // before the bytes reach the socket -- httpd assembles the whole reply
        // into the connection's write buffer and flushes it afterwards, so no
        // point inside a body writer is any later. The reference acks at the
        // same moment for the same reason (its w.Write buffers too), so this
        // matches rather than compromises.
        //
        // What it does mean is that close() must not tear the server down while
        // a reply is still buffered; see the drain there.
        rep->_content = std::move(body);
        o->resolve(nullptr);
        co_return std::move(rep);
    }

    // A long-lived response, written and flushed per batch. seastar's
    // write_body() hands the handler the output stream, which is what makes a
    // chunked stream expressible at all here.
    seastar::future<std::unique_ptr<seastar::http::reply>>
    do_stream(std::unique_ptr<seastar::http::request> req,
              std::unique_ptr<seastar::http::reply> rep) {
        if (!verb_allowed(*req)) {
            rep->_status = seastar::http::reply::status_type::method_not_allowed;
            rep->_content = "Incorrect method\n";
            return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(
                std::move(rep));
        }
        // `client_gone` is the abort source seastar's httpd aborts when this
        // connection's read side ends -- see patches/0001. Without it a client
        // that dies leaves this handler popping batches for ever and ACKING them
        // as delivered: a peer's FIN leaves the socket writable, so every write
        // still succeeds and the existing write-error path never fires. Measured
        // before the patch: of eight messages with one client killed, the
        // surviving client received four and the pipeline reported all eight
        // acked.
        seastar::abort_source* gone = req->client_gone;
        rep->write_body("txt", [this, gone](seastar::output_stream<char>&& out)
                                   -> seastar::future<> {
            auto os = std::move(out);
            std::exception_ptr err;
            try {
                for (;;) {
                    // Checked BEFORE taking a batch, so a dead handler stops
                    // competing rather than taking one more and losing it.
                    if (gone && gone->abort_requested()) break;
                    offered_ptr o;
                    try {
                        // Raced against the disconnect: pop alone would park for
                        // ever on an idle stream whose client had already gone.
                        o = co_await _queue.pop(gone);
                    } catch (...) {
                        break;                    // shutting down, or client gone
                    }
                    // A batch of one is its bytes; several are newline-joined
                    // AND newline-terminated, then a further newline separates
                    // batches -- so a multi-message batch ends in a blank line.
                    // Copied from the reference rather than invented: a reader
                    // splitting on blank lines depends on it.
                    std::string data;
                    if (o->payload.size() == 1) {
                        data = o->payload[0].as_bytes();
                    } else {
                        for (const auto& m : o->payload) { data += m.as_bytes(); data += '\n'; }
                    }
                    data += '\n';
                    std::exception_ptr wrote;
                    try {
                        co_await os.write(data);
                        co_await os.flush();
                    } catch (...) {
                        wrote = std::current_exception();
                    }
                    // The batch is resolved with whatever the write did, so a
                    // client that disconnected mid-stream nacks rather than
                    // silently losing the batch it was holding.
                    o->resolve(wrote);
                    if (wrote) { err = wrote; break; }
                }
            } catch (...) {
                err = std::current_exception();
            }
            // Closed in place: see the note on socket_output::close() for why a
            // network output_stream must not be moved before closing.
            try { co_await os.close(); }
            catch (const std::exception& e) { hlog.debug("stream close failed: {}", e.what()); }
            if (err) hlog.debug("http_server stream ended: {}", describe(err));
        });
        return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(
            std::move(rep));
    }

    server_output_config                         _cfg;
    rendezvous<offered_ptr>                      _queue;
    std::unique_ptr<seastar::httpd::http_server> _server;
    seastar::gate                                _handlers;
    seastar::optimized_optional<seastar::abort_source::subscription> _abort_sub;
};

seastar::future<std::unique_ptr<seastar::http::reply>>
served_endpoint::handle(const seastar::sstring&, std::unique_ptr<seastar::http::request> req,
                        std::unique_ptr<seastar::http::reply> rep) {
    return _out.handle(std::move(req), std::move(rep), _streaming);
}

// ---- http_client output -----------------------------------------------------

class http_client_output final : public output {
public:
    http_client_output(client_output_config cfg, rate_limit_ptr limit)
        : _cfg(std::move(cfg)), _url(parse_url(_cfg.url)), _verb(upper(_cfg.verb)),
          _limit(std::move(limit)) {
        _headers = compile_headers(_cfg.headers);
    }

    seastar::future<> connect(seastar::abort_source&) override {
        const auto addr = co_await resolve(_url.ep);
        // One client, several connections: seastar's http::client pools them,
        // and max_in_flight decides how many requests may be outstanding.
        _client = std::make_unique<seastar::http::client>(addr);
    }

    seastar::future<> write_batch(batch b, seastar::abort_source& as) override {
        if (b.empty()) co_return;
        if (_cfg.batch_as_lines) {
            std::string body;
            for (const auto& m : b) { body += m.as_bytes(); body += '\n'; }
            co_await send(std::move(body), b.front(), as);
            co_return;
        }
        for (const auto& m : b) co_await send(m.as_bytes(), m, as);
    }

    seastar::future<> close() override {
        if (!_client) co_return;
        auto c = std::move(_client);
        try { co_await c->close(); }
        catch (const std::exception& e) { hlog.warn("http_client close failed: {}", e.what()); }
    }

    connection_status status() const override {
        connection_status s;
        s.connected = _client != nullptr;
        s.label = "http_client";
        return s;
    }

    size_t max_in_flight() const override {
        return static_cast<size_t>(std::max(1, _cfg.max_in_flight));
    }

private:
    seastar::future<> send(const std::string& body, const message& m, seastar::abort_source& as) {
        auto delay = std::chrono::duration_cast<std::chrono::milliseconds>(
            _cfg.retry_period.ns);
        const auto cap = std::chrono::duration_cast<std::chrono::milliseconds>(
            _cfg.max_retry_backoff.ns);
        // `retries` counts RETRIES, so the first attempt is not one of them --
        // `retries: 0` means try once and nack, which is what the reference does.
        for (int attempt = 0; ; ++attempt) {
            // Before every attempt, as on the input side and for the same reason.
            if (!co_await rate_limit_wait(_limit, as))
                throw seastar::abort_requested_exception();
            std::exception_ptr err;
            try {
                co_await attempt_once(body, const_cast<message*>(&m), as);
                co_return;
            } catch (...) {
                err = std::current_exception();
            }
            if (attempt >= _cfg.retries || as.abort_requested())
                std::rethrow_exception(err);
            hlog.warn("http_client request failed (attempt {} of {}), retrying in {}ms: {}",
                      attempt + 1, _cfg.retries + 1, delay.count(), describe(err));
            try { co_await seastar::sleep_abortable(delay, as); }
            catch (const seastar::sleep_aborted&) { std::rethrow_exception(err); }
            delay = std::min(cap, delay * 2);
        }
    }

    seastar::future<> attempt_once(const std::string& body, message* m,
                                   seastar::abort_source& as) {
        auto req = seastar::http::request::make(_verb, _url.host_hdr, _url.path);
        // Interpolated against THIS message, which is what the reference does.
        std::string content_type = "application/octet-stream";
        for (const auto& [k, f] : _headers) {
            const std::string v = eval_header(f, m);
            req._headers[k] = v;
            // Content-Type is set by write_body; a `headers` entry for it wins,
            // because a config that names one meant it.
            if (k == "Content-Type") content_type = v;
        }
        req.write_body(content_type, seastar::sstring(body));

        // The status is checked HERE rather than through make_request's
        // `expected` argument, so the error names the code that came back.
        auto status_code = std::make_shared<int>(0);
        auto handler = [status_code](const seastar::http::reply& rep,
                                seastar::input_stream<char>&& in) -> seastar::future<> {
            *status_code = static_cast<int>(rep._status);
            // The body must be drained even when it is not wanted, or the
            // connection cannot be reused for the next request.
            auto stream = std::move(in);
            co_await seastar::util::read_entire_stream_contiguous(stream);
            co_await stream.close();
        };
        co_await seastar::with_timeout(
            seastar::lowres_clock::now() +
                std::chrono::duration_cast<std::chrono::milliseconds>(_cfg.timeout.ns),
            _client->make_request(std::move(req), std::move(handler), std::nullopt, &as));
        // 2xx succeeds, matching the reference's default `successful_on`.
        if (*status_code < 200 || *status_code > 299)
            throw std::runtime_error("http_client: " + _cfg.url + " answered " +
                                     std::to_string(*status_code));
    }

    client_output_config                     _cfg;
    header_fns                               _headers;
    parsed_url                               _url;
    std::string                              _verb;
    rate_limit_ptr                           _limit;
    std::unique_ptr<seastar::http::client>   _client;
};

} // namespace

input_ptr make_http_client_input(const client_input_config& c, rate_limit_ptr limit,
                                 const scanner_spec& stream_scanner) {
    input_ptr in = std::make_unique<http_client_input>(c, std::move(limit), stream_scanner);
    // Applied here rather than by each caller, so the interpreted and compiled
    // paths cannot disagree about whether a nack replays or drops.
    // `auto_replay_nacks` is applied by build_input_kind, for every input
    // alike; wrapping here as well would replay a nack twice.
    return in;
}

// Refused rather than ignored, per the project's rule for anything unbuilt: a
// config that lints clean and then serves nothing on the path it named is worse
// than one that will not start. See the TODO on server_input_config::ws_path.
[[noreturn]] void reject_ws_path() {
    throw std::runtime_error(
        "`ws_path` is not implemented by swordfish: websocket cannot share a listener with the HTTP endpoints here (seastar's httpd gives a handler no way to claim the connection, and its websocket server dispatches by subprotocol rather than by path). Use the `websocket` input or output, which are clients and are implemented, or leave `ws_path` empty.");
}

input_ptr make_http_server_input(const server_input_config& c, rate_limit_ptr limit) {
    if (!c.ws_path.empty()) reject_ws_path();
    return std::make_unique<http_server_input>(c, std::move(limit));
}
input_ptr make_websocket_input(const websocket_input_config& c) {
    input_ptr in = std::make_unique<websocket_input>(c);
    // `auto_replay_nacks` is applied by build_input_kind, for every input
    // alike; wrapping here as well would replay a nack twice.
    return in;
}

output_ptr make_websocket_output(const websocket_output_config& c) {
    return std::make_unique<websocket_output>(c);
}

output_ptr make_http_server_output(const server_output_config& c) {
    if (!c.ws_path.empty()) reject_ws_path();
    return std::make_unique<http_server_output>(c);
}

output_ptr make_http_client_output(const client_output_config& c, rate_limit_ptr limit) {
    return std::make_unique<http_client_output>(c, std::move(limit));
}
output_ptr make_sync_response_output() {
    return std::make_unique<sync_response_output>();
}

void register_components() {
    auto http_client_in = make_input_def<client_input_config>(
        "http_client",
        [](const client_input_config& c, const component_config& cc) {
            // Resolved inside the factory, which runs per shard: see
            // rate_limit_resolver().
            auto rl = rate_limit_resolver(cc);
            auto sc = scanner_for(cc, "stream.scanner");
            return [c, rl, sc] { return make_http_client_input(c, rl(), sc); };
        },
        [](const client_input_config& c, const component_config& cc) {
            return "sf::http::make_http_client_input(" +
                   cfg::emit_cpp(c, "sf::http::client_input_config") + ", " +
                   emit_rate_limit(cc) + ", " +
                   emit_scanner_spec(scanner_for(cc, "stream.scanner")) + ")";
        },
        "swordfish/components/http.hh");
    // The reference puts this input's scanner under `stream`, not at the top
    // level; a top-level one is refused rather than lifted and dropped.
    http_client_in.scanner_paths = {"stream.scanner"};
    register_input(std::move(http_client_in));

    register_input(make_input_def<websocket_input_config>(
        "websocket",
        [](const websocket_input_config& c, const component_config&) {
            return [c] { return make_websocket_input(c); };
        },
        [](const websocket_input_config& c, const component_config&) {
            return "sf::http::make_websocket_input(" +
                   cfg::emit_cpp(c, "sf::http::websocket_input_config") + ")";
        },
        "swordfish/components/http.hh"));

    register_output(make_output_def<websocket_output_config>(
        "websocket",
        [](const websocket_output_config& c, const component_config&) {
            return [c] { return make_websocket_output(c); };
        },
        [](const websocket_output_config& c, const component_config&) {
            return "sf::http::make_websocket_output(" +
                   cfg::emit_cpp(c, "sf::http::websocket_output_config") + ")";
        },
        "swordfish/components/http.hh"));

    register_input(make_input_def<server_input_config>(
        "http_server",
        [](const server_input_config& c, const component_config& cc) {
            // Resolved inside the factory, which runs per shard: see
            // rate_limit_resolver().
            auto rl = rate_limit_resolver(cc);
            return [c, rl] { return make_http_server_input(c, rl()); };
        },
        [](const server_input_config& c, const component_config& cc) {
            return "sf::http::make_http_server_input(" +
                   cfg::emit_cpp(c, "sf::http::server_input_config") + ", " +
                   emit_rate_limit(cc) + ")";
        },
        "swordfish/components/http.hh"));

    register_output(make_output_def<server_output_config>(
        "http_server",
        [](const server_output_config& c, const component_config&) {
            return [c] { return make_http_server_output(c); };
        },
        [](const server_output_config& c, const component_config&) {
            return "sf::http::make_http_server_output(" +
                   cfg::emit_cpp(c, "sf::http::server_output_config") + ")";
        },
        "swordfish/components/http.hh"));

    // `http_client` is one of the three outputs the reference lets carry a
    // batching policy.
    auto http_client_out = make_output_def<client_output_config>(
        "http_client",
        [](const client_output_config& c, const component_config& cc) {
            // Resolved inside the factory, which runs per shard: see
            // rate_limit_resolver().
            auto rl = rate_limit_resolver(cc);
            return [c, rl] { return make_http_client_output(c, rl()); };
        },
        [](const client_output_config& c, const component_config& cc) {
            return "sf::http::make_http_client_output(" +
                   cfg::emit_cpp(c, "sf::http::client_output_config") + ", " +
                   emit_rate_limit(cc) + ")";
        },
        "swordfish/components/http.hh");
    http_client_out.batching = true;
    register_output(std::move(http_client_out));

    // `sync_response` has no configuration at all, so it needs a config type
    // only to satisfy the registry. An empty spec_of still rejects any field
    // written under it, which is the point.
    register_output(make_output_def<sync_response_output_config>(
        "sync_response",
        [](const sync_response_output_config&, const component_config&) {
            return [] { return make_sync_response_output(); };
        },
        [](const sync_response_output_config&, const component_config&) {
            return std::string("sf::http::make_sync_response_output()");
        },
        "swordfish/components/http.hh"));
}

} // namespace sf::http
