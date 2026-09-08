#include "swordfish/runtime/observe.hh"

#include <seastar/core/coroutine.hh>
#include <seastar/http/function_handlers.hh>
#include <seastar/http/httpd.hh>
#include <seastar/net/inet_address.hh>

#include <algorithm>
#include <seastar/util/log.hh>

#include <stdexcept>
#include <utility>

// Overridden by the build; the fallbacks keep this compilable on its own.
#ifndef SWORDFISH_VERSION
#define SWORDFISH_VERSION "0.0.0-dev"
#endif
#ifndef SWORDFISH_BUILT
#define SWORDFISH_BUILT "unknown"
#endif

namespace sf {

static seastar::logger olog("sf.observe");

namespace {

// Only the metrics we genuinely measure are emitted. Redpanda Connect exposes
// nineteen; several of them -- the connection_up/lost/failed family, the
// latency summaries, the per-processor counters -- need accounting this runtime
// does not do yet. Emitting them as a constant zero would be worse than leaving
// them out: a dashboard would show a flat line and read it as "no errors"
// rather than "not measured".
struct metric {
    const char* name;
    const char* type;
    uint64_t    value;
};

// One component's series. The families follow the reference's naming, which is
// why each stage gets a different set: an input is `received`, an output is
// `sent`, a processor is both.
std::vector<metric> metrics_of_component(const component_stats& cs) {
    switch (cs.id.kind) {
    case comp_kind::input:
        return {
            {"input_received",           "counter", cs.c.received},
            {"input_connection_up",      "gauge",   cs.c.connection_up},
        };
    case comp_kind::processor:
        return {
            {"processor_received",       "counter", cs.c.received},
            {"processor_batch_received", "counter", cs.c.batch_received},
            {"processor_sent",           "counter", cs.c.sent},
            {"processor_batch_sent",     "counter", cs.c.batch_sent},
            {"processor_error",          "counter", cs.c.errors},
        };
    case comp_kind::output:
        return {
            {"output_sent",              "counter", cs.c.sent},
            {"output_batch_sent",        "counter", cs.c.batch_sent},
            {"output_error",             "counter", cs.c.errors},
            {"output_connection_up",     "gauge",   cs.c.connection_up},
        };
    }
    return {};
}

// The whole-stream figures, for a pipeline whose components are not identified
// -- a hand-built spec, or a binary compiled before component identities
// existed. Everything here carries path="root", which is what EVERY series used
// to carry: a dashboard grouping by `path` saw the input, the processors and
// the output collapsed into one line.
std::vector<metric> metrics_of(const observed& o) {
    return {
        {"input_received",           "counter", o.messages_in},
        {"output_sent",              "counter", o.messages_out},
        {"output_batch_sent",        "counter", o.batches_out},
        {"output_error",             "counter", o.nacks},
        {"processor_batch_received", "counter", o.batches_in},
        {"processor_error",          "counter", o.proc_errors},
        // Swordfish-specific, no Redpanda Connect equivalent. Prefixed so it is
        // obvious which names a Benthos dashboard will not know about. Reported
        // once for the stream rather than per component, because that is what
        // they measure: an ack belongs to the source, not to a stage.
        {"swordfish_acked",          "counter", o.acks},
        {"swordfish_filtered",       "counter", o.filtered},
    };
}

std::string json_escape(const std::string& s) {
    std::string out;
    for (char c : s) {
        switch (c) {
        case '"':  out += "\\\""; break;
        case '\\': out += "\\\\"; break;
        case '\n': out += "\\n";  break;
        default:   out += c;
        }
    }
    return out;
}

} // namespace

// Accepts the forms the reference accepts, which are more than "host:port":
//
//   ":4195"        -- an empty host means every interface, as Go's net.Listen
//                     reads it. This threw "http.address has no host".
//   "[::1]:4195"   -- an IPv6 literal in brackets. The brackets were handed to
//                     seastar::net::inet_address unstripped, which threw an
//                     exception whose entire text was the address: `sfconfig
//                     lint` passed the config, `swordfish build` produced a
//                     binary, and the SHIPPED binary then died with
//                     `pipeline failed: [::1]`.
std::pair<std::string, uint16_t> parse_host_port(const std::string& addr) {
    // An IPv6 literal's own colons must not be mistaken for the port separator,
    // so the bracketed form is split on the bracket.
    if (!addr.empty() && addr.front() == '[') {
        const size_t close = addr.find(']');
        if (close == std::string::npos)
            throw std::runtime_error("http.address has no closing ']': \"" + addr + "\"");
        if (close + 1 >= addr.size() || addr[close + 1] != ':')
            throw std::runtime_error("http.address needs a port after the address: \"" +
                                     addr + "\"");
        const std::string host6 = addr.substr(1, close - 1);
        const std::string port6 = addr.substr(close + 2);
        if (port6.empty() || port6.find_first_not_of("0123456789") != std::string::npos)
            throw std::runtime_error("http.address port is not a number: \"" + port6 + "\"");
        const unsigned long n6 = std::stoul(port6);
        if (n6 == 0 || n6 > 65535)
            throw std::runtime_error("http.address port out of range 1-65535: \"" + port6 + "\"");
        return {host6, static_cast<uint16_t>(n6)};
    }
    const size_t colon = addr.rfind(':');
    if (colon == std::string::npos || colon + 1 == addr.size())
        throw std::runtime_error("http.address must be host:port, got \"" + addr + "\"");
    std::string host = addr.substr(0, colon);
    const std::string port = addr.substr(colon + 1);
    // An empty host is every interface, which is what ":4195" means to Go's
    // net.Listen and to the reference.
    if (host.empty()) host = "0.0.0.0";
    if (port.find_first_not_of("0123456789") != std::string::npos)
        throw std::runtime_error("http.address port is not a number: \"" + port + "\"");
    // Parsed as unsigned long so 99999 is out of RANGE rather than wrapping.
    const unsigned long n = std::stoul(port);
    if (n == 0 || n > 65535)
        throw std::runtime_error("http.address port out of range 1-65535: \"" + port + "\"");
    return {host, static_cast<uint16_t>(n)};
}


// A label value in Prometheus text: backslash, quote and newline escape.
std::string label_escape(const std::string& s) {
    std::string out;
    for (const char c : s) {
        if (c == '\\')      out += "\\\\";
        else if (c == '"')  out += "\\\"";
        else if (c == '\n') out += "\\n";
        else                out += c;
    }
    return out;
}

std::string render_prometheus(const observed& o) {
    std::string out;
    // HELP and TYPE are emitted ONCE per family, before its first series.
    // Prometheus rejects a repeated TYPE for the same metric name, and with a
    // series per component the same family now appears many times.
    std::vector<std::string> declared;
    const auto declare = [&](const metric& m) {
        if (std::find(declared.begin(), declared.end(), m.name) != declared.end()) return;
        declared.emplace_back(m.name);
        out += "# HELP ";  out += m.name; out += " Swordfish ";
        out += m.type;     out += " metric\n";
        out += "# TYPE ";  out += m.name; out += " "; out += m.type; out += "\n";
    };
    const auto series = [&](const metric& m, const std::string& path, const std::string& label) {
        declare(m);
        out += m.name;
        out += "{label=\"" + label_escape(label) + "\",path=\"" + label_escape(path) + "\"} ";
        out += std::to_string(m.value);
        out += "\n";
    };

    // Per component when the stream identified them, which is the shape a
    // Redpanda Connect dashboard expects: one series per component, keyed on
    // `path` and `label`.
    for (const auto& cs : o.components)
        for (const auto& m : metrics_of_component(cs)) series(m, cs.id.path, cs.id.label);

    // The stream-wide figures. When components were identified this is only the
    // swordfish_* pair, because the rest would double-count what is already
    // reported per component.
    for (const auto& m : metrics_of(o)) {
        const std::string n = m.name;
        if (!o.components.empty() && n.rfind("swordfish_", 0) != 0) continue;
        series(m, "root", "");
    }
    return out;
}

struct observe_server::impl {
    http_spec                                    spec;
    std::function<seastar::future<observed>()>   gather;
    std::function<void(seastar::httpd::routes&)> extra;
    seastar::httpd::http_server_control          server;
    bool                                         started = false;
};

observe_server::observe_server(http_spec spec,
                               std::function<seastar::future<observed>()> gather,
                               std::function<void(seastar::httpd::routes&)> extra)
    : _i(std::make_unique<impl>()) {
    _i->spec = std::move(spec);
    _i->gather = std::move(gather);
    _i->extra = std::move(extra);
}

observe_server::~observe_server() = default;

seastar::future<> observe_server::start() {
    if (!_i->spec.enabled) co_return;

    // Validated rather than cast. A bare static_cast<uint16_t> of stoi() took
    // `:99999` down to port 34463 and then LOGGED "endpoints on ...:99999" --
    // an operator probing the port they configured would find nothing there and
    // a log line insisting it was listening. `:-1` did the same via 65535, and
    // a non-numeric port surfaced as the diagnostic "stoi".
    const auto [host, port] = parse_host_port(_i->spec.address);

    co_await _i->server.start("swordfish-http");
    _i->started = true;

    // Content streaming, but ONLY when a caller has added routes that read a
    // request body -- `swordfish streams` POSTs a config to /streams/{id}.
    // With it off, seastar reads the whole body into the request's deprecated
    // `content` field and leaves `content_stream` already consumed, so a
    // handler reading the stream sees nothing: the streams API answered "the
    // request body must be a stream config" to every POST that had one.
    //
    // Left OFF for the observability-only case, which reads no bodies, so that
    // path behaves exactly as it did. A handler that ignores a streamed body is
    // safe either way -- seastar closes the connection rather than letting the
    // next request read the leftovers.
    if (_i->extra) {
        co_await _i->server.server().invoke_on_all(
            [](seastar::httpd::http_server& s) { s.set_content_streaming(true); });
    }

    auto* i = _i.get();
    co_await _i->server.set_routes([i](seastar::httpd::routes& r) {
        namespace hh = seastar::httpd;
        using namespace seastar;

        auto text = [](sstring body) {
            return [body = std::move(body)](std::unique_ptr<http::request>,
                                            std::unique_ptr<http::reply> rep) {
                rep->write_body("txt", body);
                return make_ready_future<std::unique_ptr<http::reply>>(std::move(rep));
            };
        };
        auto ready = [i](std::unique_ptr<http::request>,
                         std::unique_ptr<http::reply> rep) {
            return i->gather().then([rep = std::move(rep)](const observed& o) mutable {
                // Shape copied from the reference: a statuses array of
                // {label, path, connected}. A probe that greps for
                // `"connected":false` keeps working.
                // Each side reported SEPARATELY, as the reference does: one
                // flag rendered into both slots said the output was down when
                // only the input was, which sends an operator to the wrong end
                // of the pipeline.
                const char* ci = o.not_ready_in  == 0 ? "true" : "false";
                const char* co = o.not_ready_out == 0 ? "true" : "false";
                sstring body = sstring("{\"statuses\":[{\"label\":\"\",\"path\":\"input\","
                                       "\"connected\":") + ci + "},"
                               "{\"label\":\"\",\"path\":\"output\",\"connected\":" + co + "}]}\n";
                if (!o.ready) rep->set_status(http::reply::status_type::service_unavailable);
                rep->write_body("json", body);
                return make_ready_future<std::unique_ptr<http::reply>>(std::move(rep));
            });
        };
        auto metrics = [i](std::unique_ptr<http::request>,
                           std::unique_ptr<http::reply> rep) {
            return i->gather().then([rep = std::move(rep)](const observed& o) mutable {
                rep->write_body("txt", sstring(render_prometheus(o)));
                return make_ready_future<std::unique_ptr<http::reply>>(std::move(rep));
            });
        };

        const std::string ver = std::string("{\"version\":\"") +
            json_escape(SWORDFISH_VERSION) + "\", \"built\":\"" +
            json_escape(SWORDFISH_BUILT) + "\"}\n";

        // Mounted at the root AND under root_path, matching the reference.
        std::vector<std::string> prefixes{""};
        if (!i->spec.root_path.empty() && i->spec.root_path != "/")
            prefixes.push_back(i->spec.root_path);
        for (const auto& p : prefixes) {
            r.add(hh::operation_type::GET, hh::url(p + "/ping"),
                  new hh::function_handler(text("pong"), "txt"));
            r.add(hh::operation_type::GET, hh::url(p + "/version"),
                  new hh::function_handler(text(sstring(ver)), "json"));
            r.add(hh::operation_type::GET, hh::url(p + "/ready"),
                  new hh::function_handler(ready, "json"));
            // /stats is the same payload as /metrics in the reference, so it is
            // the same handler here rather than a second format to keep in step.
            r.add(hh::operation_type::GET, hh::url(p + "/metrics"),
                  new hh::function_handler(metrics, "txt"));
            r.add(hh::operation_type::GET, hh::url(p + "/stats"),
                  new hh::function_handler(metrics, "txt"));
        }
        // Last, so a caller's routes are added to a fully-formed table and a
        // caller could in principle replace one of the above.
        if (i->extra) i->extra(r);
    });

    co_await _i->server.listen(seastar::socket_address(seastar::net::inet_address(host), port));
    olog.info("observability endpoints on {} (also under {})", _i->spec.address,
              _i->spec.root_path);
}

seastar::future<> observe_server::stop() {
    if (!_i->started) co_return;
    _i->started = false;
    co_await _i->server.stop();
}

} // namespace sf
