// The `http_server` input and the `http_client` output, plus `sync_response`.
//
// The configs and their spec_of tables live in the HEADER for the same reason
// Kafka's do: generated code has to name them. `swordfish build` emits a
// `sf::http::server_input_config{...}` literal and hands it to the same factory
// the interpreter calls, so both modes meet at one line.
//
// Field names and defaults are the reference's, taken from
// connect-main/docs/modules/components/pages/{inputs,outputs}/http_*.adoc, which
// is public documentation rather than enterprise-licensed source. Fields not
// listed here -- `tls`, `oauth`, `oauth2`, `jwt`, `cors`,
// `multipart` -- are NOT silently ignored: the config framework
// rejects an unknown field by name, so a config asking for TLS fails to lint
// rather than sending in the clear.
#pragma once

#include "swordfish/config/spec.hh"
#include "swordfish/runtime/component.hh"
#include "swordfish/runtime/rate_limit.hh"
#include "swordfish/runtime/scanner.hh"

#include <chrono>
#include <map>
#include <string>
#include <vector>

namespace sf::http {

using namespace std::chrono_literals;

struct sync_response_config {
    int                                status = 200;
    // The reference's default, and it matters: it is what a single-message
    // response is labelled with.
    // INTERPOLATED, as the reference's are: "This field supports interpolation
    // functions". A per-message correlation id, a computed Content-Type or a
    // routing header is the ordinary use, and sending the literal `${! ... }`
    // text made a config that lints clean in both implementations misroute or be
    // rejected by the receiver, silently, with no error on either side.
    std::map<std::string, cfg::interpolation> headers{
        {"Content-Type", cfg::interpolation{"application/octet-stream"}}};
    bool operator==(const sync_response_config&) const = default;
};

// `websocket`, both directions, as CLIENTS -- which is what the reference's
// `websocket` input and output are. Its server side lives on `http_server`'s
// `ws_path`, and that is not implemented here; see the note on `ws_path` below.
// `connection`, a nested object in the reference. A struct of its own rather
// than a dotted field name, because a spec_of field name is a literal YAML key
// -- the same shape `stream` and `sync_response` take.
struct websocket_connection_config {
    // "An optional limit to the number of consecutive retry attempts that will
    // be made before abandoning the connection altogether and gracefully
    // terminating the input... If set to zero connections will never be
    // reattempted upon a failure. If set below zero this field is ignored
    // (effectively unset)."
    int64_t max_retries = -1;
    bool operator==(const websocket_connection_config&) const = default;
};

struct websocket_input_config {
    std::string   url;
    // Sent once on connect. The reference uses it to subscribe to a feed.
    std::string   open_message;
    std::string   open_message_type = "binary";
    // 0 disables the limit, as in the reference.
    int64_t       max_message_size = 0;
    // `connection.max_retries`, the reference's field: "an optional limit to the
    // number of consecutive retry attempts that will be made before abandoning
    // the connection altogether and gracefully terminating the input... If set
    // to zero connections will never be reattempted upon a failure. If set below
    // zero this field is ignored (effectively unset)."
    //
    // It replaces a swordfish-only `reconnect_period`, which the reference lints
    // as unrecognised -- the same trap `poll_period` was. The WAIT between
    // attempts is not configurable there either, so it is a fixed second here.
    websocket_connection_config connection;
    bool operator==(const websocket_input_config&) const = default;
};

struct websocket_output_config {
    std::string   url;
    bool operator==(const websocket_output_config&) const = default;
};

struct server_input_config {
    // Required, unlike the reference, where an empty address means "share the
    // service-wide HTTP server on :4195". Swordfish compiles to a binary a user
    // redistributes and so opens no port that was not asked for -- the same
    // decision the `http` block records.
    std::string              address;
    std::string              path = "/post";
    std::vector<std::string> allowed_verbs{"POST"};
    // TODO(websocket-on-one-listener): serving websocket on the SAME address as
    // the HTTP endpoints is not implemented, so a non-empty `ws_path` is refused
    // by name rather than ignored. It is not a small gap to close: seastar's
    // httpd::connection owns its socket privately and offers a handler no way to
    // take it over, and seastar's websocket::server is a separate listener that
    // dispatches by SUBPROTOCOL rather than by path -- so it can neither share
    // the port nor honour a path. The options are (a) a second listener on its
    // own port, which needs a swordfish-only `ws_address` and would make a
    // reference config need editing to run here, or (b) a hook in seastar's
    // http_server letting a handler claim the connection, after which the
    // swordfish side is small: websocket::basic_connection takes an established
    // socket, its send_data() frames a whole buffer, and sha1_base64() is
    // exported for the handshake. (b) is the one to revisit; it was left until
    // the client components existed and it is known whether anything wants
    // `ws_path` at all. Default empty means "off", so only an explicit value is
    // an error.
    std::string              ws_path;
    // How long a request waits for the pipeline before it is answered with 503.
    // Without a bound, a stalled output would hold every connection open.
    cfg::duration            timeout{5000ms};
    // The label of a `rate_limit_resources` entry. On this side the limit does
    // not delay: a request that finds no permit is answered 429 with a
    // Retry-After, as in the reference. Holding a client's connection open to
    // wait would consume the very resource the limit protects.
    std::string              rate_limit;
    sync_response_config     sync_response;
    bool operator==(const server_input_config&) const = default;
};

struct server_output_config {
    // Required for the same reason the input's is: swordfish opens no port a
    // config did not ask for.
    std::string              address;
    // Three endpoints on one listener, as in the reference. `path` answers one
    // GET with one batch; `stream_path` holds the connection open and writes
    // each batch as it arrives.
    std::string              path = "/get";
    std::string              stream_path = "/get/stream";
    std::vector<std::string> allowed_verbs{"GET"};
    // See the TODO on server_input_config::ws_path.
    std::string              ws_path;
    // How long a discrete GET waits for a batch before it is answered with 408.
    cfg::duration            timeout{5000ms};
    bool operator==(const server_output_config&) const = default;
};

struct client_output_config {
    std::string                        url;
    std::string                        verb = "POST";
    // INTERPOLATED, as the reference's are: "This field supports interpolation
    // functions". A per-message correlation id, a computed Content-Type or a
    // routing header is the ordinary use, and sending the literal `${! ... }`
    // text made a config that lints clean in both implementations misroute or be
    // rejected by the receiver, silently, with no error on either side.
    std::map<std::string, cfg::interpolation> headers;
    cfg::duration                      timeout{5000ms};
    // The reference retries a failed request `retries` times with a backoff
    // between `retry_period` and `max_retry_backoff`, then nacks.
    int                                retries = 3;
    cfg::duration                      retry_period{1000ms};
    cfg::duration                      max_retry_backoff{300000ms};
    // Send the whole batch as one request body, newline-delimited, rather than
    // one request per message. The reference spells the general form of this
    // `batch_as_multipart`; this is the simpler, more common shape and is named
    // differently so it cannot be mistaken for multipart/mixed.
    bool                               batch_as_lines = false;
    // The label of a `rate_limit_resources` entry; empty means unthrottled.
    std::string                        rate_limit;
    int                                max_in_flight = 64;
    bool operator==(const client_output_config&) const = default;
};

struct client_stream_config {
    bool        enabled   = false;
    bool        reconnect = true;
    bool operator==(const client_stream_config&) const = default;
};

struct client_input_config {
    std::string                        url;
    std::string                        verb = "GET";
    // INTERPOLATED, as the reference's are: "This field supports interpolation
    // functions". A per-message correlation id, a computed Content-Type or a
    // routing header is the ordinary use, and sending the literal `${! ... }`
    // text made a config that lints clean in both implementations misroute or be
    // rejected by the receiver, silently, with no error on either side.
    std::map<std::string, cfg::interpolation> headers;
    cfg::duration                      timeout{5000ms};
    int                                retries = 3;
    cfg::duration                      retry_period{1000ms};
    cfg::duration                      max_retry_backoff{300000ms};
    // A body to send with each request. Interpolated, as it is in the
    // reference, so a poll can carry a cursor or a timestamp.
    cfg::interpolation                 payload;
    bool                               drop_empty_bodies = true;
    client_stream_config               stream;
    // The label of a `rate_limit_resources` entry. Empty means unthrottled,
    // which for a polling input means as fast as the target answers -- the
    // reference's default too, and the reason its own documentation example for
    // this input configures a rate limit.
    std::string                        rate_limit;
    bool operator==(const client_input_config&) const = default;
};

// `sync_response` takes no configuration. It still needs a type, because the
// registry derives parsing, documentation and emission from one; an empty
// spec_of also means any field written under it is rejected by name rather
// than ignored.
struct sync_response_output_config {
    bool operator==(const sync_response_output_config&) const = default;
};

input_ptr  make_websocket_input(const websocket_input_config& c);
output_ptr make_websocket_output(const websocket_output_config& c);
// The rate limit is a separate argument rather than a config field because a
// `rate_limit:` names a LABEL whose count and interval live in a document-level
// block. Two components naming one label must share the permits, which is what
// sf::shared_rate_limit() gives them; passing the resolved object keeps the
// lookup in one place for both the interpreter and the generated code.
// `stream_scanner` is the scanner from `stream.scanner`, lifted out of the body
// during parsing for the reason make_socket_input's is.
input_ptr  make_http_client_input(const client_input_config& c, rate_limit_ptr limit,
                                  const scanner_spec& stream_scanner);
output_ptr make_http_server_output(const server_output_config& c);
input_ptr  make_http_server_input(const server_input_config& c, rate_limit_ptr limit);
output_ptr make_http_client_output(const client_output_config& c, rate_limit_ptr limit);
output_ptr make_sync_response_output();

// Registration is EXPLICIT, called from register_builtin_io(). The reference
// from there is also what keeps the linker from dropping this translation unit
// out of a static library -- the trap that silently emptied the processor
// registry once.
void register_components();

} // namespace sf::http

namespace sf::cfg {

template <> struct spec_of<sf::http::sync_response_output_config> {
    static constexpr std::string_view cpp_type = "sf::http::sync_response_output_config";
    static constexpr auto value = object();
};

template <> struct spec_of<sf::http::sync_response_config> {
    static constexpr std::string_view cpp_type = "sf::http::sync_response_config";
    static constexpr auto value = object(
        field("status", &sf::http::sync_response_config::status)
            .describe("The status code returned with synchronous responses."),
        field("headers", &sf::http::sync_response_config::headers)
            .describe("Headers to set on a synchronous response.").advanced_()
    );
};

template <> struct spec_of<sf::http::client_stream_config> {
    static constexpr std::string_view cpp_type = "sf::http::client_stream_config";
    static constexpr auto value = object(
        field("enabled", &sf::http::client_stream_config::enabled)
            .describe("Hold the response open and read messages from it as they arrive."),
        field("reconnect", &sf::http::client_stream_config::reconnect)
            .describe("Re-establish the connection when the stream ends.")
    );
};

template <> struct spec_of<sf::http::client_input_config> {
    static constexpr std::string_view cpp_type = "sf::http::client_input_config";
    static constexpr auto value = object(
        field("url", &sf::http::client_input_config::url)
            .describe("The URL to fetch from.").require(),
        field("verb", &sf::http::client_input_config::verb)
            .describe("The HTTP method to use."),
        field("headers", &sf::http::client_input_config::headers)
            .describe("Headers to add to every request."),
        field("timeout", &sf::http::client_input_config::timeout)
            .describe("How long to wait for a response before failing."),
        field("retries", &sf::http::client_input_config::retries)
            .describe("How many times a failed request is retried before it is "
                      "reported."),
        field("retry_period", &sf::http::client_input_config::retry_period)
            .describe("The base wait between retries; it doubles each attempt.")
            .advanced_(),
        field("max_retry_backoff", &sf::http::client_input_config::max_retry_backoff)
            .describe("The ceiling on that wait.").advanced_(),
        field("payload", &sf::http::client_input_config::payload)
            .describe("An optional body to send with each request."),
        field("drop_empty_bodies", &sf::http::client_input_config::drop_empty_bodies)
            .describe("Whether an empty response body is skipped rather than "
                      "becoming an empty message."),
        field("stream", &sf::http::client_input_config::stream)
            .describe("Streaming mode: one long-lived request, read as it arrives."),
        field("rate_limit", &sf::http::client_input_config::rate_limit)
            .describe("An optional rate limit to throttle requests by.")
    );
};

template <> struct spec_of<sf::http::websocket_connection_config> {
    static constexpr std::string_view cpp_type = "sf::http::websocket_connection_config";
    static constexpr auto value = object(
        field("max_retries", &sf::http::websocket_connection_config::max_retries)
            .describe("How many consecutive reconnects to attempt before the input "
                      "terminates. 0 never reconnects; below zero is unlimited.")
    );
};

template <> struct spec_of<sf::http::websocket_input_config> {
    static constexpr std::string_view cpp_type = "sf::http::websocket_input_config";
    static constexpr std::string_view msg_types[] = {"binary", "text"};
    static constexpr auto value = object(
        field("url", &sf::http::websocket_input_config::url)
            .describe("The websocket URL to connect to, ws://host:port/path.").require(),
        field("open_message", &sf::http::websocket_input_config::open_message)
            .describe("An optional message sent to the server once connected."),
        field("open_message_type", &sf::http::websocket_input_config::open_message_type)
            .describe("Whether `open_message` is sent as a binary or a text frame.")
            .options(msg_types),
        field("max_message_size", &sf::http::websocket_input_config::max_message_size)
            .describe("The largest message accepted, in bytes. 0 disables the limit."),
        field("connection", &sf::http::websocket_input_config::connection)
            .describe("How reconnection is attempted.").advanced_()
    );
};

template <> struct spec_of<sf::http::websocket_output_config> {
    static constexpr std::string_view cpp_type = "sf::http::websocket_output_config";
    static constexpr auto value = object(
        field("url", &sf::http::websocket_output_config::url)
            .describe("The websocket URL to connect to, ws://host:port/path.").require()
    );
};

template <> struct spec_of<sf::http::server_input_config> {
    static constexpr std::string_view cpp_type = "sf::http::server_input_config";
    static constexpr auto value = object(
        field("address", &sf::http::server_input_config::address)
            .describe("The address to bind the server to. Required: swordfish "
                      "opens no port that a config did not ask for.").require(),
        field("path", &sf::http::server_input_config::path)
            .describe("The path from which messages are accepted."),
        field("allowed_verbs", &sf::http::server_input_config::allowed_verbs)
            .describe("The HTTP methods this endpoint accepts."),
        field("ws_path", &sf::http::server_input_config::ws_path)
            .describe("Not implemented: websocket cannot share this listener. A "
                      "non-empty value is refused rather than ignored.").advanced_(),
        field("timeout", &sf::http::server_input_config::timeout)
            .describe("How long a request waits for the pipeline before it is "
                      "answered with 503."),
        field("rate_limit", &sf::http::server_input_config::rate_limit)
            .describe("An optional rate limit to throttle requests by. A request "
                      "over the limit is answered 429 rather than delayed."),
        field("sync_response", &sf::http::server_input_config::sync_response)
            .describe("How a response assembled by a `sync_response` output is "
                      "returned to the caller.")
    );
};

template <> struct spec_of<sf::http::server_output_config> {
    static constexpr std::string_view cpp_type = "sf::http::server_output_config";
    static constexpr auto value = object(
        field("address", &sf::http::server_output_config::address)
            .describe("The address to bind the server to. Required: swordfish "
                      "opens no port that a config did not ask for.").require(),
        field("path", &sf::http::server_output_config::path)
            .describe("The path from which one batch is served per request."),
        field("stream_path", &sf::http::server_output_config::stream_path)
            .describe("The path from which a continuous stream of batches is served."),
        field("allowed_verbs", &sf::http::server_output_config::allowed_verbs)
            .describe("The HTTP methods these endpoints accept."),
        field("ws_path", &sf::http::server_output_config::ws_path)
            .describe("Not implemented: websocket cannot share this listener. A "
                      "non-empty value is refused rather than ignored.").advanced_(),
        field("timeout", &sf::http::server_output_config::timeout)
            .describe("How long a request on `path` waits for a batch before it "
                      "is answered with 408.")
    );
};

template <> struct spec_of<sf::http::client_output_config> {
    static constexpr std::string_view cpp_type = "sf::http::client_output_config";
    static constexpr auto value = object(
        field("url", &sf::http::client_output_config::url)
            .describe("The URL to send messages to.").require(),
        field("verb", &sf::http::client_output_config::verb)
            .describe("The HTTP method to use."),
        field("headers", &sf::http::client_output_config::headers)
            .describe("Headers to add to every request."),
        field("timeout", &sf::http::client_output_config::timeout)
            .describe("How long to wait for a response before failing."),
        field("retries", &sf::http::client_output_config::retries)
            .describe("How many times a failed request is retried before the "
                      "batch is nacked."),
        field("retry_period", &sf::http::client_output_config::retry_period)
            .describe("The base wait between retries; it doubles each attempt.")
            .advanced_(),
        field("max_retry_backoff", &sf::http::client_output_config::max_retry_backoff)
            .describe("The ceiling on that wait.").advanced_(),
        field("batch_as_lines", &sf::http::client_output_config::batch_as_lines)
            .describe("Send a whole batch as one newline-delimited body rather "
                      "than one request per message."),
        field("rate_limit", &sf::http::client_output_config::rate_limit)
            .describe("An optional rate limit to throttle requests by."),
        field("max_in_flight", &sf::http::client_output_config::max_in_flight)
            .describe("How many requests may be outstanding at once.").advanced_()
    );
};

} // namespace sf::cfg
