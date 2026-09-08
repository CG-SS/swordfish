// Request and response framing: the envelope around the generated message
// bodies in src/kafka/generated/protocol.hh.
//
// Every exchange on the wire is
//
//     int32 length
//     request header (v1 classic, or v2 flexible with tagged fields)
//     request body
//
// and the response mirrors it with a header carrying the correlation id back.
// Which header version applies follows from whether the API VERSION is flexible,
// with one exception noted at response_header_version().
#pragma once

#include "swordfish/kafka/wire.hh"

#include <optional>
#include <string>

namespace sf::kafka {

// Kafka's api keys, for the APIs we speak. The generated structs carry these
// too; this enum exists so a switch over them is readable.
enum class api : int16_t {
    produce            = 0,
    fetch              = 1,
    list_offsets       = 2,
    metadata           = 3,
    offset_commit      = 8,
    offset_fetch       = 9,
    find_coordinator   = 10,
    join_group         = 11,
    heartbeat          = 12,
    leave_group        = 13,
    sync_group         = 14,
    describe_groups    = 15,
    sasl_handshake     = 17,
    api_versions       = 18,
    sasl_authenticate  = 36,
};

// A request header is v2 when the api version uses flexible encoding, v1
// otherwise. v0 (no client id) is never used by a client.
constexpr int16_t request_header_version(bool flexible) { return flexible ? 2 : 1; }

// The response header is v1 when flexible -- EXCEPT for ApiVersions, which is
// always v0. That call is how the client discovers which versions the broker
// supports, so it cannot already know whether the response would be flexible;
// the protocol pins it to the old header to break the circularity.
constexpr int16_t response_header_version(api key, bool flexible) {
    if (key == api::api_versions) return 0;
    return flexible ? 1 : 0;
}

struct request_header {
    int16_t                    api_key = 0;
    int16_t                    api_version = 0;
    int32_t                    correlation_id = 0;
    std::optional<std::string> client_id;

    void encode(writer& w, int16_t header_version) const {
        w.i16(api_key);
        w.i16(api_version);
        w.i32(correlation_id);
        if (header_version >= 1) {
            // The client id is a classic nullable string even in a v2 header:
            // the header's own encoding does not switch to compact strings.
            w.nullable_string(client_id, false);
        }
        if (header_version >= 2) w.empty_tags();
    }
};

struct response_header {
    int32_t correlation_id = 0;

    void decode(reader& r, int16_t header_version) {
        correlation_id = r.i32();
        if (header_version >= 1) r.skip_tags();
    }
};

// Frame a complete request: length prefix, header, body. `T` is one of the
// generated message structs.
template <class T>
std::string encode_request(const T& body, int16_t version, int32_t correlation_id,
                           const std::optional<std::string>& client_id) {
    writer w;
    w.with_i32_length([&] {
        const request_header h{T::api_key, version, correlation_id, client_id};
        h.encode(w, request_header_version(T::flexible(version)));
        body.encode(w, version);
    });
    return w.take();
}

// Decode a response body that has already had its length prefix stripped. The
// correlation id is returned so the caller can match it to a pending request.
template <class T>
int32_t decode_response(std::string_view frame, int16_t version, T& out) {
    reader r(frame);
    response_header h;
    h.decode(r, response_header_version(static_cast<api>(T::api_key),
                                        T::flexible(version)));
    out.decode(r, version);
    return h.correlation_id;
}

} // namespace sf::kafka
