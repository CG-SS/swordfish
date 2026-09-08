// The operational surface: /ping, /ready, /version, /metrics, /stats.
//
// Endpoint paths, payload shapes and metric names are taken from what
// redpanda-connect 4.107.2 actually serves, captured by running it and probing,
// not from documentation. Dashboards and readiness probes written against
// Redpanda Connect are meant to work here unchanged, which is the whole reason
// to match the names rather than invent better ones.
#pragma once

#include <seastar/core/future.hh>
#include <seastar/http/httpd.hh>

#include "swordfish/runtime/component_stats.hh"

#include <cstdint>
#include <functional>
#include <memory>
#include <string>

namespace sf {

// The `http` block. Defaults match Redpanda Connect's, including the port.
struct http_spec {
    bool        enabled   = false;
    std::string address   = "0.0.0.0:4195";
    // Every endpoint is served BOTH here and at the root, because the reference
    // does: `/benthos/ready` and `/ready` both answer.
    std::string root_path = "/benthos";
};

// What /metrics and /stats report. Filled by summing each shard's
// stream_service::stats; the server itself does no accounting.
struct observed {
    uint64_t messages_in  = 0;
    uint64_t messages_out = 0;
    uint64_t batches_in   = 0;
    uint64_t batches_out  = 0;
    uint64_t acks         = 0;
    uint64_t nacks        = 0;
    uint64_t filtered     = 0;
    uint64_t proc_errors  = 0;
    // Shards whose input (resp. output) is not connected. Summed rather than
    // ANDed so they compose with the counters, and kept apart so /ready can name
    // the side that is down, as the reference's does.
    uint64_t not_ready_in  = 0;
    uint64_t not_ready_out = 0;
    bool     ready        = false;
    // Per component, so /metrics can report one series per component the way
    // the reference does -- `input_received{path="root.input"}`,
    // `processor_error{path="root.pipeline.processors.1",label="dedupe"}`. A
    // dashboard that groups by `path` saw every component's series collapsed
    // into one before this, because every series carried path="root".
    std::vector<component_stats> components;
};

// Both entry points -- `swordfish run` and a compiled binary -- gather the same
// counters the same way. Written once here rather than copied into each, which
// is how it started and how one of them would have drifted.
// Splits an `http.address` into host and port, accepting every form the
// reference accepts -- "host:port", ":port" (every interface) and
// "[ipv6]:port". Exposed so the CONFIG parser can validate the field, which it
// could not before: a bad address passed `sfconfig lint`, produced a binary,
// and killed the shipped binary at startup with a message whose entire text was
// the address.
std::pair<std::string, uint16_t> parse_host_port(const std::string& addr);

template <class Sharded>
seastar::future<observed> gather_from(Sharded& shards) {
    return shards.map_reduce0(
        [](auto& s) {
            const auto st = s.stats();
            observed o;
            o.messages_in = st.messages_in;  o.messages_out = st.messages_out;
            o.batches_in  = st.batches_in;   o.batches_out  = st.batches_out;
            o.acks        = st.acks;         o.nacks        = st.nacks;
            o.filtered    = st.filtered;     o.proc_errors  = st.proc_errors;
            o.not_ready_in  = st.not_ready_in;
            o.not_ready_out = st.not_ready_out;
            o.components    = st.components;
            return o;
        },
        observed{},
        [](observed a, const observed& b) {
            a.messages_in += b.messages_in;  a.messages_out += b.messages_out;
            a.batches_in  += b.batches_in;   a.batches_out  += b.batches_out;
            a.acks        += b.acks;         a.nacks        += b.nacks;
            a.filtered    += b.filtered;     a.proc_errors  += b.proc_errors;
            a.not_ready_in  += b.not_ready_in;
            a.not_ready_out += b.not_ready_out;
            // Summed by INDEX: every shard builds the same stream from the same
            // spec, so component i is the same component everywhere. The first
            // shard to report supplies the identities and the rest add counts;
            // a shard that has not started yet contributes an empty list and is
            // skipped rather than truncating the result.
            if (a.components.empty()) {
                a.components = b.components;
            } else if (!b.components.empty()) {
                for (size_t i = 0; i < a.components.size() && i < b.components.size(); ++i)
                    a.components[i].c += b.components[i].c;
            }
            return a;
        }).then([](observed o) {
            // Read from the components, not hardcoded. `/ready` used to answer
            // 200 and "connected":true for a pipeline whose input had never
            // connected -- so Kubernetes routed traffic to a pod consuming
            // nothing, and never evicted one whose source was later lost. The
            // whole connection_status graph the connectors already fill in was
            // unreachable.
            o.ready = o.not_ready_in == 0 && o.not_ready_out == 0;
            return o;
        });
}

class observe_server {
public:
    // `gather` is called per request rather than on a timer: a scrape should
    // see the counters as they are, and the cost is one cross-shard map over a
    // handful of integers.
    //
    // `extra` runs inside the same set_routes() call and lets a caller add its
    // own endpoints -- `swordfish streams` puts /streams and /streams/{id}
    // there. It is a hook rather than a second server because the reference
    // serves the streams API and the observability endpoints on ONE address:
    // the root config's `http` block governs both, and /ready has to answer for
    // every stream at once.
    observe_server(http_spec spec, std::function<seastar::future<observed>()> gather,
                   std::function<void(seastar::httpd::routes&)> extra = {});
    ~observe_server();

    seastar::future<> start();
    seastar::future<> stop();

private:
    struct impl;
    std::unique_ptr<impl> _i;
};

// Exposed for testing: the Prometheus text a given set of counters produces.
std::string render_prometheus(const observed& o);

} // namespace sf
