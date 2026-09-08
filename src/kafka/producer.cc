#include "swordfish/kafka/producer.hh"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>
#include <seastar/util/log.hh>

#include <chrono>

namespace sf::kafka {

static seastar::logger plog("sf.kafka.producer");

// MurmurHash2, the variant Kafka's DefaultPartitioner uses. Ported from the
// algorithm rather than from any implementation; the constants are the ones
// MurmurHash2 defines and the seed is Kafka's.
int32_t murmur2(std::string_view data) noexcept {
    constexpr uint32_t seed = 0x9747b28cu;
    constexpr uint32_t m = 0x5bd1e995u;
    constexpr int r = 24;

    const size_t len = data.size();
    uint32_t h = seed ^ static_cast<uint32_t>(len);
    size_t i = 0;
    for (; i + 4 <= len; i += 4) {
        uint32_t k = static_cast<uint8_t>(data[i])
                   | (static_cast<uint32_t>(static_cast<uint8_t>(data[i + 1])) << 8)
                   | (static_cast<uint32_t>(static_cast<uint8_t>(data[i + 2])) << 16)
                   | (static_cast<uint32_t>(static_cast<uint8_t>(data[i + 3])) << 24);
        k *= m;
        k ^= k >> r;
        k *= m;
        h *= m;
        h ^= k;
    }
    switch (len - i) {
    case 3: h ^= static_cast<uint32_t>(static_cast<uint8_t>(data[i + 2])) << 16; [[fallthrough]];
    case 2: h ^= static_cast<uint32_t>(static_cast<uint8_t>(data[i + 1])) << 8;  [[fallthrough]];
    case 1: h ^= static_cast<uint32_t>(static_cast<uint8_t>(data[i]));
            h *= m;
            break;
    default: break;
    }
    h ^= h >> 13;
    h *= m;
    h ^= h >> 15;
    return static_cast<int32_t>(h);
}

int32_t partition_for_key(std::string_view key, int32_t partition_count) noexcept {
    if (partition_count <= 0) return 0;
    // toPositive(murmur2(key)) % count. The mask, not abs(): abs(INT_MIN) is
    // undefined and Kafka's own clients mask, so a different choice here would
    // put the same key on a different partition.
    const int32_t h = murmur2(key) & 0x7fffffff;
    return h % partition_count;
}

producer::producer(producer_config cfg) : _cfg(std::move(cfg)) {}
producer::~producer() = default;

seastar::future<> producer::start() {
    _cluster = std::make_unique<cluster>(_cfg.seed_brokers, _cfg.client_id);
    // An interpolated topic has no single name to prefetch; send_to refreshes
    // per topic as it goes.
    if (!_cfg.topic.empty() && !_cfg.topic_is_interpolated)
        co_await _cluster->refresh({_cfg.topic}, /*create=*/true);
    else
        co_await _cluster->refresh({}, /*create=*/true);
}

seastar::future<> producer::stop() {
    if (_cluster) co_await _cluster->stop();
}

seastar::future<> producer::send(std::vector<produce_record> records) {
    return send_to(_cfg.topic, std::move(records));
}

// A coroutine parameter must be by value: a reference would dangle at the first
// suspension, and this one is read after several. The signature is on one line
// so the inline suppression lands on the parameter cppcheck flags.
// cppcheck-suppress passedByValue
seastar::future<> producer::send_to(const std::string& topic, std::vector<produce_record> records) {
    if (records.empty()) co_return;
    std::chrono::milliseconds backoff{100};
    for (int attempt = 0; ; ++attempt) {
        bool retry = false;
        try {
            co_await send_once(topic, records);
            co_return;
        } catch (const broker_error& e) {
            if (!is_retriable(e.code) || attempt >= _cfg.max_retries) throw;
            plog.debug("produce to {}: {}, retrying", topic, e.what());
            retry = true;
        }
        // Outside the handler: co_await is not allowed inside one. A retriable
        // produce error nearly always means the leader moved, so metadata is
        // refreshed before trying again.
        if (retry) {
            co_await seastar::sleep(backoff);
            backoff = std::min(backoff * 2, std::chrono::milliseconds{2000});
            co_await _cluster->refresh({topic}, /*create=*/true);
        }
    }
}

seastar::future<> producer::send_once(const std::string& topic,
                                      const std::vector<produce_record>& records) {
    const topic_info* ti = _cluster->topic(topic);
    if (!ti) {
        // A broker with auto-creation on CREATES the topic in response to the
        // metadata request but does not have a leader for it in that same
        // response, so one refresh is not enough. This threw `unknown topic`
        // immediately, which -- once nacks began replaying -- meant a pipeline
        // writing to a topic that does not exist yet retried for ever with no
        // error in the log. The reference creates the topic and carries on.
        for (int i = 0; i < 5 && !ti; ++i) {
            if (i) co_await seastar::sleep(std::chrono::milliseconds{100});
            co_await _cluster->refresh({topic}, /*create=*/true);
            ti = _cluster->topic(topic);
        }
        if (!ti) throw protocol_error("unknown topic: " + topic);
    }
    const int32_t count = static_cast<int32_t>(ti->partitions.size());
    if (count == 0) throw protocol_error("topic has no partitions: " + topic);

    const int64_t now = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    // Group by partition first: a record batch belongs to exactly one.
    const uint32_t base_cursor = _cursor[topic];
    uint32_t spread = 0;
    std::map<int32_t, std::vector<const produce_record*>> by_partition;
    for (const auto& r : records) {
        int32_t p;
        if (r.partition) {
            p = *r.partition;
        } else if (r.key) {
            p = partition_for_key(*r.key, count);
        } else {
            // Keyless records spread round-robin, which is what keeps a
            // partition from becoming a hotspot when there are no keys.
            //
            // The cursor advances here, inside send_once, which a RETRY calls
            // again -- so a resend would scatter the same records across
            // different partitions than the first attempt. At-least-once means
            // duplicates are expected; duplicates on a different partition than
            // the original are worse, because a consumer keyed by partition
            // sees them as unrelated. Hence base_cursor: each attempt starts
            // from where this call started, not from where the last one ended.
            p = static_cast<int32_t>((base_cursor + spread++) % static_cast<uint32_t>(count));
        }
        by_partition[p].push_back(&r);
    }

    // Then by leader: one Produce request per broker, carrying every batch it
    // leads.
    std::map<int32_t, std::vector<int32_t>> by_leader;
    for (const auto& [p, _] : by_partition) {
        const partition_info* pi = ti->find(p);
        if (!pi || pi->leader < 0)
            throw broker_error(err::leader_not_available,
                               "no leader for " + topic + "/" + std::to_string(p));
        by_leader[pi->leader].push_back(p);
    }

    // Advanced only once the whole call has decided its partitions, so a retry
    // reproduces the same layout.
    _cursor[topic] = base_cursor + spread;

    for (const auto& [node, parts] : by_leader) {
        auto* conn = co_await _cluster->broker(node);
        produce_request req;
        req.acks = _cfg.acks;
        req.timeout_ms = static_cast<int32_t>(_cfg.timeout.count());
        produce_request_topic_produce_data td;
        td.name = topic;                         // through v12
        td.topic_id = ti->id;                    // from v13
        for (int32_t p : parts) {
            record_batch rb;
            rb.codec = _cfg.codec;
            rb.base_timestamp = now;
            rb.max_timestamp = now;
            const auto& recs = by_partition[p];
            rb.records.resize(recs.size());
            for (size_t i = 0; i < recs.size(); ++i) {
                auto& out = rb.records[i];
                out.offset_delta = static_cast<int32_t>(i);
                const int64_t ts = recs[i]->timestamp ? recs[i]->timestamp : now;
                out.timestamp_delta = ts - now;
                out.key = recs[i]->key;
                out.value = recs[i]->value;
                out.headers = recs[i]->headers;
                rb.max_timestamp = std::max(rb.max_timestamp, ts);
            }
            writer w;
            rb.encode(w);
            produce_request_partition_produce_data pd;
            pd.index = p;
            pd.records = w.take();
            td.partition_data.push_back(std::move(pd));
        }
        req.topic_data.push_back(std::move(td));

        const int16_t v = conn->negotiated(produce_request::api_key);
        // acks=0 means the broker sends NOTHING back, so the request must not be
        // one that waits for an answer. It used to be: the send parked for the
        // full 30s request timeout, marked the connection dead, failed every
        // pending request and threw -- and the retry then rewrote the same
        // records. Measured with three messages and `acks: 0`: nine records
        // reached the topic in three physical copies, the process never
        // terminated, and stderr carried not one diagnostic line. `acks` is an
        // advertised, lintable field, so this was reachable from a config that
        // both linters passed.
        if (_cfg.acks == 0) {
            co_await conn->send_oneway<produce_request>(req, v);
            continue;
        }
        auto resp = co_await conn->send<produce_request, produce_response>(req, v);
        for (const auto& t : resp.responses)
            for (const auto& p : t.partition_responses)
                if (p.error_code != err::none)
                    throw broker_error(p.error_code,
                                       "Produce " + topic + "/" +
                                       std::to_string(p.index) + ": " +
                                       describe_error(p.error_code));
    }
}

} // namespace sf::kafka
