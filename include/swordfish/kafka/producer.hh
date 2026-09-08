// Producing to a cluster: choose a partition, batch, send to that partition's
// leader, retry when the leader moves.
//
// Batching is per (topic, partition) because a record batch belongs to exactly
// one partition -- it carries a single base offset that the leader assigns. A
// Produce request may then carry several such batches to the same broker, which
// is where the amortisation comes from.
#pragma once

#include "swordfish/kafka/cluster.hh"
#include "swordfish/kafka/records.hh"

#include <seastar/core/future.hh>

#include <chrono>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace sf::kafka {

// Kafka's own hash, used by every client so that a key lands on the same
// partition regardless of which client wrote it. It is MurmurHash2 with
// Kafka's seed, and the result is masked to a positive int32 -- getting either
// detail wrong quietly breaks key affinity rather than failing.
int32_t murmur2(std::string_view data) noexcept;
int32_t partition_for_key(std::string_view key, int32_t partition_count) noexcept;

struct produce_record {
    std::optional<std::string> key;
    std::string                value;
    std::vector<record_header> headers;
    int64_t                    timestamp = 0;      // 0 = now
    // Set to pin a record to a partition; otherwise the key decides, and a
    // record without a key is spread round-robin.
    std::optional<int32_t>     partition;
};

struct producer_config {
    std::vector<std::string> seed_brokers;
    // The topic as WRITTEN. When it carries an interpolation the literal is not
    // a topic name at all, so `topic_is_interpolated` says so and start() must
    // not try to fetch metadata for it -- doing that hung the producer for ever
    // against a name containing `${`, `}` and `!`, which Kafka forbids.
    std::string              topic;
    bool                     topic_is_interpolated = false;
    std::string              client_id = "swordfish";
    compression              codec = compression::none;
    // -1 waits for all in-sync replicas, 1 for the leader only, 0 for nothing.
    // Anything other than -1 gives up durability guarantees the pipeline's
    // at-least-once contract depends on.
    int16_t                  acks = -1;
    std::chrono::milliseconds timeout{30000};
    int                      max_retries = 4;
};

class producer {
public:
    explicit producer(producer_config cfg);
    ~producer();

    seastar::future<> start();
    seastar::future<> stop();

    // Send a batch and wait for the broker to acknowledge it. Returns when
    // every record is durable to the configured `acks` level, which is what
    // lets the pipeline ack its source.
    seastar::future<> send(std::vector<produce_record> records);

    // Topic override, for a `topic` field that is itself an interpolation.
    seastar::future<> send_to(const std::string& topic,
                              std::vector<produce_record> records);

private:
    seastar::future<> send_once(const std::string& topic,
                                const std::vector<produce_record>& records);

    producer_config          _cfg;
    std::unique_ptr<cluster> _cluster;
    // Round-robin cursor for keyless records, per topic.
    std::map<std::string, uint32_t> _cursor;
};

} // namespace sf::kafka
