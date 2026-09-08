// The `kafka` input and output as pipeline components: their configs, their
// factories, and the registration hook.
//
// The configs and their spec_of tables live in the HEADER rather than beside
// the implementation, because generated code has to name them: `swordfish
// build` emits a `sf::kafka::input_config{...}` literal and hands it to the
// same factory the interpreter calls, so both paths meet at one line.
#pragma once

#include "swordfish/config/spec.hh"
#include "swordfish/runtime/component.hh"

#include <chrono>
#include <string>
#include <vector>

namespace sf::kafka {

using namespace std::chrono_literals;

struct input_config {
    std::vector<std::string> seed_brokers;
    std::vector<std::string> addresses;      // legacy alias; see validate() below
    std::vector<std::string> topics;
    std::string              consumer_group;
    std::string              client_id = "swordfish";
    bool                     start_from_oldest = true;
    std::string              partition_strategy = "range";
    int                      fetch_max_bytes = 52428800;
    cfg::duration            fetch_max_wait{500ms};
    cfg::duration            session_timeout{45000ms};
    bool operator==(const input_config&) const = default;

    // `addresses` is the legacy spelling of `seed_brokers`, accepted so a config
    // written for the reference's sarama-shaped `kafka` runs here unchanged.
    //
    // The reference ships TWO Kafka connectors: `kafka` (stable, broker list
    // `addresses`) and `kafka_franz` (beta, `seed_brokers`). Swordfish
    // implements the franz-go shape under the name `kafka`, so without this
    // alias no config linted on both implementations, in either direction.
    // `seed_brokers` is the real field and the one to use; `addresses` is here
    // for compatibility and WINS NOTHING -- if both are given, `seed_brokers`
    // is what takes effect.
    static void validate(input_config& c, const cfg::ynode& n, const std::string& path,
                         cfg::lints& ls) {
        if (c.seed_brokers.empty()) c.seed_brokers = c.addresses;
        // Cleared once merged, so exactly one field carries the answer. A
        // compiled binary emits this struct verbatim, and leaving the alias
        // populated put a second, dead broker list into the generated source --
        // ignored by make_kafka_input, but the next reader would have to work
        // that out.
        c.addresses.clear();
        if (c.seed_brokers.empty())
            ls.push_back({cfg::lint_level::error, n.pos, cfg::join_path(path, "seed_brokers"),
                          "required field is missing (`addresses` is accepted as a "
                          "legacy alias for it)"});
    }
};

struct output_config {
    std::vector<std::string> seed_brokers;
    std::vector<std::string> addresses;      // legacy alias; see validate() below
    // INTERPOLATED, as the reference's is: "This field supports interpolation
    // functions". It was a plain string, so `topic: 'topic-${! count("t") }'`
    // lint`ed clean on both implementations and then meant different things: the
    // reference created topic-1 and topic-2, while swordfish sent to a topic
    // whose literal name contains ${ } and !, which Kafka forbids -- so the
    // produce could never succeed and the run hung for ever with no error, no
    // warning and nothing in the log.
    cfg::interpolation       topic;
    std::string              client_id = "swordfish";
    std::string              compression = "none";
    // The metadata key holding the record key, matching how the reference
    // exposes it: a mapping sets `meta kafka_key`, and the output reads it.
    std::string              key_metadata = "kafka_key";
    int                      acks = -1;
    cfg::duration            timeout{30000ms};
    bool operator==(const output_config&) const = default;

    // The same legacy alias as on the input; see input_config::validate.
    static void validate(output_config& c, const cfg::ynode& n, const std::string& path,
                         cfg::lints& ls) {
        if (c.seed_brokers.empty()) c.seed_brokers = c.addresses;
        // Cleared once merged, so exactly one field carries the answer. A
        // compiled binary emits this struct verbatim, and leaving the alias
        // populated put a second, dead broker list into the generated source --
        // ignored by make_kafka_input, but the next reader would have to work
        // that out.
        c.addresses.clear();
        if (c.seed_brokers.empty())
            ls.push_back({cfg::lint_level::error, n.pos, cfg::join_path(path, "seed_brokers"),
                          "required field is missing (`addresses` is accepted as a "
                          "legacy alias for it)"});
    }
};

input_ptr  make_kafka_input(const input_config& c);
output_ptr make_kafka_output(const output_config& c);

// Registration is EXPLICIT, called by whichever binary links this library. It
// is not a static initialiser: these live in a static library, and a linker
// drops an object file nothing references -- exactly how the processor registry
// came up empty once.
void register_components();

} // namespace sf::kafka

namespace sf::cfg {

template <> struct spec_of<sf::kafka::input_config> {
    static constexpr std::string_view cpp_type = "sf::kafka::input_config";
    static constexpr std::string_view strategies[] = {"range", "roundrobin"};
    static constexpr auto value = object(
        field("seed_brokers", &sf::kafka::input_config::seed_brokers)
            .describe("A list of broker addresses to connect to. Required, unless "
                      "given under its legacy alias `addresses`."),
        field("addresses", &sf::kafka::input_config::addresses)
            .describe("A legacy alias for `seed_brokers`, accepted so a config "
                      "written for Redpanda Connect's sarama-shaped `kafka` input runs "
                      "unchanged. Prefer `seed_brokers`; if both are set, "
                      "`seed_brokers` is the one that takes effect.").advanced_(),
        field("topics", &sf::kafka::input_config::topics)
            .describe("A list of topics to consume from.").require(),
        field("consumer_group", &sf::kafka::input_config::consumer_group)
            .describe("An optional consumer group to consume as. Without one, "
                      "every partition is read and nothing is committed."),
        field("client_id", &sf::kafka::input_config::client_id)
            .describe("An identifier reported to the broker.").advanced_(),
        field("start_from_oldest", &sf::kafka::input_config::start_from_oldest)
            .describe("Whether to consume from the oldest offset when the group "
                      "has no committed offset."),
        field("partition_strategy", &sf::kafka::input_config::partition_strategy)
            .describe("How partitions are divided between group members.")
            .options(strategies).advanced_(),
        field("fetch_max_bytes", &sf::kafka::input_config::fetch_max_bytes)
            .describe("The maximum bytes a broker returns per fetch.").advanced_(),
        field("fetch_max_wait", &sf::kafka::input_config::fetch_max_wait)
            .describe("How long a broker waits for data before answering.").advanced_(),
        field("session_timeout", &sf::kafka::input_config::session_timeout)
            .describe("How long the coordinator waits before evicting a silent "
                      "member.").advanced_()
    );
};

template <> struct spec_of<sf::kafka::output_config> {
    static constexpr std::string_view cpp_type = "sf::kafka::output_config";
    static constexpr std::string_view codecs[] = {"none", "gzip", "snappy", "lz4", "zstd"};
    static constexpr auto value = object(
        field("seed_brokers", &sf::kafka::output_config::seed_brokers)
            .describe("A list of broker addresses to connect to. Required, unless "
                      "given under its legacy alias `addresses`."),
        field("addresses", &sf::kafka::output_config::addresses)
            .describe("A legacy alias for `seed_brokers`, accepted so a config "
                      "written for Redpanda Connect's sarama-shaped `kafka` output runs "
                      "unchanged. Prefer `seed_brokers`; if both are set, "
                      "`seed_brokers` is the one that takes effect.").advanced_(),
        field("topic", &sf::kafka::output_config::topic)
            .describe("The topic to publish to. This field supports interpolation "
                      "functions.").require(),
        field("client_id", &sf::kafka::output_config::client_id)
            .describe("An identifier reported to the broker.").advanced_(),
        field("compression", &sf::kafka::output_config::compression)
            .describe("The compression codec applied to record batches.")
            .options(codecs),
        field("key_metadata", &sf::kafka::output_config::key_metadata)
            .describe("The metadata key holding each record's key. A record "
                      "without one is distributed round-robin.").advanced_(),
        field("acks", &sf::kafka::output_config::acks)
            .describe("Replicas that must acknowledge a write: -1 all in-sync, "
                      "1 the leader, 0 none.").advanced_(),
        field("timeout", &sf::kafka::output_config::timeout)
            .describe("How long the broker may take to acknowledge a write.").advanced_()
    );
};

} // namespace sf::cfg
