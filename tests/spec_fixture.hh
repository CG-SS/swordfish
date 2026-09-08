// A FIXTURE for the spec_of framework, not a live component config.
//
// It began as the Kafka input's config, before the real one existed
// (swordfish/kafka/components.hh). It is kept because it exercises corners of
// the descriptor machinery the live configs do not: a nested object, an
// optional nested object, and an enumerated string. Nothing constructs a
// component from it.
//
// It lives in tests/ precisely so it cannot be mistaken for the real thing --
// two structs both called "the Kafka input config" is how a field gets added to
// the one nobody reads.
#pragma once

#include "swordfish/config/spec.hh"

namespace sf::in {

using namespace std::chrono_literals;

struct tls_config {
    bool        enabled          = false;
    bool        skip_cert_verify = false;
    std::string root_cas_file;
    bool operator==(const tls_config&) const = default;
};

struct sasl_config {
    std::string mechanism = "none";
    std::string username;
    std::string password;
    bool operator==(const sasl_config&) const = default;
};

struct kafka_input_config {
    std::vector<std::string> seed_brokers;
    std::vector<std::string> topics;
    std::string              consumer_group;
    bool                     start_from_oldest = true;
    bool                     regexp_topics     = false;
    int                      fetch_max_bytes   = 52428800;
    cfg::duration            fetch_max_wait    = 500ms;
    int                      checkpoint_limit  = 1024;
    std::string              auto_replay_nacks = "true";
    std::optional<tls_config> tls;
    sasl_config              sasl;
    bool operator==(const kafka_input_config&) const = default;
};

} // namespace sf::in

namespace sf::cfg {

template <> struct spec_of<sf::in::tls_config> {
    static constexpr std::string_view cpp_type = "sf::in::tls_config";
    static constexpr auto value = object(
        field("enabled", &sf::in::tls_config::enabled)
            .describe("Whether custom TLS settings are enabled."),
        field("skip_cert_verify", &sf::in::tls_config::skip_cert_verify)
            .describe("Whether to skip server side certificate verification.").advanced_(),
        field("root_cas_file", &sf::in::tls_config::root_cas_file)
            .describe("An optional path of a root certificate authority file to use.")
    );
};

template <> struct spec_of<sf::in::sasl_config> {
    static constexpr std::string_view cpp_type = "sf::in::sasl_config";
    static constexpr std::string_view mechanisms[] =
        {"none", "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "OAUTHBEARER"};
    static constexpr auto value = object(
        field("mechanism", &sf::in::sasl_config::mechanism)
            .describe("The SASL mechanism to use.").options(mechanisms),
        field("username", &sf::in::sasl_config::username).describe("A username to provide."),
        field("password", &sf::in::sasl_config::password).describe("A password to provide.")
    );
};

template <> struct spec_of<sf::in::kafka_input_config> {
    static constexpr std::string_view cpp_type = "sf::in::kafka_input_config";
    static constexpr auto value = object(
        field("seed_brokers", &sf::in::kafka_input_config::seed_brokers)
            .describe("A list of broker addresses to connect to.").require(),
        field("topics", &sf::in::kafka_input_config::topics)
            .describe("A list of topics to consume from.").require(),
        field("consumer_group", &sf::in::kafka_input_config::consumer_group)
            .describe("An optional consumer group to consume as."),
        field("start_from_oldest", &sf::in::kafka_input_config::start_from_oldest)
            .describe("Whether to consume from the oldest offset when no commit exists."),
        field("regexp_topics", &sf::in::kafka_input_config::regexp_topics)
            .describe("Whether listed topics should be interpreted as regular expressions.").advanced_(),
        field("fetch_max_bytes", &sf::in::kafka_input_config::fetch_max_bytes)
            .describe("Sets the maximum amount of bytes a broker will try to send during a fetch.").advanced_(),
        field("fetch_max_wait", &sf::in::kafka_input_config::fetch_max_wait)
            .describe("Sets the maximum time a broker will wait before sending a fetch response.").advanced_(),
        field("checkpoint_limit", &sf::in::kafka_input_config::checkpoint_limit)
            .describe("Records to process before the consumer blocks awaiting acknowledgements.").advanced_(),
        field("auto_replay_nacks", &sf::in::kafka_input_config::auto_replay_nacks)
            .describe("Whether messages that are rejected should be replayed indefinitely.").advanced_(),
        field("tls", &sf::in::kafka_input_config::tls)
            .describe("Custom TLS settings can be used to override system defaults."),
        field("sasl", &sf::in::kafka_input_config::sasl)
            .describe("Specify one or more methods of SASL authentication.")
    );
};

} // namespace sf::cfg
