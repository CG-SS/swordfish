// The `kafka` input and output, as pipeline components.
//
// Registered rather than hard-wired: this is the first connector whose config
// is large enough that the hand-rolled input_spec/output_spec would have to
// grow a dozen fields for one component (see components/registry.hh).
//
// Registration is EXPLICIT, via register_components(). These live in a static
// library, and a linker drops an object file nothing references -- the same
// trap that silently emptied the processor registry once already.
#include "swordfish/kafka/components.hh"
#include "swordfish/kafka/consumer.hh"
#include "swordfish/kafka/producer.hh"
#include "swordfish/components/registry.hh"
#include "swordfish/runtime/components.hh"
#include "swordfish/runtime/transform.hh"
#include "swordfish/blobl/parse.hh"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>
#include <seastar/util/log.hh>

#include <algorithm>

#include <chrono>

namespace sf::kafka {

static seastar::logger complog("sf.kafka.component");

using namespace std::chrono_literals;

} // namespace sf::kafka

namespace sf::kafka {

namespace {

// ---- the input --------------------------------------------------------------

// The retry ladder for a failed poll. The ceiling is short on purpose: a broker
// that comes back should be noticed in seconds, not minutes.
constexpr std::chrono::milliseconds poll_backoff_min{250};
constexpr std::chrono::milliseconds poll_backoff_max{5000};

class kafka_input final : public input {
public:
    explicit kafka_input(consumer_config cfg) : _cfg(std::move(cfg)) {}

    seastar::future<> connect(seastar::abort_source& as) override {
        _consumer = std::make_shared<consumer>(_cfg);
        co_await _consumer->start(as);
    }

    seastar::future<std::optional<std::pair<batch, ack_fn>>>
    read_batch(seastar::abort_source& as) override {
        while (!as.abort_requested()) {
            // A FAILED POLL DOES NOT END THE INPUT.
            //
            // poll() throwing used to propagate out of read_batch, and
            // stream::input_loop catches everything, logs "input layer failed"
            // and leaves the loop for good. Measured: `kill -9` on the broker
            // JVM ended consumption for the life of the process -- the process
            // exited with the pipeline reporting a clean in=3 out=3 -- and it
            // never came back when the broker did. The reference survives the
            // same kill and resumes.
            //
            // A Kafka topic does not end, so nothing a poll can report is an
            // end of input; the only thing that ends this loop is the abort.
            // Errors are logged at error level rather than swallowed, because a
            // broker that never returns must not look like an idle topic.
            std::vector<fetched_record> recs;
            std::exception_ptr err;
            try {
                recs = co_await _consumer->poll(as);
            } catch (const seastar::abort_requested_exception&) {
                co_return std::nullopt;
            } catch (...) {
                err = std::current_exception();
            }
            if (err) {
                if (as.abort_requested()) co_return std::nullopt;
                complog.error("kafka poll failed, retrying in {}ms: {}",
                           _backoff.count(), err);
                bool slept = true;
                try { co_await seastar::sleep_abortable(_backoff, as); }
                catch (...) { slept = false; }
                if (!slept) co_return std::nullopt;
                _backoff = std::min(poll_backoff_max, _backoff * 2);
                continue;
            }
            // Consecutive, not cumulative: one successful poll after an outage
            // puts the ladder back at the bottom, so the next unrelated blip is
            // retried promptly rather than at the interval the last one reached.
            _backoff = poll_backoff_min;
            if (recs.empty()) {
                // An empty poll is the broker's max_wait elapsing with nothing
                // new. A Kafka topic never ends, so this is not end-of-input --
                // returning nullopt here would stop the pipeline on an idle
                // topic.
                continue;
            }
            batch b;
            b.reserve(recs.size());
            // Every record in the batch must be acked before any of their
            // offsets can be committed, so the ack carries the whole set.
            auto positions = std::make_shared<std::vector<fetched_record>>();
            for (auto& r : recs) {
                message m(r.value);
                m.meta().set("kafka_topic", value(r.topic));
                m.meta().set("kafka_partition", value(static_cast<int64_t>(r.partition)));
                m.meta().set("kafka_offset", value(r.offset));
                m.meta().set("kafka_timestamp_ms", value(r.timestamp));
                if (r.key) m.meta().set("kafka_key", value(*r.key));
                for (const auto& h : r.headers)
                    if (h.value) m.meta().set(h.key, value(*h.value));
                b.push_back(std::move(m));
                positions->push_back(std::move(r));
            }
            // Shared, not raw: an ack may arrive after the input is closed --
            // a slow output finishing during shutdown -- and a raw pointer
            // would be dangling by then.
            auto c = _consumer;
            ack_fn ack = [c, positions](std::exception_ptr e) -> seastar::future<> {
                // A nack leaves the offsets uncommitted, so the records are
                // redelivered from the last commit -- which is what makes this
                // at-least-once rather than at-most-once.
                if (!e)
                    for (const auto& r : *positions) c->ack(r.topic, r.partition, r.offset);
                return seastar::make_ready_future<>();
            };
            co_return std::make_pair(std::move(b), std::move(ack));
        }
        co_return std::nullopt;
    }

    seastar::future<> close() override {
        if (_consumer) co_await _consumer->stop();
    }

    connection_status status() const override {
        connection_status s;
        s.connected = _consumer != nullptr;
        s.label = "kafka";
        return s;
    }

private:
    consumer_config           _cfg;
    std::shared_ptr<consumer> _consumer;
    std::chrono::milliseconds _backoff = poll_backoff_min;
};

// ---- the output -------------------------------------------------------------

class kafka_output final : public output {
public:
    // `topic_fn` is empty for a topic with no interpolation in it, which is the
    // common case and costs nothing.
    kafka_output(producer_config cfg, std::string key_meta, transform_fn topic_fn)
        : _cfg(std::move(cfg)), _key_meta(std::move(key_meta)),
          _topic_fn(std::move(topic_fn)) {}

    seastar::future<> connect(seastar::abort_source&) override {
        _producer = std::make_unique<producer>(_cfg);
        co_await _producer->start();
    }

    seastar::future<> write_batch(batch b, seastar::abort_source&) override {
        if (b.empty()) co_return;
        // Grouped by the topic each message resolves to, so one batch can fan
        // out across topics in a single produce per topic -- which is what makes
        // `topic: 'events-${! json("kind") }'` useful rather than merely legal.
        // The order within a topic is preserved, which is the order that matters
        // to Kafka.
        std::map<std::string, std::vector<produce_record>> by_topic;
        for (auto& m : b) {
            produce_record r;
            r.value = m.as_bytes();
            if (const value* k = m.meta().find(_key_meta))
                r.key = k->to_display_string();
            std::string topic = _cfg.topic;
            if (_topic_fn) {
                exec_ctx ctx;
                value parsed;
                bool structured = true;
                try { parsed = m.as_structured(); } catch (...) { structured = false; }
                ctx.this_v  = structured ? &parsed : nullptr;
                ctx.msg     = &m;
                ctx.meta_in = &m.meta();
                topic = _topic_fn(ctx).to_display_string();
            }
            by_topic[std::move(topic)].push_back(std::move(r));
        }
        for (auto& [topic, recs] : by_topic)
            co_await _producer->send_to(topic, std::move(recs));
    }

    seastar::future<> close() override {
        if (_producer) co_await _producer->stop();
    }

    connection_status status() const override {
        connection_status s;
        s.connected = _producer != nullptr;
        s.label = "kafka";
        return s;
    }

    // Kafka acknowledges a whole request, so several in flight is the ordinary
    // way to keep the link busy.
    size_t max_in_flight() const override { return 4; }

private:
    producer_config           _cfg;
    std::string               _key_meta;
    transform_fn              _topic_fn;
    std::unique_ptr<producer> _producer;
};

} // namespace

input_ptr make_kafka_input(const input_config& c) {
    consumer_config cc;
    cc.seed_brokers = c.seed_brokers;
    cc.topics = c.topics;
    cc.consumer_group = c.consumer_group;
    cc.client_id = c.client_id;
    cc.strategy = c.partition_strategy;
    cc.start_from_oldest = c.start_from_oldest;
    cc.fetch_max_bytes = c.fetch_max_bytes;
    cc.fetch_max_wait =
        std::chrono::duration_cast<std::chrono::milliseconds>(c.fetch_max_wait.ns);
    cc.session_timeout =
        std::chrono::duration_cast<std::chrono::milliseconds>(c.session_timeout.ns);
    return std::make_unique<kafka_input>(std::move(cc));
}

output_ptr make_kafka_output(const output_config& c) {
    producer_config pc;
    pc.seed_brokers = c.seed_brokers;
    // The literal spelling is kept for the non-interpolated case and for
    // connect()'s metadata refresh; the transform is built here so both backends
    // go through this one factory and cannot disagree about what the topic is.
    pc.topic = c.topic.source;
    transform_fn topic_fn;
    if (cfg::is_interpolated(c.topic.source)) {
        pc.topic_is_interpolated = true;
        topic_fn = interpreted_transform(
            blobl::parse_query(cfg::interpolation_to_query(c.topic.source)));
    }
    pc.client_id = c.client_id;
    pc.codec = compression_from_name(c.compression);
    pc.acks = static_cast<int16_t>(c.acks);
    pc.timeout = std::chrono::duration_cast<std::chrono::milliseconds>(c.timeout.ns);
    return std::make_unique<kafka_output>(std::move(pc), c.key_metadata,
                                          std::move(topic_fn));
}

void register_components() {
    register_input(make_input_def<input_config>(
        "kafka", [](const input_config& c, const component_config&) {
            return [c] { return make_kafka_input(c); };
        },
        [](const input_config& c, const component_config&) {
            // The generated main() constructs the same config struct from a
            // literal and hands it to the same factory, so compiled and
            // interpreted runs share one code path below this line.
            return "sf::kafka::make_kafka_input(" +
                   cfg::emit_cpp(c, "sf::kafka::input_config") + ")";
        },
        "swordfish/kafka/components.hh"));

    auto kafka_out = make_output_def<output_config>(
        "kafka", [](const output_config& c, const component_config&) {
            return [c] { return make_kafka_output(c); };
        },
        [](const output_config& c, const component_config&) {
            return "sf::kafka::make_kafka_output(" +
                   cfg::emit_cpp(c, "sf::kafka::output_config") + ")";
        },
        "swordfish/kafka/components.hh");
    // The reference allows a batching policy here, and a producer is the
    // clearest case for one.
    kafka_out.batching = true;
    register_output(std::move(kafka_out));
}

} // namespace sf::kafka
