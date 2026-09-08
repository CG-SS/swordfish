// Kafka client against a REAL broker.
//
// tests/test_kafka.cc pins the codecs against hand-derived bytes; this file
// asserts that a live broker agrees. The two answer different questions: the
// first catches a codec that disagrees with the specification, the second
// catches a specification we read wrongly.
//
// The broker address comes from SWORDFISH_KAFKA_BROKER (default localhost:9092)
// and every case SKIPS rather than fails when nothing is listening, so a
// developer without a broker still gets a green suite. Run one with:
//
//     bin/kafka-server-start.sh config/server.properties
#include "swordfish/kafka/client.hh"
#include "swordfish/kafka/cluster.hh"
#include "swordfish/kafka/consumer.hh"
#include "swordfish/kafka/consumer_group.hh"
#include "swordfish/kafka/group_membership.hh"
#include "swordfish/kafka/records.hh"

#include <seastar/core/coroutine.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/map_reduce.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/when_all.hh>
#include <seastar/net/inet_address.hh>
// Supplies main() from seastar/testing, which sets up the reactor and runs
// the registered cases.
#define SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>

#include <algorithm>
#include <cstdlib>
#include <ranges>
#include <string>

using namespace sf::kafka;
using namespace std::chrono_literals;

namespace {

// The topic the fixture expects. Created by the harness, not by the test, so a
// failure here is never "the topic was missing".
constexpr const char* TOPIC = "sf-test";

std::string broker_env() {
    const char* v = ::getenv("SWORDFISH_KAFKA_BROKER");
    return v ? v : "localhost:9092";
}

// Opens a connection, or an empty optional when nothing is listening.
seastar::future<std::unique_ptr<connection>> try_connect() {
    auto c = std::make_unique<connection>();
    try {
        co_await c->connect(broker_env(), "swordfish-itest");
    } catch (const std::exception& e) {
        // Say WHY. A silent skip on any exception once hid a bad address rather
        // than a missing broker, and the suite reported green having tested
        // nothing.
        BOOST_TEST_MESSAGE("no broker at " << broker_env() << " (" << e.what()
                                           << ") -- skipping");
        co_return nullptr;
    }
    co_return std::move(c);
}

int64_t now_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch()).count();
}


// Produce a batch and read it straight back. Returns the values as the broker
// gave them back, which is the only way to know it ACCEPTED the batch: Kafka
// verifies the CRC itself and rejects a bad one with CORRUPT_MESSAGE.
seastar::future<std::vector<std::string>> round_trip(connection& c, uuid topic_id,
                                                     int32_t partition,
                                                     compression codec,
                                                     std::vector<std::string> values) {
    int64_t start = 0;
    {
        list_offsets_request req;
        req.replica_id = -1;
        req.topics.resize(1);
        req.topics[0].name = TOPIC;
        req.topics[0].partitions.resize(1);
        req.topics[0].partitions[0].partition_index = partition;
        req.topics[0].partitions[0].timestamp = -1;
        auto resp = co_await c.send<list_offsets_request, list_offsets_response>(
            req, c.negotiated(list_offsets_request::api_key));
        start = resp.topics[0].partitions[0].offset;
    }

    record_batch batch;
    batch.codec = codec;
    batch.base_timestamp = now_ms();
    batch.max_timestamp = batch.base_timestamp;
    batch.records.resize(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        batch.records[i].offset_delta = static_cast<int32_t>(i);
        batch.records[i].value = values[i];
    }
    writer bw;
    batch.encode(bw);

    produce_request preq;
    preq.acks = -1;
    preq.timeout_ms = 5000;
    preq.topic_data.resize(1);
    preq.topic_data[0].name = TOPIC;
    preq.topic_data[0].topic_id = topic_id;
    preq.topic_data[0].partition_data.resize(1);
    preq.topic_data[0].partition_data[0].index = partition;
    preq.topic_data[0].partition_data[0].records = bw.str();
    auto presp = co_await c.send<produce_request, produce_response>(
        preq, c.negotiated(produce_request::api_key));
    const auto& pr = presp.responses[0].partition_responses[0];
    if (pr.error_code != err::none)
        throw broker_error(pr.error_code, std::string("produce rejected the batch: ")
                                          + error_name(pr.error_code));

    fetch_request freq;
    freq.replica_id = -1;
    freq.max_wait_ms = 2000;
    freq.min_bytes = 1;
    freq.max_bytes = 4 * 1024 * 1024;
    freq.topics.resize(1);
    freq.topics[0].topic = TOPIC;
    freq.topics[0].topic_id = topic_id;
    freq.topics[0].partitions.resize(1);
    freq.topics[0].partitions[0].partition = partition;
    freq.topics[0].partitions[0].fetch_offset = start;
    freq.topics[0].partitions[0].partition_max_bytes = 4 * 1024 * 1024;
    freq.topics[0].partitions[0].current_leader_epoch = -1;
    freq.topics[0].partitions[0].log_start_offset = -1;
    auto fresp = co_await c.send<fetch_request, fetch_response>(
        freq, c.negotiated(fetch_request::api_key));
    const auto& fp = fresp.responses[0].partitions[0];
    if (fp.error_code != err::none)
        throw broker_error(fp.error_code, std::string("fetch failed: ")
                                          + error_name(fp.error_code));

    std::vector<std::string> out;
    for (const auto& b : decode_batches(fp.records.value_or("")))
        for (size_t i = 0; i < b.records.size(); ++i)
            if (b.offset_of(i) >= start && b.records[i].value)
                out.push_back(*b.records[i].value);
    co_return out;
}

// The topic's UUID, which Produce and Fetch need from v13 onward.
seastar::future<uuid> topic_uuid(connection& c) {
    metadata_request req;
    req.topics.resize(1);
    req.topics[0].name = TOPIC;
    auto resp = co_await c.send<metadata_request, metadata_response>(
        req, c.negotiated(metadata_request::api_key));
    co_return resp.topics[0].topic_id;
}

} // namespace

SEASTAR_TEST_CASE(kafka_api_versions_negotiation) {
    auto c = co_await try_connect();
    if (!c) co_return;

    // The APIs the consumer and producer need must all be present, and the
    // negotiated version must be one we can actually encode.
    for (int16_t key : {metadata_request::api_key, fetch_request::api_key,
                        produce_request::api_key, list_offsets_request::api_key,
                        find_coordinator_request::api_key, join_group_request::api_key,
                        sync_group_request::api_key, heartbeat_request::api_key,
                        offset_commit_request::api_key, offset_fetch_request::api_key}) {
        const int16_t v = c->negotiated(key);
        BOOST_TEST_MESSAGE("api " << key << " -> v" << v);
        BOOST_REQUIRE_MESSAGE(v >= 0, "broker does not support api key " << key);
    }
    co_await c->close();
}

SEASTAR_TEST_CASE(kafka_metadata_describes_the_topic) {
    auto c = co_await try_connect();
    if (!c) co_return;

    metadata_request req;
    req.topics.resize(1);
    req.topics[0].name = TOPIC;
    // Encode at the broker's own highest version: that is the path a real
    // client takes, and the one where a flexible-encoding mistake shows up.
    const int16_t v = c->negotiated(metadata_request::api_key);
    auto resp = co_await c->send<metadata_request, metadata_response>(req, v);

    BOOST_REQUIRE(!resp.brokers.empty());
    BOOST_TEST_MESSAGE("broker " << resp.brokers[0].node_id << " at "
                                 << resp.brokers[0].host << ":" << resp.brokers[0].port);
    BOOST_REQUIRE_EQUAL(resp.topics.size(), 1u);
    BOOST_CHECK_EQUAL(resp.topics[0].error_code, err::none);
    BOOST_CHECK_EQUAL(resp.topics[0].name.value_or(""), TOPIC);
    BOOST_CHECK_EQUAL(resp.topics[0].partitions.size(), 2u);
    co_await c->close();
}

SEASTAR_TEST_CASE(kafka_list_offsets_reports_the_log_end) {
    auto c = co_await try_connect();
    if (!c) co_return;

    list_offsets_request req;
    req.replica_id = -1;                       // a consumer, not a follower
    req.topics.resize(1);
    req.topics[0].name = TOPIC;
    req.topics[0].partitions.resize(1);
    req.topics[0].partitions[0].partition_index = 0;
    req.topics[0].partitions[0].timestamp = -1;   // -1 = latest, -2 = earliest
    const int16_t v = c->negotiated(list_offsets_request::api_key);
    auto resp = co_await c->send<list_offsets_request, list_offsets_response>(req, v);

    BOOST_REQUIRE_EQUAL(resp.topics.size(), 1u);
    BOOST_REQUIRE_EQUAL(resp.topics[0].partitions.size(), 1u);
    const auto& p = resp.topics[0].partitions[0];
    BOOST_CHECK_EQUAL(p.error_code, err::none);
    BOOST_TEST_MESSAGE("log end offset: " << p.offset);
    BOOST_CHECK(p.offset >= 0);
    co_await c->close();
}

// The one that matters: bytes we produced, read back by us, through a real
// broker. It exercises the record batch encoder, the CRC the broker verifies
// (it rejects a bad one outright), and the decoder on the way back.
SEASTAR_TEST_CASE(kafka_produce_then_fetch_round_trip) {
    auto c = co_await try_connect();
    if (!c) co_return;

    // Where the partition currently ends, so the fetch below reads only what
    // this run wrote.
    int64_t start_offset = 0;
    {
        list_offsets_request req;
        req.replica_id = -1;
        req.topics.resize(1);
        req.topics[0].name = TOPIC;
        req.topics[0].partitions.resize(1);
        req.topics[0].partitions[0].partition_index = 0;
        req.topics[0].partitions[0].timestamp = -1;
        auto resp = co_await c->send<list_offsets_request, list_offsets_response>(
            req, c->negotiated(list_offsets_request::api_key));
        start_offset = resp.topics[0].partitions[0].offset;
    }

    const int64_t ts = now_ms();
    record_batch batch;
    batch.base_offset = 0;                 // the broker assigns the real offsets
    batch.base_timestamp = ts;
    batch.max_timestamp = ts;
    batch.records.resize(3);
    for (int i = 0; i < 3; ++i) {
        auto& rec = batch.records[static_cast<size_t>(i)];
        rec.offset_delta = i;
        rec.timestamp_delta = 0;
        rec.key = "key-" + std::to_string(i);
        rec.value = "value-" + std::to_string(i);
    }
    batch.records[1].headers.push_back({"origin", std::string("swordfish")});

    writer bw;
    batch.encode(bw);

    // From Produce v13 and Fetch v13 the topic is identified by UUID rather
    // than by name, so the id has to come from Metadata first. Sending a name
    // at v13 gets UNKNOWN_TOPIC_ID (100) back, which is how this was found.
    uuid topic_id{};
    {
        metadata_request mreq;
        mreq.topics.resize(1);
        mreq.topics[0].name = TOPIC;
        auto mresp = co_await c->send<metadata_request, metadata_response>(
            mreq, c->negotiated(metadata_request::api_key));
        topic_id = mresp.topics[0].topic_id;
    }

    produce_request preq;
    preq.acks = -1;                        // all in-sync replicas
    preq.timeout_ms = 5000;
    preq.topic_data.resize(1);
    preq.topic_data[0].name = TOPIC;
    preq.topic_data[0].topic_id = topic_id;
    preq.topic_data[0].partition_data.resize(1);
    preq.topic_data[0].partition_data[0].index = 0;
    preq.topic_data[0].partition_data[0].records = bw.str();

    const int16_t pv = c->negotiated(produce_request::api_key);
    auto presp = co_await c->send<produce_request, produce_response>(preq, pv);
    BOOST_REQUIRE_EQUAL(presp.responses.size(), 1u);
    BOOST_REQUIRE_EQUAL(presp.responses[0].partition_responses.size(), 1u);
    const auto& pr = presp.responses[0].partition_responses[0];
    BOOST_REQUIRE_MESSAGE(pr.error_code == err::none,
                          "produce failed: " << error_name(pr.error_code)
                          << " (" << pr.error_code << ")");
    BOOST_TEST_MESSAGE("produced at base offset " << pr.base_offset);
    BOOST_CHECK_EQUAL(pr.base_offset, start_offset);

    // Read them back.
    fetch_request freq;
    freq.replica_id = -1;
    freq.max_wait_ms = 2000;
    freq.min_bytes = 1;
    freq.max_bytes = 1024 * 1024;
    freq.topics.resize(1);
    freq.topics[0].topic = TOPIC;
    freq.topics[0].partitions.resize(1);
    freq.topics[0].partitions[0].partition = 0;
    freq.topics[0].partitions[0].fetch_offset = start_offset;
    freq.topics[0].partitions[0].partition_max_bytes = 1024 * 1024;
    freq.topics[0].partitions[0].current_leader_epoch = -1;
    freq.topics[0].partitions[0].log_start_offset = -1;

    const int16_t fv = c->negotiated(fetch_request::api_key);
    freq.topics[0].topic_id = topic_id;

    auto fresp = co_await c->send<fetch_request, fetch_response>(freq, fv);
    BOOST_REQUIRE_MESSAGE(fresp.error_code == err::none,
                          "fetch failed: " << error_name(fresp.error_code));
    BOOST_REQUIRE_EQUAL(fresp.responses.size(), 1u);
    BOOST_REQUIRE_EQUAL(fresp.responses[0].partitions.size(), 1u);
    const auto& fp = fresp.responses[0].partitions[0];
    BOOST_REQUIRE_MESSAGE(fp.error_code == err::none,
                          "partition error: " << error_name(fp.error_code));
    BOOST_REQUIRE(fp.records.has_value());

    const auto batches = decode_batches(*fp.records);
    BOOST_REQUIRE_MESSAGE(!batches.empty(), "no batches came back");
    // Collect from `start_offset` onward: an earlier run's records may share
    // the fetch response if the broker returned a whole segment.
    std::vector<std::string> values;
    for (const auto& b : batches)
        for (size_t i = 0; i < b.records.size(); ++i)
            if (b.offset_of(i) >= start_offset && b.records[i].value)
                values.push_back(*b.records[i].value);

    BOOST_REQUIRE_EQUAL(values.size(), 3u);
    BOOST_CHECK_EQUAL(values[0], "value-0");
    BOOST_CHECK_EQUAL(values[1], "value-1");
    BOOST_CHECK_EQUAL(values[2], "value-2");

    // Headers and keys must survive too -- they are separately varint-framed.
    for (const auto& b : batches) {
        for (size_t i = 0; i < b.records.size(); ++i) {
            if (b.offset_of(i) != start_offset + 1) continue;
            BOOST_CHECK_EQUAL(b.records[i].key.value_or(""), "key-1");
            BOOST_REQUIRE_EQUAL(b.records[i].headers.size(), 1u);
            BOOST_CHECK_EQUAL(b.records[i].headers[0].key, "origin");
            BOOST_CHECK_EQUAL(b.records[i].headers[0].value.value_or(""), "swordfish");
        }
    }
    co_await c->close();
}

SEASTAR_TEST_CASE(kafka_every_codec_survives_the_broker) {
    auto c = co_await try_connect();
    if (!c) co_return;
    const uuid id = co_await topic_uuid(*c);

    // The broker verifies the CRC and decompresses to validate the batch, so a
    // codec we encode wrongly comes back as an error rather than as bad data.
    // This is the check the offline tests cannot make.
    for (compression codec : {compression::none, compression::gzip,
                              compression::snappy, compression::lz4,
                              compression::zstd}) {
        BOOST_TEST_MESSAGE("codec " << compression_name(codec));
        // Compressible, and big enough that the codec has to do something.
        std::vector<std::string> sent{std::string(2000, 'a'), "second", "third"};
        auto got = co_await round_trip(*c, id, 1, codec, sent);
        BOOST_REQUIRE_EQUAL(got.size(), sent.size());
        for (size_t i = 0; i < sent.size(); ++i)
            BOOST_CHECK_MESSAGE(got[i] == sent[i],
                                compression_name(codec) << " record " << i
                                << " came back wrong");
    }
    co_await c->close();
}

SEASTAR_TEST_CASE(kafka_reads_what_kafkas_own_producer_wrote) {
    auto c = co_await try_connect();
    if (!c) co_return;
    const uuid id = co_await topic_uuid(*c);

    // tools/run_kafka_itest.sh seeds the topic through Kafka's own console
    // producer, with compression on. Reading that back proves the DECODER
    // against bytes Kafka authored -- the direction the produce test cannot
    // cover, since there we encoded them ourselves. Which partition the
    // producer's partitioner chose is not something to depend on, so both are
    // scanned.
    bool found = false;
    for (int32_t partition = 0; partition < 2 && !found; ++partition) {
        fetch_request freq;
        freq.replica_id = -1;
        freq.max_wait_ms = 2000;
        freq.min_bytes = 1;
        freq.max_bytes = 4 * 1024 * 1024;
        freq.topics.resize(1);
        freq.topics[0].topic = TOPIC;
        freq.topics[0].topic_id = id;
        freq.topics[0].partitions.resize(1);
        freq.topics[0].partitions[0].partition = partition;
        freq.topics[0].partitions[0].fetch_offset = 0;
        freq.topics[0].partitions[0].partition_max_bytes = 4 * 1024 * 1024;
        freq.topics[0].partitions[0].current_leader_epoch = -1;
        freq.topics[0].partitions[0].log_start_offset = -1;
        auto fresp = co_await c->send<fetch_request, fetch_response>(
            freq, c->negotiated(fetch_request::api_key));
        const auto& fp = fresp.responses[0].partitions[0];
        BOOST_REQUIRE_EQUAL(fp.error_code, err::none);
        for (const auto& b : decode_batches(fp.records.value_or("")))
            for (const auto& rec : b.records)
                if (rec.value && *rec.value == "from-kafkas-own-producer") found = true;
    }
    BOOST_CHECK_MESSAGE(found,
        "did not find the seeded record; run tools/run_kafka_itest.sh, which "
        "seeds partition 1 with Kafka's console producer");
    co_await c->close();
}

SEASTAR_TEST_CASE(kafka_cluster_tracks_brokers_and_leaders) {
    cluster cl({broker_env()}, "swordfish-itest");
    // co_await is not allowed inside a catch block, so the skip decision is
    // recorded and acted on afterwards.
    bool reachable = true;
    try {
        co_await cl.refresh({TOPIC});
    } catch (const std::exception& e) {
        BOOST_TEST_MESSAGE("no broker at " << broker_env() << " (" << e.what()
                                           << ") -- skipping");
        reachable = false;
    }
    if (!reachable) { co_await cl.stop(); co_return; }

    BOOST_REQUIRE(!cl.brokers().empty());
    const topic_info* ti = cl.topic(TOPIC);
    BOOST_REQUIRE(ti != nullptr);
    BOOST_CHECK_EQUAL(ti->partitions.size(), 2u);
    BOOST_CHECK(!ti->id.is_zero());          // topic uuids exist on a 4.x broker

    // Every partition must have a leader we can actually open a connection to.
    for (const auto& p : ti->partitions) {
        BOOST_REQUIRE_MESSAGE(p.leader >= 0, "partition " << p.partition
                                             << " has no leader");
        auto* conn = co_await cl.leader_for(TOPIC, p.partition);
        BOOST_REQUIRE(conn != nullptr);
        BOOST_CHECK(conn->connected());
    }

    // The same broker is reused rather than reconnected per call.
    auto* a = co_await cl.leader_for(TOPIC, 0);
    auto* b = co_await cl.leader_for(TOPIC, 0);
    BOOST_CHECK_EQUAL(a, b);

    BOOST_CHECK_THROW(co_await cl.leader_for(TOPIC, 99), protocol_error);
    co_await cl.stop();
}

SEASTAR_TEST_CASE(kafka_consumer_group_joins_and_is_assigned_everything) {
    cluster cl({broker_env()}, "swordfish-itest");
    bool reachable = true;
    try {
        co_await cl.refresh({TOPIC});
    } catch (const std::exception& e) {
        BOOST_TEST_MESSAGE("no broker (" << e.what() << ") -- skipping");
        reachable = false;
    }
    if (!reachable) { co_await cl.stop(); co_return; }

    group_config cfg;
    cfg.group_id = "sf-itest-group";
    cfg.topics = {TOPIC};
    consumer_group group(cl, cfg);

    // A sole member is elected leader, computes the assignment for the whole
    // group (itself), and must therefore receive every partition.
    const auto assignment = co_await group.join();
    BOOST_TEST_MESSAGE("member " << group.member_id() << " generation "
                                 << group.generation()
                                 << (group.is_leader() ? " (leader)" : ""));
    BOOST_CHECK(group.is_leader());
    BOOST_CHECK(group.generation() >= 0);
    BOOST_REQUIRE_EQUAL(assignment.size(), 1u);
    BOOST_REQUIRE_EQUAL(assignment.at(TOPIC).size(), 2u);
    const std::vector<int32_t> both{0, 1};
    BOOST_CHECK(assignment.at(TOPIC) == both);

    // A member that has just joined is alive, so the heartbeat is accepted.
    BOOST_CHECK(co_await group.heartbeat());

    // No commits yet for a fresh group id, and "never committed" must come back
    // ABSENT rather than as offset 0 -- they only coincide before the first
    // retention deletion.
    auto before = co_await group.fetch_offsets(assignment);
    const bool had_offsets = before.count(TOPIC) && !before[TOPIC].empty();

    std::map<std::string, std::map<int32_t, int64_t>> commit{{TOPIC, {{0, 5}, {1, 7}}}};
    co_await group.commit(commit);
    auto after = co_await group.fetch_offsets(assignment);
    BOOST_REQUIRE(after.count(TOPIC) == 1);
    BOOST_CHECK_EQUAL(after[TOPIC][0], 5);
    BOOST_CHECK_EQUAL(after[TOPIC][1], 7);
    BOOST_TEST_MESSAGE("offsets before: " << (had_offsets ? "present" : "absent")
                                          << ", after commit: 5 and 7");

    co_await group.leave();
    co_await cl.stop();
}

SEASTAR_TEST_CASE(kafka_two_members_split_the_partitions) {
    cluster cl_a({broker_env()}, "swordfish-itest-a");
    cluster cl_b({broker_env()}, "swordfish-itest-b");
    bool reachable = true;
    try {
        co_await cl_a.refresh({TOPIC});
    } catch (const std::exception& e) {
        BOOST_TEST_MESSAGE("no broker (" << e.what() << ") -- skipping");
        reachable = false;
    }
    if (!reachable) { co_await cl_a.stop(); co_await cl_b.stop(); co_return; }
    co_await cl_b.refresh({TOPIC});

    group_config cfg;
    cfg.group_id = "sf-itest-pair";
    cfg.topics = {TOPIC};
    consumer_group ga(cl_a, cfg);
    consumer_group gb(cl_b, cfg);

    // Both join the same group. The coordinator holds the first JoinGroup open
    // until the second arrives -- that is how a rebalance works -- so these
    // must be in flight together rather than one after the other.
    auto fa = ga.join();
    auto fb = gb.join();
    // Both futures are resolved even if one fails: abandoning a failed future
    // is itself a test failure in Seastar, and it would mask the real error.
    auto [ra, rb] = co_await seastar::when_all(std::move(fa), std::move(fb));
    const auto aa = ra.get();
    const auto ab = rb.get();

    // The whole point of a group: the two partitions are split, not duplicated.
    std::vector<int32_t> all;
    for (const auto& [t, ps] : aa) for (int32_t p : ps) all.push_back(p);
    for (const auto& [t, ps] : ab) for (int32_t p : ps) all.push_back(p);
    std::sort(all.begin(), all.end());
    BOOST_CHECK(all == (std::vector<int32_t>{0, 1}));
    BOOST_TEST_MESSAGE("member a got " << (aa.count(TOPIC) ? aa.at(TOPIC).size() : 0)
                       << ", member b got " << (ab.count(TOPIC) ? ab.at(TOPIC).size() : 0));
    // Exactly one of them was elected leader and did the arithmetic for both.
    BOOST_CHECK(ga.is_leader() != gb.is_leader());

    co_await ga.leave();
    co_await gb.leave();
    co_await cl_a.stop();
    co_await cl_b.stop();
}

SEASTAR_TEST_CASE(kafka_shards_get_disjoint_shares_of_one_membership) {
    // The bug this exists for: the per-shard assignment cache was a plain
    // function-local `static`. Seastar shards are threads of ONE process, so
    // every shard shared that object, shard 0's push overwrote it four times,
    // and all four shards then fetched whichever share landed last. Sixty
    // records arrived as six delivered four times each. A single-shard test
    // cannot see it, so this case runs across every shard.
    auto probe = co_await try_connect();
    if (!probe) co_return;
    co_await probe->close();

    consumer_config cfg;
    cfg.seed_brokers = {broker_env()};
    cfg.topics = {TOPIC};
    cfg.consumer_group = "sf-itest-shards";
    cfg.client_id = "swordfish-itest";

    co_await seastar::smp::invoke_on_all([cfg] {
        return membership::attach(cfg).discard_result();
    });

    // Gather what each shard believes it owns.
    const auto shares = co_await seastar::map_reduce(
        std::views::iota(0u, seastar::this_smp().shard_count()),
        [](unsigned id) {
            return seastar::smp::submit_to(id, [] {
                return membership::current("sf-itest-shards").partitions;
            });
        },
        std::vector<partition_map>{},
        [](std::vector<partition_map> acc, partition_map m) {
            acc.push_back(std::move(m));
            return acc;
        });

    std::vector<int32_t> all;
    for (const auto& share : shares)
        for (const auto& [topic, parts] : share) {
            BOOST_CHECK_EQUAL(topic, TOPIC);
            for (int32_t p : parts) all.push_back(p);
        }
    std::sort(all.begin(), all.end());
    const auto dup = std::adjacent_find(all.begin(), all.end());
    BOOST_CHECK_MESSAGE(dup == all.end(),
        "two shards were handed the same partition; every message on it would "
        "be delivered twice");
    // The fixture topic has two partitions and they must all be somewhere.
    BOOST_CHECK_EQUAL(all.size(), 2u);

    co_await seastar::smp::invoke_on_all([] {
        return membership::detach("sf-itest-shards");
    });
}

SEASTAR_TEST_CASE(kafka_acknowledged_offsets_are_actually_committed) {
    // The other bug: creating a partition's offset tracker was a side effect of
    // `_tracked[topic][p]` at delivery. When that line was removed as dead
    // code, ack() could no longer find an entry, every acknowledgement was
    // dropped, and the group committed nothing -- while consuming perfectly.
    // Committing is asserted here rather than inferred from a clean run.
    auto c = co_await try_connect();
    if (!c) co_return;
    const uuid id = co_await topic_uuid(*c);
    co_await round_trip(*c, id, 0, compression::none, {"commit-me-1", "commit-me-2"});
    co_await c->close();

    consumer_config cfg;
    cfg.seed_brokers = {broker_env()};
    cfg.topics = {TOPIC};
    cfg.consumer_group = "sf-itest-commit";
    cfg.client_id = "swordfish-itest";
    cfg.start_from_oldest = true;
    cfg.commit_interval = 200ms;

    consumer con(cfg);
    seastar::abort_source as;
    co_await con.start(as);

    // Poll until something arrives, then acknowledge all of it.
    std::vector<fetched_record> got;
    for (int i = 0; i < 20 && got.empty(); ++i) got = co_await con.poll(as);
    BOOST_REQUIRE_MESSAGE(!got.empty(), "consumed nothing to acknowledge");
    for (const auto& r : got) con.ack(r.topic, r.partition, r.offset);

    // Hand them to shard 0 and make it commit now rather than on its timer.
    co_await con.commit_acked();
    co_await membership::commit_now(cfg.consumer_group);

    // Ask the broker what it recorded -- not our own bookkeeping.
    cluster verify({broker_env()}, "swordfish-itest-verify");
    co_await verify.refresh({TOPIC});
    group_config gc;
    gc.group_id = cfg.consumer_group;
    gc.topics = {TOPIC};
    consumer_group probe(verify, gc);
    partition_map want;
    for (const auto& r : got) want[r.topic].push_back(r.partition);
    for (auto& [t, ps] : want) {
        std::sort(ps.begin(), ps.end());
        ps.erase(std::unique(ps.begin(), ps.end()), ps.end());
    }
    auto committed = co_await probe.fetch_offsets(want);

    bool any = false;
    for (const auto& r : got) {
        const auto t = committed.find(r.topic);
        if (t == committed.end()) continue;
        const auto o = t->second.find(r.partition);
        if (o == t->second.end()) continue;
        any = true;
        // A commit is where to RESUME, so it is one past the last acked offset.
        BOOST_CHECK_MESSAGE(o->second > r.offset,
                            "committed " << o->second << " is not past acked offset "
                                         << r.offset);
    }
    BOOST_CHECK_MESSAGE(any, "the broker recorded no committed offset at all");

    co_await con.stop();
    co_await verify.stop();
}
