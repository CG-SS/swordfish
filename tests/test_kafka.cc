// Kafka wire protocol: primitives, generated codecs, and framing.
//
// There is no broker in this environment, so conformance is asserted against
// byte sequences derived BY HAND from the schemas in third_party/kafka-protocol
// -- the same files the generator reads, read independently. A round-trip test
// alone would only prove the codec agrees with itself, which is exactly the
// failure mode a generated codec is prone to.
#include "swordfish/kafka/protocol.hh"
#include "swordfish/kafka/consumer_group.hh"
#include "swordfish/kafka/records.hh"
#include "swordfish/kafka/wire.hh"
#include "messages.hh"

#include <catch_amalgamated.hpp>

#include <string>

using namespace sf::kafka;

namespace {

// Hex, so an expected frame can be written the way the protocol spec shows it.
std::string hex(std::string_view raw) {
    static const char* H = "0123456789abcdef";
    std::string out;
    out.reserve(raw.size() * 2);
    for (unsigned char c : raw) { out += H[c >> 4]; out += H[c & 15]; }
    return out;
}

std::string unhex(std::string_view h) {
    auto nib = [](char c) { return c <= '9' ? c - '0' : (c | 32) - 'a' + 10; };
    std::string out;
    for (size_t i = 0; i + 1 < h.size(); i += 2)
        out += static_cast<char>(nib(h[i]) * 16 + nib(h[i + 1]));
    return out;
}

} // namespace

TEST_CASE("kafka wire: integers and varints", "[kafka][wire]") {
    writer w;
    w.i16(-2); w.i32(-2); w.i64(-2); w.u32(0xDEADBEEF);
    CHECK(hex(w.view()) == "fffe" "fffffffe" "fffffffffffffffe" "deadbeef");

    // Unsigned LEB128, low group first: 300 is 0b100101100 -> 0xAC 0x02.
    writer v;
    v.uvarint(0); v.uvarint(1); v.uvarint(127); v.uvarint(128); v.uvarint(300);
    CHECK(hex(v.view()) == "00" "01" "7f" "8001" "ac02");

    // Zigzag maps small negatives to small unsigned values, which is the point
    // inside record batches where deltas are usually tiny.
    writer z;
    z.varint(0); z.varint(-1); z.varint(1); z.varint(-2); z.varint(2);
    CHECK(hex(z.view()) == "00" "01" "02" "03" "04");

    reader r(z.view());
    CHECK(r.varint() == 0);
    CHECK(r.varint() == -1);
    CHECK(r.varint() == 1);
    CHECK(r.varint() == -2);
    CHECK(r.varint() == 2);
    CHECK(r.empty());
}

TEST_CASE("kafka wire: classic and flexible strings differ", "[kafka][wire]") {
    // Classic: int16 length, -1 for null. Flexible: uvarint of length+1, 0 null.
    writer c;
    c.string("abc", false);
    c.nullable_string(std::nullopt, false);
    CHECK(hex(c.view()) == "0003" "616263" "ffff");

    writer f;
    f.string("abc", true);
    f.nullable_string(std::nullopt, true);
    CHECK(hex(f.view()) == "04" "616263" "00");

    // An empty array and a null array are different values on the wire, and the
    // reader has to keep them apart: -1 versus 0 entries.
    writer a;
    a.array_len(0, false); a.null_array(false);
    a.array_len(0, true);  a.null_array(true);
    CHECK(hex(a.view()) == "00000000" "ffffffff" "01" "00");

    reader r(a.view());
    CHECK(r.array_len(false) == 0);
    CHECK(r.array_len(false) == -1);
    CHECK(r.array_len(true) == 0);
    CHECK(r.array_len(true) == -1);
}

TEST_CASE("kafka wire: truncation is an error, not a silent zero", "[kafka][wire]") {
    reader r(std::string_view("\x00\x01", 2));
    CHECK_THROWS_AS(r.i32(), protocol_error);
    reader s(std::string_view("\x00\x05z", 3));
    CHECK_THROWS_AS(s.string(false), protocol_error);
}

TEST_CASE("kafka wire: unknown tagged fields are skipped", "[kafka][wire]") {
    // Forward compatibility: a newer broker may send tags we do not know, and
    // skipping them by their declared size is both correct and required.
    writer w;
    w.uvarint(2);                       // two tagged fields
    w.uvarint(99); w.uvarint(3); w.raw("abc");
    w.uvarint(7);  w.uvarint(1); w.raw("z");
    w.i32(0x1234);                      // a field after the section
    reader r(w.view());
    r.skip_tags();
    CHECK(r.i32() == 0x1234);
    CHECK(r.empty());
}

TEST_CASE("kafka: ApiVersions request framing, byte for byte", "[kafka][protocol]") {
    // v0 is classic, so the request header is v1: api key, api version,
    // correlation id, then a classic nullable client id. The v0 body is empty --
    // ClientSoftwareName only exists from v3.
    api_versions_request req;
    const std::string f0 = encode_request(req, 0, 1, std::string("sf"));
    CHECK(hex(f0) == "0000000c"          // length = 12
                     "0012"              // api key 18
                     "0000"              // api version 0
                     "00000001"          // correlation id
                     "0002" "7366");     // client id "sf"

    // v3 is the first flexible version: the header gains a tagged section, and
    // the body's strings become compact. The client id stays a CLASSIC string --
    // the header's own encoding does not switch.
    req.client_software_name = "swordfish";
    req.client_software_version = "0.1";
    const std::string f3 = encode_request(req, 3, 7, std::string("sf"));
    CHECK(hex(f3) == "0000001c"          // length = 28
                     "0012" "0003" "00000007"
                     "0002" "7366"       // client id, classic
                     "00"                // header tagged fields: none
                     "0a" "73776f726466697368"    // compact "swordfish", len+1
                     "04" "302e31"       // compact "0.1"
                     "00");              // body tagged fields: none
}

TEST_CASE("kafka: ApiVersions response uses a v0 header at every version",
          "[kafka][protocol]") {
    // The exception in response_header_version(): this call is how the client
    // learns which versions the broker supports, so it cannot already know
    // whether the response would be flexible. The protocol pins the header to v0
    // to break the circularity. Decoding it as v1 would eat a byte of the body.
    CHECK(response_header_version(api::api_versions, true) == 0);
    CHECK(response_header_version(api::api_versions, false) == 0);
    CHECK(response_header_version(api::metadata, true) == 1);
    CHECK(response_header_version(api::metadata, false) == 0);

    // A v3 response: correlation id, then a FLEXIBLE body.
    const std::string frame = unhex(
        "0000002a"          // correlation id 42 -- header v0, no tags
        "0000"              // error code 0
        "02"                // api keys: compact array of 1
        "0012" "0000" "0004" "00"   // key 18, min 0, max 4, tags
        "00000000"          // throttle time
        "00");              // response tagged fields
    api_versions_response resp;
    const int32_t corr = decode_response(frame, 3, resp);
    CHECK(corr == 42);
    CHECK(resp.error_code == 0);
    REQUIRE(resp.api_keys.size() == 1);
    CHECK(resp.api_keys[0].api_key == 18);
    CHECK(resp.api_keys[0].max_version == 4);
}

TEST_CASE("kafka: Metadata v1 response decodes, classic encoding",
          "[kafka][protocol]") {
    const std::string frame = unhex(
        "00000005"                      // correlation id 5 (header v0)
        "00000001"                      // brokers: 1
        "00000001"                      // node id 1
        "0009" "6c6f63616c686f7374"     // host "localhost"
        "00002382"                      // port 9090
        "ffff"                          // rack: null
        "00000001"                      // controller id 1
        "00000001"                      // topics: 1
        "0000"                          // error code
        "0004" "64656d6f"               // name "demo"
        "00"                            // is_internal false
        "00000001"                      // partitions: 1
        "0000"                          // error code
        "00000000"                      // partition index 0
        "00000001"                      // leader 1
        "00000001" "00000001"           // replicas: [1]
        "00000001" "00000001");         // isr: [1]
    metadata_response resp;
    CHECK(decode_response(frame, 1, resp) == 5);
    REQUIRE(resp.brokers.size() == 1);
    CHECK(resp.brokers[0].node_id == 1);
    CHECK(resp.brokers[0].host == "localhost");
    CHECK(resp.brokers[0].port == 9090);
    CHECK_FALSE(resp.brokers[0].rack.has_value());
    CHECK(resp.controller_id == 1);
    REQUIRE(resp.topics.size() == 1);
    CHECK(resp.topics[0].name == "demo");
    REQUIRE(resp.topics[0].partitions.size() == 1);
    CHECK(resp.topics[0].partitions[0].leader_id == 1);
    CHECK(resp.topics[0].partitions[0].replica_nodes == std::vector<int32_t>{1});
    // throttle_time_ms only exists from v3, so it keeps its default here.
    CHECK(resp.throttle_time_ms == 0);
}

TEST_CASE("kafka: a length off the wire cannot ask for an allocation",
          "[kafka][protocol]") {
    // Both of these drive a resize/reserve straight from a number the peer sent.
    // Unchecked, a corrupt frame, a version mismatch that desynchronises the
    // reader, or a hostile peer asks for up to two billion elements before a
    // single one is read. Every array element in every Kafka schema is at least
    // one byte, so a length past the bytes remaining cannot be honest.
    SECTION("array_len") {
        // int32 array length of 2,000,000,000 followed by four bytes.
        std::string buf;
        buf += '\x77'; buf += '\x35'; buf += '\x94'; buf += '\x00';   // 0x77359400
        buf += "abcd";
        reader r(buf);
        CHECK_THROWS_AS(r.array_len(false), protocol_error);
    }
    SECTION("array_len accepts an honest length") {
        std::string buf;
        buf += '\x00'; buf += '\x00'; buf += '\x00'; buf += '\x02';
        buf += "ab";
        reader r(buf);
        CHECK(r.array_len(false) == 2);
    }
    SECTION("array_len still distinguishes null from empty") {
        std::string nul;
        nul += '\xff'; nul += '\xff'; nul += '\xff'; nul += '\xff';   // -1
        reader rn(nul);
        CHECK(rn.array_len(false) == -1);
        std::string empty(4, '\0');
        reader re(empty);
        CHECK(re.array_len(false) == 0);
    }
}

TEST_CASE("kafka: generated codecs round-trip across versions",
          "[kafka][protocol]") {
    // Breadth rather than depth: every version of a message must survive
    // encode-then-decode, which catches a field guarded by the wrong version
    // range in one direction only.
    metadata_request req;
    req.topics.resize(1);
    req.topics[0].name = "demo";
    for (int16_t v = 0; v <= metadata_request::max_version; ++v) {
        writer w;
        req.encode(w, v);
        reader r(w.view());
        metadata_request back;
        back.decode(r, v);
        INFO("metadata_request version " << v);
        CHECK(r.empty());
        REQUIRE(back.topics.size() == 1);
        CHECK(back.topics[0].name == "demo");
    }

    fetch_request fr;
    fr.max_wait_ms = 500;
    fr.min_bytes = 1;
    fr.topics.resize(1);
    fr.topics[0].topic = "demo";
    fr.topics[0].partitions.resize(1);
    fr.topics[0].partitions[0].partition = 0;
    fr.topics[0].partitions[0].fetch_offset = 17;
    for (int16_t v = 4; v <= fetch_request::max_version; ++v) {
        writer w;
        fr.encode(w, v);
        reader r(w.view());
        fetch_request back;
        back.decode(r, v);
        INFO("fetch_request version " << v);
        CHECK(r.empty());
        REQUIRE(back.topics.size() == 1);
        REQUIRE(back.topics[0].partitions.size() == 1);
        CHECK(back.topics[0].partitions[0].fetch_offset == 17);
        CHECK(back.max_wait_ms == 500);
    }
}

TEST_CASE("kafka: CRC-32C against the standard check vectors", "[kafka][records]") {
    // Castagnoli, the polynomial Kafka uses for record batches -- NOT the
    // CRC-32 of zlib. Getting the two confused produces batches every broker
    // rejects, so these are pinned against the published vectors.
    CHECK(crc32c("") == 0x00000000u);
    CHECK(crc32c("a") == 0xc1d04330u);
    CHECK(crc32c("123456789") == 0xe3069283u);           // the standard check value
    CHECK(crc32c(std::string(32, '\0')) == 0x8a9136aau);
    CHECK(crc32c(std::string(32, '\xff')) == 0x62a8ab43u);

    // Seeding must chain, or the hardware path cannot process a buffer in
    // pieces the way the software path does.
    const std::string all = "123456789";
    CHECK(crc32c(all.substr(4), crc32c(all.substr(0, 4))) == crc32c(all));
}

TEST_CASE("kafka: record batch round-trips", "[kafka][records]") {
    record_batch b;
    b.base_offset = 1000;
    b.base_timestamp = 1700000000000LL;
    b.max_timestamp = 1700000000042LL;
    b.records.resize(3);
    for (int i = 0; i < 3; ++i) {
        b.records[static_cast<size_t>(i)].offset_delta = i;
        b.records[static_cast<size_t>(i)].timestamp_delta = i * 14;
        b.records[static_cast<size_t>(i)].value = "value-" + std::to_string(i);
    }
    b.records[0].key = "k0";
    b.records[1].headers.push_back({"trace", std::string("abc")});
    b.records[2].headers.push_back({"nulled", std::nullopt});

    writer w;
    b.encode(w);
    reader r(w.view());
    const record_batch back = record_batch::decode(r);

    CHECK(r.empty());
    CHECK(back.base_offset == 1000);
    CHECK(back.base_timestamp == 1700000000000LL);
    REQUIRE(back.records.size() == 3);
    CHECK(back.records[0].key == std::optional<std::string>("k0"));
    CHECK_FALSE(back.records[1].key.has_value());
    CHECK(back.records[2].value == std::optional<std::string>("value-2"));
    CHECK(back.records[1].headers.at(0).key == "trace");
    CHECK(back.records[2].headers.at(0).value == std::nullopt);
    // The absolute offset is what an ack has to commit.
    CHECK(back.offset_of(0) == 1000);
    CHECK(back.offset_of(2) == 1002);
}

TEST_CASE("kafka: a corrupt batch is rejected by its CRC", "[kafka][records]") {
    record_batch b;
    b.records.resize(1);
    b.records[0].value = "payload";
    writer w;
    b.encode(w);
    std::string bytes = w.take();
    // Flip a bit inside the CRC-covered region: base_offset(8) length(4)
    // leader_epoch(4) magic(1) crc(4) = 21 bytes of preamble.
    bytes[25] = static_cast<char>(bytes[25] ^ 0x01);
    reader r(bytes);
    CHECK_THROWS_AS(record_batch::decode(r), protocol_error);
}

TEST_CASE("kafka: every compression codec round-trips a batch",
          "[kafka][records][compression]") {
    // Kafka's gzip is a gzip stream and its lz4 is the lz4 FRAME format, not the
    // raw block format -- both are easy to get wrong and neither fails loudly.
    const compression all[] = {compression::none, compression::gzip,
                              compression::snappy, compression::lz4,
                              compression::zstd};
    // Repetitive enough that a codec that silently did nothing would stand out.
    const std::string payload(4096, 'x');
    for (compression c : all) {
        INFO("codec " << compression_name(c));
        CHECK(decompress(c, compress(c, payload)) == payload);

        record_batch b;
        b.codec = c;
        b.records.resize(2);
        b.records[0].value = payload;
        b.records[1].offset_delta = 1;
        b.records[1].value = "second";
        writer w;
        b.encode(w);
        reader r(w.view());
        const record_batch back = record_batch::decode(r);
        CHECK(back.codec == c);
        REQUIRE(back.records.size() == 2);
        CHECK(back.records[0].value == std::optional<std::string>(payload));
        CHECK(back.records[1].value == std::optional<std::string>("second"));
    }
}

TEST_CASE("kafka: a fetch blob holds several batches, the last maybe truncated",
          "[kafka][records]") {
    writer w;
    for (int i = 0; i < 3; ++i) {
        record_batch b;
        b.base_offset = i * 10;
        b.records.resize(1);
        b.records[0].value = "batch-" + std::to_string(i);
        b.encode(w);
    }
    const std::string blob = w.take();
    CHECK(decode_batches(blob).size() == 3);

    // The broker cuts the response at max_bytes, so a trailing partial batch is
    // normal. Dropping it is correct; throwing would stall the partition.
    const auto partial = decode_batches(blob.substr(0, blob.size() - 5));
    CHECK(partial.size() == 2);
    CHECK(partial[1].base_offset == 10);
}

// ---- consumer protocol blobs and assignment strategies ---------------------
// These need no broker: the blobs are pure encoding, and the strategies are
// pure arithmetic that every member of a group must compute identically.

TEST_CASE("kafka: consumer protocol blobs round-trip", "[kafka][group]") {
    // JoinGroup and SyncGroup carry these as opaque bytes. They are versioned
    // INSIDE themselves with a leading int16 that is not one of the schema's
    // fields, and they use the classic encoding even when the enclosing request
    // is flexible.
    const std::vector<std::string> topics{"orders", "shipments"};
    const auto sub = encode_subscription(topics);
    CHECK(decode_subscription(sub) == topics);
    CHECK(static_cast<unsigned char>(sub[1]) == 0);   // protocol version 0

    partition_map pm{{"orders", {0, 2, 4}}, {"shipments", {1}}};
    const auto blob = encode_assignment(pm);
    CHECK(decode_assignment(blob) == pm);

    // No assignment is a normal outcome for a member that joined a group with
    // more members than partitions, not a decoding failure.
    CHECK(decode_assignment("").empty());
}

TEST_CASE("kafka: range assignment gives contiguous runs", "[kafka][group]") {
    std::vector<joined_member> members{{"m1", {"t"}}, {"m2", {"t"}}, {"m3", {"t"}}};
    const std::map<std::string, int32_t> counts{{"t", 7}};

    // 7 partitions over 3 members: the first 7 % 3 == 1 member takes an extra.
    CHECK(assign_range(members, "m1", counts) == partition_map{{"t", {0, 1, 2}}});
    CHECK(assign_range(members, "m2", counts) == partition_map{{"t", {3, 4}}});
    CHECK(assign_range(members, "m3", counts) == partition_map{{"t", {5, 6}}});

    // Every partition assigned exactly once is the property that matters; a
    // strategy that drops one silently stalls a partition forever.
    std::vector<int32_t> all;
    for (const auto& m : members)
        for (int32_t p : assign_range(members, m.member_id, counts).at("t"))
            all.push_back(p);
    std::sort(all.begin(), all.end());
    CHECK(all == std::vector<int32_t>{0, 1, 2, 3, 4, 5, 6});

    // More members than partitions: the surplus get nothing, which is legal.
    std::vector<joined_member> many{{"a", {"t"}}, {"b", {"t"}}, {"c", {"t"}}};
    const std::map<std::string, int32_t> one{{"t", 1}};
    CHECK(assign_range(many, "a", one) == partition_map{{"t", {0}}});
    CHECK(assign_range(many, "c", one).empty());
}

TEST_CASE("kafka: roundrobin spreads across topics", "[kafka][group]") {
    std::vector<joined_member> members{{"m1", {"a", "b"}}, {"m2", {"a", "b"}}};
    const std::map<std::string, int32_t> counts{{"a", 3}, {"b", 1}};

    // One cursor runs across every topic-partition, so the odd partition of "a"
    // and the single partition of "b" do not both land on the same member --
    // which is exactly what range assignment would do.
    const auto m1 = assign_roundrobin(members, "m1", counts);
    const auto m2 = assign_roundrobin(members, "m2", counts);
    size_t n1 = 0, n2 = 0;
    for (const auto& [t, ps] : m1) n1 += ps.size();
    for (const auto& [t, ps] : m2) n2 += ps.size();
    CHECK(n1 + n2 == 4);
    CHECK(n1 == 2);
    CHECK(n2 == 2);
}

TEST_CASE("kafka: a member only gets topics it subscribed to", "[kafka][group]") {
    // Members of one group may subscribe to different topics, and the leader
    // assigns for all of them. Handing a member a topic it never asked for is a
    // bug that only shows up in a mixed group.
    std::vector<joined_member> members{{"m1", {"a"}}, {"m2", {"b"}}};
    const std::map<std::string, int32_t> counts{{"a", 2}, {"b", 2}};
    const auto m1 = assign_range(members, "m1", counts);
    const auto m2 = assign_range(members, "m2", counts);
    CHECK(m1 == partition_map{{"a", {0, 1}}});
    CHECK(m2 == partition_map{{"b", {0, 1}}});
}

// ---- partitioning ----------------------------------------------------------
#include "swordfish/kafka/producer.hh"

TEST_CASE("kafka: murmur2 matches Kafka's own Utils.murmur2", "[kafka][producer]") {
    // These are not invented: they were produced by running
    // org.apache.kafka.common.utils.Utils.murmur2 from kafka-clients-4.3.1.jar
    // over the same inputs. Key affinity is a CROSS-CLIENT contract -- a record
    // written by a Java producer and one written here must land on the same
    // partition -- so agreeing with Kafka is the whole requirement, and a hash
    // that is merely self-consistent silently breaks it.
    CHECK(murmur2("") == 275646681);
    CHECK(murmur2("a") == -1563381124);
    CHECK(murmur2("21") == -973932308);
    CHECK(murmur2("foobar") == -790332482);
    CHECK(murmur2("hello world") == 1221641059);
    CHECK(murmur2("a-little-bit-long-string") == -985981536);
    CHECK(murmur2("a-little-bit-longer-string") == -1486304829);
    CHECK(murmur2("lkjh234lkjsdff88uk") == 844987329);
    CHECK(murmur2("key-0") == 29210041);
    CHECK(murmur2("key-1") == 193331640);
    CHECK(murmur2("key-2") == 852269702);
    CHECK(murmur2("éèê") == -1401640301);          // hashed as UTF-8 bytes
    CHECK(murmur2("0123456789abcdef") == 1438427052);
}

TEST_CASE("kafka: key to partition uses a mask, not abs", "[kafka][producer]") {
    // toPositive() masks the sign bit. abs() would be undefined at INT_MIN and,
    // more to the point, would map some keys elsewhere than Kafka does.
    CHECK(partition_for_key("a", 4) == ((-1563381124 & 0x7fffffff) % 4));
    CHECK(partition_for_key("foobar", 3) == ((-790332482 & 0x7fffffff) % 3));

    // The same key always lands on the same partition, and a single-partition
    // topic is not a special case.
    for (int32_t n : {1, 2, 3, 7, 16}) {
        const int32_t p = partition_for_key("stable-key", n);
        CHECK(p >= 0);
        CHECK(p < n);
        CHECK(partition_for_key("stable-key", n) == p);
    }
    // A degenerate count must not divide by zero.
    CHECK(partition_for_key("k", 0) == 0);
}

// ---- offset tracking -------------------------------------------------------
#include "swordfish/kafka/consumer.hh"

TEST_CASE("kafka: the commit never passes an offset still in flight",
          "[kafka][consumer]") {
    // Delivery is sequential but acknowledgement is not. Committing the highest
    // acked offset would skip anything still in flight below it, and those
    // messages would be lost rather than redelivered on a crash -- the exact
    // failure at-least-once exists to prevent.
    //
    // Every one of these tests DELIVERS before it acks. The versions that did
    // not are why two critical defects survived: acks alone cannot tell the
    // tracker where the partition actually is.
    offset_tracker t;
    for (int64_t o = 0; o <= 4; ++o) t.deliver(o);
    // Delivered and unacked: the resume point is the first of them. Committing
    // it is a no-op for position but pins a fresh group at its start offset, and
    // it can only ever cause redelivery, never a skip.
    REQUIRE(t.committable().has_value());
    CHECK(*t.committable() == 0);

    t.ack(2);                                       // out of order
    REQUIRE(t.committable().has_value());
    CHECK(*t.committable() == 0);                   // 0 is the oldest unfinished

    t.ack(0);
    CHECK(*t.committable() == 1);                   // resume AT 1, still in flight

    t.ack(1);
    CHECK(*t.committable() == 3);                   // 0,1,2 done; 3 is oldest

    t.mark_committed(3);
    CHECK_FALSE(t.committable().has_value());       // nothing new since

    t.ack(4);
    CHECK_FALSE(t.committable().has_value());       // 3 is still outstanding
    t.ack(3);
    CHECK(*t.committable() == 5);                   // all five done, resume past 4
}

TEST_CASE("kafka: a duplicate ack does not move the commit backwards",
          "[kafka][consumer]") {
    offset_tracker t;
    t.deliver(0);
    t.ack(0);
    t.ack(0);                                       // redelivered, or acked twice
    CHECK(*t.committable() == 1);
    t.deliver(1);
    t.ack(1);
    CHECK(*t.committable() == 2);
}

TEST_CASE("kafka: a group resumed from a committed offset still commits",
          "[kafka][consumer]") {
    // REGRESSION. The tracker used to advance a watermark through a run
    // starting at offset 0, so a partition resumed at 100 could never close one
    // and committed NOTHING for the life of the process. Measured against a
    // live broker: run 2 of a group read ten records, acked all ten, and
    // kafka-consumer-groups still reported CURRENT-OFFSET 10 with LAG 10; every
    // restart replayed the same records.
    offset_tracker t;
    t.mark_committed(100);                          // what the group came back with
    CHECK_FALSE(t.committable().has_value());       // nothing new yet

    for (int64_t o = 100; o <= 109; ++o) t.deliver(o);
    for (int64_t o = 100; o <= 109; ++o) t.ack(o);
    REQUIRE(t.committable().has_value());
    CHECK(*t.committable() == 110);
}

TEST_CASE("kafka: a hole in a partition's offsets does not freeze the commit",
          "[kafka][consumer]") {
    // REGRESSION. Offsets are not dense. A compacted topic keeps 0 and then
    // 6..11 with 1..5 cleaned away, and a contiguous run starting at 1 can never
    // close: the group committed 1 and sat at LAG 11 while the reference
    // committed 12. Transaction markers make the same hole on any topic written
    // transactionally.
    offset_tracker t;
    for (int64_t o : {0, 6, 7, 8, 9, 10, 11}) t.deliver(o);
    for (int64_t o : {0, 6, 7, 8, 9, 10, 11}) t.ack(o);
    REQUIRE(t.committable().has_value());
    CHECK(*t.committable() == 12);

    // And a hole with something still in flight below it resumes at that one.
    offset_tracker u;
    for (int64_t o : {0, 1, 2, 4, 5}) u.deliver(o);  // 3 is a transaction marker
    u.ack(0); u.ack(1); u.ack(4); u.ack(5);
    CHECK(*u.committable() == 2);
    u.ack(2);
    CHECK(*u.committable() == 6);
}

TEST_CASE("kafka: nothing delivered and nothing committed is not committable",
          "[kafka][consumer]") {
    // Committing 0 here would rewind a group that has read nothing yet.
    offset_tracker t;
    CHECK_FALSE(t.committable().has_value());
    t.ack(7);                                       // an ack for a record it never had
    CHECK_FALSE(t.committable().has_value());
}

// ---- shard split -----------------------------------------------------------
#include "swordfish/kafka/group_membership.hh"

TEST_CASE("kafka: the shard split covers every partition exactly once",
          "[kafka][sharding]") {
    // One process is ONE group member, and the partitions it is assigned are
    // divided across its shards here. A split that drops a partition stalls it
    // forever; one that overlaps delivers those messages once per shard. Both
    // are silent, so the property is asserted directly.
    const partition_map all{{"a", {0, 1, 2, 3, 4, 5}}, {"b", {0, 1}}};

    for (unsigned shards : {1u, 2u, 3u, 4u, 8u}) {
        std::map<std::string, std::vector<int32_t>> seen;
        for (unsigned s = 0; s < shards; ++s)
            for (const auto& [topic, parts] : shard_share(all, s, shards))
                for (int32_t p : parts) seen[topic].push_back(p);
        for (auto& [topic, parts] : seen) std::sort(parts.begin(), parts.end());
        INFO("shards = " << shards);
        CHECK(seen == all);
    }

    // Four shards over six partitions: 0 and 4 to shard 0, and so on.
    CHECK(shard_share(all, 0, 4) == partition_map{{"a", {0, 4}}, {"b", {0}}});
    CHECK(shard_share(all, 1, 4) == partition_map{{"a", {1, 5}}, {"b", {1}}});
    CHECK(shard_share(all, 2, 4) == partition_map{{"a", {2}}});
    CHECK(shard_share(all, 3, 4) == partition_map{{"a", {3}}});

    // More shards than partitions: the surplus get nothing, which is legal and
    // must not be an empty-but-present topic entry either.
    CHECK(shard_share(partition_map{{"a", {0}}}, 1, 4).empty());
    // A single shard owns everything.
    CHECK(shard_share(all, 0, 1) == all);
}
