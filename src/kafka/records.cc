#include "swordfish/kafka/records.hh"

#include "swordfish/codecs.hh"

#include <array>
#include <cstring>

#if defined(__x86_64__)
#include <nmmintrin.h>
#endif

namespace sf::kafka {

// ---- CRC-32C ---------------------------------------------------------------

namespace {

// Castagnoli, reflected: polynomial 0x1EDC6F41 becomes 0x82F63B78.
constexpr uint32_t CRC32C_POLY = 0x82F63B78u;

std::array<uint32_t, 256> make_table() {
    std::array<uint32_t, 256> t{};
    for (uint32_t i = 0; i < 256; ++i) {
        uint32_t c = i;
        for (int k = 0; k < 8; ++k) c = (c & 1) ? (c >> 1) ^ CRC32C_POLY : c >> 1;
        t[i] = c;
    }
    return t;
}
const std::array<uint32_t, 256>& table() {
    static const std::array<uint32_t, 256> t = make_table();
    return t;
}

uint32_t crc32c_sw(std::string_view d, uint32_t crc) noexcept {
    const auto& t = table();
    crc = ~crc;
    for (unsigned char c : d) crc = t[(crc ^ c) & 0xFF] ^ (crc >> 8);
    return ~crc;
}

#if defined(__x86_64__)
__attribute__((target("sse4.2")))
uint32_t crc32c_hw(std::string_view d, uint32_t crc) noexcept {
    uint32_t c = ~crc;
    const char* p = d.data();
    size_t n = d.size();
    // Eight bytes at a time; the instruction takes a 64-bit operand and
    // returns a 32-bit running value.
    for (; n >= 8; p += 8, n -= 8) {
        uint64_t v;
        std::memcpy(&v, p, 8);
        c = static_cast<uint32_t>(_mm_crc32_u64(c, v));
    }
    for (; n; ++p, --n) c = _mm_crc32_u8(c, static_cast<uint8_t>(*p));
    return ~c;
}

bool have_sse42() {
    static const bool yes = __builtin_cpu_supports("sse4.2");
    return yes;
}
#endif

} // namespace

uint32_t crc32c(std::string_view data, uint32_t seed) noexcept {
#if defined(__x86_64__)
    if (have_sse42()) return crc32c_hw(data, seed);
#endif
    return crc32c_sw(data, seed);
}

// ---- compression -----------------------------------------------------------

// cppcheck-suppress unusedFunction
//   Used by the Kafka tests, which are not in this analysis's scope.
const char* compression_name(compression c) noexcept {
    switch (c) {
    case compression::none:   return "none";
    case compression::gzip:   return "gzip";
    case compression::snappy: return "snappy";
    case compression::lz4:    return "lz4";
    case compression::zstd:   return "zstd";
    }
    return "unknown";
}

compression compression_from_name(std::string_view n) {
    if (n == "none")   return compression::none;
    if (n == "gzip")   return compression::gzip;
    if (n == "snappy") return compression::snappy;
    if (n == "lz4")    return compression::lz4;
    if (n == "zstd")   return compression::zstd;
    throw protocol_error("unknown compression codec: " + std::string(n));
}

// The codecs themselves live in sf::codec (include/swordfish/codecs.hh) because
// Bloblang's compress()/decompress() need the same ones. What stays here is the
// mapping from Kafka's attribute bits onto them, and the translation of a codec
// failure into a protocol_error.
namespace {
template <class F>
std::string as_protocol_error(F&& f) {
    try { return f(); }
    catch (const eval_error& e) { throw protocol_error(e.what()); }
}
} // namespace

std::string compress(compression c, std::string_view in) {
    switch (c) {
    case compression::none:   return std::string(in);
    case compression::gzip:   return as_protocol_error([&]{ return codec::gzip_compress(in); });
    case compression::snappy: return as_protocol_error([&]{ return codec::snappy_compress(in); });
    case compression::lz4:    return as_protocol_error([&]{ return codec::lz4_compress(in); });
    case compression::zstd:   return as_protocol_error([&]{ return codec::zstd_compress(in); });
    }
    throw protocol_error("unknown compression codec");
}

std::string decompress(compression c, std::string_view in) {
    switch (c) {
    case compression::none:   return std::string(in);
    case compression::gzip:   return as_protocol_error([&]{ return codec::gzip_decompress(in); });
    case compression::snappy: return as_protocol_error([&]{ return codec::snappy_decompress(in); });
    case compression::lz4:    return as_protocol_error([&]{ return codec::lz4_decompress(in); });
    case compression::zstd:   return as_protocol_error([&]{ return codec::zstd_decompress(in); });
    }
    throw protocol_error("unknown compression codec");
}

// ---- records ---------------------------------------------------------------

namespace {

constexpr int8_t  MAGIC_V2 = 2;
constexpr int16_t ATTR_CODEC_MASK   = 0x0007;
constexpr int16_t ATTR_TIMESTAMP    = 0x0008;
constexpr int16_t ATTR_TRANSACTIONAL = 0x0010;
constexpr int16_t ATTR_CONTROL      = 0x0020;

void encode_record(writer& w, const record& rec) {
    // The record's own length prefix is a VARINT, so the body is built first
    // and its size measured -- unlike the batch, which uses a fixed int32 and
    // can be patched in place.
    writer b;
    b.i8(0);                          // per-record attributes: unused, must be 0
    b.varlong(rec.timestamp_delta);
    b.varint(rec.offset_delta);
    if (rec.key) { b.varint(static_cast<int32_t>(rec.key->size())); b.raw(*rec.key); }
    else           b.varint(-1);
    if (rec.value) { b.varint(static_cast<int32_t>(rec.value->size())); b.raw(*rec.value); }
    else             b.varint(-1);
    b.varint(static_cast<int32_t>(rec.headers.size()));
    for (const auto& h : rec.headers) {
        b.varint(static_cast<int32_t>(h.key.size()));
        b.raw(h.key);
        if (h.value) { b.varint(static_cast<int32_t>(h.value->size())); b.raw(*h.value); }
        else           b.varint(-1);
    }
    w.varint(static_cast<int32_t>(b.size()));
    w.raw(b.view());
}

record decode_record(reader& r) {
    const int32_t len = r.varint();
    // varint is zigzag-decoded, so a negative value is representable and this
    // check is not the tautology it looks like.
    if (len < 0) throw protocol_error("record has a negative length");
    reader body(r.raw(static_cast<size_t>(len)));
    record rec;
    body.i8();                        // attributes, unused
    rec.timestamp_delta = body.varlong();
    rec.offset_delta = body.varint();
    const int32_t klen = body.varint();
    if (klen >= 0) rec.key = std::string(body.raw(static_cast<size_t>(klen)));
    const int32_t vlen = body.varint();
    if (vlen >= 0) rec.value = std::string(body.raw(static_cast<size_t>(vlen)));
    const int32_t nh = body.varint();
    if (nh > 0) {
        rec.headers.reserve(static_cast<size_t>(nh));
        for (int32_t i = 0; i < nh; ++i) {
            record_header h;
            const int32_t hk = body.varint();
            if (hk < 0) throw protocol_error("record header key is null");
            h.key = std::string(body.raw(static_cast<size_t>(hk)));
            const int32_t hv = body.varint();
            if (hv >= 0) h.value = std::string(body.raw(static_cast<size_t>(hv)));
            rec.headers.push_back(std::move(h));
        }
    }
    return rec;
}

} // namespace

void record_batch::encode(writer& w) const {
    writer recs;
    for (const auto& rec : records) encode_record(recs, rec);
    const std::string payload = compress(codec, recs.view());

    int16_t attrs = static_cast<int16_t>(static_cast<uint8_t>(codec)) & ATTR_CODEC_MASK;
    if (timestamp_type)  attrs |= ATTR_TIMESTAMP;
    if (transactional)   attrs |= ATTR_TRANSACTIONAL;
    if (control)         attrs |= ATTR_CONTROL;

    // Everything the CRC covers, built separately so it can be hashed before
    // being appended -- the CRC sits BEFORE the bytes it protects.
    writer after_crc;
    after_crc.i16(attrs);
    after_crc.i32(records.empty() ? 0 : records.back().offset_delta);
    after_crc.i64(base_timestamp);
    after_crc.i64(max_timestamp);
    after_crc.i64(producer_id);
    after_crc.i16(producer_epoch);
    after_crc.i32(base_sequence);
    after_crc.i32(static_cast<int32_t>(records.size()));
    after_crc.raw(payload);

    w.i64(base_offset);
    // batch_length counts everything after itself.
    w.i32(static_cast<int32_t>(4 + 1 + 4 + after_crc.size()));
    w.i32(partition_leader_epoch);
    w.i8(MAGIC_V2);
    w.u32(crc32c(after_crc.view()));
    w.raw(after_crc.view());
}

record_batch record_batch::decode(reader& r) {
    record_batch b;
    b.base_offset = r.i64();
    const int32_t batch_length = r.i32();
    // cppcheck-suppress knownConditionTrueFalse
    //   The length comes off the wire and may be anything.
    if (batch_length < 0) throw protocol_error("batch has a negative length");
    // Bound the rest of the parse to this batch, so a corrupt inner length
    // cannot read into the next batch.
    reader body(r.raw(static_cast<size_t>(batch_length)));
    b.partition_leader_epoch = body.i32();
    const int8_t magic = body.i8();
    if (magic != MAGIC_V2)
        throw protocol_error("unsupported record batch magic " + std::to_string(magic));
    const uint32_t want_crc = body.u32();
    const std::string_view covered = body.rest();
    const uint32_t got_crc = crc32c(covered);
    if (got_crc != want_crc)
        throw protocol_error("record batch CRC mismatch");

    const int16_t attrs = body.i16();
    b.codec = static_cast<compression>(attrs & ATTR_CODEC_MASK);
    b.timestamp_type = (attrs & ATTR_TIMESTAMP) ? 1 : 0;
    b.transactional  = (attrs & ATTR_TRANSACTIONAL) != 0;
    b.control        = (attrs & ATTR_CONTROL) != 0;
    body.i32();                        // last_offset_delta, implied by the records
    b.base_timestamp = body.i64();
    b.max_timestamp = body.i64();
    b.producer_id = body.i64();
    b.producer_epoch = body.i16();
    b.base_sequence = body.i32();
    const int32_t count = body.i32();
    // cppcheck-suppress knownConditionTrueFalse
    //   Off the wire: a corrupt batch may claim a negative count.
    if (count < 0) throw protocol_error("record batch has a negative record count");

    const std::string plain = decompress(b.codec, body.rest());
    reader rr(plain);
    // Checked against the bytes actually there before reserving. The count comes
    // off the wire and a record cannot be shorter than one byte, so a count
    // larger than the decompressed size is impossible -- and reserving on it
    // asks for an allocation of up to two billion records before reading one.
    if (static_cast<uint64_t>(count) > plain.size())
        throw protocol_error("record batch claims " + std::to_string(count) +
                             " records in " + std::to_string(plain.size()) + " bytes");
    b.records.reserve(static_cast<size_t>(count));
    for (int32_t i = 0; i < count; ++i) b.records.push_back(decode_record(rr));
    return b;
}

std::vector<record_batch> decode_batches(std::string_view blob) {
    std::vector<record_batch> out;
    reader r(blob);
    // A batch header is 61 bytes; anything shorter cannot be one.
    constexpr size_t header_bytes = 8 + 4 + 4 + 1 + 4 + 2 + 4 + 8 + 8 + 8 + 2 + 4 + 4;
    while (r.remaining() >= header_bytes) {
        const size_t at = r.pos();
        // Peek the length so a batch truncated by the fetch limit can be
        // dropped rather than throwing: the broker expects a re-fetch.
        reader peek(blob.substr(at));
        peek.i64();
        const int32_t len = peek.i32();
        // cppcheck-suppress knownConditionTrueFalse
        //   Off the wire: a corrupt blob may claim a negative batch length.
        if (len < 0 || static_cast<size_t>(len) + 12 > r.remaining()) break;
        out.push_back(record_batch::decode(r));
    }
    return out;
}

} // namespace sf::kafka
