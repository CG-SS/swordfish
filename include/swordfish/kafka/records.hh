// Record batches: the payload format inside Fetch and Produce.
//
// A batch is the unit of compression, of CRC, and of offset assignment, so it
// is decoded as a whole rather than record by record. Only magic v2 is
// supported -- v0/v1 message sets were removed from the protocol in Kafka 4.0,
// and a v2-only client cannot receive them from a 4.x broker.
//
// Layout (KIP-98), all big-endian:
//
//     int64  base_offset
//     int32  batch_length          bytes after this field
//     int32  partition_leader_epoch
//     int8   magic                 == 2
//     uint32 crc                   CRC-32C of everything after this field
//     int16  attributes            compression in bits 0-2, see `compression`
//     int32  last_offset_delta
//     int64  base_timestamp
//     int64  max_timestamp
//     int64  producer_id
//     int16  producer_epoch
//     int32  base_sequence
//     int32  record_count
//     ...    records, compressed as one blob when attributes say so
#pragma once

#include "swordfish/kafka/wire.hh"

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace sf::kafka {

enum class compression : uint8_t {
    none = 0, gzip = 1, snappy = 2, lz4 = 3, zstd = 4,
};

const char* compression_name(compression c) noexcept;
// Throws if the name is not one of the five the protocol defines.
compression compression_from_name(std::string_view name);

// CRC-32C (Castagnoli). Uses the SSE4.2 instruction where the CPU has it and a
// table otherwise, chosen once at first use: the library is prebuilt, so it
// cannot assume the build machine's ISA is the run machine's.
uint32_t crc32c(std::string_view data, uint32_t seed = 0) noexcept;

struct record_header {
    std::string                key;
    std::optional<std::string> value;
};

struct record {
    int64_t                    timestamp_delta = 0;
    int32_t                    offset_delta = 0;
    std::optional<std::string> key;
    std::optional<std::string> value;
    std::vector<record_header> headers;
};

struct record_batch {
    int64_t     base_offset = 0;
    int32_t     partition_leader_epoch = -1;
    compression codec = compression::none;
    bool        transactional = false;
    bool        control = false;
    // 0 = create time (the producer's), 1 = log append time (the broker's).
    int8_t      timestamp_type = 0;
    int64_t     base_timestamp = 0;
    int64_t     max_timestamp = 0;
    int64_t     producer_id = -1;
    int16_t     producer_epoch = -1;
    int32_t     base_sequence = -1;
    std::vector<record> records;

    // The absolute offset of a record, which is what an ack has to commit.
    int64_t offset_of(size_t i) const noexcept {
        return base_offset + records[i].offset_delta;
    }

    void        encode(writer& w) const;
    static record_batch decode(reader& r);
};

// A Fetch response carries a partition's records as one opaque blob that may
// hold SEVERAL batches back to back, and the last one may be truncated at the
// fetch limit. A truncated trailing batch is dropped rather than reported: the
// broker expects the client to re-fetch it, and treating it as corruption would
// stall the partition.
std::vector<record_batch> decode_batches(std::string_view blob);

// Compression, exposed for testing. `decompress` needs the uncompressed size
// hint only for codecs that do not store it.
std::string compress(compression c, std::string_view in);
std::string decompress(compression c, std::string_view in);

} // namespace sf::kafka
