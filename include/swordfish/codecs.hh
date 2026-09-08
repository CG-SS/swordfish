// Compression codecs, shared by the Bloblang `compress`/`decompress` methods
// and by the Kafka record batch codec.
//
// These lived in src/kafka/records.cc first, because Kafka was the only caller.
// Bloblang needs the same five plus three more, and two implementations of gzip
// framing in one binary is exactly the kind of duplication that drifts -- one
// gets a bug fix and the other does not. They now live here, in the base
// library, and the Kafka codec calls them.
//
// Every function throws sf::eval_error on malformed input rather than returning
// a status, matching the rest of the value-level API.
#pragma once

#include <string>
#include <string_view>

namespace sf::codec {

// The algorithm names a CONFIG may use, declared once.
//
// There were three independent copies of these lists -- the `compress`
// processor, the `decompress` processor and the `decompress` scanner -- plus
// the dispatchers that switch on the name. Adding an algorithm meant finding
// every one of them, and `zstd` reached two of the three: a config the
// reference runs was refused by a list that had simply never been updated.
// Sharing the arrays makes the declarative half of that impossible; the
// dispatchers still switch on the name, because each calls different functions.
//
// The asymmetry between the two is real and matches the reference: there is a
// bzip2 DECODER here and no encoder, so `bzip2` is nameable only for decoding.
inline constexpr std::string_view compress_algorithms[] = {
    "gzip", "pgzip", "zlib", "flate", "snappy", "lz4", "zstd"};
inline constexpr std::string_view decompress_algorithms[] = {
    "gzip", "pgzip", "zlib", "bzip2", "flate", "snappy", "lz4", "zstd"};

// RFC 1952: a deflate stream inside a gzip wrapper with a CRC and a length.
std::string gzip_compress(std::string_view in, int level = -1);
std::string gzip_decompress(std::string_view in);

// RFC 1950: a deflate stream inside the smaller zlib wrapper.
std::string zlib_compress(std::string_view in, int level = -1);
std::string zlib_decompress(std::string_view in);

// RFC 1951: raw deflate, no wrapper at all. Go calls this "flate".
std::string flate_compress(std::string_view in, int level = -1);
std::string flate_decompress(std::string_view in);

// Snappy's BLOCK format, which carries the uncompressed length as a varint
// prefix. Distinct from the stream format, which adds framing and checksums;
// the reference uses the block form for both Kafka and Bloblang.
std::string snappy_compress(std::string_view in);
std::string snappy_decompress(std::string_view in);

// The LZ4 FRAME format (LZ4F), not the raw block format. A raw-block decoder
// rejects a frame outright, so the distinction is not cosmetic.
std::string lz4_compress(std::string_view in);
std::string lz4_decompress(std::string_view in);

std::string zstd_compress(std::string_view in, int level = 3);
std::string zstd_decompress(std::string_view in);

// Decompression only, which is all the reference offers for bzip2.
std::string bzip2_decompress(std::string_view in);

} // namespace sf::codec
