// Bloblang compress() and decompress().
//
// Thin dispatch onto sf::codec. The interesting part is the algorithm names:
// `pgzip` is Go's parallel gzip writer, whose OUTPUT is ordinary gzip, so it
// maps to the same codec -- parallelism is a property of the producer, not of
// the format, and a separate implementation would only be able to differ by
// being wrong.
#include "swordfish/codecs.hh"
#include "swordfish/methods.hh"
#include "swordfish/value.hh"

#include "methods_util.hh"

#include <string>

namespace sf::m {

value compress(const value& v, const value& algorithm, const value& level_v) {
    const std::string& in = want_string(v);
    const std::string& alg = want_string(algorithm);
    // -1 is Go's "default compression" sentinel; each codec maps it to its own
    // default rather than treating it as a level.
    const int level = level_v.is_number() ? static_cast<int>(level_v.as_i64()) : -1;
    if (alg == "gzip" || alg == "pgzip") return value::bytes(codec::gzip_compress(in, level));
    if (alg == "zlib")   return value::bytes(codec::zlib_compress(in, level));
    if (alg == "flate")  return value::bytes(codec::flate_compress(in, level));
    if (alg == "snappy") return value::bytes(codec::snappy_compress(in));
    if (alg == "lz4")    return value::bytes(codec::lz4_compress(in));
    if (alg == "zstd")   return value::bytes(codec::zstd_compress(in, level));
    throw eval_error("unrecognized compression algorithm: " + alg);
}

value decompress(const value& v, const value& algorithm) {
    const std::string& in = want_string(v);
    const std::string& alg = want_string(algorithm);
    if (alg == "gzip" || alg == "pgzip") return value::bytes(codec::gzip_decompress(in));
    if (alg == "zlib")   return value::bytes(codec::zlib_decompress(in));
    if (alg == "flate")  return value::bytes(codec::flate_decompress(in));
    if (alg == "snappy") return value::bytes(codec::snappy_decompress(in));
    if (alg == "lz4")    return value::bytes(codec::lz4_decompress(in));
    if (alg == "zstd")   return value::bytes(codec::zstd_decompress(in));
    if (alg == "bzip2")  return value::bytes(codec::bzip2_decompress(in));
    throw eval_error("unrecognized decompression algorithm: " + alg);
}

} // namespace sf::m
