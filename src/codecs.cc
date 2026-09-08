// Compression codecs. See include/swordfish/codecs.hh for why they live here
// rather than next to their first caller.
#include "swordfish/codecs.hh"
#include "swordfish/value.hh"

#include <bzlib.h>
#include <lz4.h>
#include <lz4frame.h>
#include <snappy.h>
#include <zlib.h>
#define ZSTD_STATIC_LINKING_ONLY
#include <zstd.h>

#include <string>

namespace sf::codec {

namespace {

// One deflate implementation for all three wrappers. zlib picks the framing
// from the window-bits bias: +16 means a gzip header (RFC 1952), the plain
// value means zlib's own (RFC 1950), and a negative value means no wrapper at
// all -- raw deflate, which Go calls "flate".
std::string deflate_to(std::string_view in, int window_bits, int level, const char* what) {
    z_stream s{};
    if (deflateInit2(&s, level, Z_DEFLATED, window_bits, 8, Z_DEFAULT_STRATEGY) != Z_OK)
        throw eval_error(std::string(what) + ": deflateInit2 failed");
    std::string out(deflateBound(&s, in.size()), '\0');
    s.next_in = reinterpret_cast<Bytef*>(const_cast<char*>(in.data()));
    s.avail_in = static_cast<uInt>(in.size());
    s.next_out = reinterpret_cast<Bytef*>(out.data());
    s.avail_out = static_cast<uInt>(out.size());
    const int rc = deflate(&s, Z_FINISH);
    const size_t produced = out.size() - s.avail_out;
    deflateEnd(&s);
    if (rc != Z_STREAM_END) throw eval_error(std::string(what) + ": deflate failed");
    out.resize(produced);
    return out;
}

std::string inflate_from(std::string_view in, int window_bits, const char* what) {
    z_stream s{};
    if (inflateInit2(&s, window_bits) != Z_OK)
        throw eval_error(std::string(what) + ": inflateInit2 failed");
    s.next_in = reinterpret_cast<Bytef*>(const_cast<char*>(in.data()));
    s.avail_in = static_cast<uInt>(in.size());
    std::string out;
    char buf[64 * 1024];
    int rc = Z_OK;
    do {
        s.next_out = reinterpret_cast<Bytef*>(buf);
        s.avail_out = sizeof buf;
        rc = inflate(&s, Z_NO_FLUSH);
        if (rc != Z_OK && rc != Z_STREAM_END && rc != Z_BUF_ERROR) {
            inflateEnd(&s);
            throw eval_error(std::string(what) + ": inflate failed");
        }
        out.append(buf, sizeof buf - s.avail_out);
        // CONCATENATED MEMBERS. gzip streams concatenate exactly as zstd frames
        // do -- `cat a.gz b.gz` is a valid gzip stream, and that is how a rotated
        // log or a multipart download arrives -- so stopping at the first
        // Z_STREAM_END silently dropped everything after it. zstd_decompress_impl
        // a few lines below carries the comment describing this trap; the same
        // fix was never applied here. Reset and keep going while input remains.
        if (rc == Z_STREAM_END && s.avail_in > 0) {
            if (inflateReset2(&s, window_bits) != Z_OK) {
                inflateEnd(&s);
                throw eval_error(std::string(what) + ": inflateReset2 failed");
            }
            rc = Z_OK;
        }
    } while (rc != Z_STREAM_END && s.avail_in > 0);
    inflateEnd(&s);
    if (rc != Z_STREAM_END) throw eval_error(std::string(what) + ": truncated stream");
    return out;
}

// Kafka's lz4 is the LZ4 FRAME format (not the raw block format), so the
// frame API is the right one -- a raw-block decoder silently rejects it.
std::string lz4_compress_impl(std::string_view in) {
    const size_t bound = LZ4F_compressFrameBound(in.size(), nullptr);
    std::string out(bound, '\0');
    const size_t n = LZ4F_compressFrame(out.data(), bound, in.data(), in.size(), nullptr);
    if (LZ4F_isError(n)) throw eval_error("lz4: compressFrame failed");
    out.resize(n);
    return out;
}

std::string lz4_decompress_impl(std::string_view in) {
    LZ4F_dctx* ctx = nullptr;
    if (LZ4F_isError(LZ4F_createDecompressionContext(&ctx, LZ4F_VERSION)))
        throw eval_error("lz4: createDecompressionContext failed");
    std::string out;
    const char* src = in.data();
    size_t left = in.size();
    char buf[64 * 1024];
    while (left > 0) {
        size_t dst_n = sizeof buf;
        size_t src_n = left;
        const size_t hint = LZ4F_decompress(ctx, buf, &dst_n, src, &src_n, nullptr);
        if (LZ4F_isError(hint)) {
            LZ4F_freeDecompressionContext(ctx);
            throw eval_error("lz4: decompress failed");
        }
        out.append(buf, dst_n);
        src += src_n;
        left -= src_n;
        if (hint == 0) break;              // frame complete
        if (src_n == 0 && dst_n == 0) break;
    }
    LZ4F_freeDecompressionContext(ctx);
    return out;
}

std::string zstd_compress_at(std::string_view in, int level) {
    const size_t bound = ZSTD_compressBound(in.size());
    std::string out(bound, '\0');
    const size_t n = ZSTD_compress(out.data(), bound, in.data(), in.size(), level);
    if (ZSTD_isError(n)) throw eval_error("zstd: compress failed");
    out.resize(n);
    return out;
}

std::string zstd_decompress_impl(std::string_view in) {
    // ALWAYS streamed, never sized from the frame header. There used to be a
    // fast path that read ZSTD_getFrameContentSize() and allocated that many
    // bytes before decompressing -- so seventeen hostile bytes declaring a 64
    // GiB frame reached std::bad_alloc and killed the input layer, without zstd
    // ever having looked at the data. The reference streams for the same reason:
    // its decoder is an io.Reader, so nothing is allocated that the input has
    // not actually produced. Growing `out` from a 64 KiB staging buffer costs a
    // few reallocations on a large frame and cannot be driven by a declared
    // size at all.
    ZSTD_DStream* ds = ZSTD_createDStream();
    if (!ds) throw eval_error("zstd: could not create the decompression stream");
    ZSTD_initDStream(ds);
    std::string out;
    char buf[64 * 1024];
    ZSTD_inBuffer ib{in.data(), in.size(), 0};
    size_t rc = 1;                      // non-zero: the frame is not complete yet
    // No `break` when a frame completes: zstd streams CONCATENATE, and stopping
    // at the first frame's end returned only its content -- `cat a.zst b.zst`
    // decompressed to "AAAA" where the reference gives "AAAABBBB". The loop ends
    // when the input is exhausted, which is the only thing that means "done".
    while (ib.pos < ib.size) {
        ZSTD_outBuffer ob{buf, sizeof buf, 0};
        rc = ZSTD_decompressStream(ds, &ob, &ib);
        if (ZSTD_isError(rc)) {
            ZSTD_freeDStream(ds);
            throw eval_error("zstd: decompressStream failed");
        }
        out.append(buf, ob.pos);
    }
    ZSTD_freeDStream(ds);
    // Input exhausted mid-frame. The reference reports "unexpected EOF" rather
    // than handing back the part that happened to decode, and emitting truncated
    // data as if it were the message is exactly the failure the scanners were
    // audited for.
    if (rc != 0 && !in.empty()) throw eval_error("zstd: truncated frame");
    return out;
}

} // namespace

std::string gzip_compress(std::string_view in, int level) {
    return deflate_to(in, 15 + 16, level, "gzip");
}
std::string gzip_decompress(std::string_view in) {
    return inflate_from(in, 15 + 16, "gzip");
}
std::string zlib_compress(std::string_view in, int level) {
    return deflate_to(in, 15, level, "zlib");
}
std::string zlib_decompress(std::string_view in) {
    return inflate_from(in, 15, "zlib");
}
std::string flate_compress(std::string_view in, int level) {
    return deflate_to(in, -15, level, "flate");
}
std::string flate_decompress(std::string_view in) {
    return inflate_from(in, -15, "flate");
}

std::string snappy_compress(std::string_view in) {
    std::string out;
    snappy::Compress(in.data(), in.size(), &out);
    return out;
}
std::string snappy_decompress(std::string_view in) {
    // Validated BEFORE the destination is sized. snappy::Uncompress reads the
    // varint preamble and resizes its output to whatever that declares before it
    // looks at the block stream, so a handful of bytes declaring a huge length
    // reached std::bad_alloc. IsValidCompressedBuffer walks the whole block
    // without allocating an output at all, so a bomb is refused for the reason
    // it is bad rather than by running out of memory. A buffer that survives
    // this really does hold that many bytes' worth of blocks, which bounds the
    // allocation by the size of the input the peer actually sent.
    if (!snappy::IsValidCompressedBuffer(in.data(), in.size()))
        throw eval_error("snappy: not a valid compressed buffer");
    std::string out;
    if (!snappy::Uncompress(in.data(), in.size(), &out))
        throw eval_error("snappy: uncompress failed");
    return out;
}

std::string lz4_compress(std::string_view in)   { return lz4_compress_impl(in); }
std::string lz4_decompress(std::string_view in) { return lz4_decompress_impl(in); }

std::string zstd_compress(std::string_view in, int level) {
    // Level -1 is Go's "default compression" sentinel, which is not a valid
    // zstd level; zstd's own default is 3.
    return zstd_compress_at(in, level < 0 ? 3 : level);
}
std::string zstd_decompress(std::string_view in) { return zstd_decompress_impl(in); }

std::string bzip2_decompress(std::string_view in) {
    bz_stream s{};
    if (BZ2_bzDecompressInit(&s, 0, 0) != BZ_OK)
        throw eval_error("bzip2: init failed");
    s.next_in = const_cast<char*>(in.data());
    s.avail_in = static_cast<unsigned>(in.size());
    std::string out;
    char buf[64 * 1024];
    int rc = BZ_OK;
    do {
        s.next_out = buf;
        s.avail_out = sizeof buf;
        rc = BZ2_bzDecompress(&s);
        if (rc != BZ_OK && rc != BZ_STREAM_END) {
            BZ2_bzDecompressEnd(&s);
            throw eval_error("bzip2: decompress failed");
        }
        out.append(buf, sizeof buf - s.avail_out);
        // Concatenated streams, as gzip and zstd have above. bzip2 has no reset,
        // so the stream is ended and re-initialised over what is left.
        if (rc == BZ_STREAM_END && s.avail_in > 0) {
            char* next = s.next_in;
            const unsigned left = s.avail_in;
            BZ2_bzDecompressEnd(&s);
            s = bz_stream{};
            if (BZ2_bzDecompressInit(&s, 0, 0) != BZ_OK)
                throw eval_error("bzip2: init failed");
            s.next_in = next;
            s.avail_in = left;
            rc = BZ_OK;
        }
    } while (rc != BZ_STREAM_END && s.avail_in > 0);
    BZ2_bzDecompressEnd(&s);
    if (rc != BZ_STREAM_END) throw eval_error("bzip2: truncated stream");
    return out;
}

} // namespace sf::codec
