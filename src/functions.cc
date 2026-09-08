// Bloblang functions beyond the first set in runtime.cc: identifiers, random
// numbers, file reads and the metadata of the message being built.
//
// Every generator here is seeded per THREAD. A Seastar shard is a thread, so a
// plain function-local static would be shared by every shard -- the same trap
// documented for the JSON parser -- and two shards drawing
// from one std::mt19937_64 without a lock is a data race as well as a
// correlation bug.
#include "swordfish/runtime.hh"

#include "encoding_util.hh"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <fstream>
#include <map>
#include <random>
#include <cstring>
#include <sstream>
#include <vector>
#include <string>

namespace sf::fn {

namespace {

std::mt19937_64& rng() {
    static thread_local std::mt19937_64 g{[] {
        std::random_device rd;
        // Two draws: random_device yields 32 bits at a time on Linux.
        return (static_cast<uint64_t>(rd()) << 32) ^ rd();
    }()};
    return g;
}

void fill_random(uint8_t* out, size_t n) {
    auto& g = rng();
    size_t i = 0;
    while (i + 8 <= n) {
        const uint64_t v = g();
        std::memcpy(out + i, &v, 8);
        i += 8;
    }
    if (i < n) {
        const uint64_t v = g();
        std::memcpy(out + i, &v, n - i);
    }
}

int64_t now_unix_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch()).count();
}

// Crockford base32: no I, L, O or U, so a transcribed identifier cannot be
// misread. 26 characters encode ULID's 128 bits.
const char* CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";
const char* BASE62 = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
// nanoid's default alphabet, URL-safe by construction.
const char* NANOID_ALPHABET =
    "_-0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

// Big-endian base-N of a byte string, used by KSUID. The division is done in
// place over a scratch copy, which is short enough that the O(n^2) does not
// matter for a 20-byte input.
std::string base_encode(std::vector<uint8_t> bytes, const char* alphabet, int base,
                        size_t width) {
    std::string out;
    while (true) {
        bool all_zero = true;
        for (uint8_t b : bytes) if (b) { all_zero = false; break; }
        if (all_zero) break;
        int rem = 0;
        for (auto& b : bytes) {
            const int cur = rem * 256 + b;
            b = static_cast<uint8_t>(cur / base);
            rem = cur % base;
        }
        out += alphabet[rem];
    }
    while (out.size() < width) out += alphabet[0];
    std::reverse(out.begin(), out.end());
    return out;
}

int64_t opt_int_or(const value& v, int64_t dflt) {
    return v.is_number() ? v.as_i64() : dflt;
}

} // namespace

value pi() { return value(3.141592653589793); }

// ---- identifiers ---------------------------------------------------------------

value uuid_v4() {
    uint8_t b[16];
    fill_random(b, sizeof b);
    b[6] = static_cast<uint8_t>((b[6] & 0x0F) | 0x40);   // version 4
    b[8] = static_cast<uint8_t>((b[8] & 0x3F) | 0x80);   // RFC 4122 variant
    return value(enc::format_uuid(std::string_view(reinterpret_cast<char*>(b), 16)));
}

value uuid_v7(const value& at) {
    uint8_t b[16];
    fill_random(b, sizeof b);
    const int64_t ms = at.is_ts() ? at.ts_nanos() / 1'000'000
                     : at.is_number() ? at.as_i64() * 1000
                     : now_unix_ms();
    // 48-bit big-endian millisecond timestamp, then version and variant.
    for (int i = 0; i < 6; ++i)
        b[i] = static_cast<uint8_t>((ms >> (40 - 8 * i)) & 0xFF);
    b[6] = static_cast<uint8_t>((b[6] & 0x0F) | 0x70);
    b[8] = static_cast<uint8_t>((b[8] & 0x3F) | 0x80);
    return value(enc::format_uuid(std::string_view(reinterpret_cast<char*>(b), 16)));
}

value ulid(const value& encoding, const value& random_source) {
    // Both randomness sources draw from the same generator here. The
    // distinction the reference draws is about speed, not about the shape of
    // the output, so a caller asking for "fast_random" is not short-changed.
    (void)random_source;
    const std::string encoding_name = encoding.is_stringy() ? encoding.as_string()
                                                            : std::string("crockford");
    uint8_t b[16];
    fill_random(b, sizeof b);
    const int64_t ms = now_unix_ms();
    for (int i = 0; i < 6; ++i)
        b[i] = static_cast<uint8_t>((ms >> (40 - 8 * i)) & 0xFF);
    if (encoding_name == "hex")
        return value(enc::hex_encode(std::string_view(reinterpret_cast<char*>(b), 16)));
    if (encoding_name != "crockford")
        throw eval_error("unrecognised ulid encoding: " + encoding_name);
    // 128 bits as 26 Crockford base-32 characters, MOST SIGNIFICANT FIRST, with
    // the first character carrying only THREE bits: 3 + 25*5 = 128. That is the
    // canonical layout, and it is what makes a ULID sort in time order, since
    // the 48-bit millisecond timestamp lands in the first ten characters.
    //
    // This was wrong twice over. It took five bits from the front for every
    // character, so the grouping was off by two bits everywhere; and it then
    // reversed the whole string, moving the timestamp to the TAIL. The result
    // was neither a valid ULID nor sortable: five consecutive calls gave a
    // random first character and a constant eight-character tail, where the
    // reference's were monotonically increasing. `ulid("hex")` was correct
    // throughout, which is what showed the bytes were right and only the base-32
    // rendering was broken.
    //
    // Written as a big-endian bit stream with two leading zero bits, which
    // reproduces oklog/ulid's MarshalTextTo exactly: dst[0] is (id[0]&224)>>5,
    // dst[1] is id[0]&31, dst[2] is (id[1]&248)>>3, and so on.
    const auto bit_at = [&b](int i) -> uint32_t {
        if (i < 0 || i >= 128) return 0;              // the two leading pad bits
        return (b[i / 8] >> (7 - i % 8)) & 1u;
    };
    std::string out(26, '0');
    for (int c = 0; c < 26; ++c) {
        uint32_t acc = 0;
        for (int k = 0; k < 5; ++k) acc |= bit_at(5 * c - 2 + k) << (4 - k);
        out[static_cast<size_t>(c)] = CROCKFORD[acc];
    }
    return value(out);
}

value ksuid() {
    // 4-byte seconds since the KSUID epoch (2014-05-13T16:53:20Z) plus 16 bytes
    // of randomness, rendered as 27 base-62 characters.
    const uint32_t secs = static_cast<uint32_t>(
        std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::system_clock::now().time_since_epoch()).count() - 1400000000LL);
    std::vector<uint8_t> b(20);
    for (int i = 0; i < 4; ++i) b[static_cast<size_t>(i)] =
        static_cast<uint8_t>((secs >> (24 - 8 * i)) & 0xFF);
    fill_random(b.data() + 4, 16);
    return value(base_encode(std::move(b), BASE62, 62, 27));
}

value nanoid(const value& length, const value& alphabet) {
    const int64_t n = opt_int_or(length, 21);
    if (n <= 0 || n > 4096) throw eval_error("nanoid length must be between 1 and 4096");
    std::string alpha = alphabet.is_stringy() ? alphabet.as_string()
                                              : std::string(NANOID_ALPHABET);
    if (alpha.empty()) throw eval_error("nanoid alphabet must not be empty");
    std::string out;
    out.reserve(static_cast<size_t>(n));
    std::uniform_int_distribution<size_t> d(0, alpha.size() - 1);
    for (int64_t i = 0; i < n; ++i) out += alpha[d(rng())];
    return value(std::move(out));
}

value snowflake_id(const value& node_id) {
    const int64_t node = opt_int_or(node_id, 1);
    if (node < 0 || node > 1023) throw eval_error("snowflake node_id must be 0-1023");
    // bwmarrin/snowflake's layout and epoch: 41 bits of milliseconds since
    // 2010-11-04T01:42:54.657Z, 10 bits of node, 12 bits of sequence.
    static constexpr int64_t EPOCH_MS = 1288834974657LL;
    static thread_local int64_t last_ms = -1;
    static thread_local int64_t seq = 0;
    int64_t ms = now_unix_ms();
    // A backwards clock step must not re-issue ids that were already handed
    // out, so time only ever moves forward from this generator's point of view.
    if (ms < last_ms) ms = last_ms;
    if (ms == last_ms) {
        seq = (seq + 1) & 0xFFF;
        // The sequence is exhausted for this millisecond. Borrowing the next
        // one keeps ids unique and ordered; the obvious alternative -- spin
        // until the clock advances -- would block the reactor for up to a
        // millisecond inside what looks like a pure function.
        if (seq == 0) ++ms;
    } else {
        seq = 0;
    }
    last_ms = ms;
    const int64_t id = ((ms - EPOCH_MS) << 22) | (node << 12) | seq;
    return value(std::to_string(id));
}

// ---- randomness --------------------------------------------------------------------

value random_int(exec_ctx& ctx, const std::string& site, const value& seed,
                 int64_t min_v, int64_t max_v) {
    if (min_v > max_v) throw eval_error("random_int: min must not exceed max");
    // "if a query is provided it will only be resolved once during the lifetime
    // of the mapping" -- so the generator is created on first use at this call
    // site and then kept.
    auto& gens = ctx.rngs;
    auto it = gens.find(site);
    if (it == gens.end()) {
        const uint64_t s = seed.is_number() ? static_cast<uint64_t>(seed.as_i64())
                                            : 0u;
        // A zero seed is the documented default and must still be usable, so it
        // is passed to the generator verbatim rather than being replaced.
        it = gens.emplace(site, std::mt19937_64{s}).first;
    }
    const uint64_t span = static_cast<uint64_t>(max_v) - static_cast<uint64_t>(min_v);
    if (span == UINT64_MAX) return value(static_cast<int64_t>(it->second()));
    return value(static_cast<int64_t>(
        static_cast<uint64_t>(min_v) + it->second() % (span + 1)));
}

// ---- counters ----------------------------------------------------------------------

value count(exec_ctx& ctx, const value& name) {
    // Keyed by NAME, and prefixed so it cannot collide with counter(), which is
    // keyed by "line:col" in the same map.
    const std::string key = "count/" + (name.is_stringy() ? name.as_string()
                                                          : name.to_json());
    return value(++ctx.counters[key]);
}

// ---- metadata ------------------------------------------------------------------------

value root_meta(const exec_ctx& ctx, const value& key) {
    if (!ctx.meta) throw eval_error("no metadata is available in this context");
    if (key.is_stringy() && !key.as_string().empty()) {
        const value* v = ctx.meta->find(key.as_string());
        return v ? *v : value();
    }
    value out = value::object();
    for (const auto& [k, v] : ctx.meta->entries()) out.set(k, v);
    return out;
}

value error_source_name(const exec_ctx& ctx) {
    const message& m = ctx.message_of("error_source_name");
    if (!m.has_error()) return value();
    return value(m.error_source().name);
}
value error_source_label(const exec_ctx& ctx) {
    const message& m = ctx.message_of("error_source_label");
    if (!m.has_error()) return value();
    return value(m.error_source().label);
}
value error_source_path(const exec_ctx& ctx) {
    const message& m = ctx.message_of("error_source_path");
    if (!m.has_error()) return value();
    return value(m.error_source().path);
}

// ---- files ---------------------------------------------------------------------------

value read_file(const value& path, const value& no_cache) {
    if (!path.is_stringy()) throw eval_error("file() requires a path");
    const std::string& p = path.as_string();
    const bool cache = !(no_cache.type() == vtype::boolean && no_cache.as_bool());
    // Per shard, because a Seastar shard is a thread and a shared cache would
    // need a lock on a path that is meant to be cheap.
    static thread_local std::map<std::string, std::string> cached;
    if (cache) {
        const auto it = cached.find(p);
        if (it != cached.end()) return value::bytes(it->second);
    }
    std::ifstream f(p, std::ios::binary);
    if (!f) throw eval_error("failed to read file " + p);
    std::ostringstream buf;
    buf << f.rdbuf();
    std::string data = buf.str();
    // Bounded: the path can be computed from message data, so an unbounded
    // cache is an unbounded memory leak driven by input.
    if (cache) {
        if (cached.size() >= 64) cached.clear();
        cached[p] = data;
    }
    return value::bytes(std::move(data));
}

value zero_bytes(const value& length) {
    const int64_t n = opt_int_or(length, -1);
    if (n < 0) throw eval_error("bytes() requires a non-negative length");
    return value::bytes(std::string(static_cast<size_t>(n), '\0'));
}

} // namespace sf::fn
