// bcrypt, for compare_bcrypt().
//
// OpenSSL has no bcrypt: it is a password KDF built on Blowfish's key schedule
// rather than on a hash, and the expensive part is running that schedule 2^cost
// times with the salt and password alternating. libcrypto still exposes the raw
// Blowfish cipher, but not the internal state bcrypt has to rewrite between
// rounds, so the cipher is implemented here over the generated initial tables.
//
// Only comparison is offered, matching the reference: producing a hash needs a
// salt and a cost policy, which is an application's decision rather than a
// mapping's.
#include "blowfish_tables.hh"
#include "swordfish/methods.hh"
#include "swordfish/value.hh"

#include "encoding_util.hh"
#include "methods_util.hh"

#include <cstdint>
#include <cctype>
#include <cstring>
#include <string>
#include <vector>

namespace sf::m {

namespace {

struct blowfish {
    uint32_t P[18];
    uint32_t S[4][256];

    void reset() {
        std::memcpy(P, bf::INIT_P, sizeof P);
        std::memcpy(S, bf::INIT_S, sizeof S);
    }

    uint32_t F(uint32_t x) const {
        return ((S[0][(x >> 24) & 0xFF] + S[1][(x >> 16) & 0xFF]) ^
                 S[2][(x >> 8) & 0xFF]) + S[3][x & 0xFF];
    }

    void encrypt(uint32_t& l, uint32_t& r) const {
        for (int i = 0; i < 16; ++i) {
            l ^= P[i];
            r ^= F(l);
            std::swap(l, r);
        }
        std::swap(l, r);
        r ^= P[16];
        l ^= P[17];
    }
};

// Four bytes of `data`, big-endian, starting at a cyclic offset. Both the key
// and the salt are consumed this way, wrapping when they run out.
uint32_t word_at(const std::string& data, size_t& off) {
    uint32_t w = 0;
    for (int i = 0; i < 4; ++i) {
        w = (w << 8) | static_cast<unsigned char>(data[off % data.size()]);
        ++off;
    }
    return w;
}

// The salted key schedule. `salt` may be empty, which is the "zero salt" pass
// of the cost loop.
void expand_key(blowfish& bf, const std::string& salt, const std::string& key) {
    size_t koff = 0;
    for (int i = 0; i < 18; ++i) bf.P[i] ^= word_at(key, koff);

    uint32_t l = 0, r = 0;
    size_t soff = 0;
    auto step = [&] {
        if (!salt.empty()) {
            l ^= word_at(salt, soff);
            r ^= word_at(salt, soff);
        }
        bf.encrypt(l, r);
    };
    for (int i = 0; i < 18; i += 2) { step(); bf.P[i] = l; bf.P[i + 1] = r; }
    for (int box = 0; box < 4; ++box)
        for (int i = 0; i < 256; i += 2) { step(); bf.S[box][i] = l; bf.S[box][i + 1] = r; }
}

// bcrypt has its own base64 alphabet, in its own order -- neither the standard
// one nor the URL-safe one -- and no padding.
const char* BCRYPT_B64 =
    "./ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

std::string bcrypt_b64_decode(std::string_view in, size_t want) {
    std::string out;
    uint32_t buf = 0;
    int bits = 0;
    for (char c : in) {
        const char* p = std::strchr(BCRYPT_B64, c);
        if (!p || !c) throw eval_error("bcrypt: invalid character in the hashed secret");
        buf = (buf << 6) | static_cast<uint32_t>(p - BCRYPT_B64);
        bits += 6;
        if (bits >= 8) {
            bits -= 8;
            out += static_cast<char>((buf >> bits) & 0xFF);
            if (out.size() == want) break;
        }
    }
    if (out.size() != want) throw eval_error("bcrypt: the hashed secret is truncated");
    return out;
}

// The highest bcrypt cost `compare_bcrypt` will verify; see the comment at the
// check for why it is lower than the format's 31.
constexpr unsigned max_compare_cost = 14;

std::string bcrypt_b64_encode(std::string_view in) {
    std::string out;
    uint32_t buf = 0;
    int bits = 0;
    for (unsigned char c : in) {
        buf = (buf << 8) | c;
        bits += 8;
        while (bits >= 6) {
            bits -= 6;
            out += BCRYPT_B64[(buf >> bits) & 0x3F];
        }
    }
    if (bits) out += BCRYPT_B64[(buf << (6 - bits)) & 0x3F];
    return out;
}

// The 23 bytes bcrypt publishes, given a cost, a 16-byte salt and a password.
std::string bcrypt_raw(unsigned cost, const std::string& salt, std::string key) {
    // The password is used with its terminating NUL, and truncated at 72 bytes
    // -- both are part of the definition, not implementation slack.
    if (key.size() > 72) key.resize(72);
    key += '\0';

    blowfish bf;
    bf.reset();
    expand_key(bf, salt, key);
    const uint64_t rounds = uint64_t{1} << cost;
    for (uint64_t i = 0; i < rounds; ++i) {
        expand_key(bf, {}, key);
        expand_key(bf, {}, salt);
    }

    // "OrpheanBeholderScryDoubt", encrypted 64 times.
    static const char MAGIC[] = "OrpheanBeholderScryDoubt";
    uint32_t w[6];
    for (int i = 0; i < 6; ++i) {
        w[i] = 0;
        for (int b = 0; b < 4; ++b)
            w[i] = (w[i] << 8) | static_cast<unsigned char>(MAGIC[i * 4 + b]);
    }
    for (int n = 0; n < 64; ++n)
        for (int i = 0; i < 6; i += 2) bf.encrypt(w[i], w[i + 1]);

    std::string out;
    for (int i = 0; i < 6; ++i)
        for (int b = 3; b >= 0; --b) out += static_cast<char>((w[i] >> (8 * b)) & 0xFF);
    out.resize(23);            // the last byte is deliberately discarded
    return out;
}

} // namespace

value compare_bcrypt(const value& v, const value& hashed) {
    const std::string& secret = want_string(v);
    const std::string& enc = want_string(hashed);
    // $2<variant>$<cost>$<22 chars of salt><31 chars of hash>
    if (enc.size() != 60 || enc[0] != '$' || enc[1] != '2' || enc[3] != '$' ||
        enc[6] != '$')
        throw eval_error("bcrypt: the hashed secret is not in the expected format");
    const char variant = enc[2];
    // 2a, 2b and 2y differ only in how they handled historical bugs with keys
    // longer than 72 bytes or with the high bit set; the algorithm below is the
    // corrected one, which is what all three mean today. 2x is the buggy
    // variant and is refused rather than silently treated as 2y.
    if (variant != 'a' && variant != 'b' && variant != 'y')
        throw eval_error(std::string("bcrypt: unsupported variant 2") + variant);
    if (!std::isdigit(static_cast<unsigned char>(enc[4])) ||
        !std::isdigit(static_cast<unsigned char>(enc[5])))
        throw eval_error("bcrypt: malformed cost");
    const unsigned cost = static_cast<unsigned>((enc[4] - '0') * 10 + (enc[5] - '0'));
    if (cost < 4 || cost > 31) throw eval_error("bcrypt: cost out of range");
    // CAPPED, and lower than the format allows. The cost is read from the hash
    // -- which is message data -- and the work is 2^cost rounds run
    // synchronously on the shard, with no await and nothing to preempt it.
    // Measured on this machine: cost 12 = 0.35s, 14 = 1.27s, 16 = 4.65s,
    // 18 = 18.5s, a clean 4x per two steps. Cost 31 is about forty-two hours of
    // frozen shard from one sixty-byte string, and the hash need not even be
    // real: a syntactically valid one that compares false costs the full amount.
    // Demonstrated live at cost 16, /ready stopped answering entirely and
    // Seastar's stall detector logged an escalating 66/124/233/442/852ms stall.
    //
    // The reference accepts up to 31 because Go runs it on an OS thread, where
    // it delays one request rather than the whole event loop. Swordfish has no
    // such thread here, so the range is narrowed and the boundary is a NAMED
    // error rather than an unbounded stall. 14 keeps every cost in ordinary use
    // -- 10 and 12 are the common ones -- and bounds the worst case to about a
    // second. A pipeline that must verify higher costs can do it in a `cpp:`
    // block, where it owns the scheduling decision.
    if (cost > max_compare_cost)
        throw eval_error("bcrypt: cost " + std::to_string(cost) + " exceeds the maximum "
                         "of " + std::to_string(max_compare_cost) + " swordfish will "
                         "verify: the work is 2^cost rounds on the shard, and the cost "
                         "comes from the hash being checked. Use a `cpp:` block if you "
                         "need a higher one");

    const std::string salt = bcrypt_b64_decode(std::string_view(enc).substr(7, 22), 16);
    const std::string want(std::string_view(enc).substr(29));
    const std::string got = bcrypt_b64_encode(bcrypt_raw(cost, salt, secret));

    // Constant time: a comparison that stops at the first difference leaks how
    // much of the hash matched.
    return value(enc::constant_time_eq(got, want));
}

} // namespace sf::m
