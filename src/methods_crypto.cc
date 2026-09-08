// Hashing, AES and password comparison.
//
// Backed by OpenSSL's EVP interface, which is already this project's TLS and
// crypto provider (OpenSSL, not GnuTLS, because
// `swordfish build` produces binaries that users redistribute and an LGPL
// dependency would carry notice obligations to them).
//
// Two of the digests are NOT OpenSSL's, and deliberately: xxhash64 and fnv32
// are not cryptographic and OpenSSL does not implement them. They are also the
// two whose result is a DECIMAL STRING rather than raw bytes, because the
// reference formats them with strconv rather than returning the digest.
#include "swordfish/methods.hh"
#include "swordfish/value.hh"

#include "encoding_util.hh"
#include "methods_util.hh"

#include <openssl/core_names.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/kdf.h>
#include <openssl/params.h>

#include <array>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

namespace sf::m {

namespace {

std::string ssl_error(const char* what) {
    const unsigned long e = ERR_get_error();
    char buf[256] = {};
    if (e) ERR_error_string_n(e, buf, sizeof buf);
    return std::string(what) + (e ? std::string(": ") + buf : std::string());
}

// ---- non-cryptographic digests ------------------------------------------------


// FNV-1, not FNV-1a: Go's hash/fnv New32() multiplies before the XOR.
uint32_t fnv32(std::string_view in) {
    uint32_t h = 2166136261u;
    for (unsigned char c : in) { h *= 16777619u; h ^= c; }
    return h;
}

// Reflected CRC-32 with a selectable polynomial, matching Go's hash/crc32,
// which stores every polynomial in reversed form. The table is built once per
// polynomial per thread: rebuilding it on every call cost 2048 operations to
// hash inputs that are often shorter than that.
const std::array<uint32_t, 256>& crc32_table(uint32_t poly) {
    static thread_local std::vector<std::pair<uint32_t, std::array<uint32_t, 256>>> cache;
    for (const auto& [p, t] : cache) if (p == poly) return t;
    std::array<uint32_t, 256> table{};
    for (uint32_t i = 0; i < 256; ++i) {
        uint32_t c = i;
        for (int k = 0; k < 8; ++k) c = (c & 1) ? (c >> 1) ^ poly : c >> 1;
        table[i] = c;
    }
    cache.emplace_back(poly, table);
    return cache.back().second;
}

uint32_t crc32_reflected(std::string_view in, uint32_t poly) {
    const auto& table = crc32_table(poly);
    uint32_t crc = 0xFFFFFFFFu;
    for (unsigned char c : in) crc = table[(crc ^ c) & 0xFF] ^ (crc >> 8);
    return crc ^ 0xFFFFFFFFu;
}

std::string be32(uint32_t v) {
    std::string out(4, '\0');
    for (int i = 0; i < 4; ++i) out[static_cast<size_t>(i)] =
        static_cast<char>((v >> (24 - 8 * i)) & 0xFF);
    return out;
}

// ---- OpenSSL wrappers ------------------------------------------------------------

const EVP_MD* digest_for(std::string_view name) {
    if (name == "md5")      return EVP_md5();
    if (name == "sha1")     return EVP_sha1();
    if (name == "sha256")   return EVP_sha256();
    if (name == "sha512")   return EVP_sha512();
    if (name == "sha3_256") return EVP_sha3_256();
    if (name == "sha3_512") return EVP_sha3_512();
    return nullptr;
}

std::string digest(const EVP_MD* md, std::string_view in) {
    std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)> ctx(EVP_MD_CTX_new(),
                                                               EVP_MD_CTX_free);
    if (!ctx) throw eval_error(ssl_error("failed to allocate a digest context"));
    unsigned char out[EVP_MAX_MD_SIZE];
    unsigned int n = 0;
    if (EVP_DigestInit_ex(ctx.get(), md, nullptr) != 1 ||
        EVP_DigestUpdate(ctx.get(), in.data(), in.size()) != 1 ||
        EVP_DigestFinal_ex(ctx.get(), out, &n) != 1)
        throw eval_error(ssl_error("digest failed"));
    return std::string(reinterpret_cast<char*>(out), n);
}

std::string hmac_of(const EVP_MD* md, std::string_view key, std::string_view in) {
    unsigned char out[EVP_MAX_MD_SIZE];
    unsigned int n = 0;
    if (!HMAC(md, key.data(), static_cast<int>(key.size()),
              reinterpret_cast<const unsigned char*>(in.data()), in.size(), out, &n))
        throw eval_error(ssl_error("hmac failed"));
    return std::string(reinterpret_cast<char*>(out), n);
}

const EVP_CIPHER* aes_cipher(std::string_view scheme, size_t key_len) {
    if (scheme == "ctr")
        return key_len == 16 ? EVP_aes_128_ctr()
             : key_len == 24 ? EVP_aes_192_ctr()
             : key_len == 32 ? EVP_aes_256_ctr() : nullptr;
    if (scheme == "ofb")
        return key_len == 16 ? EVP_aes_128_ofb()
             : key_len == 24 ? EVP_aes_192_ofb()
             : key_len == 32 ? EVP_aes_256_ofb() : nullptr;
    if (scheme == "cbc")
        return key_len == 16 ? EVP_aes_128_cbc()
             : key_len == 24 ? EVP_aes_192_cbc()
             : key_len == 32 ? EVP_aes_256_cbc() : nullptr;
    if (scheme == "gcm")
        return key_len == 16 ? EVP_aes_128_gcm()
             : key_len == 24 ? EVP_aes_192_gcm()
             : key_len == 32 ? EVP_aes_256_gcm() : nullptr;
    return nullptr;
}

std::string aes_run(std::string_view scheme, std::string_view key, std::string_view iv,
                    std::string_view in, bool encrypt) {
    const EVP_CIPHER* c = aes_cipher(scheme, key.size());
    if (!c) {
        if (scheme != "ctr" && scheme != "ofb" && scheme != "cbc" && scheme != "gcm")
            throw eval_error("unrecognized encryption type: " + std::string(scheme));
        throw eval_error("crypto/aes: invalid key size " + std::to_string(key.size()));
    }
    // The block modes need an IV exactly one block long; GCM takes a nonce of
    // any length, so its check is left to OpenSSL.
    if (scheme != "gcm" && iv.size() != 16)
        throw eval_error("the key must match the initialisation vector size");
    if (scheme == "cbc" && in.size() % 16 != 0)
        throw eval_error(encrypt ? "plaintext is not a multiple of the block size"
                                 : "ciphertext is not a multiple of the block size");

    std::unique_ptr<EVP_CIPHER_CTX, decltype(&EVP_CIPHER_CTX_free)> ctx(
        EVP_CIPHER_CTX_new(), EVP_CIPHER_CTX_free);
    if (!ctx) throw eval_error(ssl_error("failed to allocate a cipher context"));

    const bool gcm = scheme == "gcm";
    static constexpr size_t GCM_TAG = 16;
    if (EVP_CipherInit_ex(ctx.get(), c, nullptr, nullptr, nullptr, encrypt ? 1 : 0) != 1)
        throw eval_error(ssl_error("cipher init failed"));
    if (gcm && EVP_CIPHER_CTX_ctrl(ctx.get(), EVP_CTRL_AEAD_SET_IVLEN,
                                   static_cast<int>(iv.size()), nullptr) != 1)
        throw eval_error(ssl_error("failed to set the GCM nonce length"));
    if (EVP_CipherInit_ex(ctx.get(), nullptr, nullptr,
                          reinterpret_cast<const unsigned char*>(key.data()),
                          reinterpret_cast<const unsigned char*>(iv.data()),
                          encrypt ? 1 : 0) != 1)
        throw eval_error(ssl_error("cipher key/iv setup failed"));
    // CBC padding is off: the reference rejects a plaintext that is not a whole
    // number of blocks rather than padding it, so adding PKCS#7 here would make
    // our ciphertext a block longer than Go's.
    EVP_CIPHER_CTX_set_padding(ctx.get(), 0);

    std::string_view body = in;
    std::string tag;
    if (gcm && !encrypt) {
        // Go's GCM Seal appends the tag; Open expects it there.
        if (in.size() < GCM_TAG) throw eval_error("ciphertext is too short for a GCM tag");
        tag = std::string(in.substr(in.size() - GCM_TAG));
        body = in.substr(0, in.size() - GCM_TAG);
        if (EVP_CIPHER_CTX_ctrl(ctx.get(), EVP_CTRL_AEAD_SET_TAG,
                                static_cast<int>(GCM_TAG), tag.data()) != 1)
            throw eval_error(ssl_error("failed to set the GCM tag"));
    }

    std::string out(body.size() + 32, '\0');
    int n = 0, total = 0;
    if (EVP_CipherUpdate(ctx.get(), reinterpret_cast<unsigned char*>(out.data()), &n,
                         reinterpret_cast<const unsigned char*>(body.data()),
                         static_cast<int>(body.size())) != 1)
        throw eval_error(ssl_error("cipher update failed"));
    total = n;
    if (EVP_CipherFinal_ex(ctx.get(),
                           reinterpret_cast<unsigned char*>(out.data()) + total, &n) != 1)
        throw eval_error(gcm && !encrypt ? "cipher: message authentication failed"
                                         : ssl_error("cipher finalisation failed"));
    total += n;
    out.resize(static_cast<size_t>(total));
    if (gcm && encrypt) {
        unsigned char t[GCM_TAG];
        if (EVP_CIPHER_CTX_ctrl(ctx.get(), EVP_CTRL_AEAD_GET_TAG,
                                static_cast<int>(GCM_TAG), t) != 1)
            throw eval_error(ssl_error("failed to read the GCM tag"));
        out.append(reinterpret_cast<char*>(t), GCM_TAG);
    }
    return out;
}

// ---- Argon2 ------------------------------------------------------------------------

// Standard base64 without padding, which is the encoding a modular crypt hash
// uses for its salt and digest. Strict: a stray character means the stored
// hash is corrupt, not that it needs unwrapping.
std::string b64_nopad_decode(std::string_view in) {
    std::string out;
    if (!enc::b64_decode(in, out, enc::B64_STD, /*strict=*/true))
        throw eval_error("invalid base64 in the hashed secret");
    return out;
}

} // namespace

// Not static: the `memory` cache selects its shard with the same function the
// reference does (xxhash.ChecksumString64), so which shard a key lands in --
// and therefore when it expires -- has to agree bit for bit.
uint64_t xxhash64(std::string_view in) {
    static constexpr uint64_t P1 = 11400714785074694791ULL;
    static constexpr uint64_t P2 = 14029467366897019727ULL;
    static constexpr uint64_t P3 =  1609587929392839161ULL;
    static constexpr uint64_t P4 =  9650029242287828579ULL;
    static constexpr uint64_t P5 =  2870177450012600261ULL;
    auto rol = [](uint64_t x, int r) { return (x << r) | (x >> (64 - r)); };
    auto rd8 = [](const char* p) { uint64_t v; std::memcpy(&v, p, 8); return v; };
    auto rd4 = [](const char* p) { uint32_t v; std::memcpy(&v, p, 4); return v; };
    auto round = [&](uint64_t acc, uint64_t v) { return rol(acc + v * P2, 31) * P1; };

    const char* p = in.data();
    const char* const end = p + in.size();
    uint64_t h;
    if (in.size() >= 32) {
        uint64_t v1 = P1 + P2, v2 = P2, v3 = 0, v4 = 0 - P1;
        const char* const limit = end - 32;
        do {
            v1 = round(v1, rd8(p)); p += 8;
            v2 = round(v2, rd8(p)); p += 8;
            v3 = round(v3, rd8(p)); p += 8;
            v4 = round(v4, rd8(p)); p += 8;
        } while (p <= limit);
        h = rol(v1, 1) + rol(v2, 7) + rol(v3, 12) + rol(v4, 18);
        auto merge = [&](uint64_t acc, uint64_t v) {
            return (acc ^ round(0, v)) * P1 + P4;
        };
        h = merge(h, v1);
        h = merge(h, v2);
        h = merge(h, v3);
        h = merge(h, v4);
    } else {
        h = P5;
    }
    h += static_cast<uint64_t>(in.size());
    while (p + 8 <= end) {
        h = rol(h ^ (rol(rd8(p) * P2, 31) * P1), 27) * P1 + P4;
        p += 8;
    }
    if (p + 4 <= end) {
        h = rol(h ^ (static_cast<uint64_t>(rd4(p)) * P1), 23) * P2 + P3;
        p += 4;
    }
    while (p < end) {
        h = rol(h ^ (static_cast<uint64_t>(static_cast<unsigned char>(*p)) * P5), 11) * P1;
        ++p;
    }
    h ^= h >> 33; h *= P2;
    h ^= h >> 29; h *= P3;
    h ^= h >> 32;
    return h;
}

// ---- hash ----------------------------------------------------------------------------

value hash(const value& v, const value& algorithm, const value& key,
           const value& polynomial) {
    const std::string& in = want_string(v);
    const std::string& alg = want_string(algorithm);

    if (alg.rfind("hmac_", 0) == 0) {
        const EVP_MD* md = digest_for(std::string_view(alg).substr(5));
        if (!md) throw eval_error("unrecognized hash type: " + alg);
        if (!key.is_stringy())
            throw eval_error(alg + " requires a key");
        return value::bytes(hmac_of(md, key.as_string(), in));
    }
    if (const EVP_MD* md = digest_for(alg)) return value::bytes(digest(md, in));
    // These two return the digest rendered in DECIMAL, not the raw bytes: Go
    // formats them with strconv before handing them back.
    if (alg == "xxhash64") return value::bytes(std::to_string(xxhash64(in)));
    if (alg == "fnv32")    return value::bytes(std::to_string(fnv32(in)));
    if (alg == "crc32") {
        const std::string poly = polynomial.is_stringy() ? polynomial.as_string()
                                                         : std::string("IEEE");
        // Reversed polynomials, as Go's hash/crc32 declares them.
        uint32_t p = 0;
        if      (poly == "IEEE")       p = 0xedb88320u;
        else if (poly == "Castagnoli") p = 0x82f63b78u;
        else if (poly == "Koopman")    p = 0xeb31d82eu;
        else throw eval_error("unsupported crc32 hash key \"" + poly + "\"");
        return value::bytes(be32(crc32_reflected(in, p)));
    }
    throw eval_error("unrecognized hash type: " + alg);
}

// ---- uuid_v5 ------------------------------------------------------------------------

value uuid_v5(const value& v, const value& ns) {
    const std::string& name = want_string(v);
    // The four predefined namespaces from RFC 9562, plus any literal UUID.
    static const std::array<std::pair<const char*, const char*>, 4> PREDEFINED{{
        {"dns",  "6ba7b810-9dad-11d1-80b4-00c04fd430c8"},
        {"url",  "6ba7b811-9dad-11d1-80b4-00c04fd430c8"},
        {"oid",  "6ba7b812-9dad-11d1-80b4-00c04fd430c8"},
        {"x500", "6ba7b814-9dad-11d1-80b4-00c04fd430c8"},
    }};
    std::string nsu = "00000000-0000-0000-0000-000000000000";   // the nil UUID
    if (ns.is_stringy() && !ns.as_string().empty()) {
        const std::string& want = ns.as_string();
        bool found = false;
        for (const auto& [k, u] : PREDEFINED)
            if (want == k) { nsu = u; found = true; break; }
        if (!found) nsu = want;
    }
    std::string raw;
    int nib = -1;
    for (char c : nsu) {
        if (c == '-') continue;
        const int d = enc::hex_nibble(c);
        if (d < 0) throw eval_error("invalid namespace UUID: " + nsu);
        if (nib < 0) nib = d;
        else { raw += static_cast<char>(nib * 16 + d); nib = -1; }
    }
    if (raw.size() != 16 || nib >= 0) throw eval_error("invalid namespace UUID: " + nsu);

    std::string sum = digest(EVP_sha1(), raw + name);
    sum.resize(16);
    sum[6] = static_cast<char>((static_cast<unsigned char>(sum[6]) & 0x0F) | 0x50);
    sum[8] = static_cast<char>((static_cast<unsigned char>(sum[8]) & 0x3F) | 0x80);
    return value(enc::format_uuid(sum));
}

// ---- AES -------------------------------------------------------------------------------

value encrypt_aes(const value& v, const value& scheme, const value& key, const value& iv) {
    // A string, not bytes: the documented example chains .encode("hex"), which
    // accepts either, but `.type()` differs and the reference returns a string.
    return value(aes_run(want_string(scheme), want_string(key), want_string(iv),
                         want_string(v), /*encrypt=*/true));
}

value decrypt_aes(const value& v, const value& scheme, const value& key, const value& iv) {
    return value::bytes(aes_run(want_string(scheme), want_string(key), want_string(iv),
                                want_string(v), /*encrypt=*/false));
}

// ---- password comparison -----------------------------------------------------------------

value compare_argon2(const value& v, const value& hashed) {
    const std::string& secret = want_string(v);
    const std::string& enc = want_string(hashed);
    if (enc.empty()) throw eval_error("argon2: the hashed secret is empty");
    // $argon2id$v=19$m=4096,t=3,p=1$<salt-b64>$<hash-b64>
    std::vector<std::string> parts;
    for (size_t i = enc[0] == '$' ? 1 : 0; i <= enc.size();) {
        const size_t d = enc.find('$', i);
        parts.push_back(enc.substr(i, d == std::string::npos ? std::string::npos : d - i));
        if (d == std::string::npos) break;
        i = d + 1;
    }
    if (parts.size() != 5)
        throw eval_error("argon2: the hashed secret is not in the expected format");
    const std::string& variant = parts[0];
    if (variant != "argon2id" && variant != "argon2i" && variant != "argon2d")
        throw eval_error("argon2: unknown variant " + variant);
    unsigned version = 19;
    if (std::sscanf(parts[1].c_str(), "v=%u", &version) != 1)
        throw eval_error("argon2: missing version");
    unsigned m = 0, t = 0, p = 0;
    if (std::sscanf(parts[2].c_str(), "m=%u,t=%u,p=%u", &m, &t, &p) != 3)
        throw eval_error("argon2: missing parameters");
    const std::string salt = b64_nopad_decode(parts[3]);
    const std::string want = b64_nopad_decode(parts[4]);

    const std::string kdf_name = variant == "argon2id" ? "ARGON2ID"
                               : variant == "argon2i"  ? "ARGON2I" : "ARGON2D";
    std::unique_ptr<EVP_KDF, decltype(&EVP_KDF_free)> kdf(
        EVP_KDF_fetch(nullptr, kdf_name.c_str(), nullptr), EVP_KDF_free);
    if (!kdf) throw eval_error("argon2 is not available in this OpenSSL build");
    std::unique_ptr<EVP_KDF_CTX, decltype(&EVP_KDF_CTX_free)> ctx(
        EVP_KDF_CTX_new(kdf.get()), EVP_KDF_CTX_free);
    if (!ctx) throw eval_error(ssl_error("failed to allocate an argon2 context"));

    uint32_t threads = p;
    OSSL_PARAM params[] = {
        OSSL_PARAM_construct_octet_string(OSSL_KDF_PARAM_PASSWORD,
                                          const_cast<char*>(secret.data()), secret.size()),
        OSSL_PARAM_construct_octet_string(OSSL_KDF_PARAM_SALT,
                                          const_cast<char*>(salt.data()), salt.size()),
        OSSL_PARAM_construct_uint(OSSL_KDF_PARAM_ITER, &t),
        OSSL_PARAM_construct_uint(OSSL_KDF_PARAM_ARGON2_MEMCOST, &m),
        OSSL_PARAM_construct_uint(OSSL_KDF_PARAM_ARGON2_LANES, &p),
        OSSL_PARAM_construct_uint(OSSL_KDF_PARAM_THREADS, &threads),
        OSSL_PARAM_construct_end(),
    };
    std::string got(want.size(), '\0');
    if (EVP_KDF_derive(ctx.get(), reinterpret_cast<unsigned char*>(got.data()),
                       got.size(), params) != 1)
        throw eval_error(ssl_error("argon2 derivation failed"));
    return value(enc::constant_time_eq(got, want));
}

} // namespace sf::m
