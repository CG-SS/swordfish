// JSON Web Tokens: sign_jwt_* and parse_jwt_*.
//
// Eighteen method names, three algorithm families and one implementation each:
// the family (HS, RS, ES) decides how the signature is produced, the digest
// width (256/384/512) is a parameter, and the registry maps each name onto the
// pair. Writing eighteen separate functions would be eighteen places for the
// base64url handling to drift.
//
// One detail is easy to get wrong and impossible to miss once wrong: an ECDSA
// JWT signature is the raw R||S pair at the curve's byte width, NOT the DER
// SEQUENCE that OpenSSL produces. A DER signature verifies with OpenSSL and is
// rejected by every other JWT implementation.
#include "swordfish/methods.hh"
#include "swordfish/value.hh"

#include "encoding_util.hh"
#include "methods_util.hh"

#include <openssl/bio.h>
#include <openssl/bn.h>
#include <openssl/ec.h>
#include <openssl/ecdsa.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/pem.h>

#include <cstring>
#include <memory>
#include <string>
#include <vector>

namespace sf::m {

namespace {

std::string jwt_error(const char* what) {
    const unsigned long e = ERR_get_error();
    char buf[256] = {};
    if (e) ERR_error_string_n(e, buf, sizeof buf);
    return std::string(what) + (e ? std::string(": ") + buf : std::string());
}

// ---- base64url without padding ----------------------------------------------

std::string b64url(std::string_view in) {
    return enc::b64_encode(in, enc::B64_URL, /*pad=*/false);
}

std::string b64url_decode(std::string_view in) {
    std::string out;
    // Strict: a character outside the alphabet means a malformed token, and
    // skipping it would let a tampered signature decode to a shorter one.
    if (!enc::b64_decode(in, out, enc::B64_URL, /*strict=*/true))
        throw eval_error("invalid base64url in the token");
    return out;
}

// ---- keys ---------------------------------------------------------------------

using pkey_ptr = std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)>;

pkey_ptr read_pem(const std::string& pem, bool private_key) {
    std::unique_ptr<BIO, decltype(&BIO_free)> bio(
        BIO_new_mem_buf(pem.data(), static_cast<int>(pem.size())), BIO_free);
    if (!bio) throw eval_error("failed to allocate a PEM buffer");
    EVP_PKEY* k = private_key
        ? PEM_read_bio_PrivateKey(bio.get(), nullptr, nullptr, nullptr)
        : PEM_read_bio_PUBKEY(bio.get(), nullptr, nullptr, nullptr);
    if (!k) throw eval_error(jwt_error(private_key ? "failed to parse the private key"
                                                   : "failed to parse the public key"));
    return pkey_ptr(k, EVP_PKEY_free);
}

const EVP_MD* md_for(int bits) {
    return bits == 256 ? EVP_sha256() : bits == 384 ? EVP_sha384() : EVP_sha512();
}

// The R and S component width for each ECDSA curve, which is what the JWT
// signature encoding is built from: 32 bytes for P-256, 48 for P-384 and 66 for
// P-521 -- note 66, not 64: the curve is 521 bits.
size_t ec_component_len(const EVP_PKEY* k) {
    const int bits = EVP_PKEY_get_bits(k);
    return static_cast<size_t>((bits + 7) / 8);
}

std::string der_to_raw(std::string_view der, size_t comp) {
    const unsigned char* p = reinterpret_cast<const unsigned char*>(der.data());
    std::unique_ptr<ECDSA_SIG, decltype(&ECDSA_SIG_free)> sig(
        d2i_ECDSA_SIG(nullptr, &p, static_cast<long>(der.size())), ECDSA_SIG_free);
    if (!sig) throw eval_error(jwt_error("failed to decode the ECDSA signature"));
    const BIGNUM* r = nullptr;
    const BIGNUM* s = nullptr;
    ECDSA_SIG_get0(sig.get(), &r, &s);
    std::string out(comp * 2, '\0');
    BN_bn2binpad(r, reinterpret_cast<unsigned char*>(out.data()), static_cast<int>(comp));
    BN_bn2binpad(s, reinterpret_cast<unsigned char*>(out.data()) + comp,
                 static_cast<int>(comp));
    return out;
}

std::string raw_to_der(std::string_view raw, size_t comp) {
    if (raw.size() != comp * 2)
        throw eval_error("ECDSA signature has the wrong length for this curve");
    std::unique_ptr<ECDSA_SIG, decltype(&ECDSA_SIG_free)> sig(ECDSA_SIG_new(),
                                                              ECDSA_SIG_free);
    BIGNUM* r = BN_bin2bn(reinterpret_cast<const unsigned char*>(raw.data()),
                          static_cast<int>(comp), nullptr);
    BIGNUM* s = BN_bin2bn(reinterpret_cast<const unsigned char*>(raw.data()) + comp,
                          static_cast<int>(comp), nullptr);
    if (!sig || !r || !s) { BN_free(r); BN_free(s); throw eval_error("out of memory"); }
    if (ECDSA_SIG_set0(sig.get(), r, s) != 1) {   // takes ownership only on success
        BN_free(r);
        BN_free(s);
        throw eval_error("failed to assemble the ECDSA signature");
    }
    unsigned char* der = nullptr;
    const int n = i2d_ECDSA_SIG(sig.get(), &der);
    if (n <= 0) throw eval_error(jwt_error("failed to encode the ECDSA signature"));
    std::string out(reinterpret_cast<char*>(der), static_cast<size_t>(n));
    OPENSSL_free(der);
    return out;
}

// ---- signing and verification ---------------------------------------------------

std::string sign_bytes(jwt_family fam, int bits, const std::string& secret,
                       std::string_view data) {
    if (fam == jwt_family::hmac) {
        unsigned char out[EVP_MAX_MD_SIZE];
        unsigned int n = 0;
        if (!HMAC(md_for(bits), secret.data(), static_cast<int>(secret.size()),
                  reinterpret_cast<const unsigned char*>(data.data()), data.size(), out, &n))
            throw eval_error(jwt_error("HMAC failed"));
        return std::string(reinterpret_cast<char*>(out), n);
    }
    pkey_ptr key = read_pem(secret, /*private_key=*/true);
    std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)> ctx(EVP_MD_CTX_new(),
                                                                EVP_MD_CTX_free);
    if (!ctx) throw eval_error("failed to allocate a signing context");
    if (EVP_DigestSignInit(ctx.get(), nullptr, md_for(bits), nullptr, key.get()) != 1)
        throw eval_error(jwt_error("signing setup failed"));
    size_t n = 0;
    if (EVP_DigestSign(ctx.get(), nullptr, &n,
                       reinterpret_cast<const unsigned char*>(data.data()),
                       data.size()) != 1)
        throw eval_error(jwt_error("signing failed"));
    std::string sig(n, '\0');
    if (EVP_DigestSign(ctx.get(), reinterpret_cast<unsigned char*>(sig.data()), &n,
                       reinterpret_cast<const unsigned char*>(data.data()),
                       data.size()) != 1)
        throw eval_error(jwt_error("signing failed"));
    sig.resize(n);
    if (fam == jwt_family::ecdsa) return der_to_raw(sig, ec_component_len(key.get()));
    return sig;
}

bool verify_bytes(jwt_family fam, int bits, const std::string& secret,
                  std::string_view data, std::string_view sig) {
    if (fam == jwt_family::hmac) {
        return enc::constant_time_eq(sign_bytes(fam, bits, secret, data), sig);
    }
    pkey_ptr key = read_pem(secret, /*private_key=*/false);
    std::string der;
    if (fam == jwt_family::ecdsa) {
        der = raw_to_der(sig, ec_component_len(key.get()));
        sig = der;
    }
    std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)> ctx(EVP_MD_CTX_new(),
                                                                EVP_MD_CTX_free);
    if (!ctx) throw eval_error("failed to allocate a verification context");
    if (EVP_DigestVerifyInit(ctx.get(), nullptr, md_for(bits), nullptr, key.get()) != 1)
        throw eval_error(jwt_error("verification setup failed"));
    const int r = EVP_DigestVerify(ctx.get(),
                                   reinterpret_cast<const unsigned char*>(sig.data()),
                                   sig.size(),
                                   reinterpret_cast<const unsigned char*>(data.data()),
                                   data.size());
    // Clear the queued error so a later unrelated call does not report it.
    if (r != 1) ERR_clear_error();
    return r == 1;
}

const char* alg_name(jwt_family fam, int bits) {
    switch (fam) {
    case jwt_family::hmac:  return bits == 256 ? "HS256" : bits == 384 ? "HS384" : "HS512";
    case jwt_family::rsa:   return bits == 256 ? "RS256" : bits == 384 ? "RS384" : "RS512";
    case jwt_family::ecdsa: return bits == 256 ? "ES256" : bits == 384 ? "ES384" : "ES512";
    }
    return "";
}

// Header fields the caller may not override: they describe the signature and
// the key, and letting a mapping set them would make the token lie about how it
// was produced.
bool reserved_header(std::string_view k) {
    return k == "alg" || k == "typ" || k == "jku" || k == "jwk" || k == "x5u" ||
           k == "x5c" || k == "x5t" || k == "x5t#S256" || k == "crit";
}

} // namespace

value sign_jwt(const value& claims, jwt_family fam, int bits, const value& secret,
               const value& headers) {
    if (claims.type() != vtype::object) wrong("object", claims);
    value header = value::object();
    header.set("alg", value(std::string(alg_name(fam, bits))));
    header.set("typ", value(std::string("JWT")));
    if (headers.type() == vtype::object) {
        for (const auto& [k, v] : headers.obj())
            if (!reserved_header(k)) header.set(k, v);
    } else if (!headers.is_null() && !headers.is_nothing()) {
        throw eval_error("headers parameter must be an object");
    }
    const std::string signing_input = b64url(header.to_json()) + "." +
                                      b64url(claims.to_json());
    const std::string sig = sign_bytes(fam, bits, want_string(secret), signing_input);
    return value(signing_input + "." + b64url(sig));
}

value parse_jwt(const value& token, jwt_family fam, int bits, const value& secret) {
    const std::string& s = want_string(token);
    const size_t d1 = s.find('.');
    const size_t d2 = d1 == std::string::npos ? std::string::npos : s.find('.', d1 + 1);
    if (d1 == std::string::npos || d2 == std::string::npos ||
        s.find('.', d2 + 1) != std::string::npos)
        throw eval_error("parsing JWT string: token contains an invalid number of segments");

    const value header = parse_json(b64url_decode(s.substr(0, d1)));
    const value* alg = header.type() == vtype::object ? header.find("alg") : nullptr;
    const std::string want_alg = alg_name(fam, bits);
    if (!alg || !alg->is_stringy() || alg->as_string() != want_alg)
        throw eval_error("parsing JWT string: incorrect signing method: " +
                         (alg && alg->is_stringy() ? alg->as_string() : std::string("none")));

    const std::string sig = b64url_decode(s.substr(d2 + 1));
    if (!verify_bytes(fam, bits, want_string(secret), s.substr(0, d2), sig))
        throw eval_error("parsing JWT string: signature is invalid");

    // Claims only. Validating exp/nbf is documented as NOT happening here.
    return parse_json(b64url_decode(s.substr(d1 + 1, d2 - d1 - 1)));
}

// The eighteen documented names.
#define SF_JWT_PAIR(name, fam, bits)                                            \
    value sign_jwt_##name(const value& v, const value& secret,                  \
                          const value& headers) {                               \
        return sign_jwt(v, fam, bits, secret, headers);                         \
    }                                                                           \
    value parse_jwt_##name(const value& v, const value& secret) {               \
        return parse_jwt(v, fam, bits, secret);                                 \
    }
SF_JWT_PAIR(hs256, jwt_family::hmac, 256)
SF_JWT_PAIR(hs384, jwt_family::hmac, 384)
SF_JWT_PAIR(hs512, jwt_family::hmac, 512)
SF_JWT_PAIR(rs256, jwt_family::rsa, 256)
SF_JWT_PAIR(rs384, jwt_family::rsa, 384)
SF_JWT_PAIR(rs512, jwt_family::rsa, 512)
SF_JWT_PAIR(es256, jwt_family::ecdsa, 256)
SF_JWT_PAIR(es384, jwt_family::ecdsa, 384)
SF_JWT_PAIR(es512, jwt_family::ecdsa, 512)
#undef SF_JWT_PAIR

} // namespace sf::m
