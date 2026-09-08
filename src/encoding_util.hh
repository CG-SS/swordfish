// Byte-level encodings shared across the method implementations.
//
// Hex, base64 and UTF-8 encoding turned up independently in six files as the
// catalogue grew -- the JWT methods needed base64url, the crypto methods needed
// unpadded base64, the XML reader needed UTF-8, three places needed a
// constant-time comparison and three needed the 8-4-4-4-12 UUID shape. Six
// copies of a codec is six chances for one of them to be subtly different from
// the others, which for base64 padding or for a constant-time compare is a
// correctness or a security difference rather than a style one.
#pragma once

#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>

namespace sf::enc {

// ---- hex ---------------------------------------------------------------------

// The value of one hex digit, or -1. Case-insensitive.
inline int hex_nibble(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}

inline std::string hex_encode(std::string_view in) {
    static const char* H = "0123456789abcdef";
    std::string out;
    out.reserve(in.size() * 2);
    for (unsigned char c : in) { out += H[c >> 4]; out += H[c & 15]; }
    return out;
}

// ---- base64 --------------------------------------------------------------------

inline constexpr const char* B64_STD =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
// base64url swaps the last two characters; Go calls it URLEncoding, and
// RawURLEncoding is the same alphabet with padding omitted.
inline constexpr const char* B64_URL =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

inline std::string b64_encode(std::string_view in, const char* alpha = B64_STD,
                              bool pad = true) {
    std::string out;
    out.reserve((in.size() + 2) / 3 * 4);
    size_t i = 0;
    for (; i + 2 < in.size(); i += 3) {
        const uint32_t n = (static_cast<unsigned char>(in[i]) << 16) |
                           (static_cast<unsigned char>(in[i + 1]) << 8) |
                            static_cast<unsigned char>(in[i + 2]);
        out += alpha[(n >> 18) & 63]; out += alpha[(n >> 12) & 63];
        out += alpha[(n >> 6) & 63];  out += alpha[n & 63];
    }
    if (i < in.size()) {
        uint32_t n = static_cast<uint32_t>(static_cast<unsigned char>(in[i])) << 16;
        const bool two = i + 1 < in.size();
        if (two) n |= static_cast<uint32_t>(static_cast<unsigned char>(in[i + 1])) << 8;
        out += alpha[(n >> 18) & 63];
        out += alpha[(n >> 12) & 63];
        if (two) out += alpha[(n >> 6) & 63];
        else if (pad) out += '=';
        if (pad) out += '=';
    }
    return out;
}

// Padding is optional on input: '=' ends the stream and a missing one is fine,
// which covers both the padded and the raw spellings with one decoder.
// `strict` rejects a character outside the alphabet instead of skipping it --
// what a JWT or a password hash wants, where stray bytes mean a malformed
// token rather than line wrapping.
inline bool b64_decode(std::string_view in, std::string& out,
                       const char* alpha = B64_STD, bool strict = false) {
    uint32_t buf = 0;
    int bits = 0;
    for (char c : in) {
        if (c == '=') break;
        const char* p = c ? std::strchr(alpha, c) : nullptr;
        if (!p) {
            if (strict) return false;
            continue;                     // whitespace and other filler
        }
        buf = (buf << 6) | static_cast<uint32_t>(p - alpha);
        bits += 6;
        if (bits >= 8) { bits -= 8; out += static_cast<char>((buf >> bits) & 0xFF); }
    }
    return true;
}

// ---- UTF-8 -----------------------------------------------------------------------

inline void append_utf8(std::string& out, uint32_t cp) {
    // A surrogate or an out-of-range code point becomes U+FFFD, which is what
    // Go's utf8.EncodeRune does rather than emitting invalid bytes.
    if (cp > 0x10FFFF || (cp >= 0xD800 && cp <= 0xDFFF)) cp = 0xFFFD;
    if (cp < 0x80) { out += static_cast<char>(cp); return; }
    if (cp < 0x800) {
        out += static_cast<char>(0xC0 | (cp >> 6));
        out += static_cast<char>(0x80 | (cp & 0x3F));
        return;
    }
    if (cp < 0x10000) {
        out += static_cast<char>(0xE0 | (cp >> 12));
        out += static_cast<char>(0x80 | ((cp >> 6) & 0x3F));
        out += static_cast<char>(0x80 | (cp & 0x3F));
        return;
    }
    out += static_cast<char>(0xF0 | (cp >> 18));
    out += static_cast<char>(0x80 | ((cp >> 12) & 0x3F));
    out += static_cast<char>(0x80 | ((cp >> 6) & 0x3F));
    out += static_cast<char>(0x80 | (cp & 0x3F));
}

// Decodes one code point, advancing `i`. An invalid byte yields U+FFFD and
// consumes one byte, matching Go's utf8.DecodeRuneInString.
inline uint32_t next_rune(std::string_view s, size_t& i) {
    const auto b0 = static_cast<unsigned char>(s[i]);
    auto cont = [&](size_t k) {
        return i + k < s.size() && (static_cast<unsigned char>(s[i + k]) & 0xC0) == 0x80;
    };
    if (b0 < 0x80) { ++i; return b0; }
    if ((b0 & 0xE0) == 0xC0 && cont(1)) {
        const uint32_t cp = ((b0 & 0x1Fu) << 6) | (static_cast<unsigned char>(s[i + 1]) & 0x3Fu);
        i += 2;
        return cp < 0x80 ? 0xFFFD : cp;
    }
    if ((b0 & 0xF0) == 0xE0 && cont(1) && cont(2)) {
        const uint32_t cp = ((b0 & 0x0Fu) << 12) |
                            ((static_cast<unsigned char>(s[i + 1]) & 0x3Fu) << 6) |
                             (static_cast<unsigned char>(s[i + 2]) & 0x3Fu);
        i += 3;
        return cp < 0x800 ? 0xFFFD : cp;
    }
    if ((b0 & 0xF8) == 0xF0 && cont(1) && cont(2) && cont(3)) {
        const uint32_t cp = ((b0 & 0x07u) << 18) |
                            ((static_cast<unsigned char>(s[i + 1]) & 0x3Fu) << 12) |
                            ((static_cast<unsigned char>(s[i + 2]) & 0x3Fu) << 6) |
                             (static_cast<unsigned char>(s[i + 3]) & 0x3Fu);
        i += 4;
        return cp < 0x10000 ? 0xFFFD : cp;
    }
    ++i;
    return 0xFFFD;
}

// The length in bytes of the UTF-8 sequence starting at s[i]. A malformed or
// truncated sequence counts as one byte, so iteration always advances and never
// reads past the end.
inline size_t utf8_rune_len(std::string_view s, size_t i) {
    const auto b = static_cast<unsigned char>(s[i]);
    const size_t n = b < 0x80          ? 1
                   : (b & 0xE0) == 0xC0 ? 2
                   : (b & 0xF0) == 0xE0 ? 3
                   : (b & 0xF8) == 0xF0 ? 4 : 1;
    if (i + n > s.size()) return 1;
    for (size_t k = 1; k < n; ++k)
        if ((static_cast<unsigned char>(s[i + k]) & 0xC0) != 0x80) return 1;
    return n;
}

// ---- comparison and identifiers -----------------------------------------------------

// Compares in time that does not depend on where the first difference is. Used
// wherever the thing being compared is a secret or a signature: an early exit
// tells an attacker how many leading bytes were right.
inline bool constant_time_eq(std::string_view a, std::string_view b) {
    if (a.size() != b.size()) return false;
    unsigned char diff = 0;
    for (size_t i = 0; i < a.size(); ++i)
        diff |= static_cast<unsigned char>(a[i]) ^ static_cast<unsigned char>(b[i]);
    return diff == 0;
}

// The 8-4-4-4-12 canonical text form, from 16 raw bytes.
inline std::string format_uuid(std::string_view raw16) {
    const std::string h = hex_encode(raw16);
    return h.substr(0, 8) + "-" + h.substr(8, 4) + "-" + h.substr(12, 4) + "-" +
           h.substr(16, 4) + "-" + h.substr(20);
}

} // namespace sf::enc
