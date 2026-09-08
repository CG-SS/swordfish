// Kafka wire primitives: the byte-level types every request and response is
// built from.
//
// Two shapes of the same protocol, selected per API version:
//
//   classic   strings and arrays are length-prefixed with a signed int16/int32,
//             where -1 means null.
//   flexible  strings and arrays use an unsigned varint holding length+1, where
//             0 means null, and every struct ends with a tagged-field section.
//
// Which one applies is not a property of the API but of the VERSION -- Metadata
// is classic through v8 and flexible from v9 -- so the generated codecs thread a
// `flexible` flag through rather than branching on the API. See
// third_party/kafka-protocol/README.md.
#pragma once

#include "swordfish/value.hh"

#include <array>
#include <cstdint>
#include <cstring>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace sf::kafka {

// A malformed or truncated response. Distinct from a broker-reported error
// code, which is a normal protocol outcome rather than a framing failure.
class protocol_error : public std::runtime_error {
public:
    explicit protocol_error(const std::string& what) : std::runtime_error(what) {}
};

// Kafka's uuid is a bare 16 bytes, big-endian, with all-zero meaning "none".
struct uuid {
    std::array<uint8_t, 16> b{};
    bool operator==(const uuid&) const = default;
    bool is_zero() const noexcept {
        for (uint8_t x : b) if (x) return false;
        return true;
    }
};

// ---- writer ----------------------------------------------------------------

class writer {
public:
    void i8 (int8_t v)  { _b.push_back(static_cast<char>(v)); }
    void u8 (uint8_t v) { _b.push_back(static_cast<char>(v)); }
    void boolean(bool v) { u8(v ? 1 : 0); }

    void i16(int16_t v) { be(static_cast<uint16_t>(v)); }
    void u16(uint16_t v) { be(v); }
    void i32(int32_t v) { be(static_cast<uint32_t>(v)); }
    void u32(uint32_t v) { be(v); }
    void i64(int64_t v) { be(static_cast<uint64_t>(v)); }
    void u64(uint64_t v) { be(v); }
    void f64(double v)  { uint64_t u; std::memcpy(&u, &v, 8); be(u); }

    // Unsigned LEB128, seven bits per byte, low group first.
    void uvarint(uint32_t v) {
        while (v >= 0x80) { u8(static_cast<uint8_t>(v) | 0x80); v >>= 7; }
        u8(static_cast<uint8_t>(v));
    }
    void uvarlong(uint64_t v) {
        while (v >= 0x80) { u8(static_cast<uint8_t>(v) | 0x80); v >>= 7; }
        u8(static_cast<uint8_t>(v));
    }
    // Zigzag: negative numbers stay small, which is the point inside record
    // batches where deltas are usually tiny and often negative.
    //
    // The `v >> 31` / `v >> 63` on a signed value is well-defined here: since
    // C++20 (P0907R4) signed right shift is specified as an arithmetic shift on
    // a two's-complement representation, so it yields -1 for negative v and 0
    // otherwise, which is exactly the mask zigzag needs. cppcheck flags it as
    // implementation-defined because it judges against an older standard; this
    // project is pinned to C++23. Verified by round-tripping every boundary
    // value including INT32_MIN/INT64_MAX against the canonical zigzag encoding.
    // cppcheck-suppress-begin [shiftNegativeLHS, shiftTooManyBitsSigned]
    void varint(int32_t v)  { uvarint((static_cast<uint32_t>(v) << 1) ^ static_cast<uint32_t>(v >> 31)); }
    void varlong(int64_t v) { uvarlong((static_cast<uint64_t>(v) << 1) ^ static_cast<uint64_t>(v >> 63)); }
    // cppcheck-suppress-end [shiftNegativeLHS, shiftTooManyBitsSigned]

    void raw(std::string_view s) { _b.append(s); }
    void uuid_(const uuid& u) { _b.append(reinterpret_cast<const char*>(u.b.data()), u.b.size()); }

    void string(std::string_view s, bool flexible) {
        if (flexible) uvarint(static_cast<uint32_t>(s.size()) + 1);
        else          i16(static_cast<int16_t>(s.size()));
        raw(s);
    }
    void nullable_string(const std::optional<std::string>& s, bool flexible) {
        if (!s) { if (flexible) uvarint(0); else i16(-1); return; }
        string(*s, flexible);
    }
    void bytes(std::string_view s, bool flexible) {
        if (flexible) uvarint(static_cast<uint32_t>(s.size()) + 1);
        else          i32(static_cast<int32_t>(s.size()));
        raw(s);
    }
    void nullable_bytes(const std::optional<std::string>& s, bool flexible) {
        if (!s) { if (flexible) uvarint(0); else i32(-1); return; }
        bytes(*s, flexible);
    }
    void array_len(size_t n, bool flexible) {
        if (flexible) uvarint(static_cast<uint32_t>(n) + 1);
        else          i32(static_cast<int32_t>(n));
    }
    void null_array(bool flexible) {
        if (flexible) uvarint(0); else i32(-1);
    }
    // An empty tagged-field section. Every flexible struct ends with one; we
    // never emit tagged fields ourselves, but must always emit the count.
    void empty_tags() { uvarint(0); }

    // Length-prefix a region written by `f`: Kafka frames whole requests, and
    // record batches carry their own length, both as an int32 that is only
    // known afterwards.
    template <class F>
    void with_i32_length(F&& f) {
        const size_t at = _b.size();
        i32(0);
        f();
        patch_i32(at, static_cast<int32_t>(_b.size() - at - 4));
    }

    void patch_i32(size_t at, int32_t v) {
        const uint32_t u = static_cast<uint32_t>(v);
        _b[at + 0] = static_cast<char>(u >> 24);
        _b[at + 1] = static_cast<char>(u >> 16);
        _b[at + 2] = static_cast<char>(u >> 8);
        _b[at + 3] = static_cast<char>(u);
    }

    size_t             size() const noexcept { return _b.size(); }
    const std::string& str()  const noexcept { return _b; }
    std::string        take() { return std::move(_b); }
    std::string_view   view() const noexcept { return _b; }

private:
    template <class U>
    void be(U v) {
        char t[sizeof(U)];
        for (size_t i = 0; i < sizeof(U); ++i)
            t[i] = static_cast<char>(v >> (8 * (sizeof(U) - 1 - i)));
        _b.append(t, sizeof(U));
    }
    std::string _b;
};

// ---- reader ----------------------------------------------------------------

// Non-owning: the caller keeps the buffer alive for the reader's lifetime,
// which is always true of a response being decoded in place.
class reader {
public:
    explicit reader(std::string_view b) noexcept : _b(b) {}

    int8_t  i8()  { return static_cast<int8_t>(u8()); }
    uint8_t u8()  { need(1); return static_cast<uint8_t>(_b[_i++]); }
    bool    boolean() { return u8() != 0; }

    int16_t  i16() { return static_cast<int16_t>(be<uint16_t>()); }
    uint16_t u16() { return be<uint16_t>(); }
    int32_t  i32() { return static_cast<int32_t>(be<uint32_t>()); }
    uint32_t u32() { return be<uint32_t>(); }
    int64_t  i64() { return static_cast<int64_t>(be<uint64_t>()); }
    uint64_t u64() { return be<uint64_t>(); }
    double   f64() { const uint64_t u = be<uint64_t>(); double d; std::memcpy(&d, &u, 8); return d; }

    uint32_t uvarint() {
        uint32_t v = 0;
        for (int shift = 0; shift <= 28; shift += 7) {
            const uint8_t byte = u8();
            v |= static_cast<uint32_t>(byte & 0x7F) << shift;
            if (!(byte & 0x80)) return v;
        }
        throw protocol_error("varint is longer than five bytes");
    }
    uint64_t uvarlong() {
        uint64_t v = 0;
        for (int shift = 0; shift <= 63; shift += 7) {
            const uint8_t byte = u8();
            v |= static_cast<uint64_t>(byte & 0x7F) << shift;
            if (!(byte & 0x80)) return v;
        }
        throw protocol_error("varlong is longer than ten bytes");
    }
    int32_t varint()  { const uint32_t u = uvarint();  return static_cast<int32_t>((u >> 1) ^ (~(u & 1) + 1)); }
    int64_t varlong() { const uint64_t u = uvarlong(); return static_cast<int64_t>((u >> 1) ^ (~(u & 1) + 1)); }

    std::string_view raw(size_t n) {
        need(n);
        const auto s = _b.substr(_i, n);
        _i += n;
        return s;
    }
    uuid uuid_() {
        const auto s = raw(16);
        uuid u;
        std::memcpy(u.b.data(), s.data(), 16);
        return u;
    }

    std::string string(bool flexible) {
        auto s = nullable_string(flexible);
        if (!s) throw protocol_error("unexpected null string");
        return std::move(*s);
    }
    std::optional<std::string> nullable_string(bool flexible) {
        int64_t n;
        if (flexible) {
            const uint32_t x = uvarint();
            if (x == 0) return std::nullopt;
            n = static_cast<int64_t>(x) - 1;
        } else {
            n = i16();
            // cppcheck-suppress knownConditionTrueFalse
            //   i16() genuinely returns negatives; the analyser loses the sign
            //   through the byte-assembly template. -1 IS the null encoding.
            if (n < 0) return std::nullopt;
        }
        return std::string(raw(static_cast<size_t>(n)));
    }
    std::string bytes(bool flexible) {
        auto s = nullable_bytes(flexible);
        if (!s) throw protocol_error("unexpected null bytes");
        return std::move(*s);
    }
    std::optional<std::string> nullable_bytes(bool flexible) {
        int64_t n;
        if (flexible) {
            const uint32_t x = uvarint();
            if (x == 0) return std::nullopt;
            n = static_cast<int64_t>(x) - 1;
        } else {
            n = i32();
            // cppcheck-suppress knownConditionTrueFalse
            //   As above: -1 is how the protocol spells null.
            if (n < 0) return std::nullopt;
        }
        return std::string(raw(static_cast<size_t>(n)));
    }
    // -1 (or 0 flexible) means a null array, which the schemas distinguish from
    // an empty one. Returns -1 for null so callers can tell them apart.
    //
    // The length is CHECKED against the bytes left. Every generated decoder
    // resizes to whatever comes back, so an unchecked value straight off the
    // wire -- from a corrupt frame, a version mismatch that desynchronises the
    // reader, or a hostile peer -- asks for an allocation of up to two billion
    // elements before a single one is read. Every array element in every Kafka
    // schema occupies at least one byte, so a length greater than the remaining
    // bytes cannot be honest, and rejecting it costs nothing on valid input.
    int64_t array_len(bool flexible) {
        int64_t n;
        if (flexible) {
            const uint32_t x = uvarint();
            if (x == 0) return -1;
            n = static_cast<int64_t>(x) - 1;
        } else {
            n = i32();
            // cppcheck-suppress knownConditionTrueFalse
            //   As elsewhere in this file: the value comes off the wire, and -1
            //   is how the protocol spells a null array.
            if (n < 0) return n;
        }
        if (static_cast<uint64_t>(n) > remaining())
            throw protocol_error("array length " + std::to_string(n) +
                                 " exceeds the " + std::to_string(remaining()) +
                                 " bytes remaining");
        return n;
    }

    // Tagged fields are the protocol's forward-compatibility escape hatch: a
    // newer broker may send tags we have never heard of, and skipping them is
    // both correct and required.
    void skip_tags() {
        const uint32_t n = uvarint();
        for (uint32_t i = 0; i < n; ++i) {
            uvarint();                                 // tag
            raw(static_cast<size_t>(uvarint()));       // size, then the body
        }
    }

    size_t remaining() const noexcept { return _b.size() - _i; }
    size_t pos() const noexcept { return _i; }
    void   seek(size_t at) { if (at > _b.size()) throw protocol_error("seek past end"); _i = at; }
    bool   empty() const noexcept { return _i >= _b.size(); }
    std::string_view rest() const noexcept { return _b.substr(_i); }

private:
    void need(size_t n) const {
        if (_i + n > _b.size())
            throw protocol_error("truncated response: wanted " + std::to_string(n) +
                                 " byte(s), " + std::to_string(_b.size() - _i) + " left");
    }
    template <class U>
    U be() {
        need(sizeof(U));
        U v = 0;
        for (size_t k = 0; k < sizeof(U); ++k)
            v = static_cast<U>(v << 8) | static_cast<uint8_t>(_b[_i + k]);
        _i += sizeof(U);
        return v;
    }
    std::string_view _b;
    size_t           _i = 0;
};

} // namespace sf::kafka
