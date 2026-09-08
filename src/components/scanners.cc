#include "swordfish/runtime/scanner.hh"

#include "scanner_avro.hh"

#include "swordfish/codecs.hh"
#include "swordfish/methods.hh"
#include "swordfish/value.hh"

#include <re2/re2.h>

#include <algorithm>
#include <cstring>
#include <optional>
#include <stdexcept>

namespace sf {
namespace {

class lines_scanner final : public scanner {
public:
    lines_scanner(size_t max_buffer, std::string delim, bool omit_empty)
        : _max(max_buffer), _delim(std::move(delim)), _omit_empty(omit_empty) {}

    std::vector<message> feed(std::string_view chunk) override {
        std::vector<message> out;
        _buf.append(chunk);
        size_t start = 0;
        for (;;) {
            const size_t nl = _buf.find(_delim, start);
            if (nl == std::string::npos) break;
            size_t end = nl;
            // CRLF is tolerated only for the DEFAULT delimiter. A custom one is
            // taken literally, because a user who chose `\r\n` or `;` did not
            // ask for anything to be trimmed off the end of it.
            if (_delim == "\n" && end > start && _buf[end - 1] == '\r') --end;
            // The COMPLETE token is measured, not just the unterminated residue
            // left at the end of the loop. Checking only the residue made
            // `max_buffer_size` inert for every token that happened to arrive
            // whole inside one read: a 500-byte line under a 100-byte limit came
            // out as a message here and failed the read with "bufio.Scanner:
            // token too long" in the reference, and which of the two you got
            // depended on where the read boundaries fell.
            // `>=`, not `>`. Go's bufio.Scanner needs room for the token AND
            // its delimiter, so a token of exactly max_buffer_size is already
            // "token too long" there -- measured at 10, 64 and 100 against
            // redpanda-connect 4.107.2, which fails at n == max and passes at
            // n == max - 1.
            if (end - start >= _max)
                throw std::runtime_error("line exceeds the scanner's maximum buffer size");
            emit(out, _buf.substr(start, end - start));
            start = nl + _delim.size();
        }
        if (start > 0) _buf.erase(0, start);
        if (_buf.size() >= _max)
            throw std::runtime_error("line exceeds the scanner's maximum buffer size");
        return out;
    }

    // A trailing line without a delimiter is still a message, as `bufio.Scanner`
    // would yield it. A trailing delimiter is not an empty final message.
    std::vector<message> finish() override {
        std::vector<message> out;
        if (!_buf.empty()) { emit(out, std::move(_buf)); _buf.clear(); }
        return out;
    }

    std::string name() const override { return "lines"; }

private:
    void emit(std::vector<message>& out, std::string line) {
        if (_omit_empty && line.empty()) return;
        out.push_back(message(std::move(line)));
    }

    std::string _buf;
    size_t      _max;
    std::string _delim;
    bool        _omit_empty;
};

// `chunker`: fixed-size byte chunks. The last chunk is whatever is left, which
// may be shorter -- the reference does not pad it.
class chunker_scanner final : public scanner {
public:
    explicit chunker_scanner(size_t size) : _size(size) {}
    std::vector<message> feed(std::string_view chunk) override {
        std::vector<message> out;
        _buf.append(chunk);
        size_t start = 0;
        while (_buf.size() - start >= _size) {
            out.push_back(message(_buf.substr(start, _size)));
            start += _size;
        }
        if (start > 0) _buf.erase(0, start);
        return out;
    }
    std::vector<message> finish() override {
        std::vector<message> out;
        if (!_buf.empty()) { out.push_back(message(std::move(_buf))); _buf.clear(); }
        return out;
    }
    std::string name() const override { return "chunker"; }
private:
    std::string _buf;
    size_t      _size;
};

class to_the_end_scanner final : public scanner {
public:
    std::vector<message> feed(std::string_view chunk) override {
        _buf.append(chunk);
        return {};
    }
    std::vector<message> finish() override {
        std::vector<message> out;
        out.push_back(message(std::move(_buf)));
        _buf.clear();
        return out;
    }
    std::string name() const override { return "to_the_end"; }
private:
    std::string _buf;
};


// ---- JSON ---------------------------------------------------------------------
//
// Both JSON scanners need the same thing: where does the next complete value in
// this buffer end? Answered by counting depth while respecting strings and
// escapes, which is enough to find a boundary without parsing the value.
// std::string::npos means "not complete yet, ask again when more has arrived".
// A JSON span, VALIDATED and re-serialised. The scanners used to emit the source
// bytes of the span they had walked, which was wrong twice over: a span that is
// structurally balanced but not valid JSON went out as a message -- `notjson`
// between two documents became a message here and made the reference report an
// error -- and the whitespace and key order of the source survived, where the
// reference decodes and re-encodes, so `{"b": 1, "a": 2}` reaches the pipeline
// as `{"a":2,"b":1}` there and unchanged here.
message canonical_json_message(std::string_view span, const char* who) {
    value v;
    try {
        v = parse_json(span);
    } catch (const std::exception& e) {
        // parse_json's own message already says "invalid JSON", so only the
        // scanner's name is added -- otherwise the reader saw it twice.
        throw std::runtime_error(std::string(who) + ": " + e.what());
    }
    return message(v.to_json());
}

size_t end_of_json_value(std::string_view s, size_t i) {
    while (i < s.size() && (s[i]==' '||s[i]=='\t'||s[i]=='\n'||s[i]=='\r')) ++i;
    if (i >= s.size()) return std::string::npos;
    int depth = 0;
    bool in_str = false, esc = false;
    for (size_t j = i; j < s.size(); ++j) {
        const char c = s[j];
        if (in_str) {
            if (esc)            esc = false;
            else if (c == '\\') esc = true;
            else if (c == '"')  in_str = false;
            continue;
        }
        if (c == '"') { in_str = true; continue; }
        // At depth 0 a closing bracket belongs to the CONTAINER around this
        // value, not to the value: it terminates a bare scalar. Checking the
        // brackets first swallowed it, so `[1, 2, 3]` lost its last element --
        // the closing `]` drove depth to -1 and the scan ran off the end.
        if (depth == 0 && j > i &&
            (c==','||c==']'||c=='}'||c==' '||c=='\t'||c=='\n'||c=='\r'))
            return j;
        if (c == '{' || c == '[') { ++depth; continue; }
        if (c == '}' || c == ']') {
            // A closing bracket where nothing is open is malformed, not the end
            // of a value. Decrementing regardless drove `depth` NEGATIVE, so the
            // scan ran past every later bracket and eventually emitted the rest
            // of the stream as one garbage message instead of reporting the
            // input.
            if (depth == 0) return std::string::npos;
            if (--depth == 0) return j + 1;
            continue;
        }
    }
    return std::string::npos;
}

// `json_documents`: a stream of concatenated JSON values, whitespace-separated.
class json_documents_scanner final : public scanner {
public:
    std::vector<message> feed(std::string_view chunk) override {
        _buf.append(chunk);
        return drain(false);
    }
    std::vector<message> finish() override { return drain(true); }
    std::string name() const override { return "json_documents"; }

private:
    std::vector<message> drain(bool at_end) {
        // The documents ALREADY read out of this chunk are handed over before
        // the failure is reported, and the failure comes back on the next call.
        // The reference does the same -- `{"a":1} notjson` gives it one message
        // and then an error -- and throwing straight from here discarded them,
        // so a stream with one bad token part-way through produced nothing at
        // all. The csv scanner carries this same `_pending` for the same reason.
        if (!_pending.empty()) {
            const std::string e = _pending;
            _pending.clear();
            throw std::runtime_error(e);
        }
        std::vector<message> out;
        size_t i = 0;
        for (;;) {
            size_t j = end_of_json_value(_buf, i);
            if (j == std::string::npos) {
                if (!at_end) break;
                size_t k = i;
                while (k < _buf.size() &&
                       (_buf[k]==' '||_buf[k]=='\t'||_buf[k]=='\n'||_buf[k]=='\r')) ++k;
                if (k >= _buf.size()) break;
                // A trailing BARE SCALAR is complete at end of stream -- nothing
                // could extend it. A value that opened a string, object or array
                // and never closed it is TRUNCATED, and emitting it produced a
                // message that is not valid JSON: `{"a":[1,2,{"b":` went out as
                // if it were a document. The reference reports "unexpected EOF".
                if (_buf[k] == '{' || _buf[k] == '[' || _buf[k] == '"') {
                    _pending = "json_documents: the stream ended inside a value";
                    break;
                }
                j = _buf.size();
            }
            size_t start = i;
            while (start < j && (_buf[start]==' '||_buf[start]=='\t'||
                                 _buf[start]=='\n'||_buf[start]=='\r')) ++start;
            if (start >= j) break;
            try {
                out.push_back(canonical_json_message(
                    std::string_view(_buf).substr(start, j - start), "json_documents"));
            } catch (const std::exception& e) {
                _pending = e.what();
                break;
            }
            i = j;
        }
        if (i > 0) _buf.erase(0, i);
        // At end of stream there is no next call to carry the failure, so it is
        // raised now -- after `out` has been handed back is impossible, so the
        // caller gets the error and finish() is what reports it.
        if (at_end && !_pending.empty() && out.empty()) {
            const std::string e = _pending;
            _pending.clear();
            throw std::runtime_error(e);
        }
        return out;
    }
    std::string _buf;
    std::string _pending;
};

// `json_array`: one top-level array, one message per element.
class json_array_scanner final : public scanner {
public:
    std::vector<message> feed(std::string_view chunk) override {
        _buf.append(chunk);
        return drain(false);
    }
    std::vector<message> finish() override {
        auto out = drain(true);
        // `_opened` still set means the last `[` never met its `]`. Reported
        // only when there is nothing to hand over first, for the reason above.
        if (_opened && out.empty())
            throw std::runtime_error(
                "json_array: the stream ended before the top-level array closed");
        return out;
    }
    std::string name() const override { return "json_array"; }

private:
    std::vector<message> drain(bool at_end) {
        // Elements already read out of this chunk are delivered before the
        // failure, which comes back on the next call -- the shape json_documents
        // and csv both use, and what the reference does.
        if (!_pending.empty()) {
            const std::string e = _pending;
            _pending.clear();
            throw std::runtime_error(e);
        }
        std::vector<message> out;
        size_t i = 0;
        const auto skip_ws = [&] {
            while (i < _buf.size() &&
                   (_buf[i]==' '||_buf[i]=='\t'||_buf[i]=='\n'||_buf[i]=='\r')) ++i;
        };
        // CONSECUTIVE arrays, not just the first. `[1,2] [3,4]` yields four
        // messages in the reference and yielded two here: the scanner latched
        // `_closed` on the first `]` and buffered everything after it for ever,
        // reporting nothing. Anything that is not another array after one closes
        // is still an error, which is what the reference says too ("failed to
        // read opening token").
        for (;;) {
            if (!_opened) {
                skip_ws();
                if (i >= _buf.size()) break;
                if (_buf[i] != '[') {
                    _pending = "json_array: expected a top-level array, got '" +
                               std::string(1, _buf[i]) + "'";
                    break;
                }
                ++i;
                _opened = true;
            }
            skip_ws();
            if (i >= _buf.size()) break;
            if (_buf[i] == ',') { ++i; continue; }
            if (_buf[i] == ']') { ++i; _opened = false; continue; }
            size_t j = end_of_json_value(_buf, i);
            if (j == std::string::npos) {
                if (!at_end) break;
                // A trailing BARE SCALAR is complete at end of stream -- nothing
                // could extend it -- which is the rule json_documents already
                // used. Treating it as mid-element lost the last element of an
                // unterminated array: `[1,2` gave the reference 1 and 2, and
                // gave swordfish only 1 before the error.
                if (_buf[i] == '{' || _buf[i] == '[' || _buf[i] == '"') {
                    _pending = "json_array: the stream ended mid-element";
                    break;
                }
                j = _buf.size();
            }
            size_t start = i;
            while (start < j && (_buf[start]==' '||_buf[start]=='\t'||
                                 _buf[start]=='\n'||_buf[start]=='\r')) ++start;
            try {
                out.push_back(canonical_json_message(
                    std::string_view(_buf).substr(start, j - start), "json_array"));
            } catch (const std::exception& e) {
                _pending = e.what();
                break;
            }
            i = j;
        }
        if (i > 0) _buf.erase(0, i);
        if (at_end && !_pending.empty() && out.empty()) {
            const std::string e = _pending;
            _pending.clear();
            throw std::runtime_error(e);
        }
        return out;
    }
    std::string _buf;
    std::string _pending;
    bool _opened = false;
};

// ---- csv ----------------------------------------------------------------------
//
// The record reader is the one in methods_parse.cc, shared with Bloblang's
// parse_csv so Go's quoting rules -- including `lazy_quotes` -- live in one
// place. What is added here is INCREMENTAL behaviour: a record that runs to the
// end of the buffer may simply be unfinished, so it is held back until more
// arrives or the stream ends.
class csv_scanner final : public scanner {
public:
    csv_scanner(char delim, bool header, bool lazy, bool continue_on_error)
        : _delim(delim), _header(header), _lazy(lazy), _continue(continue_on_error) {}

    std::vector<message> feed(std::string_view chunk) override {
        _buf.append(chunk);
        return drain(false);
    }
    std::vector<message> finish() override {
        auto out = drain(true);
        // The stream is over, so there is no next call to raise it on.
        if (!_pending.empty()) {
            const std::string e = _pending;
            _pending.clear();
            throw std::runtime_error(e);
        }
        return out;
    }
    std::string name() const override { return "csv"; }

private:
    std::vector<message> drain(bool at_end) {
        // An error raised part-way through a chunk must not discard the records
        // ALREADY read from it: the reference delivers every record before the
        // bad one and then reports. So the failure is held and thrown on the
        // next call, once what is in hand has been handed over.
        if (!_pending.empty()) {
            const std::string e = _pending;
            _pending.clear();
            throw std::runtime_error(e);
        }
        std::vector<message> out;
        size_t i = 0;
        for (;;) {
            if (i >= _buf.size()) break;
            size_t j = i;
            std::vector<std::string> row;
            bool got = false;
            std::string err;
            try {
                // `at_end` matters here rather than only in the check below: a
                // record that runs out of input INSIDE A QUOTED FIELD used to
                // throw before the hold-back could see it, so any CSV whose
                // quoted field happened to straddle a read boundary killed the
                // whole input layer. read_csv_record rewinds and returns false
                // instead when there may still be more bytes.
                got = m::read_csv_record(_buf, j, _delim, _lazy, row, at_end);
            } catch (const std::exception& e) {
                if (!_continue) throw;
                err = e.what();
                got = true;
                j = _buf.size();
            }
            if (!got) break;
            // Ran to the end of what we hold: unless the stream is over, the
            // record may still be growing.
            if (j >= _buf.size() && !at_end) break;
            i = j;
            if (err.empty()) {
                try {
                    if (auto msg = row_to_message(row)) out.push_back(std::move(*msg));
                } catch (const std::exception& e) {
                    _pending = e.what();
                    break;
                }
            } else {
                // `continue_on_error`: an empty message carrying the error, then
                // on to the next row -- the reference's shape exactly.
                message bad("");
                bad.set_error(err);
                out.push_back(std::move(bad));
            }
        }
        if (i > 0) _buf.erase(0, i);
        return out;
    }

    std::optional<message> row_to_message(const std::vector<std::string>& row) {
        // A blank trailing line is not a record.
        if (row.size() == 1 && row[0].empty()) return std::nullopt;
        if (_header && !_have_header) {
            _head = row;
            _have_header = true;
            _fields = row.size();
            return std::nullopt;
        }
        if (_fields == 0) _fields = row.size();
        // Go's encoding/csv, which the reference uses, requires every record to
        // have the field count the FIRST record established, and errors with
        // "record on line N: wrong number of fields" otherwise. This padded a
        // short row with empty strings and truncated a long one, so a misaligned
        // file produced plausible, wrong records in silence. `continue_on_error`
        // is what makes it survivable, and even then the reference maps only the
        // fields that are THERE rather than inventing empty ones.
        //
        // Under `continue_on_error` the record is still delivered, but it CARRIES
        // THE ERROR: the reference sets "record on line N: wrong number of
        // fields" on that message, so `error()` is non-null and `errored()` is
        // true. Emitting it clean meant a `switch` or `fallback` routing on
        // `errored()` never saw the ragged row at all -- the values matched the
        // reference exactly and only the error was missing, which is the kind of
        // difference a diff of message bodies cannot show.
        std::string ragged;
        if (row.size() != _fields) {
            ragged = "record on line " +
                     std::to_string(_row_index + (_header ? 2 : 1)) +
                     ": wrong number of fields";
            if (!_continue) throw std::runtime_error("csv: " + ragged);
        }
        value v;
        if (_header) {
            v = value::object();
            const size_t n = std::min(_head.size(), row.size());
            for (size_t k = 0; k < n; ++k) v.set(_head[k], value(row[k]));
        } else {
            std::vector<value> cells;
            cells.reserve(row.size());
            for (const auto& f : row) cells.push_back(value(f));
            v = value::array(std::move(cells));
        }
        message m = message::from_value(std::move(v));
        // The reference's wording exactly, and UNPREFIXED: the "csv: " prefix
        // belongs to the thrown form, which becomes a scanner failure rather
        // than a message.
        if (!ragged.empty()) m.set_error(ragged);
        // A STRING, not a number: Benthos metadata values are strings, and the
        // reference emits "0" where an integer 0 would render differently the
        // moment a mapping puts it in a document.
        m.meta().set("csv_row", value(std::to_string(_row_index++)));
        return m;
    }

    std::string _buf;
    std::string _pending;      // an error held until the rows before it are out
    char        _delim;
    bool        _header, _lazy, _continue;
    bool        _have_header = false;
    // The field count every record must have, set by the first one seen.
    size_t      _fields = 0;
    std::vector<std::string> _head;
    int64_t     _row_index = 0;
};

// ---- re_match -----------------------------------------------------------------
//
// A segment runs from one match to just before the NEXT one, so the first match
// opens the first message rather than closing it -- and the text before the
// first match is a message of its own, not something to discard.
//
// This follows scanner_re_match.go's split function literally, because two
// details of it are easy to get wrong and both were:
//
//  * matches are NON-OVERLAPPING (Go's FindAllIndex). Resuming the search one
//    byte past the segment start let a self-matching pattern match itself:
//    `aXa` over `aXaXa` split into `aX` and `aXa` here and stayed one message
//    there.
//  * the leading region IS emitted. Dropping it silently lost everything before
//    the first match -- and the whole stream when nothing matched at all.
class re_match_scanner final : public scanner {
public:
    re_match_scanner(const std::string& pattern, size_t max_buffer)
        : _re(pattern, RE2::Quiet), _max(max_buffer) {
        if (!_re.ok())
            throw std::runtime_error("re_match: bad pattern: " + _re.error());
    }

    std::vector<message> feed(std::string_view chunk) override {
        _buf.append(chunk);
        return drain(false);
    }
    std::vector<message> finish() override { return drain(true); }
    std::string name() const override { return "re_match"; }

private:
    // Up to `want` non-overlapping matches, which is what FindAllIndex returns.
    std::vector<std::pair<size_t, size_t>> find_matches(size_t want) const {
        std::vector<std::pair<size_t, size_t>> locs;
        const re2::StringPiece hay(_buf);
        size_t pos = 0;
        while (locs.size() < want && pos <= _buf.size()) {
            re2::StringPiece m;
            if (!_re.Match(hay, pos, _buf.size(), RE2::UNANCHORED, &m, 1)) break;
            const size_t start = static_cast<size_t>(m.data() - _buf.data());
            const size_t end   = start + m.size();
            locs.emplace_back(start, end);
            // A zero-width match would otherwise sit still for ever.
            pos = end > start ? end : start + 1;
        }
        return locs;
    }

    std::vector<message> drain(bool at_end) {
        std::vector<message> out;
        for (;;) {
            if (_buf.empty()) break;
            const auto locs = find_matches(2);
            size_t advance = 0;
            if (locs.empty()) {
                if (!at_end) break;          // a later chunk may still match
                advance = _buf.size();
            } else if (locs.size() == 1) {
                if (!at_end) break;          // the second match decides where this ends
                advance = locs[0].first == 0 ? _buf.size() : locs[0].first;
            } else {
                advance = locs[0].first == 0 ? locs[1].first : locs[0].first;
            }
            if (advance == 0) break;         // no progress to be made
            // Measured before it is emitted, for the reason the `lines` scanner
            // measures its token: a residual check only ever sees the tail, so a
            // segment that arrived whole inside one read slipped past the limit.
            if (advance >= _max)
                throw std::runtime_error("re_match: segment exceeds the maximum buffer size");
            out.push_back(message(_buf.substr(0, advance)));
            _buf.erase(0, advance);
        }
        if (_buf.size() >= _max)
            throw std::runtime_error("re_match: segment exceeds the maximum buffer size");
        return out;
    }

    RE2         _re;
    std::string _buf;
    size_t      _max;
};

// ---- tar ----------------------------------------------------------------------
//
// USTAR: 512-byte header, then the file rounded up to a 512-byte boundary. Only
// what is needed to walk the archive is decoded -- name, size, type -- because
// the scanner's job is to hand over each member's bytes, not to restore a
// filesystem.
class tar_scanner final : public scanner {
public:
    std::vector<message> feed(std::string_view chunk) override {
        _buf.append(chunk);
        return drain();
    }
    std::vector<message> finish() override {
        auto out = drain();
        // Trailing zero blocks are the archive's end marker, so leftover NULs
        // are not a truncation. Anything else is.
        if (_buf.find_first_not_of('\0') != std::string::npos)
            throw std::runtime_error("tar: the archive ended mid-entry");
        return out;
    }
    std::string name() const override { return "tar"; }
private:
    // A tar numeric field, read the way Go's archive/tar reads one
    // (strconv.go, parseNumeric/parseOctal). Three cases, and getting any of
    // them wrong loses the whole archive rather than one member, because a
    // false "invalid tar header" aborts the scan:
    //
    //  * BASE-256. A field whose top bit is set is a big-endian two's-complement
    //    integer in the remaining bits -- what GNU and star write for a member
    //    at or above 8 GiB. Without this branch such an archive was refused
    //    outright, taking its ordinary members down with it.
    //  * ALL PADDING. Go trims " \0" from both ends and returns ZERO for what is
    //    left of an empty field. A directory entry's size is written that way,
    //    so rejecting it threw away every archive containing a directory.
    //  * OCTAL DIGITS with optional space/NUL padding. Anything else really is a
    //    corrupt header: reading it as zero turned a garbage size field into an
    //    empty message where the reference reports "invalid tar header".
    static bool octal(const char* p, size_t n, size_t& out) {
        const auto* u = reinterpret_cast<const unsigned char*>(p);
        if (n > 0 && (u[0] & 0x80)) {
            // Sign comes from bit 6 of the first byte; a negative size is not
            // something a member can have, so it is a corrupt header here.
            if (u[0] & 0x40) return false;
            uint64_t v = 0;
            for (size_t i = 0; i < n; ++i) {
                const unsigned char c = i == 0 ? (u[i] & 0x7f) : u[i];
                if (v >> 56) return false;                    // would overflow
                v = (v << 8) | c;
            }
            if (v >> 63) return false;                        // Go's overflow check
            out = static_cast<size_t>(v);
            return true;
        }
        size_t v = 0;
        size_t i = 0;
        while (i < n && (p[i] == ' ' || p[i] == '\0')) ++i;          // leading pad
        const size_t first = i;
        for (; i < n && p[i] >= '0' && p[i] <= '7'; ++i) v = v * 8 + (p[i] - '0');
        if (i == first) {
            // Nothing but padding. Zero, not an error -- `bytes.Trim(b, " \x00")`
            // then `if len(b) == 0 { return 0 }`.
            for (size_t k = first; k < n; ++k)
                if (p[k] != ' ' && p[k] != '\0') return false;
            out = 0;
            return true;
        }
        for (; i < n; ++i)
            if (p[i] != ' ' && p[i] != '\0') return false;           // trailing junk
        out = v;
        return true;
    }

    // The header's own checksum: every byte summed with the checksum field read
    // as spaces. Without it a corrupt header was walked as if it were sound, and
    // its garbage name and size became a message.
    static bool header_ok(const char* h) {
        size_t want = 0;
        if (!octal(h + 148, 8, want)) return false;
        size_t sum = 0;
        for (size_t i = 0; i < 512; ++i)
            sum += (i >= 148 && i < 156) ? ' ' : static_cast<unsigned char>(h[i]);
        return sum == want;
    }
    std::vector<message> drain() {
        std::vector<message> out;
        for (;;) {
            if (_buf.size() < 512) break;
            // Two consecutive zero blocks end the archive; one is enough to stop.
            if (_buf.compare(0, 512, std::string(512, '\0')) == 0) { _buf.clear(); break; }
            if (!header_ok(_buf.data()))
                throw std::runtime_error("tar: invalid tar header");
            size_t size = 0;
            if (!octal(_buf.data() + 124, 12, size))
                throw std::runtime_error("tar: invalid tar header");
            const size_t padded = (size + 511) / 512 * 512;
            // The member's DATA is what a message needs; the padding after it is
            // not. Requiring the padding dropped the last complete member of a
            // truncated archive, which the reference still delivers.
            if (_buf.size() < 512 + size) break;
            const char type = _buf[156];
            // A GNU long-name entry ('L') is not a member: its BODY is the name
            // of the member that follows. Held and applied to the next header,
            // which is the only way a path over 100 bytes survives in a GNU
            // archive.
            if (type == 'L') {
                _pending_name.assign(_buf.data() + 512, size);
                while (!_pending_name.empty() && _pending_name.back() == '\0')
                    _pending_name.pop_back();
                _buf.erase(0, std::min(512 + padded, _buf.size()));
                continue;
            }
            // EVERY entry becomes a message, not just regular files. Emitting
            // only '0'/'\0' dropped directories, symlinks and everything else,
            // so an archive of three files in two directories yielded three
            // messages here against the reference's six -- and the difference
            // showed up as missing data rather than as an error. A directory has
            // no payload, so its message is empty, which is exactly what the
            // reference produces. Skipped: the PAX/GNU metadata entries ('x',
            // 'g', 'L', 'K'), which describe the NEXT member rather than being
            // one -- the reference's archive/tar consumes those too.
            if (type != 'x' && type != 'g' && type != 'K') {
                std::string member = _pending_name.empty() ? full_name(_buf.data())
                                                           : _pending_name;
                message m(_buf.substr(512, size));
                m.meta().set("tar_name", value(std::move(member)));
                out.push_back(std::move(m));
            }
            _pending_name.clear();
            _buf.erase(0, std::min(512 + padded, _buf.size()));
        }
        return out;
    }
    // The ustar PREFIX field. A path longer than 100 bytes is split in two: the
    // last 100 go in `name` at offset 0, the rest in `prefix` at offset 345, and
    // the full path is prefix + "/" + name. Reading `name` alone truncated every
    // such path to its tail -- `.../dddd.../eeeee` came back as `eeeee` cut at
    // 100 bytes total -- with no indication anything was missing.
    static std::string full_name(const char* h) {
        const std::string name(h, strnlen(h, 100));
        // Only ustar and POSIX archives have a prefix field; a v7 header has
        // padding there, which is NUL, so the check costs nothing.
        const std::string prefix(h + 345, strnlen(h + 345, 155));
        if (prefix.empty()) return name;
        return prefix + "/" + name;
    }

    std::string _buf;
    // A GNU 'L' entry's body, waiting for the header it names.
    std::string _pending_name;
};

// ---- the wrapping scanners ----------------------------------------------------

// `skip_bom`: strip ONE leading byte-order mark, then delegate.
//
// Three things here were wrong at once, and they are one function's worth of
// state, so they are fixed together (scanner_skip_bom.go is the reference):
//
//  * only the UTF-8 mark was known, so a UTF-16 or UTF-32 file kept its two or
//    four leading bytes and every downstream parse tripped over them;
//  * it stripped marks in a LOOP, so the doubled mark that concatenating two
//    BOM-prefixed files produces was swallowed entirely, where the reference
//    passes the second through as data -- hiding corrupt input rather than
//    surfacing it;
//  * the partial-mark hold-back returned WITHOUT erasing what it had already
//    matched, and its prefix test also matched an empty residue, so a stream
//    that ended on a mark boundary -- a file that is nothing but a BOM, say --
//    had that mark delivered as message content by finish().
class skip_bom_scanner final : public scanner {
public:
    explicit skip_bom_scanner(scanner_ptr into) : _into(std::move(into)) {}

    std::vector<message> feed(std::string_view chunk) override {
        if (_done) return _into->feed(chunk);
        _head.append(chunk);
        // Not yet decidable: shorter than the longest mark, and what is here is
        // still a proper prefix of one. Held until more arrives, or until
        // finish() says there is no more.
        if (_head.size() < longest_mark() && is_partial_mark(_head)) return {};
        strip_one_mark();
        _done = true;
        std::string rest = std::move(_head);
        _head.clear();
        return _into->feed(rest);
    }

    std::vector<message> finish() override {
        std::vector<message> out;
        if (!_done) {
            // The stream ended inside the hold-back. The reference reads UP TO
            // the longest mark and matches against whatever it got, so a
            // two-byte file holding just a UTF-16 mark yields nothing at all.
            strip_one_mark();
            _done = true;
            if (!_head.empty()) out = _into->feed(_head);
        }
        _head.clear();
        auto tail = _into->finish();
        for (auto& m : tail) out.push_back(std::move(m));
        return out;
    }
    std::string name() const override { return "skip_bom"; }

private:
    // Longest first, which is what makes UTF-32 LE (FF FE 00 00) win over
    // UTF-16 LE (FF FE) on the same opening bytes.
    static const std::vector<std::string>& marks() {
        static const std::vector<std::string> m{
            std::string("\x00\x00\xFE\xFF", 4),   // UTF-32 BE
            std::string("\xFF\xFE\x00\x00", 4),   // UTF-32 LE
            std::string("\xEF\xBB\xBF", 3),        // UTF-8
            std::string("\xFE\xFF", 2),             // UTF-16 BE
            std::string("\xFF\xFE", 2),             // UTF-16 LE
        };
        return m;
    }
    static size_t longest_mark() { return marks().front().size(); }

    static bool is_partial_mark(const std::string& s) {
        for (const auto& m : marks())
            if (s.size() < m.size() && m.compare(0, s.size(), s) == 0) return true;
        return false;
    }

    void strip_one_mark() {
        for (const auto& m : marks())
            if (_head.size() >= m.size() && _head.compare(0, m.size(), m) == 0) {
                _head.erase(0, m.size());
                return;                       // exactly one, as the reference does
            }
    }

    scanner_ptr _into;
    std::string _head;
    bool        _done = false;
};

// `decompress`: decompress, then feed a child scanner.
//
// The whole stream is buffered and decompressed at finish(), because swordfish's
// codecs are whole-buffer. The MESSAGES are identical to the reference's; what
// differs is that they all arrive at the end rather than as the stream is read,
// so a very large compressed source costs memory here that it would not there.
// Stated rather than hidden -- it is a resource difference, not a semantic one.
class decompress_scanner final : public scanner {
public:
    decompress_scanner(std::string algorithm, scanner_ptr into)
        : _alg(std::move(algorithm)), _into(std::move(into)) {}
    std::vector<message> feed(std::string_view chunk) override {
        _buf.append(chunk);
        return {};
    }
    std::vector<message> finish() override {
        std::string plain;
        if (!_buf.empty()) plain = decompress_all(_buf);
        _buf.clear();
        auto out = _into->feed(plain);
        auto tail = _into->finish();
        for (auto& m : tail) out.push_back(std::move(m));
        return out;
    }
    std::string name() const override { return "decompress"; }
private:
    std::string decompress_all(const std::string& in) const {
        if (_alg == "gzip" || _alg == "pgzip") return codec::gzip_decompress(in);
        if (_alg == "zlib")   return codec::zlib_decompress(in);
        if (_alg == "flate")  return codec::flate_decompress(in);
        if (_alg == "snappy") return codec::snappy_decompress(in);
        if (_alg == "lz4")    return codec::lz4_decompress(in);
        if (_alg == "zstd")   return codec::zstd_decompress(in);
        if (_alg == "bzip2")  return codec::bzip2_decompress(in);
        throw std::runtime_error("decompress: unrecognised algorithm '" + _alg + "'");
    }
    std::string _alg;
    scanner_ptr _into;
    std::string _buf;
};

} // namespace

// cppcheck-suppress unusedFunction ; used by tests/test_runtime.cc, which
// exercises the framing directly rather than through a config.
scanner_ptr make_lines_scanner(size_t max_buffer) {
    return std::make_unique<lines_scanner>(max_buffer, "\n", false);
}
scanner_ptr make_to_the_end_scanner() {
    return std::make_unique<to_the_end_scanner>();
}

const std::vector<std::string_view>& scanner_kinds() {
    // `avro` is conditional: it needs the vendored avro-cpp, which a build
    // without fmt installed does not have. Leaving it out of the list there is
    // what makes such a build say "not implemented by swordfish" instead of
    // accepting the config and failing on the first byte of the file.
    static const std::vector<std::string_view> k = [] {
        std::vector<std::string_view> v{
            "chunker", "csv", "decompress", "json_array", "json_documents",
            "lines", "re_match", "skip_bom", "switch", "tar", "to_the_end"};
        if (avro_scanner_available()) v.insert(v.begin(), "avro");
        return v;
    }();
    return k;
}

const std::vector<std::string_view>& reference_scanner_kinds() {
    // connect-main/docs/modules/components/pages/scanners/, which is public
    // documentation rather than enterprise-licensed source.
    static const std::vector<std::string_view> k{
        "avro", "chunker", "csv", "decompress", "json_array", "json_documents",
        "lines", "re_match", "skip_bom", "switch", "tar", "to_the_end"};
    return k;
}

scanner_ptr make_scanner(const scanner_spec& spec, std::string_view source_name) {
    const std::string& k = spec.kind;
    const auto child = [&](size_t i) {
        return i < spec.children.size() ? make_scanner(spec.children[i], source_name)
                                        : make_to_the_end_scanner();
    };
    const size_t max_buf = spec.max_buffer_size > 0
        ? static_cast<size_t>(spec.max_buffer_size) : 65536;

    if (k == "lines" || k.empty())
        return std::make_unique<lines_scanner>(
            max_buf, spec.custom_delimiter.empty() ? "\n" : spec.custom_delimiter,
            spec.omit_empty);
    if (k == "to_the_end")     return make_to_the_end_scanner();
    if (k == "json_documents") return std::make_unique<json_documents_scanner>();
    if (k == "json_array")     return std::make_unique<json_array_scanner>();
    if (k == "tar")            return std::make_unique<tar_scanner>();
    if (k == "avro")           return make_avro_scanner(spec.raw_json);
    if (k == "chunker") {
        if (spec.size <= 0)
            throw std::runtime_error("chunker: `size` must be greater than zero");
        return std::make_unique<chunker_scanner>(static_cast<size_t>(spec.size));
    }
    if (k == "csv") {
        // The reference takes a STRING for `custom_delimiter` and then requires
        // one character of it, so the check belongs here rather than in the type.
        char delim = ',';
        if (!spec.custom_delimiter.empty()) {
            if (spec.custom_delimiter.size() != 1)
                throw std::runtime_error(
                    "csv: `custom_delimiter` must be a single character");
            delim = spec.custom_delimiter[0];
        }
        return std::make_unique<csv_scanner>(delim, spec.parse_header_row,
                                             spec.lazy_quotes, spec.continue_on_error);
    }
    if (k == "re_match")   return std::make_unique<re_match_scanner>(spec.pattern, max_buf);
    if (k == "skip_bom")   return std::make_unique<skip_bom_scanner>(child(0));
    if (k == "decompress") return std::make_unique<decompress_scanner>(spec.algorithm, child(0));
    if (k == "switch") {
        // The FIRST candidate whose `re_match_name` matches the source wins, and
        // one with no pattern is the catch-all. Resolved when the scanner is
        // built rather than as data arrives, because the thing being matched is
        // the source's name and that is known up front.
        for (size_t i = 0; i < spec.children.size(); ++i) {
            const std::string& pat = i < spec.child_matches.size()
                ? spec.child_matches[i] : std::string();
            if (pat.empty()) return make_scanner(spec.children[i], source_name);
            RE2 re(pat, RE2::Quiet);
            if (!re.ok())
                throw std::runtime_error("switch: bad `re_match_name`: " + re.error());
            if (RE2::PartialMatch(re2::StringPiece(source_name.data(), source_name.size()), re))
                return make_scanner(spec.children[i], source_name);
        }
        // The reference rejects the data rather than guessing, and says the same.
        throw std::runtime_error(
            "switch: no candidate matched the source name '" + std::string(source_name) +
            "'; add a candidate with no `re_match_name` as a catch-all");
    }

    std::string implemented;
    for (auto kk : scanner_kinds())
        implemented += (implemented.empty() ? "" : ", ") + std::string(kk);
    const auto& all = reference_scanner_kinds();
    const bool known = std::find(all.begin(), all.end(), k) != all.end();
    throw std::runtime_error(
        "scanner '" + k + "' is " +
        (known ? "not implemented by swordfish" : "not a scanner") +
        "; swordfish implements " + implemented);
}

} // namespace sf
