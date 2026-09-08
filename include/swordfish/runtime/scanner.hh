// Scanners turn a byte stream into messages. Benthos models these as a separate
// component kind because file, s3, gcs, azure, socket and sftp all need the same
// framing logic.
#pragma once

#include "swordfish/message.hh"

#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace sf {

class scanner {
public:
    virtual ~scanner() = default;

    // Feed a chunk of input. Returns whatever complete messages it completes.
    virtual std::vector<message> feed(std::string_view chunk) = 0;

    // Called once the stream ends, to flush any partial trailing message.
    virtual std::vector<message> finish() = 0;

    virtual std::string name() const = 0;
};

using scanner_ptr = std::unique_ptr<scanner>;

// Production code builds scanners through make_scanner() below, keyed by a
// spec: with eleven kinds and options on most, one entry point is what keeps
// the interpreter and the code generator building the same thing. The two
// named factories below remain for the unit tests, which exercise the framing
// of a scanner directly rather than through a config.
scanner_ptr make_lines_scanner(size_t max_buffer = 64 * 1024 * 1024);
scanner_ptr make_to_the_end_scanner();

// How a scanner was configured. A single struct rather than one config type per
// scanner, because a scanner is chosen by a DYNAMIC key -- `scanner: { csv: {} }`
// -- which the spec_of field model cannot express; the same reason `batching`,
// `branch` and the composite outputs are lifted out of their configs and parsed
// by hand. Fields that do not apply to `kind` are simply unused.
struct scanner_spec {
    std::string kind = "lines";

    // lines
    std::string custom_delimiter;          // empty = "\n"
    int64_t     max_buffer_size = 65536;   // also re_match
    bool        omit_empty = false;

    // csv
    bool        parse_header_row  = true;
    bool        lazy_quotes       = false;
    bool        continue_on_error = false;

    // chunker
    int64_t     size = 0;

    // re_match
    std::string pattern;

    // decompress
    std::string algorithm;

    // avro
    bool        raw_json = false;

    // `skip_bom` and `decompress` wrap exactly one child (`into`); `switch`
    // wraps one per candidate. A vector covers all three rather than a
    // separately-named optional, so the recursion has one shape.
    std::vector<scanner_spec> children;
    // `switch` only: the `re_match_name` of each candidate, positionally paired
    // with `children`. An empty pattern is the catch-all the reference
    // documents. Kept beside the children rather than inside them because it
    // describes when a child is CHOSEN, not how it scans.
    std::vector<std::string>  child_matches;

    bool operator==(const scanner_spec&) const = default;
};

// Builds a scanner from its spec. `source_name` is the name of the thing being
// read -- a filename for `file`, empty elsewhere -- and exists for `switch`,
// which selects a child by matching against it.
scanner_ptr make_scanner(const scanner_spec& spec, std::string_view source_name = {});

// The names make_scanner() accepts. Used to validate a config and to say, in
// the error, what the alternatives are.
const std::vector<std::string_view>& scanner_kinds();

// Every scanner the reference documents, implemented or not. A name in here but
// not in scanner_kinds() is refused as UNIMPLEMENTED; a name in neither is
// refused as unknown. The two are different mistakes and get different
// messages -- "swordfish has not built this yet" versus "you made a typo".
const std::vector<std::string_view>& reference_scanner_kinds();

} // namespace sf
