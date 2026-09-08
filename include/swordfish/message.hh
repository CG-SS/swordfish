// sf::message / sf::batch — the unit of flow.
//
// Mirrors benthos-main/internal/message/data.go: two representations (raw bytes
// and structured) with lazy conversion, plus copy-on-write so shallow copies are
// cheap. That design is not incidental in Benthos and is not reinvented here.
#pragma once

#include "swordfish/value.hh"

#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace sf {

// Metadata is a small sorted vector, not a hash map: entries are few, lookups
// are rare relative to iteration, and sorted order makes output deterministic.
class metadata {
public:
    const value* find(std::string_view k) const noexcept;
    void         set(std::string_view k, value v);
    void         remove(std::string_view k);
    size_t       size()  const noexcept { return e_.size(); }
    bool         empty() const noexcept { return e_.empty(); }
    const std::vector<std::pair<std::string, value>>& entries() const noexcept { return e_; }
    void         clear() noexcept { e_.clear(); }
private:
    std::vector<std::pair<std::string, value>> e_;
};

// Which component produced a processing error. Held behind a pointer so a
// message that never fails carries one word rather than three strings, and
// shared because a batch's error backup is a shallow copy.
// Where a `sync_response` output writes, when the input that produced this
// message can answer synchronously. Defined in runtime/sync_response.hh;
// forward-declared here so the core message type does not depend on the runtime.
class response_store;

struct error_source_info {
    std::string name;    // the component's type, e.g. "mapping"
    std::string label;   // the user's `label:`, empty when none was set
    std::string path;    // where it sits in the config, e.g. "pipeline.processors.1"
};

class message {
public:
    message() = default;
    explicit message(std::string raw)
        : raw_(std::make_shared<const std::string>(std::move(raw))) {}
    static message from_value(value v) { message m; m.structured_ = std::move(v); return m; }

    // Lazy conversion in both directions, as Benthos does.
    const std::string& as_bytes() const;
    const value&       as_structured() const;
    value&             as_structured_mut();

    void set_bytes(std::string raw);
    void set_structured(value v);

    // Store the result of a mapping. A string or byte root becomes the message
    // content VERBATIM rather than a JSON-quoted string -- executor.go does
    // SetBytes for a string and SetStructuredMut for everything else, so
    // `root = this.name.uppercase()` emits FOO, not "FOO". Returns false when
    // the result is `nothing`, which means "leave the message unchanged".
    bool set_mapped(value v);

    metadata&       meta()       noexcept { return meta_; }
    const metadata& meta() const noexcept { return meta_; }

    // Processing errors travel with the message rather than aborting the batch.
    bool               has_error() const noexcept { return !err_.empty(); }
    const std::string& error()     const noexcept { return err_; }
    void               set_error(std::string e)   { err_ = std::move(e); }
    void set_error(std::string e, std::shared_ptr<const error_source_info> src) {
        err_ = std::move(e);
        err_src_ = std::move(src);
    }
    // Empty fields rather than a null: error_source_label() is documented to
    // return an empty string when no label is set, so the absence of a source
    // and the absence of a label are the same answer.
    const error_source_info& error_source() const noexcept {
        static const error_source_info none;
        return err_src_ ? *err_src_ : none;
    }
    void               clear_error()   noexcept   { err_.clear(); err_src_.reset(); }

    // The synchronous-response store, if the input attached one. Shared rather
    // than owned: every copy of a message made by a split, a branch or a
    // fan-out must write into the SAME store, or a response assembled from a
    // recombined batch would be missing whatever the other copies produced.
    const std::shared_ptr<response_store>& response() const noexcept { return resp_; }
    void set_response(std::shared_ptr<response_store> s) noexcept { resp_ = std::move(s); }

    message shallow_copy() const { return *this; }   // payloads are refcounted

private:
    // Refcounted so shallow_copy really is shallow. A plain std::string here
    // meant every "shallow" copy duplicated the payload -- and the pipeline
    // makes one per batch as its error backup.
    mutable std::shared_ptr<const std::string> raw_;
    mutable std::optional<value> structured_;
    metadata                     meta_;
    std::string                  err_;
    std::shared_ptr<const error_source_info> err_src_;
    std::shared_ptr<response_store>          resp_;
};

using batch = std::vector<message>;


// Appends a payload followed by a line terminator, adding one only when the
// payload does not already end in a newline.
//
// The line-oriented outputs -- `stdout`, `file` and `socket` -- all used to
// append unconditionally, so a message whose content already ended in "\n"
// gained a second one and every `to_the_end` round trip through them differed
// from the reference by one byte. Verified against redpanda-connect 4.107.2 on
// all three.
inline void append_line(std::string& out, std::string_view payload) {
    out.append(payload);
    if (payload.empty() || payload.back() != '\n') out += '\n';
}

} // namespace sf
