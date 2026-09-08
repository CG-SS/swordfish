// `swordfish test` — the Benthos config unit-test format.
//
// Derived from benthos-main (Apache-2.0): internal/config/test/{case,input,
// output}.go define the schema, internal/cli/test/command.go the discovery
// rules. The goal is that an existing Redpanda Connect test suite runs here
// unmodified, so the field names, the defaults and the failure text are theirs,
// not ours.
#pragma once

#include "swordfish/config/yaml.hh"
#include "swordfish/message.hh"

#include <map>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace sf::testing {

// One message in an input batch. Exactly one of the content forms is used;
// `metadata` applies on top of whichever it is.
struct input_message {
    std::optional<std::string> content;
    std::optional<std::string> json_content;   // rendered compactly, as Benthos does
    std::optional<std::string> file_content;   // path, relative to the test file
    std::map<std::string, std::string> metadata;
    cfg::position where{};
};

// One assertion against one output message. A condition map may carry several,
// and all of them must hold.
struct output_conditions {
    std::optional<std::string> content_equals;
    std::optional<std::string> content_matches;   // regex
    std::optional<std::string> json_equals;
    std::optional<std::string> json_contains;
    std::optional<std::string> bloblang;          // must evaluate true
    // The message's bytes against a file's, path relative to the test file --
    // the mirror of `file_content` on the input side. The reference's own corpus
    // uses it (config/test/files_for_content.yaml), and it was silently dropped
    // here: that case's ONLY assertion was this one, so it asserted nothing and
    // the suite reported success.
    std::optional<std::string> file_equals;
    std::map<std::string, std::string> metadata_equals;
    cfg::position where{};
};

struct test_case {
    std::string name;
    std::map<std::string, std::string> environment;
    // A JSON Pointer into the config document. Benthos defaults this to
    // /pipeline/processors, and a test that omits it means "the whole pipeline".
    std::string target_processors = "/pipeline/processors";
    std::string target_mapping;                   // a .blobl file instead of processors
    std::vector<std::vector<input_message>>     input_batches;
    std::vector<std::vector<output_conditions>> output_batches;
    // Each key is either a JSON pointer (`/pipeline/processors/-`, where a
    // trailing `-` appends) or a component LABEL. The value is a whole
    // processor config that replaces the target.
    std::vector<std::pair<std::string, cfg::ynode>> mocks;
    cfg::position where{};
};

struct case_failure {
    std::string name;
    int         line = 0;
    std::string reason;
    std::string str() const;
};

struct file_result {
    std::string               path;
    bool                      ran = false;        // false when there was nothing to run
    // Why, when !ran -- and whether that reason is a FAILURE. "no tests" is a
    // legitimate skip; a file that would not parse is not, and counting the two
    // together let a test suite report success for a file it never read. The
    // reason itself was written and never printed, so the YAML error was
    // discarded as well.
    std::string               skip_reason;
    bool                      skip_is_failure = false;
    std::vector<case_failure> failures;
    size_t                    cases = 0;
};

// Parses the `tests:` sequence out of an already-loaded document.
std::vector<test_case> parse_cases(const cfg::ynode& root);

// Resolves a JSON Pointer against a document, or nullptr. Only the subset the
// test format needs: /a/b/0 style, with ~0 and ~1 escapes.
const cfg::ynode* json_pointer(const cfg::ynode& root, std::string_view ptr);

// Substitutes a case's mocks into a COPY of the config document. Pointer keys
// are applied before label keys, matching Benthos. Throws naming the key when a
// target cannot be resolved -- silently skipping a mock would run the real
// component, which for `http` means a live network call.
void apply_mocks(cfg::ynode& doc,
                 const std::vector<std::pair<std::string, cfg::ynode>>& mocks);

// Checks one message against one condition map, appending a reason per failure.
// `base_dir` is the directory of the test file, which `file_equals` resolves
// against exactly as `file_content` does.
void check_conditions(const output_conditions& c, const message& m,
                      const std::string& base_dir,
                      const std::string& case_name, size_t batch_i, size_t msg_i,
                      std::vector<case_failure>& out);

} // namespace sf::testing
