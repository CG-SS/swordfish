// The built-in components' fields. See builtin_docs.hh for why they live here.
//
// Every entry below was taken from the hand-written parser in
// parse_pipeline.cc -- the field names from its `only_fields()` call, the
// defaults from the code that reads each field. tests/test_list_check.sh
// asserts the names still agree with what the parser accepts, so the pair
// cannot drift apart silently.

#include "swordfish/components/builtin_docs.hh"

#include <algorithm>
#include <string>

namespace sf {
namespace {

const std::vector<builtin_component>& table() {
    static const std::vector<builtin_component> t = {
        {builtin_class::input, "generate",
         "Produces messages from a Bloblang mapping, on an interval.",
         {
             {"mapping", "string", "\"root = {}\"", "A Bloblang mapping evaluated per message."},
             {"interval", "duration", "1s",
              "How long to wait between messages. `0s` produces them as fast as the "
              "pipeline accepts them."},
             {"count", "int", "0", "How many messages to produce; 0 is unbounded."},
             {"auto_replay_nacks", "bool", "true",
              "Redeliver a nacked message rather than dropping it."},
             {"batch_size", "int", "", "", false, true, false},
         }},
        {builtin_class::input, "file",
         "Reads one or more files, framed by a scanner.",
         {
             {"paths", "array of string", "", "The files to read. Globs are expanded.", true},
             {"scanner", "object", "{ lines: {} }",
              "How the bytes are split into messages."},
             {"codec", "string", "",
              "The deprecated form of `scanner`. Accepted for the forms that map onto "
              "one, refused by name for the rest.", false, false, true, true},
             {"delete_on_finish", "bool", "", "", false, true, false},
             {"auto_replay_nacks", "bool", "true",
              "Redeliver a nacked message rather than dropping it."},
         }},
        {builtin_class::input, "stdin",
         "Reads standard input, framed by a scanner.",
         {
             {"scanner", "object", "{ lines: {} }",
              "How the bytes are split into messages."},
             {"codec", "string", "", "The deprecated form of `scanner`.",
              false, false, true, true},
             {"auto_replay_nacks", "bool", "true",
              "Redeliver a nacked message rather than dropping it."},
         }},
        {builtin_class::input, "broker",
         "Reads from several inputs at once.",
         {
             {"inputs", "array of input", "", "The inputs to read from.", true},
             {"copies", "int", "1", "How many times to repeat each input."},
             {"batching", "object", "{}",
              "Combines several source batches into one before the pipeline sees them."},
         }},
        {builtin_class::input, "sequence",
         "Reads each input to exhaustion, in order.",
         {
             {"inputs", "array of input", "", "The inputs to read, in order.", true},
             // Accepted when it asks for nothing -- an empty block, or
             // `type: none` -- and refused by name for any other `type`. The
             // same shape the `http` block's cert_file/cors/basic_auth take, so
             // it is neither "supported" nor flatly "unimplemented": marking it
             // the latter said lint would refuse `sharded_join: {}`, and lint
             // accepts it.
             {"sharded_join", "object", "{}",
              "Only `type: none` is implemented; any other type is refused by name.",
              false, false, true},
         }},
        {builtin_class::output, "stdout",
         "Writes each message to standard output.",
         {
             {"codec", "string", "\"lines\"",
              "How messages are framed on the way out. `lines` is what swordfish "
              "writes; every other value is refused by name."},
         }},
        {builtin_class::output, "drop",
         "Discards every message, acknowledging it. Useful for benchmarks and for "
         "a pipeline whose work is its side effects.",
         {}},
        {builtin_class::output, "file",
         "Writes messages to a file.",
         {
             {"path", "interpolated string", "", "The file to write to.", true},
             {"codec", "string", "\"lines\"", "How messages are framed on the way out."},
         }},
        {builtin_class::output, "broker",
         "Writes to several outputs.",
         {
             {"outputs", "array of output", "", "The outputs to write to.", true},
             {"pattern", "string", "\"fan_out\"",
              "How a message is distributed: fan_out, fan_out_fail_fast, "
              "round_robin or greedy."},
             {"copies", "int", "1", "How many times to repeat each output."},
             {"batching", "object", "{}", "Batches messages before writing them."},
         }},
        {builtin_class::output, "switch",
         "Routes each message to the first case whose check passes.",
         {
             {"cases", "array of object", "", "The candidate outputs and their checks.", true},
             {"strict_mode", "bool", "false",
              "Fail a message that matches no case, rather than dropping it."},
             {"retry_until_success", "bool", "false",
              "Retry a failing case indefinitely rather than nacking."},
         }},
    };
    return t;
}

} // namespace

const std::vector<builtin_component>& builtin_components() { return table(); }

const builtin_component* find_builtin(builtin_class cls, std::string_view kind) {
    const auto& t = table();
    const auto it = std::find_if(t.begin(), t.end(), [&](const builtin_component& c) {
        return c.cls == cls && c.kind == kind;
    });
    return it == t.end() ? nullptr : &*it;
}

std::vector<std::string_view> builtin_field_names(builtin_class cls, std::string_view kind) {
    std::vector<std::string_view> out;
    if (const builtin_component* c = find_builtin(cls, kind)) {
        out.reserve(c->fields.size());
        // Only what the parser's own only_fields() list held. A field marked
        // `unimplemented` has usually been refused already by
        // unimplemented_fields(), so including it here would ACCEPT a key the
        // parser means to reject -- the one direction in which a documentation
        // table must not change behaviour.
        for (const auto& f : c->fields) if (f.in_allowed) out.push_back(f.name);
    }
    return out;
}

std::string describe_builtin(const builtin_component& c) {
    // The shape describe<T>() produces, so `swordfish list` reads as one
    // document: the kind on its own line, then each field indented two, then its
    // description indented six.
    std::string out(c.kind);
    out += "\n";
    for (const auto& f : c.fields) {
        out += "  ";
        out += f.name;
        out += "  <";
        out += f.type;
        out += ">  ";
        if (f.unimplemented)         out += "(not implemented by swordfish)";
        else if (f.required)         out += "(required)";
        else if (!f.default_text.empty()) { out += "default: "; out += f.default_text; }
        out += "\n";
        if (!f.description.empty()) {
            out += "      ";
            out += f.description;
            out += "\n";
        } else if (f.unimplemented) {
            out += "      Redpanda Connect has this field; swordfish refuses it by name "
                   "rather than accepting it and doing nothing.\n";
        }
    }
    return out;
}

std::string scaffold_builtin(const builtin_component& c, int indent) {
    // The same shape cfg::scaffold<T>() produces for a registered component, so
    // `swordfish create` emits one document rather than two dialects. Built from
    // the same table `list` renders and the parser takes its field names from.
    const std::string pad(static_cast<size_t>(indent), ' ');
    std::string out;
    for (const auto& f : c.fields) {
        // A field swordfish refuses is not something to start someone off with,
        // and a superseded one would collide with the field that replaced it.
        if (f.unimplemented || f.deprecated) continue;
        out += pad;
        out += f.name;
        out += ":";
        if (!f.default_text.empty() && f.default_text != "{}" && f.default_text != "[]") {
            out += " ";
            out += f.default_text;
        } else if (f.type.rfind("array", 0) == 0) {
            out += " []";
        } else if (f.type == "object" || f.default_text == "{}") {
            out += " {}";
        } else {
            // Required and scalar: an empty value of the right shape, which is
            // what the reference does too -- its create writes `mapping: ""` and
            // marks it required rather than leaving the key out.
            out += " \"\"";
        }
        if (f.required) out += "   # (required)";
        if (!f.description.empty())
            out += (f.required ? " " : "   # ") + std::string(f.description);
        out += "\n";
    }
    return out;
}

} // namespace sf
