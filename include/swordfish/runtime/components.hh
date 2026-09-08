#pragma once
#include "swordfish/runtime/batching.hh"
#include "swordfish/runtime/cache.hh"
#include "swordfish/runtime/rate_limit.hh"
#include "swordfish/runtime/component.hh"
#include "swordfish/runtime/transform.hh"
#include "swordfish/runtime/scanner.hh"
#include <chrono>

namespace sf {
input_ptr     make_generate_input(transform_fn t, uint64_t count,
                                  std::chrono::milliseconds interval);
// `mutate` selects `mutation` semantics: root starts at the input document
// rather than at `nothing`, so fields the mapping does not mention survive.
// `mapping` and `bloblang` pass false and are genuinely identical to each
// other; only `mutation` differs. Confirmed against redpanda-connect 4.107.2.
// `label` and `path` are the component's own identity, reported by
// error_source_label() / error_source_path() on a message this processor fails.
// A processor that catches its own errors cannot get them from the stream layer,
// which only sees a processor that threw -- so they are passed in, and were
// empty for every message until they were.
processor_ptr make_mapping_processor(transform_fn t, bool mutate = false,
                                     std::string label = {}, std::string path = {});

// `branch`: a request mapping selects what the children see, and a result
// mapping merges what they produce back into the original message.
processor_ptr make_branch_processor(transform_fn request, std::vector<processor_ptr> children,
                                   transform_fn result);

// `broker` reads every child at once; `sequence` reads them in order. Both take
// already-built children, so the caller decides how each is constructed --
// which matters because the built-ins carry their own shard rules.
input_ptr make_broker_input(std::vector<input_ptr> children);
input_ptr make_sequence_input(std::vector<input_ptr> children);

// `auto_replay_nacks`, which every input the reference has that lacks its own
// redelivery carries and defaults to true. A nacked batch is held and offered
// again rather than dropped, so an output failure costs latency instead of
// data. Without it a `reject` output, a failing `fallback` tier or any transient
// write error silently loses messages -- the exact class of bug the ack
// machinery exists to prevent.
//
// Not applied to `kafka`: its consumer redelivers from the last committed
// offset, so wrapping it would hold a second copy for no gain.
input_ptr make_auto_retry_input(input_ptr inner);

// `processors:` beside an input's kind. Runs on what the input produced, before
// the batch reaches the pipeline layer; splits fan out through one ack_group so
// the source is acked once, when every batch it produced has been resolved.
input_ptr make_processed_input(input_ptr inner, std::vector<processor_ptr> procs);

// `workflow`: groups of named branches, run in the given order. `order` is
// required -- see the comment on workflow_processor for why inference is a
// named error rather than a guess.
processor_ptr make_workflow_processor(
    std::vector<std::vector<std::pair<std::string, processor_ptr>>> order,
    std::vector<std::string> meta_path);
processor_ptr make_codec_processor(std::string algorithm, int level, bool compressing);
processor_ptr make_archive_processor(std::string format);
processor_ptr make_select_parts_processor(std::vector<int64_t> parts);
processor_ptr make_insert_part_processor(int64_t index, transform_fn content);
processor_ptr make_bounds_check_processor(int64_t max_part, int64_t min_part,
                                          int64_t max_parts, int64_t min_parts);
processor_ptr make_split_processor(int64_t size, int64_t byte_size);

// `unarchive`: one message in, many out. json_array, json_map, json_documents
// and lines are implemented; tar, zip, binary and csv are rejected by name at
// config time rather than silently accepted.
processor_ptr make_unarchive_processor(std::string format);

// `dedupe`: drops a message whose key was seen before. Refuses to build at
// --smp > 1, where a per-shard cache would deduplicate per core rather than
// per pipeline.
processor_ptr make_dedupe_processor(cache_ptr c, transform_fn key, bool drop_on_err);
output_ptr    make_stdout_output();
output_ptr    make_drop_output();

input_ptr     make_file_input(std::string path, scanner_ptr sc, size_t batch_size = 1);

// `input.file`'s `paths`: globs expanded, read in order, one scanner each.
//
// ONE implementation, called by BOTH backends, and that is the whole point of
// it existing. `emit_main.cc` used to render the swordfish-only singular `path`
// instead, so a config written the reference's way -- with `paths:` and no
// `path:` -- ran correctly under `swordfish run` and compiled to a binary that
// opened the empty string. Interpreted and compiled disagreeing about what a
// config MEANS is the one thing the compiler thesis cannot tolerate, so the
// expansion lives here rather than once per backend.
input_ptr     make_files_input(const std::vector<std::string>& paths,
                               const scanner_spec& scanner, size_t batch_size = 1);

// The `memory` buffer, as an input DECORATOR.
//
// A buffer sits between the input and the pipeline, and what it really does is
// change when the source is acknowledged: "stores consumed messages in memory
// and acknowledges them at the input level". Wrapping the input is therefore
// the honest shape -- it is the input's acknowledgement behaviour that changes,
// and everything downstream is untouched. It also means both backends get it by
// wrapping one factory, exactly as `auto_replay_nacks` does.
//
// This INTENTIONALLY weakens delivery: a message sitting in the buffer has
// already been acked upstream, so a crash loses it. That is the reference's
// documented trade and the reason `none` is the default.
//
// `limit_bytes` is the estimated payload size at which the fill loop stops
// reading, applying backpressure upstream. A single batch larger than the whole
// limit is DROPPED, with an error logged once, and acked -- not refused by name,
// which is what this said and is not what happens. It cannot be nacked either:
// `auto_replay_nacks` would replay it straight back into the same impossible
// wait, which is the livelock the first version had. The reference drops it too,
// silently; swordfish says so.
// `batching` is the reference's `batch_policy.enabled`, passed explicitly rather
// than inferred: a policy with no trigger can never flush, and mistaking one for
// "batching is on" would hold every message until the source ended.
input_ptr     make_buffered_input(input_ptr inner, int64_t limit_bytes, bool batching,
                                  batch_policy policy,
                                  std::vector<processor_ptr> batch_procs);

// `input.broker`'s `batching` policy: several source batches combined into one.
//
// The mirror image of the output side, and the acks run the other way. On the
// output side one source batch fanned OUT to several writes and `ack_group`
// aggregated them; here several source batches fan IN to one assembled batch,
// and each source is acked only once every assembled batch it contributed to
// has been resolved -- so a source batch whose messages straddle a trigger is
// held until both halves land. `ack_group` does that too, one group per source
// batch, sealed when its last message has been drawn.
//
// A leftover deadlocks here exactly as it does on the output side, and for the
// same reason rather than a different one: the flush waits for the source to
// report end of input, and an auto-replaying source will not report it while an
// ack is outstanding -- which the held batch's is. `redpanda-connect` 4.107.2
// behaves identically on the identical config, so this is fidelity; the advice
// is the same on both sides, which is to pair `count` with a `period`.
input_ptr     make_batched_input(input_ptr inner, batch_policy policy,
                                 std::vector<processor_ptr> batch_procs);

input_ptr     make_stdin_input(scanner_ptr sc, size_t batch_size = 1);
// `path_fn` resolves an interpolated `path` per message; leave it empty for a
// path with no interpolation, which is then opened once at connect().
output_ptr    make_file_output(std::string path, transform_fn path_fn = {});

// `rate_limit`: holds each batch until the named limit permits it. Blocking is
// the whole behaviour, so the abort source is what makes shutdown possible.
processor_ptr make_rate_limit_processor(rate_limit_ptr limit);

processor_ptr make_noop_processor();
processor_ptr make_log_processor(std::string level, transform_fn t);
processor_ptr make_sleep_processor(std::chrono::milliseconds d);
processor_ptr make_try_processor(std::vector<processor_ptr> children,
                                 std::string label = {}, std::string path = {});
processor_ptr make_catch_processor(std::vector<processor_ptr> children);
processor_ptr make_for_each_processor(std::vector<processor_ptr> children);
// `group_by`: N batches out, one per group in group order, unmatched last.
// `switch`: ONE batch out, restored to the order it arrived in. The routing rule
// is identical and shared; only the assembly differs, and the reference makes
// exactly that distinction (processor_switch.go vs processor_group_by.go).
processor_ptr make_group_by_processor(
        std::vector<std::pair<transform_fn, std::vector<processor_ptr>>> cases);

processor_ptr make_switch_processor(
    std::vector<std::pair<transform_fn, std::vector<processor_ptr>>> cases);

// ---- composite outputs ------------------------------------------------------
//
// These hold other outputs, so like the input `broker` they take already-built
// children and leave the construction rules to the caller.

// `reject`: every write fails, with `message` rendered per batch. It exists so a
// pipeline can push a failure back upstream instead of routing it to a dead
// letter queue, which is why an empty message is refused: the text IS the
// component's whole output.
output_ptr make_reject_output(transform_fn message);

// `broker`: one of six patterns over the same children.
//   fan_out[_fail_fast]            -- every child gets every batch, in parallel
//   fan_out_sequential[_fail_fast] -- every child, one after another
//   round_robin                    -- one child per batch, in turn
//   greedy                         -- one child per batch, whichever is free
// The non-fail-fast fan_out patterns wrap each child in make_retry_output(); the
// wrapping is done by the CALLER, because a broker of one child collapses to
// that child and must still be retried.
output_ptr make_broker_output(std::vector<output_ptr> children, const std::string& pattern);

// `fallback`: try each child in turn until one succeeds. A child that failed
// adds `fallback_error` metadata to what the next one sees.
output_ptr make_fallback_output(std::vector<output_ptr> children);

// `switch`: route each message by a check. A message matching no case is
// dropped, unless `strict_mode`, where the batch is nacked instead.
struct switch_output_case {
    transform_fn check;             // empty = always passes
    output_ptr   out;
    bool         continue_ = false; // keep testing later cases after a match
};
output_ptr make_switch_output(std::vector<switch_output_case> cases, bool strict_mode);

// Retries a failed write for as long as the abort source permits, with an
// exponential backoff. `fan_out` and `fan_out_sequential` are defined in terms
// of it, and `switch`'s `retry_until_success` selects it per case.
output_ptr make_retry_output(output_ptr inner);

// An output's `batching:` policy. Messages accumulate until a trigger fires, the
// batching processors run over what accumulated, and the result is written as
// one batch. A policy with no trigger and no processors returns `inner`
// unwrapped: wrapping it would hold every message until shutdown.
// The triggers are passed as scalars rather than as the config struct: the
// struct's `check` is Bloblang SOURCE, and generated code carries a compiled
// function instead, so emitting the struct whole would render that field as a
// nullptr string literal. Both modes hand the same four things across.
output_ptr make_batched_output(output_ptr inner, int64_t count, int64_t byte_size,
                               std::chrono::milliseconds period, transform_fn check,
                               std::vector<processor_ptr> procs, bool strict_errors);

// `error_handling.strict` AT THE POINT OF WRITING: returns the exception to nack
// with when any message in `b` carries a processing error, and nullptr
// otherwise.
//
// One function because the check belongs at three places and they must not
// drift. The reference puts it in a single place -- the wrapper that owns the
// actual writer (benthos internal/component/output/async_writer.go) -- but its
// output processors run outside that wrapper, so one site there covers every
// path. Swordfish's layering is a chain of wrappers, so an error can enter
// ABOVE the write (an input or pipeline processor, caught in stream::do_write),
// INSIDE an output's own `processors:` (caught in processed_output), or inside
// a `batching.processors:` list (caught in batched_output). Checking in only the
// first of those was the state before 2026-09-07, and the other two wrote and
// acked messages the reference rejects.
//
// GRANULARITY. The reference rejects per MESSAGE: it splits a mixed batch,
// writes the clean part and nacks the rest. Swordfish acks per TRANSACTION, so
// one errored message nacks the batch it arrived in and its clean siblings are
// replayed with it. That is the same deliberate choice the main pipeline
// already documents, for the same reason -- under at-least-once a replayed
// sibling is recoverable and a dropped one is not -- and for any source that
// produces one message per batch the two are identical.
std::exception_ptr strict_rejection(const batch& b);

// An output's own `processors:`, applied to the messages routed to it and to no
// others. Wrapping rather than a field on every output means one implementation
// covers all of them, including the composites.
output_ptr make_processed_output(output_ptr inner, std::vector<processor_ptr> procs,
                                 bool strict_errors);
} // namespace sf
