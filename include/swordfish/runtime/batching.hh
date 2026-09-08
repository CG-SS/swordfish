// The batching policy: accumulate messages until one of four triggers fires.
//
// Semantics are the reference's, from benthos-main/internal/batch/policy
// (Apache-2.0), and the details matter more than they look:
//
//   - `count` and `byte_size` are checked AFTER the message is appended, so the
//     message that crosses the threshold is part of the batch it completed, not
//     the start of the next one.
//   - `byte_size` serialises each message to measure it, which is expensive, so
//     it is only computed when a byte trigger is actually configured.
//   - `check` is a Bloblang query evaluated against the message just added, with
//     the accumulated batch as its context, and only when nothing else has
//     already triggered. A check that throws does NOT trigger, and says so in
//     the log rather than silently ending a batch early.
//   - `period` is the only trigger that can fire with no message arriving, so
//     it needs a timer rather than a test on add().
//
// A policy with none of the four set is a no-op and callers should not wrap
// anything in it.
#pragma once

#include "swordfish/config/spec.hh"
#include "swordfish/message.hh"
#include "swordfish/runtime/transform.hh"

#include <chrono>
#include <string>

namespace sf {

struct batch_policy_config {
    // `processors` is lifted out during parsing, as a nested processor list has
    // no field type -- the same treatment `branch` and the composite outputs
    // get.
    int64_t       count = 0;
    int64_t       byte_size = 0;
    cfg::duration period{std::chrono::nanoseconds{0}};
    cfg::bloblang check{"", true};
    bool operator==(const batch_policy_config&) const = default;

    // True when no trigger is configured. Wrapping an output in a policy that
    // can never fire would hold every message until shutdown.
    bool is_noop() const {
        return count <= 0 && byte_size <= 0 && period.ns.count() <= 0 && check.source.empty();
    }
};

class batch_policy {
public:
    batch_policy(int64_t count, int64_t byte_size, std::chrono::milliseconds period,
                 transform_fn check);

    // Appends one message. Returns true when the policy says to flush now.
    bool add(message m);

    bool   empty() const noexcept { return _parts.empty(); }
    size_t size()  const noexcept { return _parts.size(); }

    // How long until the period trigger is due. Zero when there is no period,
    // or when it is already overdue.
    std::chrono::milliseconds until_next() const;

    // Removes and returns everything accumulated, and restarts the period. The
    // CALLER runs the batching processors over the result: they can filter the
    // batch away entirely or fail, and those two outcomes mean different things
    // to the messages waiting on it.
    batch take();

private:
    int64_t      _count;
    int64_t      _byte_size;
    std::chrono::milliseconds _period;
    transform_fn _check;

    batch                                  _parts;
    int64_t                                _size_tally = 0;
    bool                                   _triggered = false;
    std::chrono::steady_clock::time_point  _last_batch;
};

} // namespace sf

namespace sf::cfg {

template <> struct spec_of<sf::batch_policy_config> {
    static constexpr std::string_view cpp_type = "sf::batch_policy_config";
    static constexpr auto value = object(
        field("count", &sf::batch_policy_config::count)
            .describe("Flush at this many messages. 0 disables the count trigger."),
        field("byte_size", &sf::batch_policy_config::byte_size)
            .describe("Flush at this many bytes. 0 disables the size trigger."),
        field("period", &sf::batch_policy_config::period)
            .describe("Flush an incomplete batch after this long, whatever its size."),
        field("check", &sf::batch_policy_config::check)
            .describe("A Bloblang query returning whether the message just added "
                      "should end the batch.")
    );
};

} // namespace sf::cfg
