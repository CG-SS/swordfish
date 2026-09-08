// Rate limits, as Benthos configures them under `rate_limit_resources`.
//
// Only `local` is implemented, and its algorithm is the reference's: a fixed
// window holding `count` permits that refills once `interval` has elapsed since
// the window opened. It is not a leaky bucket and does not smooth traffic --
// a burst of `count` may all pass at once and the next one waits for the window
// to turn over.
//
// `access()` returns how long to WAIT rather than waiting itself, which is the
// reference's shape and the useful one here: the caller owns the abort source,
// so a pipeline shutting down can abandon the wait instead of being held by it.
#pragma once

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>

#include <chrono>
#include <memory>
#include <string>

namespace sf {

class rate_limit {
public:
    virtual ~rate_limit() = default;
    // Zero means go now. Anything else is how long to wait before asking again.
    // NANOSECONDS, not milliseconds. The interval is a duration off the config
    // and Benthos keeps it as a nanosecond time.Duration; truncating it here
    // shortened the window, which UNDER-throttles -- the one direction a rate
    // limit must never err. Measured over a 1200-line file: `interval: 1900us`
    // ran indistinguishably from `interval: 1ms`, while the reference's run
    // lengthened proportionally. A sub-millisecond interval was worse than
    // imprecise: it truncated to zero and was then refused at construction, so
    // `interval: 500us` passed `sfconfig lint` and failed at run time.
    virtual std::chrono::nanoseconds access() = 0;
};

// Shared, not owned: several components naming one label must contend for the
// same permits, which is the entire point of a rate limit being a resource.
using rate_limit_ptr = std::shared_ptr<rate_limit>;

// The per-shard budget. Swordfish runs a pipeline per core while Benthos is one
// process, so a limit applied per shard unchanged would permit `count` times the
// shard count -- silently, and only under load, which is the failure mode this
// project treats as unacceptable. The budget is therefore DIVIDED across shards:
// the aggregate never exceeds what was configured, at the cost of a busy shard
// being throttled while an idle one has permits to spare.
//
// A `count` smaller than the shard count cannot be divided that way without
// either exceeding the limit or starving a shard of permits entirely, so that
// combination is refused by name rather than rounded.
rate_limit_ptr make_local_rate_limit(int64_t count, std::chrono::nanoseconds interval);

// One instance per label PER SHARD, so two components naming the same limit
// share its permits. Shard-local because the permits are: there is no
// cross-shard coordination, which is what the division above compensates for.
rate_limit_ptr shared_rate_limit(const std::string& label, int64_t count,
                                 std::chrono::nanoseconds interval);

// Asks repeatedly, sleeping for as long as `access()` says, until it grants one.
// Repeatedly and not once, because a fixed window can hand back a wait that ends
// exactly when a competing caller takes the permit it was waiting for.
//
// Returns false when the abort source fired first. The CALLER decides what an
// abandoned wait means: an input stops, while a processor forwards the batch
// anyway, and getting that backwards either drops a message at shutdown or
// hangs waiting for one. A null limit returns true immediately, so a component
// with no `rate_limit` configured pays nothing.
seastar::future<bool> rate_limit_wait(const rate_limit_ptr& limit,
                                      seastar::abort_source& as);

} // namespace sf
