#include "swordfish/runtime/rate_limit.hh"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/smp.hh>

#include <map>
#include <stdexcept>

namespace sf {

namespace {

// benthos-main/internal/impl/pure/rate_limit_local.go, unchanged in substance:
// decrement, and when the permits run out either report the time left in the
// window or open a new one.
class local_rate_limit final : public rate_limit {
public:
    local_rate_limit(int64_t size, std::chrono::nanoseconds period)
        : _size(size), _period(period), _bucket(size),
          _last_refresh(std::chrono::steady_clock::now()) {}

    std::chrono::nanoseconds access() override {
        --_bucket;
        if (_bucket >= 0) return std::chrono::nanoseconds{0};

        _bucket = 0;
        const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - _last_refresh);
        if (elapsed < _period) return _period - elapsed;

        // The window has turned over. This access takes the first permit of the
        // new one, which is why the refill is size-1 rather than size.
        _bucket = _size - 1;
        _last_refresh = std::chrono::steady_clock::now();
        return std::chrono::nanoseconds{0};
    }

private:
    int64_t                               _size;
    std::chrono::nanoseconds              _period;
    int64_t                               _bucket;
    std::chrono::steady_clock::time_point _last_refresh;
};

} // namespace

rate_limit_ptr make_local_rate_limit(int64_t count, std::chrono::nanoseconds interval) {
    if (count <= 0)
        throw std::runtime_error("rate_limit `count` must be greater than zero");
    if (interval.count() <= 0)
        throw std::runtime_error("rate_limit `interval` must be greater than zero");

    const auto shards = static_cast<int64_t>(seastar::this_smp().shard_count());
    if (shards > 1) {
        if (count < shards)
            throw std::runtime_error(
                "rate_limit `count` of " + std::to_string(count) + " cannot be divided "
                "across " + std::to_string(shards) + " shards: swordfish has no "
                "cross-shard rate limit, so each shard takes a share of the budget, and "
                "a count below the shard count would either exceed the limit or starve "
                "a shard entirely. Raise `count` to at least the shard count, or run "
                "with --smp 1");
        // Integer division drops the remainder, which under-throttles by less
        // than one permit per window. Rounding up would exceed the configured
        // rate, which is the one direction a rate limit must never go.
        count /= shards;
    }
    return std::make_shared<local_rate_limit>(count, interval);
}

rate_limit_ptr shared_rate_limit(const std::string& label, int64_t count,
                                 std::chrono::nanoseconds interval) {
    // thread_local IS shard-local under Seastar, and shard-local is what the
    // permits are: a map shared across shards would need a lock and would still
    // be wrong, because the division above already assumes each shard holds its
    // own budget.
    //
    // The settings are remembered alongside the limit so that asking for one
    // label with DIFFERENT settings is refused rather than silently answered
    // with the first caller's. One process normally runs one pipeline and cannot
    // hit that, but `swordfish test` builds several documents in a row, and a
    // second document reusing a label would otherwise be throttled by settings
    // it never wrote -- wrong, and invisible in the output.
    struct entry {
        rate_limit_ptr            limit;
        int64_t                   count = 0;
        std::chrono::nanoseconds interval{0};
    };
    thread_local std::map<std::string, entry> per_shard;
    auto it = per_shard.find(label);
    if (it != per_shard.end()) {
        if (it->second.count != count || it->second.interval != interval)
            throw std::runtime_error(
                "rate limit '" + label + "' was already created in this process with "
                "count=" + std::to_string(it->second.count) + " interval=" +
                std::to_string(it->second.interval.count()) + "ns, and is now asked for "
                "with count=" + std::to_string(count) + " interval=" +
                std::to_string(interval.count()) + "ns: one label is one limit, so the "
                "second request cannot be honoured without silently ignoring it");
        return it->second.limit;
    }
    auto rl = make_local_rate_limit(count, interval);
    per_shard.emplace(label, entry{rl, count, interval});
    return rl;
}

seastar::future<bool> rate_limit_wait(const rate_limit_ptr& limit,
                                      seastar::abort_source& as) {
    if (!limit) co_return true;
    while (!as.abort_requested()) {
        const auto wait = limit->access();
        if (wait.count() <= 0) co_return true;
        try { co_await seastar::sleep_abortable(wait, as); }
        catch (const seastar::sleep_aborted&) { co_return false; }
    }
    co_return false;
}

} // namespace sf
