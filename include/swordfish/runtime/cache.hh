// Caches, as Benthos configures them under `cache_resources`.
//
// `memory`, `lru` and `file` are implemented. All three are per-SHARD, which is
// the important caveat: Benthos is one process with one cache, while Swordfish
// runs a pipeline per core. A `dedupe` over four shards backed by a per-shard
// cache would deduplicate within each shard and let duplicates through between
// them -- silently, and only under load. Components that need a cache therefore
// refuse to build at `--smp > 1` rather than approximate it (see
// make_dedupe_processor).
#pragma once

#include <seastar/core/future.hh>

#include <chrono>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace sf {

class cache {
public:
    virtual ~cache() = default;
    // Inserts only if absent. Resolves to false when the key was already there,
    // which is what `dedupe` tests -- distinguishing "added" from "present" in
    // one operation is why this is not get-then-set.
    //
    // A future, because the `file` cache is backed by the filesystem and doing
    // that with blocking syscalls would stall the reactor. The in-memory kinds
    // resolve immediately.
    virtual seastar::future<bool> add(const std::string& key) = 0;
};

// SHARED, not owned. A `cache_resources` entry names ONE cache, and two
// components naming that label must contend for the same entries -- which is
// the whole point of it being a resource. make_cache() handed each caller its
// own instance, so two `dedupe` processors over one label deduplicated
// separately and the second passed everything the first had already seen. The
// pointer is shared per label, per SHARD (caches are shard-local by design).
using cache_ptr = std::shared_ptr<cache>;

// How a cache was configured. One struct for all three kinds, for the reason
// scanner_spec is one struct: a cache is chosen by a DYNAMIC key -- 
// `cache_resources: [{label: x, lru: {...}}]` -- which the spec_of field model
// cannot express. Fields that do not apply to `kind` are unused.
struct cache_spec {
    std::string kind = "memory";
    // The `cache_resources` label. Carried so make_cache can hand every
    // component naming it the SAME instance; empty means an unlabelled cache,
    // which is not shared with anything.
    std::string label;

    // memory. The reference's default is FIVE MINUTES, not "never": a config
    // that omits `default_ttl` gets expiry there, and swordfish used to give it
    // an unbounded cache instead.
    std::chrono::milliseconds default_ttl{300000};
    // The period between sweeps of expired entries. The reference disables
    // expiry entirely when this is set to an empty string, which is what
    // `compaction_disabled` records -- distinct from a zero interval.
    std::chrono::milliseconds compaction_interval{60000};
    bool                      compaction_disabled = false;
    // memory: how many independent shards the map is split into. This was
    // accepted and ignored, on the reasoning that it only relieves lock
    // contention swordfish does not have -- and that is wrong. Each shard
    // carries its OWN last-compaction time and `Add` sweeps only the shard it
    // touched, so the field decides WHEN an entry expires and therefore which
    // messages a `dedupe` passes. Measured on the reference with one config and
    // nothing else changed: `shards: 1` emitted `A B A`, `shards: 2` emitted
    // `A B`.
    int64_t                   shards = 1;

    // lru: the maximum number of entries before the least recently used is
    // evicted.
    int64_t     cap = 1000;

    // file: the directory each item is stored in, one file per key.
    std::string directory;

    // `init_values`, on `memory` and `lru`: keys present before the first
    // message. Only the KEYS are kept -- the cache interface above stores no
    // values, because `dedupe` is the only consumer and asks only whether a key
    // is present. A `cache` processor would need the values, and would need
    // this interface widened first.
    std::vector<std::string> init_keys;

    bool operator==(const cache_spec&) const = default;
};

// The one factory. Throws a named error for a kind or option it does not
// implement, so a config reaching construction unresolved fails by name.
cache_ptr make_cache(const cache_spec& spec);

} // namespace sf
