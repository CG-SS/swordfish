#include "swordfish/runtime/cache.hh"
#include "swordfish/methods.hh"

#include <seastar/core/coroutine.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/seastar.hh>

#include <filesystem>
#include <list>
#include <map>
#include <stdexcept>
#include <unordered_map>

namespace sf {

namespace {

// `memory`: a map with a TTL per entry, swept periodically, SHARDED.
//
// The sharding is not a performance detail that could be left out. Each shard
// carries its own last-compaction time and a write sweeps only the shard it
// touched, so `shards` decides when an entry expires and therefore which
// messages a `dedupe` passes. Measured on the reference with one config and
// nothing else changed: `shards: 1` emitted `A B A`, `shards: 2` emitted `A B`.
// Which shard a key lands in has to match too, so the selector is the
// reference's: xxhash64 of the key, modulo the shard count.
class memory_cache final : public cache {
public:
    memory_cache(std::chrono::milliseconds ttl, std::chrono::milliseconds compaction,
                 const std::vector<std::string>& init, int64_t shards)
        : _ttl(ttl), _compaction(compaction),
          _shards(static_cast<size_t>(std::max<int64_t>(1, shards))) {
        const auto now = seastar::lowres_clock::now();
        for (auto& sh : _shards) sh.last_sweep = now;
        // `init_values` are documented as EXEMPT from the TTL: "these values can
        // be overridden during execution, at which point the configured TTL is
        // respected as usual". The reference stores them with a ZERO expiry and
        // isExpired() returns false for that; time_point::max() says the same
        // thing without a second map. They are placed through the same shard
        // selector, as newMemCache does.
        for (const auto& k : init)
            shard_for(k).entries.emplace(k, seastar::lowres_clock::time_point::max());
    }

    seastar::future<bool> add(const std::string& key) override {
        const auto now = seastar::lowres_clock::now();
        auto& sh = shard_for(key);

        // PRESENCE, not liveness. The reference's Add returns
        // ErrKeyAlreadyExists for any key in the map and does not compact before
        // looking (cache_memory.go: the presence check precedes
        // `shard.compaction()`), so an entry past its TTL still counts as a
        // duplicate until a sweep actually removes it. Expiring lazily here
        // instead made a `dedupe` stop deduplicating as soon as `default_ttl`
        // elapsed: three identical messages two seconds apart under a 1s TTL
        // came out three times, against once from the reference.
        if (sh.entries.find(key) != sh.entries.end()) co_return false;

        // Compaction on the WRITE path only, again as the reference does it --
        // "this process is only triggered on writes to the cache". A workload
        // that only repeats keys it has already seen therefore never sweeps,
        // which is the reference's own asymmetry rather than an oversight here;
        // taking the sweep and the lazy expiry both is what diverged.
        //
        // Gated on the compaction interval ALONE. It used to require a non-zero
        // TTL as well, which is not what shard.compaction() does -- it tests
        // only `compInterval` -- and that mattered because of the line below.
        if (_compaction.count() > 0 && now - sh.last_sweep >= _compaction) {
            sh.last_sweep = now;
            for (auto it = sh.entries.begin(); it != sh.entries.end();)
                it = (it->second <= now) ? sh.entries.erase(it) : std::next(it);
        }

        // `now + _ttl`, ALWAYS. A zero TTL used to store time_point::max(),
        // reading `default_ttl: 0` as "never expires" -- but the reference
        // computes `expires = time.Now().Add(m.defaultTTL)` unconditionally, so
        // zero yields an expiry that is already in the past and the entry is
        // swept by the next compaction. Measured: the reference emitted `A B A`
        // where swordfish emitted `A B`. A never-expiring entry is a zero
        // `expires` there, which is reserved for init_values above.
        sh.entries.emplace(key, now + _ttl);
        co_return true;
    }

private:
    struct shard {
        std::map<std::string, seastar::lowres_clock::time_point> entries;
        seastar::lowres_clock::time_point                        last_sweep{};
    };

    shard& shard_for(const std::string& key) {
        if (_shards.size() == 1) return _shards[0];
        return _shards[m::xxhash64(key) % _shards.size()];
    }

    std::chrono::milliseconds _ttl;
    std::chrono::milliseconds _compaction;
    std::vector<shard>        _shards;
};

// `lru`: a fixed-capacity cache that evicts the least recently used entry.
//
// The list holds keys in recency order, most recent at the front, and the map
// points at each key's position -- so a hit is a splice and an eviction is a
// pop, both constant time. The reference uses hashicorp/golang-lru; the
// `standard` algorithm is this one.
class lru_cache final : public cache {
public:
    lru_cache(size_t cap, const std::vector<std::string>& init) : _cap(cap) {
        for (const auto& k : init) insert(k);
    }

    seastar::future<bool> add(const std::string& key) override {
        // A hit does NOT renew recency, because the reference's dedupe path
        // PEEKS: `lruCacheAdapter.unsafeAdd` calls `inner.Peek(key)` and returns
        // ErrKeyAlreadyExists on a hit, and `inner.Add` -- the recency-updating
        // call -- is reached only on a miss (cache_lru.go:264-271). Splicing the
        // hit to the front here kept a repeated key alive that the reference
        // lets fall out of a full cache, so swordfish DROPPED a message the
        // reference delivers: `A B C A` at cap 3 came out `A B C`.
        //
        // Renewing would be right for a general get/hit, but `add` is the only
        // operation this interface has and dedupe is its only caller.
        if (_index.find(key) != _index.end()) co_return false;
        insert(key);
        co_return true;
    }

private:
    void insert(const std::string& key) {
        // IDEMPOTENT. `_index.emplace` declines a key that is already there, but
        // the `push_front` did not, so a repeated key -- which duplicate
        // `init_values` entries produce -- left TWO nodes in `_order` against one
        // index entry pointing at the older. Eviction then erased the index for a
        // key whose other node was still live, and the next occurrence of it was
        // reported as new: a `dedupe` passed a byte-identical duplicate through,
        // permanently, for the life of the process.
        if (const auto it = _index.find(key); it != _index.end()) {
            _order.splice(_order.begin(), _order, it->second);
            return;
        }
        _order.push_front(key);
        _index.emplace(key, _order.begin());
        while (_index.size() > _cap) {
            _index.erase(_order.back());
            _order.pop_back();
        }
    }

    size_t _cap;
    std::list<std::string>                                             _order;
    std::unordered_map<std::string, std::list<std::string>::iterator>  _index;
};

// `file`: one file per key, inside a directory.
//
// "This type currently offers no form of item expiry or garbage collection, and
// is intended to be used for development and debugging purposes only" -- the
// reference's own words, and the reason there is no TTL here.
//
// The insert is the file CREATION, with O_EXCL: the filesystem decides who won,
// so there is no check-then-create window even between processes.
class file_cache final : public cache {
public:
    explicit file_cache(std::string dir) : _dir(std::move(dir)) {}

    seastar::future<bool> add(const std::string& key) override {
        // The reference joins the key onto the directory, so a key containing a
        // separator names a path. Benthos does not create intermediate
        // directories, so such a key fails there too -- just with ENOENT, or by
        // writing outside the directory for a key beginning "..". Refused by
        // name instead, which is the same outcome said clearly.
        // A '/' is ALLOWED: the reference joins the key onto the directory
        // (`filepath.Join(f.dir, key)`, cache_file.go:72), so a key naming a
        // subdirectory works whenever that subdirectory exists, and fails with
        // ENOENT when it does not. Refusing it outright meant swordfish dropped
        // every message under the default `drop_on_err: true` where the
        // reference wrote the files and passed them on.
        //
        // What is still refused is an ESCAPE. The reference will happily write
        // outside its own directory for a key beginning "..", and a dedupe key
        // is message data: that is a path traversal driven by whatever the
        // source sends. Refused by name here, which is a deliberate and stated
        // divergence rather than an oversight.
        for (const auto& part : {std::string(".."), std::string(".")})
            if (key == part || key.rfind(part + "/", 0) == 0 ||
                key.find("/" + part + "/") != std::string::npos ||
                (key.size() > part.size() &&
                 key.compare(key.size() - part.size() - 1, part.size() + 1, "/" + part) == 0))
                throw std::runtime_error(
                    "file cache: the key '" + key + "' walks out of the cache "
                    "directory; keys become paths under it and a key is message "
                    "data, so this is refused rather than followed");
        // A NUL is refused for the same reason and by the same rule. The path
        // reaches open() as a C string, so `a\0X` and `a\0Y` both named the file
        // `a`: the second got EEXIST and was reported as a DUPLICATE, and a
        // `dedupe` over binary content silently dropped a distinct message. A
        // filename cannot contain a NUL on any POSIX system, so this is a key
        // the cache genuinely cannot store rather than one it stores badly.
        if (key.find('\0') != std::string::npos)
            throw std::runtime_error(
                "file cache: the key contains a NUL byte; keys become file names "
                "directly, and a file name cannot contain one");
        const std::string path = _dir + "/" + key;
        try {
            auto f = co_await seastar::open_file_dma(
                path, seastar::open_flags::wo | seastar::open_flags::create |
                      seastar::open_flags::exclusive);
            co_await f.close();
            co_return true;
        } catch (const std::system_error& e) {
            if (e.code() == std::errc::file_exists) co_return false;
            throw;
        }
    }

private:
    std::string _dir;
};

} // namespace

// One instance per label per shard. The registry is `thread_local`, which under
// seastar means per-shard: caches are shard-local by design, so two components
// on the same shard share and two shards do not.
cache_ptr make_cache(const cache_spec& spec) {
    if (!spec.label.empty()) {
        static thread_local std::map<std::string, cache_ptr> shared;
        auto it = shared.find(spec.label);
        if (it != shared.end()) return it->second;
        cache_spec unlabelled = spec;
        unlabelled.label.clear();               // build it, then remember it
        cache_ptr made = make_cache(unlabelled);
        shared.emplace(spec.label, made);
        return made;
    }
    if (spec.kind == "memory")
        return std::make_shared<memory_cache>(
            spec.default_ttl,
            spec.compaction_disabled ? std::chrono::milliseconds{0} : spec.compaction_interval,
            spec.init_keys, spec.shards);
    if (spec.kind == "lru") {
        if (spec.cap <= 0)
            throw std::runtime_error("lru cache: `cap` must be greater than zero");
        return std::make_shared<lru_cache>(static_cast<size_t>(spec.cap), spec.init_keys);
    }
    if (spec.kind == "file") {
        if (spec.directory.empty())
            throw std::runtime_error("file cache: `directory` is required");
        // Created up front rather than on the first miss, so a directory that
        // cannot be made is reported when the pipeline starts.
        std::error_code ec;
        std::filesystem::create_directories(spec.directory, ec);
        if (ec)
            throw std::runtime_error("file cache: cannot create directory '" +
                                     spec.directory + "': " + ec.message());
        return std::make_shared<file_cache>(spec.directory);
    }
    throw std::runtime_error("cache '" + spec.kind + "' is not implemented by swordfish");
}

} // namespace sf
