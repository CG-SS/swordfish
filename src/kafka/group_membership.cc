#include "swordfish/kafka/group_membership.hh"
#include "swordfish/kafka/consumer.hh"

#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/timer.hh>
#include <seastar/util/log.hh>

#include <algorithm>
#include <memory>
#include <set>
#include <vector>

namespace sf::kafka {

static seastar::logger mlog("sf.kafka.membership");

partition_map shard_share(const partition_map& all, unsigned shard, unsigned shards) {
    partition_map mine;
    if (shards == 0) return mine;
    for (const auto& [topic, parts] : all)
        for (int32_t p : parts)
            if (static_cast<unsigned>(p) % shards == shard) mine[topic].push_back(p);
    return mine;
}

namespace {

// ---- per-shard cache --------------------------------------------------------
// Written only by a push from shard 0, read by that shard's poll loop.
//
// thread_local, NOT a plain function-local static. Seastar shards are threads
// of one process, so a `static` here is a SINGLE object every shard shares:
// the four pushes from publish() all wrote the same map, the last one won, and
// every shard then fetched the same partitions. The symptom was six records
// delivered four times each instead of sixty delivered once -- silent
// duplication, with a green test suite, because the single-shard tests never
// exercised it.
thread_local std::map<std::string, shard_assignment> tl_cache;

std::map<std::string, shard_assignment>& local_cache() { return tl_cache; }

void apply_locally(const std::string& group_id, shard_assignment a) {
    local_cache()[group_id] = std::move(a);
}

// ---- the owner, shard 0 only -------------------------------------------------

struct owner {
    consumer_config                 cfg;
    std::unique_ptr<cluster>        cl;
    std::unique_ptr<consumer_group> group;
    partition_map                   full;
    offset_table                    committed;   // as of the last join
    int32_t                         generation = -1;
    // The shards currently attached. The refcount is derived from this rather
    // than incremented per call because consumer::rejoin() attaches AGAIN on
    // every rebalance: counting calls would push refs permanently above the
    // shard count, no detach would ever reach zero, and the owner would survive
    // into the static map's destruction -- after the reactor has stopped, where
    // its sockets can no longer be closed and the process dies on the way out.
    std::set<unsigned>              attached;
    unsigned                        refs = 0;
    offset_table                    staged;      // offered by shards, not yet committed
    seastar::timer<>                heartbeat;
    seastar::timer<>                commit;
    // One join serves every shard that asks while it is running, and the slot is
    // filled from a promise BEFORE the work starts -- a coroutine that fails
    // before its first suspension would otherwise complete the chain, reset a
    // slot that was not there yet, and latch the next join out forever.
    std::optional<seastar::shared_future<>> joining;
    seastar::gate                   gate;
    bool                            stopping = false;
};

// Deliberately a plain static rather than thread_local: there is one owner per
// process and it is touched only from shard 0. Every function below that
// reaches it is either called through smp::submit_to(0, ...) or is a helper of
// one.
std::map<std::string, std::unique_ptr<owner>>& owners() {
    static std::map<std::string, std::unique_ptr<owner>> m;
    return m;
}

owner* find_owner(const std::string& group_id) {
    auto it = owners().find(group_id);
    return it == owners().end() ? nullptr : it->second.get();
}

// Push each shard its share: one round per rebalance, nothing on the steady
// state path.
//
// Each shard's slice is computed HERE and sent already divided, rather than
// broadcasting the whole assignment for every shard to filter. That is not an
// optimisation -- smp::invoke_on_all requires a nothrow-move-constructible
// functor, and a lambda capturing std::map is not one, because libstdc++ does
// not declare map's move constructor noexcept. Sending small per-shard payloads
// through submit_to sidesteps the constraint and copies less.
seastar::future<> publish(owner& o) {
    const std::string group_id = o.cfg.consumer_group;
    const int32_t generation = o.generation;
    const unsigned shards = seastar::this_smp().shard_count();

    std::vector<unsigned> ids(shards);
    for (unsigned i = 0; i < shards; ++i) ids[i] = i;

    return seastar::parallel_for_each(std::move(ids),
        [&o, group_id, generation, shards](unsigned id) {
            shard_assignment a;
            a.generation = generation;
            a.partitions = shard_share(o.full, id, shards);
            // Only the offsets for partitions this shard actually owns; the
            // rest would be state a shard could act on by mistake.
            for (const auto& [topic, parts] : a.partitions)
                for (int32_t p : parts) {
                    const auto t = o.committed.find(topic);
                    if (t == o.committed.end()) continue;
                    const auto c = t->second.find(p);
                    if (c != t->second.end()) a.committed[topic][p] = c->second;
                }
            return seastar::smp::submit_to(id,
                [group_id, a = std::move(a)]() mutable {
                    apply_locally(group_id, std::move(a));
                });
        });
}

// Defined below; do_join calls it to revoke before the join.
seastar::future<> commit_staged(owner& o);

seastar::future<> do_join(owner& o) {
    // REVOKE FIRST. Anything staged was acknowledged for partitions held under
    // the CURRENT generation, so it is committed here, before the join gives
    // those partitions away. The header has always advertised this path ("used
    // at shutdown and before giving partitions up in a rebalance") and nothing
    // performed it: the staged table survived the join untouched and was then
    // committed under the NEW generation, for partitions another member had
    // taken over. Kafka accepts that -- the classic OffsetCommit validates the
    // group, generation and member id, not partition ownership, which was
    // verified against a live broker -- so the other member's progress was
    // silently overwritten with this one's.
    if (o.generation >= 0) co_await commit_staged(o);

    o.full = co_await o.group->join();
    o.generation = o.group->generation();

    // And drop whatever the revoke could not commit. commit_staged puts offsets
    // back when the commit fails, which is right for a retry within a
    // generation and wrong across one: those partitions may not be ours any
    // more, and re-offering them after the join is the same overwrite by
    // another route. Losing them costs at most a replay from the last successful
    // commit, which is what at-least-once already promises.
    if (!o.staged.empty()) {
        offset_table kept;
        size_t dropped = 0;
        for (auto& [topic, parts] : o.staged) {
            const auto t = o.full.find(topic);
            for (const auto& [p, off] : parts) {
                if (t != o.full.end() &&
                    std::find(t->second.begin(), t->second.end(), p) != t->second.end())
                    kept[topic][p] = off;
                else
                    ++dropped;
            }
        }
        if (dropped)
            mlog.warn("group {}: dropping {} staged offset(s) for partition(s) this "
                      "member no longer owns", o.cfg.consumer_group, dropped);
        o.staged.swap(kept);
    }

    o.committed = co_await o.group->fetch_offsets(o.full);
    mlog.info("group {}: generation {}, {} partition(s) over {} shard(s)",
              o.cfg.consumer_group, o.generation,
              [&] { size_t n = 0; for (const auto& [t, ps] : o.full) n += ps.size(); return n; }(),
              seastar::this_smp().shard_count());
    co_await publish(o);
}

seastar::future<> ensure_joined(owner& o) {
    if (o.generation >= 0 && !o.joining) co_return;
    if (o.joining) co_return co_await o.joining->get_future();

    seastar::promise<> p;
    o.joining.emplace(p.get_future());
    auto result = o.joining->get_future();
    (void)do_join(o).then_wrapped([&o, p = std::move(p)](seastar::future<> f) mutable {
        o.joining.reset();                 // cleared before waiters resume
        if (f.failed()) p.set_exception(f.get_exception());
        else            p.set_value();
    });
    co_await std::move(result);
}

seastar::future<> commit_staged(owner& o) {
    if (o.staged.empty() || o.generation < 0) co_return;
    offset_table to_commit;
    to_commit.swap(o.staged);
    try {
        co_await o.group->commit(to_commit);
    } catch (const std::exception& e) {
        // Put them back: a failed commit must not lose the progress, or the
        // next commit would skip these partitions and a crash would replay
        // further than necessary.
        for (const auto& [topic, parts] : to_commit)
            for (const auto& [p, off] : parts) {
                auto& slot = o.staged[topic][p];
                slot = std::max(slot, off);
            }
        mlog.warn("group {}: commit failed ({}), will retry", o.cfg.consumer_group, e.what());
    }
}

void arm_timers(owner& o) {
    o.heartbeat.set_callback([&o] {
        if (o.stopping) return;
        (void)seastar::with_gate(o.gate, [&o] {
            return o.group->heartbeat().then([&o](bool alive) {
                // A background heartbeat, deliberately: liveness must not depend
                // on the pipeline draining. A stalled output should be caught by
                // a poll-interval check, not by the group silently evicting us.
                if (alive) return seastar::make_ready_future<>();
                mlog.info("group {}: rebalance, rejoining", o.cfg.consumer_group);
                o.generation = -1;
                return ensure_joined(o);
            });
        }).handle_exception([&o](std::exception_ptr e) {
            mlog.warn("group {}: heartbeat failed: {}", o.cfg.consumer_group, e);
        });
    });
    o.heartbeat.arm_periodic(o.cfg.session_timeout / 3);

    o.commit.set_callback([&o] {
        if (o.stopping) return;
        (void)seastar::with_gate(o.gate, [&o] { return commit_staged(o); })
            .handle_exception([&o](std::exception_ptr e) {
                mlog.warn("group {}: commit failed: {}", o.cfg.consumer_group, e);
            });
    });
    o.commit.arm_periodic(o.cfg.commit_interval);
}

seastar::future<> attach_on_zero(consumer_config cfg, unsigned from_shard) {
    const std::string id = cfg.consumer_group;
    owner* o = find_owner(id);
    if (!o) {
        auto fresh = std::make_unique<owner>();
        fresh->cfg = cfg;
        fresh->cl = std::make_unique<cluster>(cfg.seed_brokers, cfg.client_id);
        group_config gc;
        gc.group_id = cfg.consumer_group;
        gc.topics = cfg.topics;
        gc.strategy = cfg.strategy;
        gc.session_timeout = cfg.session_timeout;
        fresh->group = std::make_unique<consumer_group>(*fresh->cl, std::move(gc));
        o = fresh.get();
        owners()[id] = std::move(fresh);
        co_await o->cl->refresh(cfg.topics);
    }
    const bool new_shard = o->attached.insert(from_shard).second;
    if (new_shard) ++o->refs;
    const bool first = new_shard && o->refs == 1;
    co_await ensure_joined(*o);
    if (first) arm_timers(*o);
}

seastar::future<> detach_on_zero(std::string id, unsigned from_shard) {
    owner* o = find_owner(id);
    if (!o) co_return;
    // A shard that never attached must not decrement, or a stray detach would
    // tear the group down under the shards still using it.
    if (o->attached.erase(from_shard) == 0) co_return;
    if (--o->refs > 0) co_return;

    o->stopping = true;
    o->heartbeat.cancel();
    o->commit.cancel();
    co_await o->gate.close();
    // Every step below is best-effort and guarded separately, and the erase
    // runs whatever happens. leave() talks to a coordinator that may be down --
    // it is the step most likely to throw here -- and an exception escaping
    // would skip the erase, stranding the owner and its live sockets in a
    // map with static storage duration. That map is destroyed after the
    // reactor has stopped, where closing a socket is no longer possible: the
    // symptom is "connection destroyed without close()" logged with no shard
    // prefix, followed by a core dump at exit.
    try {
        co_await commit_staged(*o);
    } catch (...) {
        mlog.warn("group {}: final commit failed; messages will be redelivered", id);
    }
    try {
        co_await o->group->leave();
    } catch (const std::exception& e) {
        mlog.warn("group {}: leave failed ({}); the coordinator will time the "
                  "member out instead", id, e.what());
    }
    try {
        co_await o->cl->stop();
    } catch (const std::exception& e) {
        mlog.warn("group {}: closing connections failed: {}", id, e.what());
    }
    owners().erase(id);
}

} // namespace

namespace membership {

seastar::future<shard_assignment> attach(consumer_config cfg) {
    const std::string id = cfg.consumer_group;
    const unsigned me = seastar::this_shard_id();
    co_await seastar::smp::submit_to(0, [cfg = std::move(cfg), me]() mutable {
        return attach_on_zero(std::move(cfg), me);
    });
    // The push in do_join() has already run on this shard by the time attach
    // returns, so this is a local read of freshly written state.
    co_return current(id);
}

seastar::future<> offer(std::string group_id, offset_table offsets) {
    if (offsets.empty()) return seastar::make_ready_future<>();
    return seastar::smp::submit_to(0, [id = std::move(group_id),
                                       offs = std::move(offsets)]() mutable {
        owner* o = find_owner(id);
        if (!o) return;
        for (const auto& [topic, parts] : offs)
            for (const auto& [p, off] : parts) {
                auto& slot = o->staged[topic][p];
                // Merged by maximum: offers from different shards never touch
                // the same partition, but a retry could arrive out of order.
                slot = std::max(slot, off);
            }
    });
}

seastar::future<> commit_now(std::string group_id) {
    return seastar::smp::submit_to(0, [id = std::move(group_id)] {
        owner* o = find_owner(id);
        if (!o) return seastar::make_ready_future<>();
        return commit_staged(*o);
    });
}

seastar::future<> detach(std::string group_id) {
    const unsigned me = seastar::this_shard_id();
    return seastar::smp::submit_to(0, [id = std::move(group_id), me]() mutable {
        return detach_on_zero(std::move(id), me);
    });
}

const shard_assignment& current(const std::string& group_id) {
    static const shard_assignment none;
    const auto it = local_cache().find(group_id);
    return it == local_cache().end() ? none : it->second;
}

} // namespace membership
} // namespace sf::kafka
