#include "swordfish/kafka/consumer.hh"
#include "swordfish/kafka/group_membership.hh"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/smp.hh>
#include <seastar/util/log.hh>

#include <algorithm>

namespace sf::kafka {

static seastar::logger culog("sf.kafka.consumer");

// ---- offset_tracker --------------------------------------------------------

void offset_tracker::deliver(int64_t offset) {
    _in_flight.insert(offset);
    // The position is what makes a hole harmless: it comes from what was read,
    // not from counting up through offsets that may not exist.
    _position = std::max(_position, offset + 1);
}

void offset_tracker::ack(int64_t offset) {
    _in_flight.erase(offset);              // a duplicate or unknown ack is a no-op
}

std::optional<int64_t> offset_tracker::committable() const {
    // Resume at the OLDEST record the pipeline has not finished with. Anything
    // past it may be acked already, and replaying it is the at-least-once
    // contract; committing past it would lose the unfinished one on a crash.
    const int64_t next = _in_flight.empty() ? _position : *_in_flight.begin();
    if (next < 0) return std::nullopt;     // nothing delivered and nothing resumed
    if (next <= _committed) return std::nullopt;
    return next;
}

void offset_tracker::mark_committed(int64_t offset) {
    _committed = offset;
    // Seeds the resume position on a rebalance. Without this a group that
    // resumed from a committed offset had no position at all until its first
    // delivery, and the old contiguity model could never close a run that did
    // not start at offset 0.
    _position = std::max(_position, offset);
}

// ---- consumer --------------------------------------------------------------

consumer::consumer(consumer_config cfg) : _cfg(std::move(cfg)) {}
consumer::~consumer() = default;

seastar::future<> consumer::start(seastar::abort_source&) {
    _cluster = std::make_unique<cluster>(_cfg.seed_brokers, _cfg.client_id);
    // A Fetch parks on the broker for up to fetch_max_wait, so the request
    // timeout has to clear that with room to spare or every long poll would
    // look like a stalled request. Kafka's own default is 30s; a consumer
    // configured to poll for longer than that gets a proportionally longer one.
    _cluster->set_request_timeout(
        std::max(std::chrono::milliseconds(30000), _cfg.fetch_max_wait * 3));
    co_await _cluster->refresh(_cfg.topics);

    if (_cfg.consumer_group.empty()) {
        // No group, so no coordinator to divide the partitions: this shard
        // takes its share by the same rule the group path uses. Taking ALL of
        // them, as this used to, means every shard reads every partition and
        // the pipeline sees each message smp::count times.
        partition_map all;
        for (const auto& t : _cfg.topics)
            if (const topic_info* ti = _cluster->topic(t))
                for (const auto& p : ti->partitions)
                    all[t].push_back(p.partition);
        _assignment = shard_share(all, seastar::this_shard_id(), seastar::this_smp().shard_count());
        co_await resolve_start_offsets();
        ensure_trackers();
    } else {
        co_await rejoin();
    }
    _started = true;
}

seastar::future<> consumer::rejoin() {
    // Shard 0 owns the group; this shard is told its share. Joining per shard
    // would make one process N members of the group -- see
    // one member per process, not one per shard.
    const shard_assignment a = co_await membership::attach(_cfg);
    _assignment = a.partitions;
    _generation = a.generation;
    _next.clear();
    _tracked.clear();

    // Committed offsets come with the assignment; anything without one falls
    // back to the configured end of the log.
    for (const auto& [topic, parts] : _assignment)
        for (int32_t p : parts) {
            const auto t = a.committed.find(topic);
            if (t != a.committed.end()) {
                const auto o = t->second.find(p);
                if (o != t->second.end()) {
                    _next[topic][p] = o->second;
                    _tracked[topic][p].mark_committed(o->second);
                    continue;
                }
            }
            _next[topic][p] = -1;          // resolve below
        }
    co_await resolve_start_offsets();
    ensure_trackers();
    _needs_rejoin = false;

    // Logged per shard because a wrong split is otherwise invisible: it shows up
    // as under- or over-consumption much later, not as an error.
    size_t n = 0;
    for (const auto& [t, ps] : _assignment) n += ps.size();
    culog.info("shard {}: generation {}, {} partition(s)",
               seastar::this_shard_id(), _generation, n);
}

// One tracker per assigned partition, created up front.
//
// It used to be created lazily by `_tracked[topic][p]` at delivery. When that
// line went away the map stayed empty, ack() could not find an entry, every
// acknowledgement was silently dropped and nothing was ever committed. Creating
// them with the assignment removes the dependency on a side effect of
// operator[] in the hot loop.
void consumer::ensure_trackers() {
    for (const auto& [topic, parts] : _assignment)
        for (int32_t p : parts) (void)_tracked[topic][p];
}

seastar::future<> consumer::resolve_start_offsets() {
    // ListOffsets with -2 for the earliest retained offset and -1 for the log
    // end. "Earliest" is not 0 once retention has deleted anything, which is
    // why this is a request rather than a constant.
    const int64_t want = _cfg.start_from_oldest ? -2 : -1;
    for (const auto& [topic, parts] : _assignment) {
        std::vector<int32_t> need;
        for (int32_t p : parts)
            if (_next[topic].find(p) == _next[topic].end() || _next[topic][p] < 0)
                need.push_back(p);
        if (need.empty()) continue;

        // One request per leader, since ListOffsets is answered by the leader.
        std::map<int32_t, std::vector<int32_t>> by_leader;
        for (int32_t p : need) {
            const topic_info* ti = _cluster->topic(topic);
            const partition_info* pi = ti ? ti->find(p) : nullptr;
            if (!pi || pi->leader < 0) continue;
            by_leader[pi->leader].push_back(p);
        }
        for (const auto& [node, ps] : by_leader) {
            auto* conn = co_await _cluster->broker(node);
            list_offsets_request req;
            req.replica_id = -1;
            list_offsets_request_list_offsets_topic lt;
            lt.name = topic;
            for (int32_t p : ps) {
                list_offsets_request_list_offsets_partition lp;
                lp.partition_index = p;
                lp.timestamp = want;
                lp.current_leader_epoch = -1;
                lt.partitions.push_back(std::move(lp));
            }
            req.topics.push_back(std::move(lt));
            const int16_t v = conn->negotiated(list_offsets_request::api_key);
            auto resp = co_await conn->send<list_offsets_request, list_offsets_response>(req, v);
            for (const auto& t : resp.topics)
                for (const auto& p : t.partitions) {
                    if (p.error_code != err::none)
                        throw broker_error(p.error_code, "ListOffsets " + t.name + "/" +
                                           std::to_string(p.partition_index) + ": " +
                                           describe_error(p.error_code));
                    _next[t.name][p.partition_index] = p.offset;
                }
        }
    }
}

// Heartbeats are shard 0's job, on a timer, so liveness does not depend on this
// shard's pipeline draining. All this shard does is notice that shard 0 has
// rebalanced -- a local read of the pushed cache, no core-to-core round trip.
void consumer::check_generation() {
    if (_cfg.consumer_group.empty()) return;
    if (membership::current(_cfg.consumer_group).generation != _generation)
        _needs_rejoin = true;
}

seastar::future<std::vector<fetched_record>> consumer::poll(seastar::abort_source& as) {
    std::vector<fetched_record> out;
    if (!_started || as.abort_requested()) co_return out;

    check_generation();
    if (_needs_rejoin) {
        // Offer what is safe before giving the partitions up: after a rebalance
        // they may belong to someone else.
        co_await commit_acked();
        co_await rejoin();
        co_return out;
    }
    // Hand finished offsets to shard 0 on the way past. Committing them is its
    // job, on its own interval; this is just the handover.
    co_await commit_acked();

    // One Fetch per leader, carrying every partition that broker leads.
    std::map<int32_t, std::vector<std::pair<std::string, int32_t>>> by_leader;
    for (const auto& [topic, parts] : _assignment)
        for (int32_t p : parts) {
            const topic_info* ti = _cluster->topic(topic);
            const partition_info* pi = ti ? ti->find(p) : nullptr;
            if (!pi || pi->leader < 0) continue;
            by_leader[pi->leader].emplace_back(topic, p);
        }

    if (by_leader.empty()) {
        // Nothing to fetch: a group with more members than partitions, or a
        // topic mid-election. Without this the caller's poll loop returns
        // instantly with no I/O to wait on and spins a core at 100%. Waiting
        // the same interval a broker would have waited keeps an idle member
        // idle.
        co_await seastar::sleep(_cfg.fetch_max_wait);
        co_return out;
    }

    bool stale_metadata = false;
    // The first error of this poll, HELD rather than thrown so that records
    // already decoded are handed to the caller first, and still rethrown below
    // if the poll ends up with nothing to return. Both halves matter:
    //
    //   - throwing immediately destroys `out` while `_next` has already been
    //     advanced past those records, so they are never delivered and never
    //     re-fetched -- a silent at-least-once violation;
    //   - swallowing it outright makes a broker that never comes back
    //     indistinguishable from an idle topic. An earlier version of this fix
    //     did exactly that, and the chaos harness caught it by asserting that a
    //     member which lived through two broker restarts must have recorded
    //     SOME error.
    std::exception_ptr pending;
    bool hard = false;             // stop fetching from any further leader
    for (const auto& [node, tps] : by_leader) {
        if (as.abort_requested() || hard) break;
        connection* conn = nullptr;
        try {
            conn = co_await _cluster->broker(node);
        } catch (...) {
            stale_metadata = true;
            continue;
        }
        const int16_t v = conn->negotiated(fetch_request::api_key);

        fetch_request req;
        req.replica_id = -1;
        req.max_wait_ms = static_cast<int32_t>(_cfg.fetch_max_wait.count());
        req.min_bytes = 1;
        req.max_bytes = _cfg.fetch_max_bytes;
        std::map<std::string, fetch_request_fetch_topic> topics;
        for (const auto& [topic, p] : tps) {
            auto& ft = topics[topic];
            ft.topic = topic;
            ft.topic_id = _cluster->topic_id(topic);     // required from v13
            fetch_request_fetch_partition fp;
            fp.partition = p;
            fp.fetch_offset = _next[topic][p];
            fp.partition_max_bytes = _cfg.fetch_max_bytes;
            fp.current_leader_epoch = -1;
            fp.log_start_offset = -1;
            ft.partitions.push_back(std::move(fp));
        }
        for (auto& [name, ft] : topics) req.topics.push_back(std::move(ft));

        // Guarded for the same reason broker() above is, and it matters more.
        // By the time a LATER leader in this loop fails, `out` already holds
        // records from earlier ones and `_next` has been advanced past them.
        // Throwing here destroys `out` but not `_next`, so those records are
        // never delivered AND never re-fetched -- a silent at-least-once
        // violation that only appears when a broker dies mid-poll. The chaos
        // test found it as 4750 messages missing with no error against them.
        fetch_response resp;
        try {
            resp = co_await conn->send<fetch_request, fetch_response>(req, v);
        } catch (...) {
            // Retriable at this level: refresh metadata and try the other
            // leaders, whose records are still worth having.
            if (!pending) pending = std::current_exception();
            stale_metadata = true;
            continue;
        }
        if (resp.error_code != err::none) {
            if (is_retriable(resp.error_code)) { stale_metadata = true; continue; }
            if (!pending) pending = std::make_exception_ptr(broker_error(
                resp.error_code, "Fetch: " + describe_error(resp.error_code)));
            break;   // leaves the per-leader loop directly; no flag needed here
        }
        for (const auto& t : resp.responses) {
            const std::string name = t.topic.empty() ? _cluster->topic_name(t.topic_id)
                                                     : t.topic;
            if (name.empty()) {
                // A uuid we cannot map back to a name: metadata is behind the
                // broker's view. Silently accepting it would file the records
                // under the empty-string topic and lose them; refreshing and
                // re-fetching is the only correct response.
                culog.warn("fetch response names a topic we do not know; refreshing");
                stale_metadata = true;
                continue;
            }
            for (const auto& p : t.partitions) {
                if (p.error_code == err::offset_out_of_range) {
                    // Retention deleted what we wanted, or the log was
                    // truncated. Restarting from the configured end is the only
                    // thing that makes progress; silently reading from 0 could
                    // replay the whole topic.
                    culog.warn("{}/{}: offset {} out of range, resetting",
                               name, p.partition_index, _next[name][p.partition_index]);
                    _next[name][p.partition_index] = -1;
                    stale_metadata = true;
                    continue;
                }
                if (p.error_code != err::none) {
                    if (is_retriable(p.error_code)) { stale_metadata = true; continue; }
                    if (!pending) pending = std::make_exception_ptr(broker_error(
                        p.error_code, "Fetch " + name + "/" +
                        std::to_string(p.partition_index) + ": " +
                        describe_error(p.error_code)));
                    // Unlike the leader-level break above, this one leaves only
                    // the innermost partition loop, so the flag is what stops
                    // the outer per-leader loop. Do not "simplify" it away.
                    hard = true;
                    break;
                }
                if (!p.records || p.records->empty()) continue;
                for (const auto& b : decode_batches(*p.records)) {
                    // A CONTROL batch is a transaction marker, not data. Its
                    // "record" is a 6-byte struct (int16 version, int32
                    // coordinator epoch) that Kafka writes to mark a commit or
                    // abort, and it occupies an offset like any other. Delivered
                    // as data it reached the pipeline as a message whose body is
                    // `000000000000`, was mapped, counted, written to the output
                    // and acked: measured on a transactional topic, swordfish
                    // delivered 9 records where the reference delivered 6.
                    //
                    // The position still has to move past it, or the fetch loop
                    // asks for the marker's offset for ever. It is NOT handed to
                    // the tracker, so the offset becomes a hole -- which is
                    // exactly the shape offset_tracker is now built to step over.
                    if (b.control) {
                        for (size_t i = 0; i < b.records.size(); ++i) {
                            const int64_t off = b.offset_of(i);
                            if (off >= _next[name][p.partition_index])
                                _next[name][p.partition_index] = off + 1;
                        }
                        continue;
                    }
                    for (size_t i = 0; i < b.records.size(); ++i) {
                        const int64_t off = b.offset_of(i);
                        // A compressed batch may start BEFORE the requested
                        // offset, because the broker sends whole batches.
                        if (off < _next[name][p.partition_index]) continue;
                        const auto& rec = b.records[i];
                        fetched_record fr;
                        fr.topic = name;
                        fr.partition = p.partition_index;
                        fr.offset = off;
                        fr.timestamp = b.base_timestamp + rec.timestamp_delta;
                        fr.key = rec.key;
                        fr.value = rec.value.value_or("");
                        fr.headers = rec.headers;
                        out.push_back(std::move(fr));
                        _next[name][p.partition_index] = off + 1;
                        // The tracker is told what was delivered. ensure_trackers()
                        // has already created the entry for every assigned
                        // partition; operator[] here would create one anyway, and
                        // a missing entry must not silently drop the record from
                        // the commit position.
                        _tracked[name][p.partition_index].deliver(off);
                    }
                }
            }
        }
    }
    if (stale_metadata || _needs_refresh) {
        // The refresh can fail -- the broker whose death set this flag is very
        // likely still down -- and it used to throw straight out of poll(),
        // past two unguarded awaits, taking `out` with it. Records already in
        // `out` have had `_next` advanced and been recorded as delivered, so
        // throwing here both loses them and strands them in flight for ever,
        // freezing the commit. That is the at-least-once violation the `pending`
        // machinery above exists to prevent, and it was guarded on the fetch
        // path and not on this one.
        //
        // So the refresh is retried on the next poll instead, and the error is
        // only surfaced when there is nothing to hand back.
        std::exception_ptr rerr;
        try {
            co_await _cluster->refresh(_cfg.topics);
            co_await resolve_start_offsets();
            _needs_refresh = false;
        } catch (...) {
            rerr = std::current_exception();
            _needs_refresh = true;
        }
        if (rerr && out.empty() && !pending) pending = rerr;
    }
    if (pending && out.empty()) std::rethrow_exception(pending);
    co_return out;
}

void consumer::ack(const std::string& topic, int32_t partition, int64_t offset) {
    auto t = _tracked.find(topic);
    if (t == _tracked.end()) return;
    auto p = t->second.find(partition);
    if (p == t->second.end()) return;
    p->second.ack(offset);
}

seastar::future<> consumer::commit_acked() {
    if (_cfg.consumer_group.empty()) co_return;
    offset_table finished;
    for (auto& [topic, parts] : _tracked)
        for (auto& [p, tracker] : parts)
            if (const auto off = tracker.committable()) finished[topic][p] = *off;
    if (finished.empty()) co_return;
    // Offered, not committed: shard 0 holds the coordinator connection and
    // batches every shard's progress into one OffsetCommit.
    co_await membership::offer(_cfg.consumer_group, finished);
    for (const auto& [topic, parts] : finished)
        for (const auto& [p, off] : parts) _tracked[topic][p].mark_committed(off);
}

seastar::future<> consumer::stop() {
    if (!_started) co_return;
    _started = false;
    if (!_cfg.consumer_group.empty()) {
        try {
            co_await commit_acked();
            // Flush now rather than waiting for the interval: the last shard to
            // detach takes the group with it.
            co_await membership::commit_now(_cfg.consumer_group);
        } catch (...) {
            culog.warn("final commit failed; messages will be redelivered");
        }
        co_await membership::detach(_cfg.consumer_group);
    }
    if (_cluster) co_await _cluster->stop();
}

} // namespace sf::kafka
