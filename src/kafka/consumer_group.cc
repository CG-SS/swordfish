#include "swordfish/kafka/consumer_group.hh"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>
#include <seastar/util/log.hh>

#include <algorithm>

namespace sf::kafka {

static seastar::logger glog("sf.kafka.group");

// ---- the embedded consumer protocol blobs ----------------------------------

// Version 0 of the consumer protocol: topics and user data only. Newer versions
// carry owned partitions and a generation, which the cooperative-sticky
// assignor needs; the eager strategies here do not, and a member may always
// speak an older version than it understands.
constexpr int16_t consumer_protocol_version = 0;

std::string encode_subscription(const std::vector<std::string>& topics) {
    consumer_protocol_subscription sub;
    sub.topics = topics;
    writer w;
    // The blob is versioned INSIDE itself: an int16 ahead of the body, which is
    // not part of the generated struct because it is the schema's version, not
    // a field.
    w.i16(consumer_protocol_version);
    sub.encode(w, consumer_protocol_version);
    return w.take();
}

std::vector<std::string> decode_subscription(std::string_view blob) {
    reader r(blob);
    const int16_t version = r.i16();
    consumer_protocol_subscription sub;
    // A member on a newer protocol version sends fields we do not know; decoding
    // at OUR version and ignoring the tail is what the protocol expects.
    sub.decode(r, std::min<int16_t>(version, consumer_protocol_subscription::max_version));
    return sub.topics;
}

std::string encode_assignment(const partition_map& assignment) {
    consumer_protocol_assignment a;
    a.assigned_partitions.reserve(assignment.size());
    for (const auto& [topic, parts] : assignment) {
        consumer_protocol_assignment_topic_partition tp;
        tp.topic = topic;
        tp.partitions = parts;
        a.assigned_partitions.push_back(std::move(tp));
    }
    writer w;
    w.i16(consumer_protocol_version);
    a.encode(w, consumer_protocol_version);
    return w.take();
}

partition_map decode_assignment(std::string_view blob) {
    partition_map out;
    if (blob.empty()) return out;         // no assignment: not an error
    reader r(blob);
    const int16_t version = r.i16();
    consumer_protocol_assignment a;
    a.decode(r, std::min<int16_t>(version, consumer_protocol_assignment::max_version));
    for (const auto& tp : a.assigned_partitions) out[tp.topic] = tp.partitions;
    return out;
}

// ---- assignment strategies -------------------------------------------------

namespace {

// Members are sorted by id so every member computes the SAME assignment from
// the same inputs. Only the leader's result is used, but determinism means a
// leader change does not reshuffle the world.
std::vector<std::string> sorted_ids(const std::vector<joined_member>& members) {
    std::vector<std::string> ids;
    ids.reserve(members.size());
    for (const auto& m : members) ids.push_back(m.member_id);
    std::sort(ids.begin(), ids.end());
    return ids;
}

std::vector<std::string> subscribers(const std::vector<joined_member>& members,
                                     const std::string& topic) {
    std::vector<std::string> out;
    for (const auto& m : members)
        if (std::find(m.topics.begin(), m.topics.end(), topic) != m.topics.end())
            out.push_back(m.member_id);
    std::sort(out.begin(), out.end());
    return out;
}

} // namespace

partition_map assign_range(const std::vector<joined_member>& members,
                           const std::string& self,
                           const std::map<std::string, int32_t>& partition_counts) {
    partition_map out;
    for (const auto& [topic, count] : partition_counts) {
        const auto subs = subscribers(members, topic);
        if (subs.empty()) continue;
        const size_t n = subs.size();
        // Contiguous runs, with the first (count % n) members taking one extra.
        const int32_t base = count / static_cast<int32_t>(n);
        const int32_t extra = count % static_cast<int32_t>(n);
        int32_t next = 0;
        for (size_t i = 0; i < n; ++i) {
            const int32_t take = base + (static_cast<int32_t>(i) < extra ? 1 : 0);
            if (subs[i] == self)
                for (int32_t p = next; p < next + take; ++p) out[topic].push_back(p);
            next += take;
        }
    }
    return out;
}

partition_map assign_roundrobin(const std::vector<joined_member>& members,
                                const std::string& self,
                                const std::map<std::string, int32_t>& partition_counts) {
    partition_map out;
    const auto ids = sorted_ids(members);
    if (ids.empty()) return out;
    // One rotating cursor across ALL topic-partitions, which is what makes this
    // balance better than range when topics have very different sizes.
    size_t cursor = 0;
    for (const auto& [topic, count] : partition_counts) {
        const auto subs = subscribers(members, topic);
        if (subs.empty()) continue;
        for (int32_t p = 0; p < count; ++p) {
            const std::string& owner = subs[cursor % subs.size()];
            ++cursor;
            if (owner == self) out[topic].push_back(p);
        }
    }
    return out;
}

// ---- the group -------------------------------------------------------------

consumer_group::consumer_group(cluster& cl, group_config cfg)
    : _cluster(cl), _cfg(std::move(cfg)) {}

seastar::future<connection*> consumer_group::coordinator() {
    if (_coordinator_id >= 0) {
        try { co_return co_await _cluster.broker(_coordinator_id); }
        catch (...) { _coordinator_id = -1; }        // it went away; ask again
    }
    // FindCoordinator is answered by ANY broker, so the ordinary metadata
    // connection will do.
    co_await _cluster.refresh(_cfg.topics);

    // COORDINATOR_NOT_AVAILABLE is the normal FIRST answer on a cluster that
    // has never had a consumer group: __consumer_offsets, the internal topic
    // holding group state, is created lazily and there is no coordinator until
    // it exists. Retrying is not a test workaround -- every real client does
    // it, and without it a consumer racing a fresh broker fails at startup.
    std::chrono::milliseconds backoff{200};
    for (int attempt = 0; ; ++attempt) {
        auto* any = co_await _cluster.broker(_cluster.brokers().at(0).node_id);
        find_coordinator_request req;
        const int16_t v = any->negotiated(find_coordinator_request::api_key);
        // From v4 the request takes a LIST of keys and answers with a list;
        // before that it is one key and a flat response.
        if (v >= 4) req.coordinator_keys = {_cfg.group_id};
        else        req.key = _cfg.group_id;
        req.key_type = 0;                            // 0 = group, 1 = transaction
        auto resp =
            co_await any->send<find_coordinator_request, find_coordinator_response>(req, v);

        int16_t code = resp.error_code;
        int32_t node = resp.node_id;
        if (v >= 4) {
            if (resp.coordinators.empty())
                throw protocol_error("FindCoordinator returned nothing");
            code = resp.coordinators[0].error_code;
            node = resp.coordinators[0].node_id;
        }
        if (code == err::none) {
            _coordinator_id = node;
            // The coordinator may be a broker metadata has not mentioned yet.
            co_await _cluster.refresh(_cfg.topics);
            co_return co_await _cluster.broker(node);
        }
        if (!is_retriable(code) || attempt >= coordinator_retries)
            throw broker_error(code, "FindCoordinator: " + describe_error(code));
        glog.debug("FindCoordinator: {}, retrying in {}ms", error_name(code),
                   backoff.count());
        co_await seastar::sleep(backoff);
        backoff = std::min(backoff * 2, std::chrono::milliseconds{2000});
    }
}

seastar::future<partition_map> consumer_group::join() {
    return retrying([this] { return join_once(); });
}

seastar::future<partition_map> consumer_group::join_once() {
    auto* co = co_await coordinator();

    // --- JoinGroup ---
    join_group_request jreq;
    jreq.group_id = _cfg.group_id;
    jreq.session_timeout_ms = static_cast<int32_t>(_cfg.session_timeout.count());
    jreq.rebalance_timeout_ms = static_cast<int32_t>(_cfg.rebalance_timeout.count());
    jreq.member_id = _member_id;                     // empty on the first attempt
    jreq.protocol_type = "consumer";
    join_group_request_protocol proto;
    proto.name = _cfg.strategy;
    proto.metadata = encode_subscription(_cfg.topics);
    jreq.protocols.push_back(std::move(proto));

    const int16_t jv = co->negotiated(join_group_request::api_key);
    auto jresp = co_await co->send<join_group_request, join_group_response>(jreq, jv);

    // From v4 the FIRST join is expected to fail with MEMBER_ID_REQUIRED, and
    // the response carries the id to use. It is not an error, it is the
    // handshake -- the coordinator is refusing to let an unidentified member
    // trigger a rebalance it might then abandon.
    if (jresp.error_code == 79 /* MEMBER_ID_REQUIRED */) {
        _member_id = jresp.member_id;
        jreq.member_id = _member_id;
        jresp = co_await co->send<join_group_request, join_group_response>(jreq, jv);
    }
    if (jresp.error_code != err::none)
        throw broker_error(jresp.error_code,
                           "JoinGroup: " + describe_error(jresp.error_code));

    _member_id = jresp.member_id;
    _generation = jresp.generation_id;
    _is_leader = (jresp.leader == _member_id);
    _protocol_name = jresp.protocol_name.value_or(_cfg.strategy);
    glog.debug("joined {} as {} (generation {}, {})", _cfg.group_id, _member_id,
               _generation, _is_leader ? "leader" : "follower");

    // --- SyncGroup ---
    sync_group_request sreq;
    sreq.group_id = _cfg.group_id;
    sreq.generation_id = _generation;
    sreq.member_id = _member_id;
    sreq.protocol_type = "consumer";
    sreq.protocol_name = _protocol_name;

    if (_is_leader) {
        // We compute the assignment for EVERY member and upload all of them.
        // The coordinator only relays; it does not check our arithmetic.
        std::vector<joined_member> members;
        members.reserve(jresp.members.size());
        for (const auto& m : jresp.members)
            members.push_back({m.member_id, decode_subscription(m.metadata)});

        // Partition counts come from metadata, which must cover every topic any
        // member asked for -- not only the ones we subscribed to.
        std::vector<std::string> all_topics;
        for (const auto& m : members)
            for (const auto& t : m.topics)
                if (std::find(all_topics.begin(), all_topics.end(), t) == all_topics.end())
                    all_topics.push_back(t);
        co_await _cluster.refresh(all_topics);

        std::map<std::string, int32_t> counts;
        for (const auto& t : all_topics)
            if (const topic_info* ti = _cluster.topic(t))
                counts[t] = static_cast<int32_t>(ti->partitions.size());

        for (const auto& m : members) {
            const partition_map pm = (_protocol_name == "roundrobin")
                ? assign_roundrobin(members, m.member_id, counts)
                : assign_range(members, m.member_id, counts);
            sync_group_request_assignment sa;
            sa.member_id = m.member_id;
            sa.assignment = encode_assignment(pm);
            sreq.assignments.push_back(std::move(sa));
        }
    }

    const int16_t sv = co->negotiated(sync_group_request::api_key);
    auto sresp = co_await co->send<sync_group_request, sync_group_response>(sreq, sv);
    if (sresp.error_code != err::none)
        throw broker_error(sresp.error_code,
                           "SyncGroup: " + describe_error(sresp.error_code));

    auto assignment = decode_assignment(sresp.assignment);
    glog.debug("assigned {} topic(s)", assignment.size());
    co_return assignment;
}

seastar::future<bool> consumer_group::heartbeat() {
    return retrying([this] { return heartbeat_once(); });
}

seastar::future<bool> consumer_group::heartbeat_once() {
    auto* co = co_await coordinator();
    heartbeat_request req;
    req.group_id = _cfg.group_id;
    req.generation_id = _generation;
    req.member_id = _member_id;
    const int16_t v = co->negotiated(heartbeat_request::api_key);
    auto resp = co_await co->send<heartbeat_request, heartbeat_response>(req, v);

    switch (resp.error_code) {
    case err::none:
        co_return true;
    case err::rebalance_in_progress:
    case err::illegal_generation:
    case err::unknown_member_id:
        // All three mean the same thing to us: our membership is stale and we
        // must join again. Only UNKNOWN_MEMBER_ID also invalidates the id.
        if (resp.error_code == err::unknown_member_id) _member_id.clear();
        co_return false;
    case err::not_coordinator:
    case err::coordinator_not_available:
        forget_coordinator();
        co_return false;
    default:
        throw broker_error(resp.error_code,
                           "Heartbeat: " + describe_error(resp.error_code));
    }
}

seastar::future<std::map<std::string, std::map<int32_t, int64_t>>>
consumer_group::fetch_offsets(const partition_map& assignment) {
    return retrying([this, assignment] { return fetch_offsets_once(assignment); });
}

seastar::future<std::map<std::string, std::map<int32_t, int64_t>>>
consumer_group::fetch_offsets_once(const partition_map& assignment) {
    auto* co = co_await coordinator();
    offset_fetch_request req;
    const int16_t v = co->negotiated(offset_fetch_request::api_key);

    // From v8 the request batches several groups; before that it is one group
    // with a flat topic list. Both shapes exist in the same struct.
    if (v >= 8) {
        offset_fetch_request_group g;
        g.group_id = _cfg.group_id;
        for (const auto& [topic, parts] : assignment) {
            offset_fetch_request_topics t;
            // Name through v9, UUID from v10. Setting both is harmless: the
            // codec writes whichever the version defines.
            t.name = topic;
            t.topic_id = _cluster.topic_id(topic);
            t.partition_indexes = parts;
            g.topics.push_back(std::move(t));
        }
        req.groups.push_back(std::move(g));
    } else {
        req.group_id = _cfg.group_id;
        for (const auto& [topic, parts] : assignment) {
            offset_fetch_request_topic t;
            t.name = topic;
            t.partition_indexes = parts;
            req.topics.push_back(std::move(t));
        }
    }
    auto resp = co_await co->send<offset_fetch_request, offset_fetch_response>(req, v);

    std::map<std::string, std::map<int32_t, int64_t>> out;
    auto take = [&](const std::string& topic, int32_t partition, int64_t offset,
                    int16_t code) {
        if (code != err::none)
            throw broker_error(code, "OffsetFetch " + topic + "/" +
                             std::to_string(partition) + ": " +
                             describe_error(code));
        // -1 means "this group has never committed here", which is different
        // from offset 0 and must stay distinguishable.
        if (offset >= 0) out[topic][partition] = offset;
    };
    if (v >= 8) {
        if (resp.groups.empty()) throw protocol_error("OffsetFetch returned no group");
        const auto& g = resp.groups[0];
        if (g.error_code != err::none)
            throw broker_error(g.error_code, "OffsetFetch: " + describe_error(g.error_code));
        for (const auto& t : g.topics) {
            // v10 echoes only the id, so the name has to come back from the
            // cluster's cache.
            const std::string name = t.name.empty() ? _cluster.topic_name(t.topic_id)
                                                    : t.name;
            for (const auto& p : t.partitions)
                take(name, p.partition_index, p.committed_offset, p.error_code);
        }
    } else {
        if (resp.error_code != err::none)
            throw broker_error(resp.error_code, "OffsetFetch: " + describe_error(resp.error_code));
        for (const auto& t : resp.topics)
            for (const auto& p : t.partitions)
                take(t.name, p.partition_index, p.committed_offset, p.error_code);
    }
    co_return out;
}

seastar::future<> consumer_group::commit(
    const std::map<std::string, std::map<int32_t, int64_t>>& offsets) {
    if (offsets.empty()) return seastar::make_ready_future<>();
    return retrying([this, offsets] { return commit_once(offsets); });
}

seastar::future<> consumer_group::commit_once(
    const std::map<std::string, std::map<int32_t, int64_t>>& offsets) {
    auto* co = co_await coordinator();
    offset_commit_request req;
    req.group_id = _cfg.group_id;
    req.generation_id_or_member_epoch = _generation;
    req.member_id = _member_id;
    for (const auto& [topic, parts] : offsets) {
        offset_commit_request_topic t;
        t.name = topic;                   // through v9
        t.topic_id = _cluster.topic_id(topic);   // from v10
        for (const auto& [partition, offset] : parts) {
            offset_commit_request_partition p;
            p.partition_index = partition;
            // The COMMITTED offset is the next one to read, not the last one
            // read. Committing the last read offset silently replays one
            // message per partition on every restart.
            p.committed_offset = offset;
            p.committed_leader_epoch = -1;
            t.partitions.push_back(std::move(p));
        }
        req.topics.push_back(std::move(t));
    }
    const int16_t v = co->negotiated(offset_commit_request::api_key);
    auto resp = co_await co->send<offset_commit_request, offset_commit_response>(req, v);
    for (const auto& t : resp.topics) {
        const std::string name = t.name.empty() ? _cluster.topic_name(t.topic_id) : t.name;
        for (const auto& p : t.partitions)
            if (p.error_code != err::none)
                throw broker_error(p.error_code,
                                   "OffsetCommit " + name + "/" +
                                   std::to_string(p.partition_index) + ": " +
                                   describe_error(p.error_code));
    }
}

seastar::future<> consumer_group::leave() {
    if (_member_id.empty()) co_return;
    connection* co = nullptr;
    try {
        co = co_await coordinator();
    } catch (...) {
        co_return;                        // shutting down; nothing to salvage
    }
    leave_group_request req;
    req.group_id = _cfg.group_id;
    const int16_t v = co->negotiated(leave_group_request::api_key);
    // From v3 several members may leave at once, so the id moved into a list.
    if (v >= 3) {
        leave_group_request_member_identity m;
        m.member_id = _member_id;
        req.members.push_back(std::move(m));
    } else {
        req.member_id = _member_id;
    }
    try {
        co_await co->send<leave_group_request, leave_group_response>(req, v);
    } catch (...) {
        // Leaving is a courtesy: it makes the group rebalance now rather than
        // after the session timeout. Failing to do so is not worth an error on
        // a shutdown path.
    }
    _member_id.clear();
    _generation = -1;
}

} // namespace sf::kafka
