#include "swordfish/kafka/cluster.hh"

#include <seastar/core/coroutine.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/sleep.hh>
#include <seastar/util/log.hh>

#include <algorithm>

namespace sf::kafka {

static seastar::logger clog("sf.kafka.cluster");

const partition_info* topic_info::find(int32_t p) const noexcept {
    for (const auto& pi : partitions) if (pi.partition == p) return &pi;
    return nullptr;
}

cluster::cluster(std::vector<std::string> seed_brokers, std::string client_id)
    : _seeds(std::move(seed_brokers)), _client_id(std::move(client_id)) {}

cluster::~cluster() = default;

seastar::future<> cluster::stop() {
    _stopping = true;
    // Each close is guarded on its own, and the maps are cleared whatever
    // happens. Closing a connection whose peer is already gone can throw, and
    // letting that escape would leave every LATER connection unclosed and both
    // maps populated -- so a cluster torn down during an outage would keep live
    // sockets until its owner is destroyed, which for a long-lived owner is
    // after the reactor has stopped. Seastar reports that as "connection
    // destroyed without close()" and the process dies on the way out.
    for (auto& [id, c] : _conns) {
        try { co_await c->close(); }
        catch (const std::exception& e) { clog.debug("close failed: {}", e.what()); }
    }
    for (auto& [addr, c] : _seed_conns) {
        try { co_await c->close(); }
        catch (const std::exception& e) { clog.debug("close failed: {}", e.what()); }
    }
    _conns.clear();
    _seed_conns.clear();
}

seastar::future<connection*> cluster::broker(int32_t node_id) {
    if (auto it = _conns.find(node_id); it != _conns.end() && it->second->connected())
        return seastar::make_ready_future<connection*>(it->second.get());
    // Someone is already opening this one: wait for their attempt rather than
    // starting a second that would replace it.
    if (auto it = _connecting.find(node_id); it != _connecting.end())
        return it->second.get_future();

    seastar::promise<connection*> p;
    _connecting.emplace(node_id, p.get_future());
    auto result = _connecting.at(node_id).get_future();
    (void)do_connect(node_id).then_wrapped(
        [this, node_id, p = std::move(p)](seastar::future<connection*> f) mutable {
            // Cleared BEFORE the waiters resume, so a caller that immediately
            // retries after a failure starts a fresh attempt.
            _connecting.erase(node_id);
            if (f.failed()) p.set_exception(f.get_exception());
            else            p.set_value(f.get());
        });
    return result;
}

seastar::future<connection*> cluster::do_connect(int32_t node_id) {
    const auto bi = std::find_if(_brokers.begin(), _brokers.end(),
                                 [&](const broker_info& b) { return b.node_id == node_id; });
    if (bi == _brokers.end())
        throw protocol_error("no such broker: node " + std::to_string(node_id));

    auto conn = std::make_unique<connection>();
    conn->set_request_timeout(_request_timeout);
    co_await conn->connect(bi->hostport(), _client_id);
    auto* raw = conn.get();
    // A dead entry may already be here; replacing it drops the old socket.
    if (auto old = _conns.find(node_id); old != _conns.end()) co_await old->second->close();
    _conns[node_id] = std::move(conn);
    co_return raw;
}

seastar::future<connection*> cluster::any_broker() {
    // Prefer a broker we already know and are already connected to.
    for (auto& [id, c] : _conns)
        if (c->connected()) co_return c.get();
    for (const auto& b : _brokers) {
        try { co_return co_await broker(b.node_id); }
        catch (...) { /* try the next one */ }
    }
    // Bootstrap: nothing known yet, so fall back to the configured seeds.
    std::exception_ptr last;
    for (const auto& addr : _seeds) {
        if (auto it = _seed_conns.find(addr); it != _seed_conns.end() && it->second->connected())
            co_return it->second.get();
        auto conn = std::make_unique<connection>();
    conn->set_request_timeout(_request_timeout);
        try {
            co_await conn->connect(addr, _client_id);
        } catch (...) {
            last = std::current_exception();
            clog.debug("seed {} unreachable", addr);
            continue;
        }
        auto* raw = conn.get();
        _seed_conns[addr] = std::move(conn);
        co_return raw;
    }
    if (last) std::rethrow_exception(last);
    throw protocol_error("no reachable broker among " + std::to_string(_seeds.size()) +
                         " seed address(es)");
}

seastar::future<> cluster::refresh(std::vector<std::string> topics, bool create) {
    // A stampede of refreshes after a leader change would ask the same question
    // many times; one request in flight answers all of them.
    if (_refresh_in_flight) return _refresh_in_flight->get_future();

    // The slot is filled from a PROMISE before the work starts. Building it
    // from do_refresh()'s future instead had a latch: a coroutine runs eagerly,
    // so one that failed before its first suspension -- any_broker() with no
    // reachable broker -- completed the chain, ran the reset on an empty
    // optional, and only then got emplaced. From that point every refresh
    // returned the same resolved future and metadata never updated again, so a
    // leader change could never be picked up.
    seastar::promise<> p;
    _refresh_in_flight.emplace(p.get_future());
    auto result = _refresh_in_flight->get_future();
    // cppcheck-suppress accessMoved
    //   do_refresh takes its vector by value, so the move lands in the
    //   coroutine's frame; nothing reads `topics` afterwards.
    (void)do_refresh(std::move(topics), create).then_wrapped(
        [this, p = std::move(p)](seastar::future<> f) mutable {
            _refresh_in_flight.reset();     // cleared before waiters resume
            if (f.failed()) p.set_exception(f.get_exception());
            else            p.set_value();
        });
    return result;
}

// cppcheck-suppress passedByValue
//   A coroutine parameter must be BY VALUE. A reference would dangle at the
//   first suspension, because the caller's argument is gone by then -- the
//   opposite of the usual advice, and a real crash rather than a style point.
seastar::future<> cluster::do_refresh(std::vector<std::string> topics, bool create) {
    auto* conn = co_await any_broker();

    metadata_request req;
    req.allow_auto_topic_creation = create;
    for (const auto& t : topics) {
        metadata_request_topic mt;
        mt.name = t;
        req.topics.push_back(std::move(mt));
    }
    const int16_t v = conn->negotiated(metadata_request::api_key);
    auto resp = co_await conn->send<metadata_request, metadata_response>(req, v);

    std::vector<broker_info> fresh;
    fresh.reserve(resp.brokers.size());
    for (const auto& b : resp.brokers)
        fresh.push_back({b.node_id, b.host, b.port});
    _brokers = std::move(fresh);

    for (const auto& t : resp.topics) {
        if (t.error_code != err::none) {
            // A topic that does not exist yet is not fatal: the caller may be
            // about to create it, or a produce may auto-create it.
            clog.debug("metadata for {}: {}", t.name.value_or("?"), error_name(t.error_code));
            continue;
        }
        topic_info ti;
        ti.name = t.name.value_or("");
        ti.id = t.topic_id;
        ti.partitions.reserve(t.partitions.size());
        for (const auto& p : t.partitions)
            ti.partitions.push_back({p.partition_index, p.leader_id, p.leader_epoch,
                                     p.replica_nodes});
        std::sort(ti.partitions.begin(), ti.partitions.end(),
                  [](const partition_info& a, const partition_info& b) {
                      return a.partition < b.partition;
                  });
        _topics[ti.name] = std::move(ti);
    }
    clog.debug("metadata: {} broker(s), {} topic(s)", _brokers.size(), _topics.size());
}

const topic_info* cluster::topic(std::string_view name) const noexcept {
    const auto it = _topics.find(name);
    return it == _topics.end() ? nullptr : &it->second;
}

uuid cluster::topic_id(std::string_view name) const noexcept {
    const topic_info* ti = topic(name);
    return ti ? ti->id : uuid{};
}

std::string cluster::topic_name(const uuid& id) const noexcept {
    for (const auto& [name, ti] : _topics)
        if (ti.id == id) return name;
    return {};
}

// cppcheck-suppress unusedFunction
//   Used by the Kafka integration tests, which are not in this scope.
seastar::future<connection*> cluster::leader_for(std::string_view name, int32_t partition) {
    const topic_info* ti = topic(name);
    if (!ti) {
        co_await refresh({std::string(name)});
        ti = topic(name);
        if (!ti) throw protocol_error("unknown topic: " + std::string(name));
    }
    const partition_info* pi = ti->find(partition);
    if (!pi)
        throw protocol_error("unknown partition " + std::to_string(partition) +
                             " of topic " + std::string(name));
    if (pi->leader < 0) {
        // Mid-election: there is no leader to talk to yet.
        co_await refresh({std::string(name)});
        ti = topic(name);
        pi = ti ? ti->find(partition) : nullptr;
        if (!pi || pi->leader < 0)
            throw broker_error(err::leader_not_available,
                               "no leader for " + std::string(name) + "/" +
                               std::to_string(partition));
    }
    co_return co_await broker(pi->leader);
}

} // namespace sf::kafka
