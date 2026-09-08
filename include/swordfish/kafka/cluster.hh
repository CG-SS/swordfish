// The cluster view: which brokers exist, who leads each partition, and one
// connection per broker.
//
// Everything above this layer addresses partitions, not sockets. A Fetch has to
// go to that partition's LEADER and nowhere else, the leader moves when a
// broker restarts or a partition is reassigned, and the broker tells you it
// moved by answering NOT_LEADER_OR_FOLLOWER rather than by pushing an update.
// So the shape of this class is: a cached map, a refresh, and a retry that
// refreshes first.
//
// One cluster per shard. Connections are not shared between shards -- the
// share-nothing rule again -- so a 16-shard consumer holds 16 connections to
// each broker it uses. That is the trade Seastar makes everywhere: more sockets
// in exchange for no locking on the data path.
#pragma once

#include "swordfish/kafka/client.hh"

#include <seastar/core/future.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/shared_ptr.hh>

#include <chrono>
#include <map>
#include <optional>
#include <memory>
#include <string>
#include <vector>

namespace sf::kafka {

struct broker_info {
    int32_t     node_id = -1;
    std::string host;
    int32_t     port = 0;
    std::string hostport() const { return host + ":" + std::to_string(port); }
};

struct partition_info {
    int32_t partition = 0;
    int32_t leader = -1;
    int32_t leader_epoch = -1;
    std::vector<int32_t> replicas;
};

struct topic_info {
    std::string    name;
    uuid           id{};
    std::vector<partition_info> partitions;

    const partition_info* find(int32_t p) const noexcept;
};

class cluster {
public:
    // `seed_brokers` are the addresses from the config; they are only used to
    // bootstrap. After the first Metadata response the cluster's own advertised
    // addresses are used, because those are the ones that route correctly from
    // wherever the client happens to be.
    cluster(std::vector<std::string> seed_brokers, std::string client_id);
    ~cluster();

    // Applied to every connection this cluster opens. A Fetch legitimately
    // parks on the broker for `fetch_max_wait`, so a consumer configured with a
    // long poll has to raise this or its own long polls would look like stalls.
    void set_request_timeout(std::chrono::milliseconds t) noexcept {
        _request_timeout = t;
    }

    seastar::future<> stop();

    // Fetch metadata for `topics` (all topics when empty) and update the cached
    // view. Safe to call repeatedly; concurrent calls share one request.
    // `create` asks the broker to CREATE any topic it does not know, which is
    // what `allow_auto_topic_creation` on the Metadata request does. A producer
    // wants that -- the reference's does, and it is how `topic:` naming a topic
    // that does not exist yet works at all -- while a consumer must not, or a
    // typo in `topics:` silently brings a topic into existence instead of
    // failing. It was hardcoded to false, so swordfish could never produce to a
    // new topic: it threw `unknown topic` and, once nacks began replaying,
    // retried for ever with nothing in the log.
    seastar::future<> refresh(std::vector<std::string> topics, bool create = false);

    const topic_info* topic(std::string_view name) const noexcept;
    // From Produce v13, Fetch v13, OffsetCommit v10 and OffsetFetch v10 a topic
    // travels as a UUID rather than a name, and the RESPONSE echoes only the
    // id -- so mapping back is needed as often as mapping forward.
    uuid        topic_id(std::string_view name) const noexcept;
    std::string topic_name(const uuid& id) const noexcept;
    const std::vector<broker_info>& brokers() const noexcept { return _brokers; }

    // A connection to a specific broker, opened on first use and reused after.
    seastar::future<connection*> broker(int32_t node_id);
    // A connection to whoever currently leads this partition.
    seastar::future<connection*> leader_for(std::string_view name, int32_t partition);

    // Deliberately NOT a generic with_leader(...) wrapper. Every API reports a
    // moved leader in a different place -- Produce and Fetch per partition,
    // ListOffsets per partition, OffsetCommit per partition, the group APIs at
    // the top level -- so a generic retry would have to be handed a predicate to
    // find the code, at which point the caller may as well own the loop. What
    // callers share is this: on a retriable code, call refresh() and try again.
    static constexpr int max_retries = 4;

private:
    seastar::future<connection*> any_broker();
    seastar::future<connection*> do_connect(int32_t node_id);
    seastar::future<> do_refresh(std::vector<std::string> topics, bool create);

    std::vector<std::string>   _seeds;
    std::string                _client_id;
    std::vector<broker_info>   _brokers;
    std::map<std::string, topic_info, std::less<>> _topics;
    std::chrono::milliseconds _request_timeout{30000};
    std::map<int32_t, std::unique_ptr<connection>> _conns;
    // Connections being opened right now. Two callers wanting the same broker
    // must SHARE one attempt: if both connected, the second would replace the
    // first in _conns and destroy it, leaving the first holding a dangling
    // pointer. poll() iterating leaders while coordinator() runs makes that
    // ordinary rather than exotic.
    std::map<int32_t, seastar::shared_future<connection*>> _connecting;
    // Bootstrap connections, keyed by address rather than node id: before the
    // first Metadata response there are no node ids to key on.
    std::map<std::string, std::unique_ptr<connection>> _seed_conns;
    // Concurrent refreshes share one request rather than stampeding the broker.
    std::optional<seastar::shared_future<>> _refresh_in_flight;
    bool                                    _stopping = false;
};

} // namespace sf::kafka
