// The process's membership of one consumer group, owned by shard 0.
//
// A Swordfish process is ONE member of a consumer group, not one per shard.
// That is deliberate, and it matters for
// reasons that are operational rather than technical: a sixteen-shard process
// joining as sixteen members shows up as sixteen rows in
// `kafka-consumer-groups.sh`, multiplies every rebalance by sixteen, and needs
// sixteen `group.instance.id`s for static membership. Every other Kafka client
// presents one process as one member, and tooling assumes it.
//
// So: shard 0 runs the group protocol -- join, sync, heartbeat, offset commit --
// and the assignment it receives is divided across shards internally by
// `partition % smp::count`. Each shard then fetches its own partitions from
// their leaders directly, so the DATA path never crosses a core. The only
// cross-core traffic is a rebalance (pushed once, with invoke_on_all) and the
// offsets a shard has finished with (offered periodically, not per message).
#pragma once

// consumer.hh for consumer_config; it does NOT include this header, so there
// is no cycle -- only consumer.cc depends on membership.
#include "swordfish/kafka/consumer.hh"

#include <seastar/core/future.hh>

#include <chrono>
#include <map>
#include <string>

namespace sf::kafka {

// topic -> partition -> offset.
using offset_table = std::map<std::string, std::map<int32_t, int64_t>>;

// Which of a group's partitions belong to one shard. `partition % shards`,
// which spreads a topic's partitions evenly and puts the same-numbered
// partition of every topic on the same shard -- the same co-location range
// assignment gives across members.
partition_map shard_share(const partition_map& all, unsigned shard, unsigned shards);

struct shard_assignment {
    // -1 until the group has been joined. A shard compares this against what
    // it last acted on to notice a rebalance without asking shard 0.
    int32_t       generation = -1;
    partition_map partitions;      // this shard's share
    offset_table  committed;       // where to resume, for those partitions only
};

// What a shard needs from the group. Every call is safe from any shard.
namespace membership {

// Join the group if this is the first shard to ask, and return this shard's
// share. Concurrent calls from several shards share one join.
seastar::future<shard_assignment> attach(consumer_config cfg);

// This shard is finished with these offsets; stage them for the next commit.
// Merged by maximum, so an offer that arrives late cannot move an offset back.
seastar::future<> offer(std::string group_id, offset_table offsets);

// Commit everything staged, now rather than on the timer. Used at shutdown and
// before giving partitions up in a rebalance.
seastar::future<> commit_now(std::string group_id);

// This shard is stopping. The last one out commits, leaves the group and closes
// the coordinator connection.
seastar::future<> detach(std::string group_id);

// This shard's current view, updated by a push from shard 0. A LOCAL read: the
// poll loop consults it every round, and paying a core-to-core round trip for
// that would defeat the point of sharding.
const shard_assignment& current(const std::string& group_id);

} // namespace membership
} // namespace sf::kafka
