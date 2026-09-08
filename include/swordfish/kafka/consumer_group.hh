// The consumer group protocol: join a group, get an assignment, keep it.
//
// The classic (pre-KIP-848) protocol, which every broker still speaks. The
// shape of it:
//
//   FindCoordinator   which broker manages this group
//   JoinGroup         everyone announces what they want to consume; the
//                     coordinator picks one member as LEADER and sends it
//                     every member's subscription
//   SyncGroup         the leader computes the assignment for all members and
//                     uploads it; everyone else uploads nothing and receives
//                     their own share
//   Heartbeat         at session_timeout/3, or the coordinator evicts us
//   OffsetFetch       where to resume
//   OffsetCommit      where we have got to
//   LeaveGroup        on clean shutdown, so the group rebalances immediately
//                     rather than after the session timeout
//
// The part that surprises people: the ASSIGNMENT IS COMPUTED BY A CLIENT, not
// by the broker. The coordinator only relays. That is why the strategies live
// here rather than server-side, and why every member must agree on which
// strategy to run -- the coordinator picks the name they all listed.
#pragma once

#include "swordfish/kafka/cluster.hh"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/timer.hh>

#include <algorithm>
#include <chrono>
#include <map>
#include <string>
#include <vector>

namespace sf::kafka {

// topic -> partitions, the shape both the subscription and the assignment use.
using partition_map = std::map<std::string, std::vector<int32_t>>;

// A member as JoinGroup reports it. NOT called group_member: glibc declares
// `int group_member(gid_t)` in <unistd.h>, so unqualified lookup finds the
// function and every use becomes an unhelpful "template argument is invalid".
struct joined_member {
    std::string   member_id;
    std::vector<std::string> topics;      // decoded from its subscription blob
};

// Assignment strategies. Every member of a group must run the same one, and the
// coordinator picks it by name from what the members offered.
//
// `range` walks each topic separately and hands out contiguous runs, which
// co-locates partition N of every topic on one member -- what you want when
// topics are joined by key. `roundrobin` spreads partitions of all topics
// evenly, which balances better when topic sizes differ.
partition_map assign_range(const std::vector<joined_member>& members,
                           const std::string& self,
                           const std::map<std::string, int32_t>& partition_counts);
partition_map assign_roundrobin(const std::vector<joined_member>& members,
                                const std::string& self,
                                const std::map<std::string, int32_t>& partition_counts);

struct group_config {
    std::string              group_id;
    std::vector<std::string> topics;
    std::string              strategy = "range";     // or "roundrobin"
    std::chrono::milliseconds session_timeout{45000};
    std::chrono::milliseconds rebalance_timeout{300000};
    // Heartbeats go out at a third of the session timeout: fast enough that one
    // lost heartbeat is not fatal, slow enough not to be chatty.
    std::chrono::milliseconds heartbeat_interval{15000};
};

class consumer_group {
public:
    consumer_group(cluster& cl, group_config cfg);

    // Join (or rejoin) and return this member's assignment.
    seastar::future<partition_map> join();

    // Committed offsets for the assigned partitions. A partition with no commit
    // comes back absent rather than as 0, because "start from the beginning"
    // and "start from offset 0" are only the same thing before the first
    // retention deletion.
    seastar::future<std::map<std::string, std::map<int32_t, int64_t>>>
    fetch_offsets(const partition_map& assignment);

    seastar::future<> commit(const std::map<std::string, std::map<int32_t, int64_t>>& offsets);

    // One heartbeat. Returns false when the coordinator says the group is
    // rebalancing or we have been evicted, which means join() again.
    seastar::future<bool> heartbeat();

    seastar::future<> leave();

    const std::string& member_id() const noexcept { return _member_id; }
    int32_t generation() const noexcept { return _generation; }
    bool    is_leader() const noexcept { return _is_leader; }

private:
    // Retry a group operation that failed with a retriable broker error,
    // forgetting the coordinator first. Coordinator moves are routine -- the
    // group's partition of __consumer_offsets can be reassigned at any time,
    // and on a fresh cluster it is still being created -- so every group call
    // needs this, not just the discovery.
    template <class F>
    auto retrying(F f) -> std::invoke_result_t<F>;

    seastar::future<connection*> coordinator();
    // The bodies the retry wrapper calls. Split out rather than inlined so the
    // retry logic exists once.
    seastar::future<partition_map> join_once();
    seastar::future<bool> heartbeat_once();
    seastar::future<std::map<std::string, std::map<int32_t, int64_t>>>
        fetch_offsets_once(const partition_map& assignment);
    seastar::future<> commit_once(
        const std::map<std::string, std::map<int32_t, int64_t>>& offsets);
    // A cluster that has never hosted a consumer group answers
    // COORDINATOR_NOT_AVAILABLE until __consumer_offsets exists. Ten attempts
    // of exponential backoff is roughly fifteen seconds, which covers it.
    static constexpr int coordinator_retries = 10;
    void forget_coordinator() { _coordinator_id = -1; }

    cluster&      _cluster;
    group_config  _cfg;
    std::string   _member_id;             // empty until the first JoinGroup
    int32_t       _generation = -1;
    int32_t       _coordinator_id = -1;
    bool          _is_leader = false;
    std::string   _protocol_name;
};

template <class F>
auto consumer_group::retrying(F f) -> std::invoke_result_t<F> {
    std::chrono::milliseconds backoff{200};
    for (int attempt = 0; ; ++attempt) {
        try {
            co_return co_await f();
        } catch (const broker_error& e) {
            if (!is_retriable(e.code) || attempt >= coordinator_retries) throw;
        }
        // The sleep is out here because co_await is not allowed in a handler.
        forget_coordinator();
        co_await seastar::sleep(backoff);
        backoff = std::min(backoff * 2, std::chrono::milliseconds{2000});
    }
}

// The opaque blobs JoinGroup and SyncGroup carry, encoded with the generated
// ConsumerProtocol codecs. Exposed for testing: getting these wrong makes a
// group that joins and then assigns nothing, which looks like an idle consumer
// rather than like a bug.
std::string encode_subscription(const std::vector<std::string>& topics);
std::vector<std::string> decode_subscription(std::string_view blob);
std::string encode_assignment(const partition_map& assignment);
partition_map decode_assignment(std::string_view blob);

} // namespace sf::kafka
