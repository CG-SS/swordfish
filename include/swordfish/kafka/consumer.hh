// A consuming member of a group: join, fetch, hand out records, commit what was
// acknowledged.
//
// The offset discipline is what makes this at-least-once, and it is the part
// worth reading carefully:
//
//   * A record is handed out with the offset AFTER it (`offset + 1`). That is
//     what a commit means -- "resume here" -- and committing the offset of the
//     last record read instead replays one message per partition on every
//     restart.
//   * An offset is only marked committable once the pipeline ACKS it. Nothing
//     is committed on delivery, so a crash between delivery and ack redelivers
//     rather than loses.
//   * Acks may complete out of order, so a partition tracks the highest
//     CONTIGUOUS acked offset -- the watermark. Committing a later offset while
//     an earlier one is still in flight would silently drop that message.
#pragma once

#include "swordfish/kafka/cluster.hh"
#include "swordfish/kafka/consumer_group.hh"
#include "swordfish/kafka/records.hh"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>

#include <chrono>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <vector>

namespace sf::kafka {

struct fetched_record {
    std::string                key_or_empty;
    std::optional<std::string> key;
    std::string                value;
    std::vector<record_header> headers;
    std::string                topic;
    int32_t                    partition = 0;
    int64_t                    offset = 0;
    int64_t                    timestamp = 0;
};

struct consumer_config {
    std::vector<std::string> seed_brokers;
    std::vector<std::string> topics;
    std::string              consumer_group;
    std::string              client_id = "swordfish";
    std::string              strategy = "range";
    // Where to start when the group has no committed offset. Matches the
    // reference's `start_from_oldest`.
    bool                     start_from_oldest = true;
    int32_t                  fetch_max_bytes = 52428800;
    std::chrono::milliseconds fetch_max_wait{500};
    std::chrono::milliseconds session_timeout{45000};
    // How often acknowledged offsets are committed. Before this existed a
    // consumer committed only on a rebalance or at shutdown, so a crash
    // replayed everything back to the last one -- possibly the whole run.
    std::chrono::milliseconds commit_interval{5000};
};

// Tracks which offsets of one partition are safe to commit.
//
// Delivery is sequential but acknowledgement is not: the pipeline may ack
// message 7 before message 5. Committing 8 at that point would skip 5 and 6 if
// the process then died.
//
// This tracks what was DELIVERED and what is still outstanding, rather than
// looking for a contiguous run of acked offsets. The difference is not stylistic
// -- the contiguity model was wrong twice over, and both were reproduced against
// a live broker:
//
//   * A partition's offsets are not dense. A compacted topic surviving offsets
//     0,6,7,8,9,10,11 has a permanent hole at 1..5, and a run starting at 1 can
//     never close: the group committed 1 and stayed there with LAG 11 while the
//     reference committed 12. Transaction markers make the same hole on any
//     topic written transactionally.
//   * Resuming a group seeds the committed offset, and a run measured from
//     `_watermark + 1` == 0 can never close either unless the partition happens
//     to start at offset 0. A restarted group committed NOTHING, for ever, and
//     replayed the same records on every restart.
//
// The rule here instead: commit the oldest offset still in flight, or, when
// nothing is in flight, the position after everything delivered. Offsets that
// were never delivered are not in flight and so cannot hold the commit back,
// which is exactly what makes a hole harmless.
class offset_tracker {
public:
    // Called for every record handed to the pipeline. The tracker cannot infer
    // this from the acks: in-order delivery does not imply dense offsets, and
    // the position after a hole is knowable only from what was actually read.
    void deliver(int64_t offset);
    void ack(int64_t offset);              // pipeline finished with it
    // The offset to COMMIT -- "resume HERE". The oldest offset still in flight,
    // or the position past everything delivered when none is. nullopt when that
    // is not ahead of what has already been committed.
    std::optional<int64_t> committable() const;
    // Records a commit, and on a rebalance seeds the resume position from the
    // group's committed offset.
    void mark_committed(int64_t offset);

private:
    int64_t          _position = -1;       // one past the highest offset delivered
    int64_t          _committed = -1;
    // Delivered and not yet acked. Bounded by the pipeline's in-flight window,
    // and cleared wholesale on a rebalance.
    std::set<int64_t> _in_flight;
};

class consumer {
public:
    explicit consumer(consumer_config cfg);
    ~consumer();

    seastar::future<> start(seastar::abort_source& as);
    seastar::future<> stop();

    // One fetch round. Returns the records it got, which may be empty when the
    // broker's max_wait elapsed with nothing new -- that is a normal poll, not
    // an end of stream.
    seastar::future<std::vector<fetched_record>> poll(seastar::abort_source& as);

    // Acknowledge a record previously returned by poll(). Committing happens
    // separately, in commit_acked().
    void ack(const std::string& topic, int32_t partition, int64_t offset);

    // Commit every partition's watermark. Called on a timer and at shutdown.
    seastar::future<> commit_acked();

    const partition_map& assignment() const noexcept { return _assignment; }
    // The group generation this consumer last acted on. Exposed for the chaos
    // harness, which has to prove a rebalance actually happened: a run that
    // survived a broker restart without the generation changing did not test
    // rebalance, it tested that nothing reached the consumer.
    int32_t generation() const noexcept { return _generation; }

private:
    seastar::future<> rejoin();
    seastar::future<> resolve_start_offsets();
    void              check_generation();
    void              ensure_trackers();

    consumer_config _cfg;
    std::unique_ptr<cluster>        _cluster;
    // No consumer_group here: shard 0 owns the membership on behalf of the
    // whole process (swordfish/kafka/group_membership.hh).
    int32_t                         _generation = -1;
    partition_map                   _assignment;
    // topic -> partition -> next offset to fetch.
    std::map<std::string, std::map<int32_t, int64_t>> _next;
    std::map<std::string, std::map<int32_t, offset_tracker>> _tracked;
    bool _started = false;
    bool _needs_rejoin = false;
    // A metadata refresh that failed, to be retried on the next poll rather than
    // thrown out of the one that decoded records.
    bool _needs_refresh = false;
};

} // namespace sf::kafka
