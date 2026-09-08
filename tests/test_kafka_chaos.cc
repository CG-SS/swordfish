// sfkafkachaos — a consuming group member that records what it received, so a
// harness can kill brokers underneath it and then check nothing was lost.
//
// This is deliberately NOT a self-contained test: the assertion lives in
// tools/run_kafka_chaos.sh, because the interesting question spans several
// processes and a cluster ("did the union of what these members saw cover
// everything that was produced, across a coordinator failover?"). What this
// binary owns is the consuming, the acking, and an honest record of what
// happened -- including whether a rebalance occurred at all.
//
// That last part matters more than it looks. A chaos test that passes because
// the chaos never reached the consumer is a green result that tested nothing,
// which is the failure mode this project has already been bitten by. So the
// binary reports the set of GENERATIONS it saw, and the harness fails the run
// if the group never rebalanced.
#include "swordfish/kafka/consumer.hh"

#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/util/log.hh>

#include <chrono>
#include <algorithm>
#include <filesystem>
#include <fstream>
#include <optional>
#include <set>
#include <string>
#include <vector>

using namespace std::chrono_literals;
static seastar::logger clog_("chaos");

namespace {

using clock_type = std::chrono::steady_clock;

struct run_options {
    std::vector<std::string> brokers;
    std::string              topic;
    std::string              group;
    std::string              out_prefix;
    std::chrono::milliseconds session_timeout{10000};
    std::chrono::seconds     run_for{120};
    std::chrono::seconds     idle_for{15};
    // While this path does NOT exist the producer is still running and the
    // member must not stop, however quiet its partitions are. Idling out during
    // the run is not a harmless early exit: the shard stops polling, so it
    // never sees the next generation, and the partitions it is given by a later
    // rebalance go unconsumed. That reads as consumer data loss and is nothing
    // of the kind. It bit this test twice -- once as an idle timeout shorter
    // than a broker restart, and once on a shard whose partitions simply had no
    // data yet, whose progress clock therefore never started at all.
    std::string              drain_file;
};

// What one shard saw. Gathered to shard 0 at the end.
struct shard_report {
    uint64_t          records = 0;
    std::set<int32_t> generations;
    uint64_t          polls = 0;
    uint64_t          errors = 0;
    std::string       last_error;
};

class chaos_consumer {
public:
    chaos_consumer(run_options opts) : _opts(std::move(opts)) {
        // Per shard, so two shards never interleave into one file, and appended
        // as records arrive rather than written at the end: this process is
        // expected to be killed mid-run, and a record of what it consumed
        // before dying is exactly what the harness needs.
        _out.open(_opts.out_prefix + "." + std::to_string(seastar::this_shard_id()),
                  std::ios::binary | std::ios::trunc);
    }

    seastar::future<> run() {
        sf::kafka::consumer_config cfg;
        cfg.seed_brokers = _opts.brokers;
        cfg.topics       = {_opts.topic};
        cfg.consumer_group = _opts.group;
        cfg.client_id    = "sfkafkachaos";
        cfg.start_from_oldest = true;
        // Short, so an outage produces several commit attempts rather than one:
        // committing only at shutdown would hide a coordinator failover.
        cfg.commit_interval = 1000ms;
        cfg.fetch_max_wait  = 300ms;
        // Short, so killing a member produces a rebalance within the run rather
        // than after it: the 45s default would outlast the whole test.
        cfg.session_timeout = _opts.session_timeout;

        sf::kafka::consumer c(std::move(cfg));
        const auto deadline = clock_type::now() + _opts.run_for;
        auto last_progress = clock_type::now();

        // A failure to start is itself survivable: the harness may bring the
        // brokers up and down around us, and the first attempt can land in a
        // window where no coordinator is available.
        while (clock_type::now() < deadline) {
            // The retry sleep is OUTSIDE the handler: co_await inside a catch
            // block is not permitted by the language.
            bool failed = false;
            try {
                co_await c.start(_as);
            } catch (const std::exception& e) {
                note_error(e.what());
                failed = true;
            }
            if (!failed) break;
            co_await seastar::sleep(500ms);
        }

        // Set when the producer has finished; the idle countdown starts here,
        // so a quiet member always gets a full idle_for of silence AFTER the
        // last message was produced before it decides the stream is drained.
        std::optional<clock_type::time_point> drain_began;

        while (clock_type::now() < deadline) {
            if (!drain_began && !_opts.drain_file.empty() &&
                std::filesystem::exists(_opts.drain_file))
                drain_began = clock_type::now();

            std::vector<sf::kafka::fetched_record> recs;
            bool failed = false;
            try {
                recs = co_await c.poll(_as);
            } catch (const std::exception& e) {
                // Expected while a broker is down. Backing off and retrying is
                // the behaviour under test, so this is counted, not fatal.
                note_error(e.what());
                failed = true;
            }
            if (failed) {
                co_await seastar::sleep(500ms);
                continue;
            }

            ++_report.polls;
            _report.generations.insert(c.generation());
            if (recs.empty()) {
                if (drain_began &&
                    clock_type::now() - std::max(last_progress, *drain_began)
                        > _opts.idle_for)
                    break;
                continue;
            }
            last_progress = clock_type::now();
            for (auto& r : recs) {
                _out << r.value << "\n";
                ++_report.records;
                c.ack(r.topic, r.partition, r.offset);
            }
            // Flushed per batch: an unflushed buffer is indistinguishable from
            // a message never received once the process is killed.
            _out.flush();
        }

        // Commit and stop are best-effort: a broker may be down right now, and
        // failing here would report a loss the consumer did not cause.
        try { co_await c.commit_acked(); }
        catch (const std::exception& e) { note_error(e.what()); }
        try { co_await c.stop(); }
        catch (const std::exception& e) { note_error(e.what()); }
    }

    shard_report take() { return std::move(_report); }

private:
    void note_error(const std::string& what) {
        ++_report.errors;
        _report.last_error = what;
    }

    run_options              _opts;
    seastar::abort_source    _as;
    shard_report             _report;
    std::ofstream            _out;
};

} // namespace

int main(int argc, char** argv) {
    namespace bpo = boost::program_options;
    seastar::app_template::config ac;
    ac.name = "sfkafkachaos";
    seastar::app_template app(std::move(ac));
    app.add_options()
        ("brokers", bpo::value<std::string>()->default_value("localhost:9092"),
         "comma-separated bootstrap brokers")
        ("topic",   bpo::value<std::string>()->default_value("sf-chaos"), "topic to consume")
        ("group",   bpo::value<std::string>()->default_value("sf-chaos-group"), "consumer group")
        ("out",     bpo::value<std::string>()->default_value("chaos-out"),
         "prefix for the per-shard files every received value is appended to")
        ("session-timeout-ms", bpo::value<int>()->default_value(10000),
         "group session timeout; how fast a killed member is noticed")
        ("run-seconds",  bpo::value<int>()->default_value(120), "hard deadline")
        ("idle-seconds", bpo::value<int>()->default_value(15),
         "stop once this long passes with no new records AND the producer is done")
        ("drain-file", bpo::value<std::string>()->default_value(""),
         "path the harness creates when the producer has finished; until it "
         "exists this member never stops for idleness");

    return app.run(argc, argv, [&app]() -> seastar::future<int> {
        auto& args = app.configuration();
        run_options opts;
        {
            const std::string list = args["brokers"].as<std::string>();
            size_t i = 0;
            while (i <= list.size()) {
                const size_t c = list.find(',', i);
                opts.brokers.push_back(list.substr(i, c == std::string::npos
                                                      ? std::string::npos : c - i));
                if (c == std::string::npos) break;
                i = c + 1;
            }
        }
        opts.topic    = args["topic"].as<std::string>();
        opts.group    = args["group"].as<std::string>();
        opts.run_for  = std::chrono::seconds(args["run-seconds"].as<int>());
        opts.idle_for = std::chrono::seconds(args["idle-seconds"].as<int>());
        opts.out_prefix = args["out"].as<std::string>();
        opts.drain_file = args["drain-file"].as<std::string>();
        opts.session_timeout =
            std::chrono::milliseconds(args["session-timeout-ms"].as<int>());

        // One consumer per shard, which is how a Swordfish process consumes:
        // shard 0 owns the group membership and the assignment is divided by
        // `partition % smp::count`. Running this at --smp > 1 is the point --
        // a single-shard chaos run would not exercise the path where a
        // rebalance has to reach every shard.
        seastar::sharded<chaos_consumer> consumers;
        co_await consumers.start(opts);

        std::exception_ptr err;
        try {
            co_await consumers.invoke_on_all(&chaos_consumer::run);
        } catch (...) {
            err = std::current_exception();
        }

        uint64_t records = 0, polls = 0, errors = 0;
        std::set<int32_t> generations;
        std::string last_error;
        if (!err) {
            for (unsigned s = 0; s < seastar::this_smp().shard_count(); ++s) {
                auto r = co_await consumers.invoke_on(s, [](chaos_consumer& c) {
                    return c.take();
                });
                records += r.records;
                generations.insert(r.generations.begin(), r.generations.end());
                polls += r.polls;
                errors += r.errors;
                if (!r.last_error.empty()) last_error = r.last_error;
            }
        }
        co_await consumers.stop();

        if (err) {
            try { std::rethrow_exception(err); }
            catch (const std::exception& e) { clog_.error("failed: {}", e.what()); }
            co_return 1;
        }

        // The harness parses this line. `generations` is what proves the chaos
        // was actually felt: one generation means the group never rebalanced,
        // and a run that never rebalanced has not tested rebalance.
        std::string gens;
        for (int32_t g : generations) gens += (gens.empty() ? "" : ",") + std::to_string(g);
        clog_.info("RESULT records={} polls={} errors={} generations=[{}] last_error={}",
                   records, polls, errors, gens,
                   last_error.empty() ? "none" : last_error);
        co_return 0;
    });
}
