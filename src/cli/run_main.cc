// swordfish run — the first time a pipeline actually moves data.
//
// Reads a Redpanda Connect–shaped YAML config, builds a stream per shard, and
// runs it on Seastar. `sharded<>` gives each shard an independent
// input -> pipeline -> output, so nothing crosses cores.
#include "swordfish/config/yaml.hh"
#include "swordfish/config/spec.hh"
#include "swordfish/config/template.hh"
#include "swordfish/blobl/parse.hh"
#include "swordfish/runtime/observe.hh"
#include "swordfish/runtime/stream.hh"
#include "swordfish/runtime/stream_service.hh"
#include "swordfish/runtime/components.hh"
#include "swordfish/runtime/build_stream.hh"

#include <seastar/core/timer.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/signal.hh>
#include <seastar/core/smp.hh>
#include <seastar/util/log.hh>

#include <fstream>
#include <sstream>
#include "swordfish/kafka/components.hh"

#include "swordfish/cli/commands.hh"

using namespace sf;
using namespace sf::cfg;

static seastar::logger rlog("sf");

int sf::cli::run_main(int argc, char** argv) {
    // Connectors in their own libraries register explicitly: a static
    // library's registrations are dropped by the linker otherwise.
    sf::kafka::register_components();
    seastar::app_template::config ac;
    ac.name = "swordfish";
    seastar::app_template app(std::move(ac));
    // Registered as a POSITIONAL option, which boost also gives the `--config`
    // spelling -- so `swordfish run config.yaml` works, which is the shape
    // redpanda-connect takes and therefore the one a user's fingers already
    // know. Registering it in both places instead makes `--config` ambiguous;
    // this is the single registration that serves both forms, and it is the
    // same trick test_main.cc uses for `--targets`.
    app.add_options()("templates,t",
                      boost::program_options::value<std::vector<std::string>>()
                          ->composing(),
                      "import config templates; accepts a file, a directory or a "
                      "glob (quote a glob so the shell does not expand it)");
    app.add_positional_options({{"config",
                                 boost::program_options::value<std::string>(),
                                 "pipeline configuration file", 1}});

    return app.run(argc, argv, [&app, argv] () -> seastar::future<int> {
        auto& args = app.configuration();
        if (!args.count("config")) {
            std::cerr << "usage: " << argv[0] << " <config.yaml>\n";
            co_return 1;
        }
        const auto path = args["config"].as<std::string>();

        // Loaded BEFORE the config is parsed: a template usage is expanded at
        // the point its kind is looked up, so the registry has to be populated
        // by then.
        if (args.count("templates")) {
            try {
                for (const auto& t : args["templates"].as<std::vector<std::string>>())
                    sf::tmpl::load_templates(t);
            } catch (const std::exception& e) {
                std::cerr << "templates: " << e.what() << "\n";
                co_return 1;
            }
        }

        stream_spec spec;
        try {
            spec = build_stream_spec(sf::cfg::load_config(path));
        } catch (const std::exception& e) {
            std::cerr << path << ": " << e.what() << "\n";
            co_return 1;
        }

        seastar::sharded<stream_service> streams;
        co_await streams.start(spec);
        // sharded<> asserts if it is destroyed without stop(). A failing
        // input or output connect would otherwise abort the process with
        // "terminate called without an active exception" instead of reporting
        // the error, so every path below must reach streams.stop().
        std::exception_ptr err;
        std::unique_ptr<sf::observe_server> obs;
        stream::stats total{};
        // Declared OUTSIDE the try: the teardown after the catch has to disarm
        // them whichever way the block exits.
        auto live  = seastar::make_lw_shared<bool>(true);
        auto force = seastar::make_lw_shared<seastar::timer<>>();
        try {
        // SIGINT/SIGTERM drain rather than kill: in-flight messages finish and
        // are acked, which is what at-least-once delivery requires on shutdown.
        //
        // BOUNDED by `shutdown_timeout`, which the signal path never applied.
        // The handler only requested a drain and the code below then waited on
        // wait_until_drained() with nothing behind it, so a pipeline blocked in a
        // processor or a stuck output stayed alive for ever -- the reference
        // forces the same pipeline down after its timeout. The timer escalates to
        // stream::stop(), which is where the graceful/forceful ratchet already
        // lives and which is idempotent, so the streams.stop() below is unharmed.
        //
        // Nothing here captures the coroutine frame BY REFERENCE. Seastar never
        // unregisters a signal handler, so one delivered after this frame is
        // gone would dereference freed memory -- and it did: a SIGTERM that
        // reached the forcing path segfaulted on shard 0 with a jump to
        // 0x5d00000001. `live` is a shared flag cleared before the streams go,
        // and every callback checks it first.
        auto* svc  = &streams;                  // valid for exactly as long as *live
        const auto budget = spec.config.shutdown_timeout;
        force->set_callback([svc, live, budget] {
            if (!*live) return;
            rlog.warn("shutdown did not complete within {}ms of the signal; forcing",
                      budget.count());
            (void)svc->invoke_on_all(&stream_service::force_stop);
        });
        for (int sig : {SIGINT, SIGTERM})
            seastar::handle_signal(sig, [svc, live, force, sig, budget] {
                if (!*live) return;
                rlog.info("signal {} received, draining", sig);
                (void)svc->invoke_on_all([](auto& s) {
                    s.drain();
                    return seastar::make_ready_future<>();
                });
                if (!force->armed()) force->arm(budget);
            }, true);

        co_await streams.invoke_on_all(&stream_service::start);

        // Started AFTER the streams, so /ready never answers true for a
        // pipeline that has not begun; stopped before them, so a scrape cannot
        // arrive mid-teardown and read half-dismantled counters.
        obs = std::make_unique<sf::observe_server>(spec.http, [&streams]() {
            return sf::gather_from(streams);
        });
        co_await obs->start();

        // Let every shard run to natural completion before stopping, or a finite
        // source would be truncated at whatever the queues happened to hold.
        co_await streams.invoke_on_all(&stream_service::wait);

        // map_reduce0, NOT invoke_on_all with a captured accumulator. The
        // lambda is copied to every shard but `total` lives in shard 0's frame,
        // and stream::stats::operator+= is a plain run of non-atomic
        // read-modify-writes on ten counters -- so shards raced and whole
        // shards' totals were lost. Measured on a `generate` count:20000 into
        // `drop`: at --smp 16, 3 runs in 30 reported in=18750, exactly one
        // shard's 1250 missing, with every counter dropping together; at --smp 8,
        // 4 in 25 reported 17500. --smp 1 was always right, which is why it
        // survived. map_reduce0 runs the reduction only on the calling shard --
        // the shape gather_from() in observe.hh already uses, which is why
        // /metrics was right while this line was not.
        total = co_await streams.map_reduce0(
            [](const stream_service& s) { return s.stats(); },
            stream::stats{},
            [](stream::stats a, const stream::stats& b) { a += b; return a; });
        } catch (...) {
            err = std::current_exception();
        }
        // From here the streams are being torn down, so the signal handlers and
        // the forcing timer must not touch them again.
        *live = false;
        force->cancel();
        // Guarded and unconditional: a failing stop here must not skip the
        // stream teardown behind it.
        // `shutdown_delay`: "a period of time to wait for metrics and traces to
        // be pulled or pushed from the process". So it runs after the stream has
        // finished and BEFORE the observability server is stopped -- stopping it
        // first would close the endpoint the delay exists to keep open.
        //
        // Applied on the error path too, because that is what the reference
        // does and because a failure is exactly when a last scrape is worth
        // having. It is a plain sleep: a signal does not cut it short.
        if (spec.config.shutdown_delay.count() > 0) {
            rlog.info("shutdown_delay: holding for {}ms before closing",
                      spec.config.shutdown_delay.count());
            co_await seastar::sleep(spec.config.shutdown_delay);
        }
        if (obs) {
            try { co_await obs->stop(); }
            catch (const std::exception& e) { rlog.warn("http server stop failed: {}", e.what()); }
        }
        co_await streams.stop();     // invokes stream_service::stop() per shard

        if (err) {
            try { std::rethrow_exception(err); }
            catch (const std::exception& e) { std::cerr << path << ": " << e.what() << "\n"; }
            co_return 1;
        }
        rlog.info("in={} out={} acks={} nacks={} filtered={} proc_errors={}",
                  total.messages_in, total.messages_out, total.acks,
                  total.nacks, total.filtered, total.proc_errors);
        co_return 0;
    });
}
