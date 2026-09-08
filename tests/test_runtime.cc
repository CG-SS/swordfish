// Runtime tests, on Seastar's own test framework.
//
// Catch2 is not used here: these cases run inside the reactor and are
// coroutines, which is exactly what SEASTAR_TEST_CASE exists for. Catch2 owns
// main() and its session model would be fighting the reactor.
//
// The ack-accounting property test is the important one:
// every source message must be acked or nacked EXACTLY once, under filtering,
// splitting, and failure. That is the bug class this kind of engine dies of.
#include "swordfish/config/yaml.hh"
#include "swordfish/runtime/build_stream.hh"
#include "swordfish/runtime/pipeline_spec.hh"
#include "swordfish/runtime/stream.hh"
#include "swordfish/runtime/components.hh"
#include "swordfish/blobl/parse.hh"
#include "swordfish/runtime/rendezvous.hh"
#include "swordfish/runtime/batching.hh"
#include "swordfish/runtime/scanner.hh"
#include "swordfish/runtime/transform.hh"

#include <seastar/core/coroutine.hh>
#include <seastar/core/loop.hh>
#include <seastar/util/later.hh>
#include <seastar/core/when_all.hh>
// Supplies main() from seastar/testing, which sets up the reactor and runs the
// registered cases.
#define SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/log.hh>

#include <algorithm>
#include <atomic>
#include <boost/iterator/counting_iterator.hpp>
#include <iostream>
#include <random>
#include <seastar/core/sleep.hh>

using namespace sf;

// Mirrors the helper the suite was written against; BOOST_CHECK carries the
// file/line, and the label rides along as the message.
#define check(cond, label) BOOST_CHECK_MESSAGE((cond), (label))

// An input that records exactly how many times each batch was resolved.
class counting_input final : public input {
public:
    counting_input(size_t n, std::vector<int>& acks, std::vector<int>& nacks)
        : _remaining(n), _acks(acks), _nacks(nacks) {}

    seastar::future<std::optional<std::pair<batch, ack_fn>>>
    read_batch(seastar::abort_source&) override {
        if (_remaining == 0) return seastar::make_ready_future<
            std::optional<std::pair<batch, ack_fn>>>(std::nullopt);
        const size_t id = _next++;
        --_remaining;
        value v = value::object();
        v.set("id", value(static_cast<int64_t>(id)));
        batch b;
        b.push_back(message::from_value(std::move(v)));
        auto* acks = &_acks; auto* nacks = &_nacks;
        ack_fn fn = [id, acks, nacks](std::exception_ptr e) {
            (e ? (*nacks)[id] : (*acks)[id]) += 1;
            return seastar::make_ready_future<>();
        };
        return seastar::make_ready_future<std::optional<std::pair<batch, ack_fn>>>(
            std::make_pair(std::move(b), std::move(fn)));
    }
private:
    size_t _remaining, _next = 0;
    std::vector<int>& _acks;
    std::vector<int>& _nacks;
};

// A processor that filters, splits, or throws, chosen per message.
class chaos_processor final : public processor {
public:
    explicit chaos_processor(unsigned seed) : _rng(seed) {}
    seastar::future<std::vector<batch>> process(batch b, seastar::abort_source&) override {
        std::uniform_int_distribution<int> d(0, 3);
        std::vector<batch> out;
        switch (d(_rng)) {
        case 0: break;                                   // filter everything
        case 1: out.push_back(std::move(b)); break;       // pass through
        case 2: {                                         // split into two
            batch a, c;
            for (auto& m : b) { a.push_back(m.shallow_copy()); c.push_back(std::move(m)); }
            out.push_back(std::move(a)); out.push_back(std::move(c));
            break;
        }
        default: throw std::runtime_error("induced processor failure");
        }
        return seastar::make_ready_future<std::vector<batch>>(std::move(out));
    }
    std::string name() const override { return "chaos"; }
private:
    std::mt19937 _rng;
};

// An output that fails a deterministic fraction of writes.
class flaky_output final : public output {
public:
    explicit flaky_output(unsigned seed) : _rng(seed) {}
    seastar::future<> write_batch(batch, seastar::abort_source&) override {
        if (std::uniform_int_distribution<int>(0, 3)(_rng) == 0)
            return seastar::make_exception_future<>(std::runtime_error("induced write failure"));
        return seastar::make_ready_future<>();
    }
    size_t max_in_flight() const override { return 4; }
private:
    std::mt19937 _rng;
};

// Runs a processor chain over one batch and returns the resulting payloads.
// Was a lambda inside the old hand-rolled main(); now a free coroutine so each
// test case can use it independently.
static seastar::future<std::vector<std::string>> run_procs(
        std::vector<processor_ptr> procs, std::vector<std::string> inputs) {
    batch b;
    for (auto& in : inputs) b.push_back(message(in));
    seastar::abort_source as;
    std::vector<batch> batches;
    batches.push_back(std::move(b));
    for (auto& p : procs) {
        std::vector<batch> next;
        for (auto& cur : batches) {
            if (cur.empty()) continue;
            auto out = co_await p->process(std::move(cur), as);
            for (auto& ob : out) if (!ob.empty()) next.push_back(std::move(ob));
        }
        batches = std::move(next);
    }
    std::vector<std::string> res;
    for (auto& bb : batches) for (auto& m : bb) res.push_back(m.as_bytes());
    co_return res;
}

seastar::future<> run_ack_property(unsigned seed, size_t n) {
    std::vector<int> acks(n, 0), nacks(n, 0);
    stream_spec spec;
    spec.make_input = [n, &acks, &nacks] {
        return input_ptr(new counting_input(n, acks, nacks));
    };
    spec.make_processors = [seed] {
        std::vector<processor_ptr> v;
        v.push_back(processor_ptr(new chaos_processor(seed)));
        v.push_back(processor_ptr(new chaos_processor(seed + 1)));
        return v;
    };
    spec.make_output = [seed] { return output_ptr(new flaky_output(seed + 2)); };
    spec.config.queue_depth = 4;

    stream s(spec);
    co_await s.start();
    co_await s.wait_until_drained();
    co_await s.stop();

    size_t resolved_once = 0, never = 0, twice = 0;
    for (size_t i = 0; i < n; ++i) {
        const int total = acks[i] + nacks[i];
        if (total == 1) ++resolved_once;
        else if (total == 0) ++never;
        else ++twice;
    }
    check(never == 0,  "seed " + std::to_string(seed) + ": no message left unresolved (" +
                       std::to_string(never) + " were)");
    check(twice == 0,  "seed " + std::to_string(seed) + ": no message resolved twice (" +
                       std::to_string(twice) + " were)");
    check(resolved_once == n, "seed " + std::to_string(seed) + ": all " +
                              std::to_string(n) + " resolved exactly once");
}


SEASTAR_TEST_CASE(acks_a_clean_pipeline_acks_every_message_once) {

    // A plain pipeline first: every message should be acked, none nacked.
    {
        std::vector<int> acks(50, 0), nacks(50, 0);
        stream_spec spec;
        spec.make_input = [&] { return input_ptr(new counting_input(50, acks, nacks)); };
        spec.make_processors = [] { return std::vector<processor_ptr>{}; };
        spec.make_output = [] { return make_drop_output(); };
        stream s(spec);
        co_await s.start();
        co_await s.wait_until_drained();
        co_await s.stop();
        size_t a = 0, na = 0;
        for (size_t i = 0; i < 50; ++i) { a += acks[i]; na += nacks[i]; }
        check(a == 50 && na == 0, "clean pipeline acks every message exactly once");
        check(s.get_stats().messages_out == 50, "all messages reached the output");
    }

    co_return;
}

SEASTAR_TEST_CASE(scanners_framing_and_chomping) {
    // ---- scanners ----
    {
        auto sc = make_lines_scanner();
        // Chunk boundaries must not split a line: framing state carries over.
        auto a = sc->feed("one\ntw");
        auto b = sc->feed("o\nthree");
        auto c = sc->finish();
        check(a.size() == 1 && a[0].as_bytes() == "one", "line completed within a chunk");
        check(b.size() == 1 && b[0].as_bytes() == "two", "line split across chunks");
        check(c.size() == 1 && c[0].as_bytes() == "three",
              "trailing line without a newline is still a message");
    }
    {
        auto sc = make_lines_scanner();
        auto a = sc->feed("a\r\nb\r\n");
        check(a.size() == 2 && a[0].as_bytes() == "a" && a[1].as_bytes() == "b",
              "CRLF line endings are tolerated");
        check(sc->finish().empty(), "a trailing newline yields no empty final message");
    }
    {
        auto sc = make_to_the_end_scanner();
        sc->feed("abc"); sc->feed("def");
        auto out = sc->finish();
        check(out.size() == 1 && out[0].as_bytes() == "abcdef",
              "to_the_end yields the whole stream as one message");
    }
    co_return;
}

// The `avro` scanner frames an OCF container incrementally, and the only way to
// prove that is to starve it: feed the same file one byte at a time and require
// the same messages a whole-file feed produces. A gate driven through a config
// cannot reach this, because the `file` input hands over comfortable chunks and
// a scanner that only worked on whole files would pass.
SEASTAR_TEST_CASE(scanners_avro_frames_across_any_chunk_boundary) {
    // Avro's `long`: zigzag, then a base-128 varint.
    const auto avro_long = [](int64_t v) {
        uint64_t u = (static_cast<uint64_t>(v) << 1) ^ static_cast<uint64_t>(v >> 63);
        std::string out;
        do {
            const auto b = static_cast<uint8_t>(u & 0x7f);
            u >>= 7;
            out += static_cast<char>(u ? (b | 0x80) : b);
        } while (u);
        return out;
    };
    const auto avro_blob = [&](std::string_view v) {
        return avro_long(static_cast<int64_t>(v.size())) + std::string(v);
    };

    const std::string schema =
        R"({"type":"record","name":"R","fields":[{"name":"n","type":"long"}]})";
    std::string sync;
    for (int i = 0; i < 16; ++i) sync += static_cast<char>(i);
    // A varint that needs two bytes, and a negative one, so a chunk can split a
    // datum in the middle of a number rather than only between records.
    std::string body;
    for (const int64_t n : {int64_t{0}, int64_t{1}, int64_t{-1}, int64_t{300}})
        body += avro_long(n);
    const std::string ocf =
        std::string("Obj\x01", 4) +
        avro_long(2) +
        avro_blob("avro.schema") + avro_blob(schema) +
        avro_blob("avro.codec") + avro_blob("null") +
        avro_long(0) + sync +
        avro_long(4) + avro_blob(body) + sync;

    const auto& kinds = scanner_kinds();
    if (std::find(kinds.begin(), kinds.end(), "avro") == kinds.end()) {
        // Not a silent skip: a build without avro-cpp has to SAY the case did
        // not run, or the suite reports green for a scanner it never exercised.
        std::cerr << "NOT RUN: avro is not built into this binary, so its "
                     "framing was not tested\n";
        co_return;
    }

    const auto read = [&](size_t chunk) {
        scanner_spec sp;
        sp.kind = "avro";
        auto sc = make_scanner(sp);
        std::vector<message> out;
        for (size_t i = 0; i < ocf.size(); i += chunk)
            for (auto& m : sc->feed(std::string_view(ocf).substr(i, chunk)))
                out.push_back(std::move(m));
        for (auto& m : sc->finish()) out.push_back(std::move(m));
        return out;
    };

    const auto whole = read(ocf.size());
    check(whole.size() == 4, "four datums from one whole-file feed");
    check(whole[0].as_bytes() == R"({"n":0})" && whole[3].as_bytes() == R"({"n":300})",
          "the datums decode to Avro JSON");

    // Every chunk size from one byte up: each one puts a different boundary
    // through the header, the block header, the datums and the sync marker.
    for (size_t chunk = 1; chunk <= ocf.size(); ++chunk) {
        const auto got = read(chunk);
        bool same = got.size() == whole.size();
        for (size_t i = 0; same && i < got.size(); ++i)
            same = got[i].as_bytes() == whole[i].as_bytes();
        if (!same) {
            check(false, "avro framing survives a chunk size of " + std::to_string(chunk));
            break;
        }
    }
    check(true, "avro framing is identical at every chunk size from 1 byte up");

    // A stream with no header is refused rather than read as zero messages: an
    // OCF file cannot be empty, and the reference reports "reading magic: EOF".
    {
        scanner_spec sp;
        sp.kind = "avro";
        auto sc = make_scanner(sp);
        bool threw = false;
        try { (void)sc->finish(); } catch (const std::exception&) { threw = true; }
        check(threw, "an empty avro stream is an error, not zero messages");
    }
    co_return;
}

SEASTAR_TEST_CASE(processors_catch_recovers_failed_messages) {
    {   // catch runs only on errored messages, and clears the error
        std::vector<processor_ptr> ps;
        ps.push_back(make_mapping_processor(interpreted_transform(blobl::parse_mapping("root = this\nroot.n = this.n * 2"))));
        std::vector<processor_ptr> recovery;
        recovery.push_back(make_mapping_processor(interpreted_transform(blobl::parse_mapping(R"(root.recovered = true)"))));
        ps.push_back(make_catch_processor(std::move(recovery)));
        auto out = co_await run_procs(std::move(ps), {R"({"n":3})", "not json"});
        bool doubled = false, recovered = false;
        for (auto& o : out) {
            if (o.find("\"n\":6") != std::string::npos) doubled = true;
            if (o.find("recovered") != std::string::npos) recovered = true;
        }
        check(doubled, "catch leaves healthy messages alone");
        check(recovered, "catch recovers a message that failed upstream");
    }
    co_return;
}

SEASTAR_TEST_CASE(processors_this_resolves_lazily) {
    {   // a mapping that never touches `this` works on unparseable input
        std::vector<processor_ptr> ps;
        ps.push_back(make_mapping_processor(interpreted_transform(blobl::parse_mapping(R"(root.tag = "x")"))));
        auto out = co_await run_procs(std::move(ps), {"not json at all"});
        check(out.size() == 1 && out[0].find("\"tag\":\"x\"") != std::string::npos,
              "`this` resolves lazily, so a mapping ignoring it survives bad input");
    }
    co_return;
}

SEASTAR_TEST_CASE(processors_switch_routing_and_passthrough) {
    {   // switch routes by check, and unmatched messages pass through
        std::vector<std::pair<transform_fn, std::vector<processor_ptr>>> cases;
        std::vector<processor_ptr> big;
        big.push_back(make_mapping_processor(interpreted_transform(blobl::parse_mapping("root = this\nroot.size = \"big\""))));
        cases.emplace_back(interpreted_transform(blobl::parse_query("this.n > 10")), std::move(big));
        std::vector<processor_ptr> ps;
        ps.push_back(make_switch_processor(std::move(cases)));
        auto out = co_await run_procs(std::move(ps), {R"({"n":50})", R"({"n":1})"});
        bool tagged = false, passthrough = false;
        for (auto& o : out) {
            if (o.find("\"size\":\"big\"") != std::string::npos) tagged = true;
            if (o == R"({"n":1})") passthrough = true;
        }
        check(tagged, "switch applies the matching case");
        check(passthrough, "switch passes unmatched messages through untouched");
    }
    co_return;
}

SEASTAR_TEST_CASE(processors_for_each_recombines) {
    {   // for_each recombines its results into one batch
        std::vector<processor_ptr> inner;
        inner.push_back(make_mapping_processor(interpreted_transform(blobl::parse_mapping("root = this\nroot.each = true"))));
        std::vector<processor_ptr> ps;
        ps.push_back(make_for_each_processor(std::move(inner)));
        auto out = co_await run_procs(std::move(ps), {R"({"a":1})", R"({"a":2})"});
        check(out.size() == 2, "for_each recombines into a single batch");
    }
    co_return;
}

SEASTAR_TEST_CASE(processors_noop_is_identity) {
    {   // noop is a true identity
        std::vector<processor_ptr> ps;
        ps.push_back(make_noop_processor());
        auto out = co_await run_procs(std::move(ps), {R"({"a":1})"});
        check(out.size() == 1 && out[0] == R"({"a":1})", "noop passes messages unchanged");
    }
    co_return;
}

SEASTAR_TEST_CASE(message_shallow_copy_shares_its_payload) {
    {   // regression: shallow_copy must not duplicate the payload
        sf::message m(std::string(4096, 'x'));
        (void)m.as_bytes();
        auto c = m.shallow_copy();
        // cppcheck-suppress mismatchingContainers
        //   Comparing the two payload ADDRESSES is the whole point of the test;
        //   cppcheck reads .data() == .data() as iterators from two containers.
        check(c.as_bytes().data() == m.as_bytes().data(),
              "shallow_copy shares the raw payload rather than duplicating it");
    }
    co_return;
}

SEASTAR_TEST_CASE(processors_metadata_round_trips) {
    {   // regression: metadata round-trips through a mapping
        std::vector<processor_ptr> ps;
        ps.push_back(make_mapping_processor(interpreted_transform(
            blobl::parse_mapping("meta kind = \"row\"\nroot = this"))));
        ps.push_back(make_mapping_processor(interpreted_transform(
            blobl::parse_mapping("root = this\nroot.kind = metadata(\"kind\")"))));
        auto out = co_await run_procs(std::move(ps), {R"({"n":1})"});
        check(out.size() == 1 && out[0].find("\"kind\":\"row\"") != std::string::npos,
              "meta written by one processor is readable by the next");
    }
    co_return;
}

SEASTAR_TEST_CASE(output_max_in_flight_permits_concurrency) {
    {   // regression: max_in_flight actually permits concurrent writes
        struct counting_output final : public output {
            int live = 0, peak = 0;
            seastar::future<> write_batch(batch, seastar::abort_source&) override {
                ++live; peak = std::max(peak, live);
                return seastar::sleep(std::chrono::milliseconds(2))
                    .then([this] { --live; });
            }
            size_t max_in_flight() const override { return 4; }
        };
        auto* out = new counting_output();
        std::vector<int> a(12, 0), na(12, 0);
        stream_spec spec;
        spec.make_input = [&] { return input_ptr(new counting_input(12, a, na)); };
        spec.make_processors = [] { return std::vector<processor_ptr>{}; };
        spec.make_output = [out] { return output_ptr(out); };
        spec.config.queue_depth = 4;
        stream s(spec);
        co_await s.start();
        co_await s.wait_until_drained();
        co_await s.stop();
        check(out->peak > 1, "max_in_flight allows more than one write at a time (peak="
                             + std::to_string(out->peak) + ")");
    }
    co_return;
}

SEASTAR_TEST_CASE(processors_variables_are_per_message) {
    {   // regression: variables must not leak between messages
        std::vector<processor_ptr> ps;
        ps.push_back(make_mapping_processor(interpreted_transform(blobl::parse_mapping(
            "root = this\nroot.saw = $prev | \"none\"\nlet prev = \"leaked\""))));
        auto out = co_await run_procs(std::move(ps), {R"({"i":1})", R"({"i":2})"});
        check(out.size() == 2, "both messages produced");
        bool leaked = false;
        for (auto& o : out) if (o.find("leaked") != std::string::npos) leaked = true;
        check(!leaked, "a variable set on one message is not visible to the next");
    }
    co_return;
}

SEASTAR_TEST_CASE(stream_a_failing_connect_propagates) {
    {   // regression: a failing connect surfaces as an error, not an abort
        struct failing_input final : public input {
            seastar::future<> connect(seastar::abort_source&) override {
                return seastar::make_exception_future<>(
                    std::runtime_error("cannot connect"));
            }
            seastar::future<std::optional<std::pair<batch, ack_fn>>>
            read_batch(seastar::abort_source&) override {
                return seastar::make_ready_future<
                    std::optional<std::pair<batch, ack_fn>>>(std::nullopt);
            }
        };
        stream_spec spec;
        spec.make_input = [] { return input_ptr(new failing_input()); };
        spec.make_processors = [] { return std::vector<processor_ptr>{}; };
        spec.make_output = [] { return make_drop_output(); };
        stream s(spec);
        bool threw = false;
        try { co_await s.start(); } catch (const std::exception&) { threw = true; }
        check(threw, "a failing connect propagates an exception from start()");
        co_await s.stop();
    }
    co_return;
}

SEASTAR_TEST_CASE(stream_draining_an_unbounded_source_loses_nothing) {
    {   // regression: an unbounded source must stop when drained, and every
        // message it produced must still be acked. This is the SIGTERM path:
        // without it a production pipeline ignores the signal entirely.
        struct endless_input final : public input {
            size_t produced = 0;
            seastar::future<std::optional<std::pair<batch, ack_fn>>>
            read_batch(seastar::abort_source& as) override {
                if (as.abort_requested())
                    return seastar::make_ready_future<
                        std::optional<std::pair<batch, ack_fn>>>(std::nullopt);
                ++produced;
                batch b;
                b.push_back(message(std::string("{}")));
                return seastar::make_ready_future<
                    std::optional<std::pair<batch, ack_fn>>>(
                        std::make_pair(std::move(b), noop_ack()));
            }
        };
        auto* in = new endless_input();
        stream_spec spec;
        spec.make_input = [in] { return input_ptr(in); };
        spec.make_processors = [] { return std::vector<processor_ptr>{}; };
        spec.make_output = [] { return make_drop_output(); };
        stream s(spec);
        co_await s.start();
        co_await seastar::sleep(std::chrono::milliseconds(20));
        s.request_drain();                       // what SIGTERM triggers
        co_await s.wait_until_drained();         // must now resolve
        co_await s.stop();
        const auto st = s.get_stats();
        check(st.messages_in > 0, "the unbounded source produced messages");
        check(st.messages_in == st.messages_out,
              "draining loses nothing: in=" + std::to_string(st.messages_in) +
              " out=" + std::to_string(st.messages_out));
        check(st.nacks == 0, "draining nacks nothing");
    }

    // Then the property test under filtering, splitting and failure.
    for (unsigned seed = 1; seed <= 8; ++seed)
        co_await run_ack_property(seed, 40);
    co_return;
}

SEASTAR_TEST_CASE(acks_hold_under_filtering_splitting_and_failure) {
    // The highest-value test in the project: every source message must be
    // resolved EXACTLY once, however the pipeline mangles it in between.
    for (unsigned seed = 1; seed <= 8; ++seed)
        co_await run_ack_property(seed, 40);
    co_return;
}

// ---- observability ----------------------------------------------------------
// The endpoint paths, the /ready JSON shape and the Prometheus metric names all
// exist to match redpanda-connect 4.107.2, so that a readiness probe or a
// dashboard written against it works here unchanged. That makes them a
// compatibility contract rather than an implementation detail, and worth
// pinning: renaming `input_received` would break a user's dashboard silently.
SEASTAR_TEST_CASE(prometheus_output_matches_the_reference_shape) {
    sf::observed o;
    o.messages_in = 7; o.messages_out = 5; o.batches_out = 2;
    o.nacks = 1; o.batches_in = 3; o.proc_errors = 4;
    const std::string text = sf::render_prometheus(o);

    for (const char* name : {"input_received", "output_sent", "output_batch_sent",
                             "output_error", "processor_batch_received",
                             "processor_error"}) {
        check(text.find(std::string("# TYPE ") + name + " counter") != std::string::npos,
              std::string("declares a TYPE line for ") + name);
        check(text.find(std::string(name) + "{label=\"\",path=\"root\"} ") != std::string::npos,
              std::string("emits ") + name + " with the reference's label set");
    }
    // The values, not just the names: a metric wired to the wrong counter is
    // worse than a missing one, because it looks like it works.
    check(text.find("input_received{label=\"\",path=\"root\"} 7") != std::string::npos,
          "input_received carries messages_in");
    check(text.find("output_sent{label=\"\",path=\"root\"} 5") != std::string::npos,
          "output_sent carries messages_out");
    check(text.find("output_error{label=\"\",path=\"root\"} 1") != std::string::npos,
          "output_error carries nacks");
    check(text.find("processor_error{label=\"\",path=\"root\"} 4") != std::string::npos,
          "processor_error carries proc_errors");

    // Every HELP has a TYPE has a sample: a scraper rejects a malformed family.
    size_t helps = 0, types = 0;
    for (size_t i = text.find("# HELP"); i != std::string::npos; i = text.find("# HELP", i + 1)) ++helps;
    for (size_t i = text.find("# TYPE"); i != std::string::npos; i = text.find("# TYPE", i + 1)) ++types;
    check(helps == types && helps > 0, "each metric family declares both HELP and TYPE");
    co_return;
}

// The http block defaults to DISABLED, unlike the reference, which serves on
// 4195 unless told otherwise. Swordfish compiles to a binary its user
// redistributes, so a listening port has to be asked for rather than inherited.
SEASTAR_TEST_CASE(http_block_is_off_unless_configured) {
    const auto no_block = sf::parse_pipeline(sf::cfg::parse_yaml(
        "input:\n  generate:\n    count: 1\n    mapping: 'root = 1'\noutput:\n  drop: {}\n"));
    check(!no_block.http.enabled, "absent http block leaves the server off");

    const auto with_block = sf::parse_pipeline(sf::cfg::parse_yaml(
        "http:\n  address: 127.0.0.1:9\n  root_path: /x\n"
        "input:\n  generate:\n    count: 1\n    mapping: 'root = 1'\noutput:\n  drop: {}\n"));
    check(with_block.http.enabled, "an http block turns it on");
    check(with_block.http.address == "127.0.0.1:9", "address is read");
    check(with_block.http.root_path == "/x", "root_path is read");

    const auto disabled = sf::parse_pipeline(sf::cfg::parse_yaml(
        "http:\n  enabled: false\n"
        "input:\n  generate:\n    count: 1\n    mapping: 'root = 1'\noutput:\n  drop: {}\n"));
    check(!disabled.http.enabled, "enabled: false wins over the block being present");
    co_return;
}

// A misconfigured listen address must be REPORTED, not quietly adjusted. The
// first version cast the port with static_cast<uint16_t>, so `:99999` bound to
// 34463 and then logged that it was serving on 99999 -- an operator probing the
// port they configured would find nothing, and the log would insist otherwise.
SEASTAR_TEST_CASE(bad_http_addresses_are_rejected_not_silently_adjusted) {
    const std::vector<std::string> bad = {
        "127.0.0.1:99999",   // wraps to 34463 if cast rather than checked
        "127.0.0.1:65536",   // one past the top
        "127.0.0.1:0",       // not a bindable port
        "127.0.0.1:-1",      // wraps to 65535 if cast
        "127.0.0.1:notaport",
        "127.0.0.1:",
        "noport",            // no colon at all
        "[::1]",             // brackets but no port
        "[::1:8080",         // no closing bracket
    };
    // `:8080` used to sit in that list, labelled "no host". It is VALID: an
    // empty host means every interface, which is how Go's net.Listen reads it
    // and what the reference accepts. Refusing it was the bug, not the check.
    //
    // Port 0 stays refused, and that IS a divergence: the reference binds an
    // ephemeral port and says nothing. An observability endpoint on a port
    // nobody can predict cannot be scraped, so it is refused by name here.
    for (const auto& a : bad) {
        sf::http_spec spec;
        spec.enabled = true;
        spec.address = a;
        sf::observe_server srv(spec, [] {
            return seastar::make_ready_future<sf::observed>(sf::observed{});
        });
        bool threw = false;
        try { co_await srv.start(); }
        catch (const std::exception&) { threw = true; }
        // Stop regardless: a half-started server must not be left listening.
        co_await srv.stop();
        check(threw, "rejects the address " + a);
    }
    // The forms that must WORK, each verified against the reference by hand.
    for (const auto& a : {std::string(":18991"), std::string("127.0.0.1:18992"),
                          std::string("[::1]:18993")}) {
        sf::http_spec spec;
        spec.enabled = true;
        spec.address = a;
        sf::observe_server srv(spec, [] {
            return seastar::make_ready_future<sf::observed>(sf::observed{});
        });
        bool threw = false;
        try { co_await srv.start(); }
        catch (const std::exception&) { threw = true; }
        co_await srv.stop();
        check(!threw, "accepts the address " + a);
    }
    co_return;
}

// `mutation` starts root at the INPUT document; `mapping` and `bloblang` start
// it empty. Treating all three as aliases silently dropped every field a
// mutation did not mention -- data loss in a common processor, and invisible
// until the Benthos unit-test corpus was run against it.
SEASTAR_TEST_CASE(mutation_keeps_fields_the_mapping_does_not_mention) {
    const auto run_one = [](const char* kind) -> seastar::future<std::string> {
        const auto cfg = sf::cfg::parse_yaml(std::string(kind) + ": 'root.n = this.n + 1'");
        auto procs = sf::build_processors({sf::parse_processor(cfg)});
        sf::batch b;
        b.push_back(sf::message(std::string(R"({"keep":"me","n":1})")));
        seastar::abort_source as;
        auto out = co_await procs[0]->process(std::move(b), as);
        co_return out.empty() || out[0].empty() ? std::string("<none>")
                                                : out[0][0].as_bytes();
    };
    check(co_await run_one("mapping")  == R"({"n":2})",
          "mapping starts root empty");
    check(co_await run_one("bloblang") == R"({"n":2})",
          "bloblang is an alias of mapping");
    check(co_await run_one("mutation") == R"({"keep":"me","n":2})",
          "mutation starts root at the input, keeping unmentioned fields");
    co_return;
}

// `from_all()` evaluates its target once per message of the BATCH. A single
// generated message per batch is not a real exercise of it -- the sum equals
// the value -- so this feeds a genuine multi-message batch.
SEASTAR_TEST_CASE(from_all_reads_every_message_in_the_batch) {
    // `from_all()` moves the batch INDEX and nothing else, so which functions
    // follow it is the whole point: the reference's own documentation says
    // "functions that support this behavior are `content`, `json` and `meta`",
    // and its implementation is literally `ctx.Index = f.index`
    // (benthos internal/bloblang/query/methods.go).
    //
    // So `json("n").from_all()` spans the batch and `this.n.from_all()` does
    // NOT -- it repeats the current message's value once per message. This test
    // used to assert that `this.n.from_all().sum()` was 42, which pinned the
    // interpreter's rebinding of `this`: behaviour the compiled backend and the
    // reference both disagreed with.
    const auto cfg = sf::cfg::parse_yaml(
        "mapping: |\n"
        "  root.jtotal = json(\"n\").from_all().sum()\n"
        "  root.ttotal = this.n.from_all().sum()\n"
        "  root.count = this.n.from_all().length()\n"
        "  root.n = this.n\n");
    auto procs = sf::build_processors({sf::parse_processor(cfg)});

    sf::batch b;
    for (int n : {1, 2, 39}) b.push_back(sf::message(R"({"n":)" + std::to_string(n) + "}"));
    seastar::abort_source as;
    auto out = co_await procs[0]->process(std::move(b), as);

    check(out.size() == 1 && out[0].size() == 3, "three messages out");
    for (const auto& m : out[0]) {
        const std::string body = m.as_bytes();
        // json() follows the index, so every message sees the whole batch.
        check(body.find("\"jtotal\":42") != std::string::npos,
              "json().from_all().sum() spans the batch: " + body);
        // `this` does not, so the sum is this message's own n three times.
        const std::string n = body.substr(body.rfind("\"n\":") + 4);
        const long own = std::stol(n);
        check(body.find("\"ttotal\":" + std::to_string(own * 3)) != std::string::npos,
              "this.from_all() repeats the current message: " + body);
        check(body.find("\"count\":3") != std::string::npos,
              "from_all() yields one entry per message");
    }
    co_return;
}

// A failing statement carries Benthos's wrapper, because a user's unit test can
// assert on the text. `throw()` makes the inner message theirs; the prefix and
// the line number are ours to match.
SEASTAR_TEST_CASE(a_failed_statement_reports_its_line) {
    const auto cfg = sf::cfg::parse_yaml(
        "mapping: |\n"
        "  root.a = 1\n"
        "  root.b = throw(\"boom\")\n");
    auto procs = sf::build_processors({sf::parse_processor(cfg)});
    sf::batch b;
    b.push_back(sf::message(std::string("{}")));
    seastar::abort_source as;
    auto out = co_await procs[0]->process(std::move(b), as);
    check(out.size() == 1 && out[0].size() == 1, "the message survives with an error");
    const std::string err = out[0][0].error();
    check(err == "failed assignment (line 2): boom",
          "reports the statement's line and the thrown message, got: " + err);
    co_return;
}

// ---- composite outputs -------------------------------------------------------
//
// The shell gate (tests/test_outputs_check.sh) compares what these WRITE against
// the reference. What it cannot see is what they do to the ACK, because stdout
// shows a delivered message and a redelivered one identically. These cases cover
// exactly that: the source must be resolved once, with the right verdict.

namespace {

// Records every batch it was given, and fails on demand.
//
// The tallies live OUTSIDE the object because a composite output owns its
// children and destroys them when it is itself destroyed; reading a member
// afterwards is a use-after-free, which is how the first version of this test
// "failed".
struct recorder {
    std::vector<std::string> seen;
    int                      writes = 0;
};

class recording_output final : public sf::output {
public:
    explicit recording_output(recorder& r, bool fail = false) : _r(r), _fail(fail) {}
    seastar::future<> write_batch(sf::batch b, seastar::abort_source&) override {
        ++_r.writes;
        for (auto& m : b) _r.seen.push_back(m.as_bytes());
        if (_fail) return seastar::make_exception_future<>(
            std::runtime_error("induced output failure"));
        return seastar::make_ready_future<>();
    }
private:
    recorder& _r;
    bool      _fail;
};

// Writes a single-message batch through an output and reports how the ack went.
// -1 means the ack never fired, which is the failure this whole layer exists to
// prevent and so is distinguished from a nack rather than folded into it.
seastar::future<int> deliver(sf::output_ptr out, std::vector<std::string> payloads) {
    sf::batch b;
    for (auto& p : payloads) b.push_back(sf::message(p));
    seastar::abort_source as;
    co_await out->connect(as);
    int verdict = -1;
    try {
        co_await out->write_batch(std::move(b), as);
        verdict = 0;
    } catch (const std::exception&) {
        verdict = 1;
    }
    co_await out->close();
    co_return verdict;
}

} // namespace

SEASTAR_TEST_CASE(output_broker_fan_out_nacks_when_any_child_fails) {
    recorder good, bad;
    std::vector<sf::output_ptr> kids;
    kids.push_back(sf::output_ptr(new recording_output(good)));
    kids.push_back(sf::output_ptr(new recording_output(bad, /*fail=*/true)));
    const int v = co_await deliver(sf::make_broker_output(std::move(kids), "fan_out_fail_fast"),
                                  {"one"});
    check(v == 1, "a fan_out whose child failed reports the failure");
    // The healthy child must still have been written to: fan_out delivers to
    // every child, and a failure elsewhere does not cancel that.
    check(good.seen == std::vector<std::string>{"one"},
          "the healthy child received the batch anyway");
    co_return;
}

SEASTAR_TEST_CASE(output_fallback_stops_at_the_first_success) {
    recorder first, second, third;
    std::vector<sf::output_ptr> kids;
    kids.push_back(sf::output_ptr(new recording_output(first, /*fail=*/true)));
    kids.push_back(sf::output_ptr(new recording_output(second)));
    kids.push_back(sf::output_ptr(new recording_output(third)));
    const int v = co_await deliver(sf::make_fallback_output(std::move(kids)), {"payload"});
    check(v == 0, "a fallback that reached a working tier succeeds");
    check(second.writes == 1, "the second tier was used");
    check(third.writes == 0, "the third tier was not: fallback stops at the first success");
    co_return;
}

SEASTAR_TEST_CASE(output_switch_strict_mode_nacks_an_unrouted_message) {
    auto never = [](sf::exec_ctx&) { return sf::value(false); };
    recorder a, b;
    {
        std::vector<sf::switch_output_case> cases;
        cases.push_back({never, sf::output_ptr(new recording_output(a)), false});
        cases.push_back({never, sf::output_ptr(new recording_output(b)), false});
        const int v = co_await deliver(sf::make_switch_output(std::move(cases), false), {"x"});
        check(v == 0, "without strict_mode an unrouted message is dropped, which succeeds");
    }
    {
        std::vector<sf::switch_output_case> cases;
        cases.push_back({never, sf::output_ptr(new recording_output(a)), false});
        cases.push_back({never, sf::output_ptr(new recording_output(b)), false});
        const int v = co_await deliver(sf::make_switch_output(std::move(cases), true), {"x"});
        check(v == 1, "with strict_mode it is nacked instead");
    }
    check(a.writes == 0 && b.writes == 0, "neither case output was written to");
    co_return;
}

// ---- auto_replay_nacks -------------------------------------------------------

SEASTAR_TEST_CASE(auto_replay_offers_a_nacked_batch_again) {
    // A source of exactly one batch, whose own ack records how it was resolved.
    struct once_input final : public sf::input {
        int* resolved;
        int  reads = 0;
        bool done = false;
        explicit once_input(int* r) : resolved(r) {}
        seastar::future<std::optional<std::pair<sf::batch, sf::ack_fn>>>
        read_batch(seastar::abort_source&) override {
            if (done) return seastar::make_ready_future<
                std::optional<std::pair<sf::batch, sf::ack_fn>>>(std::nullopt);
            done = true;
            ++reads;
            sf::batch b;
            b.push_back(sf::message(std::string("only")));
            int* r = resolved;
            sf::ack_fn fn = [r](std::exception_ptr e) {
                *r += e ? 100 : 1;
                return seastar::make_ready_future<>();
            };
            return seastar::make_ready_future<
                std::optional<std::pair<sf::batch, sf::ack_fn>>>(
                    std::make_pair(std::move(b), std::move(fn)));
        }
    };

    int resolved = 0;
    auto* src = new once_input(&resolved);
    auto in = sf::make_auto_retry_input(sf::input_ptr(src));
    seastar::abort_source as;

    // Nack twice, then accept. The wrapper must offer the same batch each time
    // and leave the SOURCE unresolved until the accepted attempt.
    int offers = 0;
    for (int attempt = 0; attempt < 3; ++attempt) {
        auto r = co_await in->read_batch(as);
        check(r.has_value(), "the batch is offered again after a nack (attempt " +
                             std::to_string(attempt) + ")");
        if (!r) break;
        ++offers;
        check(r->first.size() == 1 && r->first[0].as_bytes() == "only",
              "the replayed batch is the original content");
        const bool last = attempt == 2;
        check(resolved == 0, "the source is not resolved before the batch is accepted");
        co_await r->second(last ? nullptr
                                : std::make_exception_ptr(std::runtime_error("nope")));
    }
    check(offers == 3, "three offers for two nacks and one accept, got " +
                       std::to_string(offers));
    check(src->reads == 1, "the underlying source was read once, not once per retry");
    check(resolved == 1, "the source was acked exactly once, on success");

    // Now that nothing is outstanding and the source is exhausted, the wrapper
    // must report end of input rather than waiting for ever.
    auto end = co_await in->read_batch(as);
    check(!end.has_value(), "end of input once the source is spent and nothing is owed");
    co_await in->close();
    co_return;
}

// ---- rendezvous --------------------------------------------------------------
//
// The property that matters is the one seastar::queue does NOT have: several
// blocked producers and several blocked consumers at once. queue keeps a single
// promise per side, so a second waiter overwrites the first's and that fiber
// fails with broken_promise. It cost a `http_server` output that delivered two
// of five messages and nacked the rest, and the same latent fault sat in the
// `http_server` and `socket_server` inputs, where every connection is a
// producer. A gate that issues requests one at a time cannot see any of it,
// which is why the invariant is asserted directly here.

SEASTAR_TEST_CASE(rendezvous_handles_many_producers_and_many_consumers) {
    constexpr int    producers = 8, per_producer = 5, consumers = 6;
    constexpr size_t total = producers * per_producer;

    sf::rendezvous<int> r(1);          // depth one: every producer must block
    std::vector<int> taken;
    int push_errors = 0;

    // Consumers first, so several are already blocked when the producers start.
    // The deadline inside pop() is a backstop, not the mechanism: it means this
    // case reports a failure rather than hanging if an item is ever lost.
    std::vector<seastar::future<>> cons;
    for (int c = 0; c < consumers; ++c)
        cons.push_back(seastar::do_until(
            [&taken] { return taken.size() >= total; },
            [&r, &taken] {
                return r.pop(seastar::lowres_clock::now() + std::chrono::seconds(5))
                        .then([&taken](int v) { taken.push_back(v); })
                        .handle_exception([](std::exception_ptr) {});
            }));

    std::vector<seastar::future<>> prods;
    for (int p = 0; p < producers; ++p)
        prods.push_back(seastar::do_for_each(
            boost::counting_iterator<int>(0), boost::counting_iterator<int>(per_producer),
            [&r, &push_errors, p](int i) {
                return r.push(p * 100 + i)
                        .handle_exception([&push_errors](std::exception_ptr) { ++push_errors; });
            }));

    co_await seastar::when_all(prods.begin(), prods.end()).discard_result();

    // Everything has been offered; wait for it to be consumed. Bounded, so a
    // lost item ends the case with a failed check instead of a hang.
    for (int guard = 0; taken.size() < total && guard < 100000; ++guard)
        co_await seastar::yield();

    // More consumers than items remain, so some are still blocked in pop() with
    // nothing left to wake them. Closing is what releases them.
    (void)r.close(std::make_exception_ptr(std::runtime_error("done")));
    co_await seastar::when_all(cons.begin(), cons.end()).discard_result();

    check(push_errors == 0, "no producer was failed by another producer blocking (" +
                            std::to_string(push_errors) + " were)");
    check(taken.size() == total,
          "every item was taken exactly once: " + std::to_string(taken.size()) +
          " of " + std::to_string(total));
    std::sort(taken.begin(), taken.end());
    check(std::adjacent_find(taken.begin(), taken.end()) == taken.end(),
          "no item was delivered to two consumers");
    co_return;
}

SEASTAR_TEST_CASE(rendezvous_close_wakes_everyone_and_hands_back_what_it_held) {
    sf::rendezvous<int> r(2);
    co_await r.push(1);
    co_await r.push(2);

    // A third push must be blocked: the buffer is full.
    bool third_failed = false;
    auto blocked = r.push(3).handle_exception([&third_failed](std::exception_ptr) {
        third_failed = true;
    });

    auto held = r.close(std::make_exception_ptr(std::runtime_error("closing")));
    co_await std::move(blocked);

    check(third_failed, "a producer blocked on a full rendezvous is failed by close()");
    check(held.size() == 2, "close() hands back what was buffered rather than dropping it, got " +
                            std::to_string(held.size()));
    check(held[0] == 1 && held[1] == 2, "in order");

    bool pop_failed = false;
    co_await r.pop().then([](int) {}).handle_exception([&pop_failed](std::exception_ptr) {
        pop_failed = true;
    });
    check(pop_failed, "a pop after close() fails rather than hanging");
    co_return;
}

// ---- batching ----------------------------------------------------------------
//
// What the shell gates cannot see: the ACK. A batching output holds many
// `write_batch` calls unresolved while one batch accumulates, and resolves them
// all with the verdict of the single write it produced. Every source message
// must still be acked exactly once, and only after the batch carrying it was
// actually written -- resolving early would ack data that never left.

SEASTAR_TEST_CASE(batching_acks_every_message_once_and_only_after_the_write) {
    constexpr size_t n = 10;
    constexpr int64_t per_batch = 3;

    recorder inner;
    std::vector<int> acks(n, 0), nacks(n, 0);

    sf::stream_spec spec;
    spec.make_input = [&] { return sf::input_ptr(new counting_input(n, acks, nacks)); };
    spec.make_processors = [] { return std::vector<sf::processor_ptr>{}; };
    spec.make_output = [&inner] {
        return sf::make_batched_output(sf::output_ptr(new recording_output(inner)),
                                       per_batch, 0, std::chrono::milliseconds{0},
                                       sf::transform_fn{}, {}, /*strict=*/false);
    };
    spec.config.queue_depth = 4;

    sf::stream s(spec);
    co_await s.start();
    co_await s.wait_until_drained();
    co_await s.stop();

    size_t once = 0;
    for (size_t i = 0; i < n; ++i) if (acks[i] + nacks[i] == 1) ++once;
    check(once == n, "every message resolved exactly once: " + std::to_string(once) +
                     " of " + std::to_string(n));
    size_t nacked = 0;
    for (size_t i = 0; i < n; ++i) nacked += static_cast<size_t>(nacks[i]);
    check(nacked == 0, "none were nacked (" + std::to_string(nacked) + " were)");

    // Ten messages at three per batch is four writes: three full and a
    // remainder flushed by close(). The remainder is the case that matters --
    // dropping it would lose data that had already been accepted.
    check(inner.writes == 4, "four writes for ten messages at three per batch, got " +
                             std::to_string(inner.writes));
    check(inner.seen.size() == n, "every message reached the inner output: " +
                                  std::to_string(inner.seen.size()) + " of " +
                                  std::to_string(n));
    co_return;
}

SEASTAR_TEST_CASE(batching_nacks_the_whole_batch_when_the_write_fails) {
    constexpr size_t n = 6;
    recorder inner;
    std::vector<int> acks(n, 0), nacks(n, 0);

    sf::stream_spec spec;
    spec.make_input = [&] { return sf::input_ptr(new counting_input(n, acks, nacks)); };
    spec.make_processors = [] { return std::vector<sf::processor_ptr>{}; };
    spec.make_output = [&inner] {
        return sf::make_batched_output(
            sf::output_ptr(new recording_output(inner, /*fail=*/true)),
            2, 0, std::chrono::milliseconds{0}, sf::transform_fn{}, {}, /*strict=*/false);
    };
    spec.config.queue_depth = 4;

    sf::stream s(spec);
    co_await s.start();
    co_await s.wait_until_drained();
    co_await s.stop();

    // Every message that was carried by a failed write must be NACKED, not
    // acked: the batch never reached the destination, so the source has to be
    // told it can send again.
    size_t nacked = 0, acked = 0;
    for (size_t i = 0; i < n; ++i) { nacked += static_cast<size_t>(nacks[i]);
                                     acked  += static_cast<size_t>(acks[i]); }
    check(nacked == n, "all " + std::to_string(n) + " were nacked, got " +
                       std::to_string(nacked));
    check(acked == 0, "none were acked (" + std::to_string(acked) + " were)");
    co_return;
}

// ---- rate limits --------------------------------------------------------------
//
// The interesting part of `rate_limit_resources` is the RESOLUTION: a component
// carries only the label, and the count and interval behind it live in a
// document-level block the component registry never sees. Getting that wrong is
// invisible in output -- the messages still arrive, just unthrottled -- so it is
// pinned here rather than left to the timing gate alone.
namespace {
sf::pipeline_spec parse_with_limit(const std::string& limit_body,
                                   const std::string& processor_body =
                                       "    - rate_limit: { resource: slow }\n") {
    return sf::parse_pipeline(sf::cfg::parse_yaml(
        "input:\n  generate: { count: 1, mapping: 'root = 1' }\n"
        "pipeline:\n  processors:\n" + processor_body +
        "output:\n  drop: {}\n"
        "rate_limit_resources:\n  - label: slow\n" + limit_body));
}

// True when parsing throws a spec_error whose message contains `needle`. The
// SUBSTRING matters: the project's rule is that an unimplemented or misspelled
// thing is refused BY NAME, and a test that only checked "it threw" would pass
// on a message that named nothing.
bool refused_with(const std::string& limit_body, const std::string& needle) {
    try {
        (void)parse_with_limit(limit_body);
    } catch (const sf::spec_error& e) {
        return std::string(e.what()).find(needle) != std::string::npos;
    } catch (...) {
        return false;
    }
    return false;
}
} // namespace

SEASTAR_TEST_CASE(rate_limit_resources_are_resolved_onto_the_component) {
    {
        const auto spec = parse_with_limit("    local: { count: 5, interval: 250ms }\n");
        const auto& cc = spec.processors.at(0);
        check(cc.rate_limit_resolved, "the label is resolved during parsing");
        check(cc.rate_limit_name == "slow", "the label is carried to construction");
        check(cc.rate_limit_count == 5, "count comes from the document");
        check(cc.rate_limit_interval == std::chrono::milliseconds{250}, "so does interval");
    }
    {
        // The reference's defaults, which apply when `local` is written empty.
        const auto spec = parse_with_limit("    local: {}\n");
        const auto& cc = spec.processors.at(0);
        check(cc.rate_limit_count == 1000, "count defaults to the reference's 1000");
        check(cc.rate_limit_interval == std::chrono::milliseconds{1000},
              "interval defaults to the reference's 1s");
    }
    {
        // Two processors naming one label. Both must resolve to the SAME label,
        // because sharing the permits is what makes it a resource rather than a
        // setting -- and the label is what sf::shared_rate_limit() keys on.
        const auto spec = parse_with_limit(
            "    local: { count: 5, interval: 250ms }\n",
            "    - rate_limit: { resource: slow }\n"
            "    - rate_limit: { resource: slow }\n");
        check(spec.processors.size() == 2, "both processors survive resolution");
        check(spec.processors.at(0).rate_limit_name == spec.processors.at(1).rate_limit_name,
              "two components naming one limit resolve to one label");
    }
    co_return;
}

// With several limits declared, the lookup must pick the one that was NAMED.
// Matching the first entry regardless would pass every test above, because they
// all declare exactly one.
SEASTAR_TEST_CASE(a_rate_limit_lookup_picks_the_label_it_was_given) {
    const auto spec = sf::parse_pipeline(sf::cfg::parse_yaml(
        "input:\n  generate: { count: 1, mapping: 'root = 1' }\n"
        "pipeline:\n  processors:\n    - rate_limit: { resource: second }\n"
        "output:\n  drop: {}\n"
        "rate_limit_resources:\n"
        "  - label: first\n    local: { count: 11, interval: 100ms }\n"
        "  - label: second\n    local: { count: 22, interval: 200ms }\n"));
    const auto& cc = spec.processors.at(0);
    check(cc.rate_limit_count == 22, "the named entry supplies the count");
    check(cc.rate_limit_interval == std::chrono::milliseconds{200},
          "and the interval");
    co_return;
}

SEASTAR_TEST_CASE(rate_limit_unimplemented_and_unknown_are_distinguished) {
    // An unimplemented KIND and a misspelled FIELD are different mistakes and
    // must not share a message: one means "swordfish cannot do this yet", the
    // other means "you made a typo".
    check(refused_with("    redis: { url: 'redis://x', key: y }\n",
                       "swordfish implements no other kind yet"),
          "a non-local rate limit is refused as unimplemented");
    check(refused_with("    local: { count: 5, bogus: 1 }\n", "has no field 'bogus'"),
          "an unrecognised field inside `local` is refused as unknown");
    check(refused_with("    local: { count: 5 }\n    nonsense: {}\n",
                       "has no field 'nonsense'"),
          "an unrecognised field on the entry itself is refused as unknown");
    // The reference rejects both of those too; accepting them here would let a
    // config lint clean under swordfish and fail under the reference, which is
    // the divergence found latest and at the worst moment.
    try {
        (void)sf::parse_pipeline(sf::cfg::parse_yaml(
            "input:\n  generate: { count: 1, mapping: 'root = 1' }\n"
            "pipeline:\n  processors:\n    - rate_limit: { resource: absent }\n"
            "output:\n  drop: {}\n"));
        check(false, "a label with no matching entry is refused");
    } catch (const sf::spec_error& e) {
        check(std::string(e.what()).find("is not declared in rate_limit_resources")
                  != std::string::npos,
              "a label with no matching entry is refused by name");
    }
    co_return;
}

// The same rule on the cache side. `compaction_interval`, `init_values` and
// `shards` are all handled now -- the first two implemented, the third accepted
// as the no-op it genuinely is here -- so what is left to pin is that an
// unrecognised field is still named rather than dropped.
SEASTAR_TEST_CASE(unimplemented_memory_cache_fields_are_refused_by_name) {
    const auto parse_cache = [](const std::string& memory_body) {
        return sf::parse_pipeline(sf::cfg::parse_yaml(
            "input:\n  generate: { count: 1, mapping: 'root = 1' }\n"
            "pipeline:\n  processors:\n"
            "    - dedupe: { cache: c, key: 'x' }\n"
            "output:\n  drop: {}\n"
            "cache_resources:\n  - label: c\n    memory: " + memory_body + "\n"));
    };
    BOOST_CHECK_NO_THROW((void)parse_cache("{ default_ttl: 60s }"));
    BOOST_CHECK_NO_THROW((void)parse_cache("{ shards: 4 }"));
    BOOST_CHECK_NO_THROW((void)parse_cache("{ init_values: { a: b } }"));
    BOOST_CHECK_NO_THROW((void)parse_cache("{ compaction_interval: 10s }"));
    for (const auto& [body, needle] : std::vector<std::pair<std::string, std::string>>{
             {"{ bogus: 1 }",              "has no field 'bogus'"},
             {"{ init_values: [ a, b ] }", "table of key/value pairs"}}) {
        bool named = false;
        try { (void)parse_cache(body); }
        catch (const sf::spec_error& e) {
            named = std::string(e.what()).find(needle) != std::string::npos;
        }
        check(named, "memory cache " + body + " is refused mentioning: " + needle);
    }
    co_return;
}

// ---- cache_resources -------------------------------------------------------------
//
// The DEFAULTS are pinned here rather than in a gate because two of them cannot
// be exercised in reasonable time: the reference's `default_ttl` is five minutes
// and its `compaction_interval` sixty seconds. Swordfish used to default
// `default_ttl` to zero -- an unbounded cache where the reference expires
// entries -- and nothing would have noticed.
SEASTAR_TEST_CASE(cache_defaults_match_the_reference) {
    const auto parse_cache_of = [](const std::string& body) {
        return sf::parse_pipeline(sf::cfg::parse_yaml(
            "input:\n  generate: { count: 1, mapping: 'root = 1' }\n"
            "pipeline:\n  processors:\n    - dedupe: { cache: c, key: 'x' }\n"
            "output:\n  drop: {}\n"
            "cache_resources:\n  - label: c\n" + body)).processors.at(0).cache;
    };
    {
        const auto c = parse_cache_of("    memory: {}\n");
        check(c.kind == "memory", "an empty body still selects the kind");
        check(c.default_ttl == std::chrono::milliseconds{300000},
              "default_ttl defaults to the reference's five minutes");
        check(c.compaction_interval == std::chrono::milliseconds{60000},
              "compaction_interval defaults to the reference's sixty seconds");
        check(!c.compaction_disabled, "and compaction is on by default");
    }
    {
        // "This field can be set to an empty string in order to disable
        // compactions/expiry entirely" -- which is NOT the same as zero.
        const auto c = parse_cache_of("    memory: { compaction_interval: \"\" }\n");
        check(c.compaction_disabled, "an empty compaction_interval disables it");
    }
    {
        // A zero TTL is "already expired", NOT "never expires". The reference
        // computes `expires = time.Now().Add(defaultTTL)` unconditionally, so
        // zero puts the expiry in the past and the next compaction sweeps the
        // entry; a never-expiring entry there is a ZERO `expires`, which is
        // reserved for init_values. The label on this check used to say the
        // opposite, and the cache implemented what the label said: measured, the
        // reference emitted `A B A` where swordfish emitted `A B`.
        const auto c = parse_cache_of("    memory: { default_ttl: 0s }\n");
        check(c.default_ttl == std::chrono::milliseconds{0},
              "an explicit zero ttl is read as zero, which means already expired");
    }
    {
        // An EMPTY default_ttl is refused, as the reference refuses it:
        // `failed to parse 'default_ttl' as a duration string`. It used to be
        // read as zero.
        bool refused = false;
        try { (void)parse_cache_of("    memory: { default_ttl: \"\" }\n"); }
        catch (const std::exception&) { refused = true; }
        check(refused, "an empty default_ttl is refused rather than read as zero");
    }
    {
        const auto c = parse_cache_of("    memory: { shards: 4 }\n");
        check(c.shards == 4, "shards is read rather than accepted and ignored");
        const auto d = parse_cache_of("    memory: {}\n");
        check(d.shards == 1, "and defaults to one");
    }
    {
        const auto c = parse_cache_of("    lru: {}\n");
        check(c.kind == "lru", "lru is selected by its key");
        check(c.cap == 1000, "cap defaults to the reference's 1000");
    }
    {
        const auto c = parse_cache_of(
            "    lru: { cap: 4, init_values: { a: 1, b: 2 } }\n");
        check(c.cap == 4, "cap is read");
        check(c.init_keys.size() == 2, "init_values contributes one key each");
    }
    {
        const auto c = parse_cache_of("    file: { directory: /tmp/x }\n");
        check(c.kind == "file", "file is selected by its key");
        check(c.directory == "/tmp/x", "and its directory is read");
    }
    co_return;
}

// The LRU's eviction order is the property a map cannot fake. This test used to
// assert the OPPOSITE of what it asserts now, and it was wrong: it pinned "a hit
// renews recency, or the cache is a FIFO wearing an LRU's name", which is true
// of a general LRU and not of THIS one.
//
// `add` is the only operation this interface has, and `dedupe` is its only
// caller. The reference's dedupe path PEEKS -- `lruCacheAdapter.unsafeAdd` calls
// `inner.Peek(key)` and returns ErrKeyAlreadyExists on a hit, so `inner.Add`,
// the recency-updating call, is reached only on a MISS (cache_lru.go:264-271).
// Renewing here kept a repeated key alive that the reference lets fall out, so
// swordfish DROPPED a message the reference delivers: `A B C A D` at cap 3 came
// out `A B C D` there and `A B C` here.
SEASTAR_TEST_CASE(lru_eviction_is_by_insertion_because_add_peeks) {
    sf::cache_spec spec;
    spec.kind = "lru";
    spec.cap = 2;
    auto c = sf::make_cache(spec);

    check(co_await c->add("a"), "a is new");
    check(co_await c->add("b"), "b is new");
    // Seeing "a" again reports the duplicate and does NOT move it, so "a" is
    // still the least recently INSERTED and is what "c" evicts.
    check(!co_await c->add("a"), "a is still present, and this does not renew it");
    check(co_await c->add("c"), "c is new and evicts something");
    // ORDER MATTERS: a miss INSERTS, which evicts in turn, so the surviving key
    // must be probed before the evicted one. Asking about "a" first would evict
    // "b" and make the second probe say nothing.
    check(!co_await c->add("b"), "b survived: it was inserted after a");
    check(co_await c->add("a"), "a was evicted, because the hit above did not renew it");
    co_return;
}

// One `cache_resources` label is ONE cache, shared by every component naming it
// -- which is what makes it a resource. make_cache handed each caller its own
// instance, so a second `dedupe` over one label passed everything the first had
// already seen. Sharing is per SHARD, since caches are shard-local by design.
SEASTAR_TEST_CASE(one_cache_label_is_one_cache) {
    sf::cache_spec a;
    a.kind = "memory";
    a.label = "shared";
    sf::cache_spec b = a;

    auto first  = sf::make_cache(a);
    auto second = sf::make_cache(b);
    check(first.get() == second.get(), "the same label gives the same instance");
    check(co_await first->add("k"), "k is new to the first");
    check(!co_await second->add("k"), "and already present in the second");

    sf::cache_spec other = a;
    other.label = "elsewhere";
    auto third = sf::make_cache(other);
    check(third.get() != first.get(), "a different label is a different cache");
    check(co_await third->add("k"), "so k is new there");

    sf::cache_spec unlabelled = a;
    unlabelled.label.clear();
    check(sf::make_cache(unlabelled).get() != sf::make_cache(unlabelled).get(),
          "an unlabelled cache is shared with nothing");
    co_return;
}

// ---- workflow `order` must cover the branches exactly -----------------------------
//
// resolve_workflow_order checked only that every name in `order` exists in
// `branches`, leaving three ways to be silently wrong, all of which the
// reference refuses by name (processor_workflow_branch_map.go:253-279): a branch
// missing from `order` never ran and its result_map never applied, a name listed
// twice ran twice, and an empty tier passed unnoticed. Pinned here rather than in
// a gate because the check is in the shared resolver both backends call.
SEASTAR_TEST_CASE(workflow_order_must_name_every_branch_once) {
    const auto build_with_order = [](const std::string& order) {
        const auto spec = sf::parse_pipeline(sf::cfg::parse_yaml(
            "input:\n  generate: { count: 1, mapping: 'root.id = 1' }\n"
            "pipeline:\n  processors:\n"
            "    - workflow:\n"
            "        meta_path: m\n"
            "        order: " + order + "\n"
            "        branches:\n"
            "          a: { request_map: 'root = this', processors: [ { mapping: 'root = {\"v\":1}' } ], result_map: 'root.a = this.v' }\n"
            "          b: { request_map: 'root = this', processors: [ { mapping: 'root = {\"v\":2}' } ], result_map: 'root.b = this.v' }\n"
            "output:\n  drop: {}\n"));
        // The check lives in the BUILD, not the parse, so the processor has to be
        // constructed for it to fire.
        (void)sf::build_processors(spec.processors);
    };
    const auto refused_with = [&](const std::string& order, const std::string& needle) {
        try { build_with_order(order); }
        catch (const std::exception& e) {
            return std::string(e.what()).find(needle) != std::string::npos;
        }
        return false;
    };
    BOOST_CHECK_NO_THROW(build_with_order("[[a],[b]]"));
    BOOST_CHECK_NO_THROW(build_with_order("[[a,b]]"));
    check(refused_with("[[a]]", "branch 'b' is missing from `order`"),
          "a branch left out of order is refused by name");
    check(refused_with("[[a],[b],[a]]", "lists branch 'a' more than once"),
          "a branch listed twice is refused by name");
    check(refused_with("[[a,b],[]]", "empty tier"),
          "an empty tier is refused by name");
    check(refused_with("[[a],[b],[c]]", "which is not declared in `branches`"),
          "and a name with no branch is still refused");
    co_return;
}
