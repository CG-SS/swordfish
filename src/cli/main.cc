// sfslice — the `blobl` driver.
//
//   sfslice emit  <mapping>              print the C++ the emitter produces
//   sfslice run   <mapping> [json]       interpret one document
//   sfslice blobl <mapping> -i file      map a JSONL stream, one doc per line
//   sfslice bench <mapping> -n N         benchmark the interpreter
//
// The compiled half of the benchmark is a separate binary produced by
// bench/build_compiled.sh, which pipes `sfslice emit` into the compiler.
#include "swordfish/blobl/parse.hh"
#include "swordfish/blobl/interp.hh"
#include "swordfish/blobl/emit.hh"

#include <chrono>
#include <cstring>
#include <fstream>
#include <iostream>
#include <string>

#include "swordfish/cli/commands.hh"

namespace {

int usage() {
    std::cerr << "usage: sfslice emit|run|blobl|bench <mapping> [args]\n";
    return 2;
}

} // namespace

int sf::cli::slice_main(int argc, char** argv) {
    if (argc < 3) return usage();
    const std::string cmd = argv[1];
    const std::string src = argv[2];

    sf::blobl::mapping m;
    try {
        m = sf::blobl::parse_mapping(src);
    } catch (const sf::blobl::parse_error& e) {
        std::cerr << "mapping parse failed:\n" << e.render(src) << "\n";
        return 1;
    }

    if (cmd == "emit") {
        std::cout << sf::blobl::emit_cpp(m);
        return 0;
    }

    sf::blobl::interp in_(m);

    if (cmd == "run") {
        const std::string doc = argc > 3 ? argv[3] : "{}";
        try {
            sf::exec_ctx ctx;
            sf::value input = sf::parse_json(doc);
            const sf::message input_msg(doc);
            ctx.msg = &input_msg;              // for content(), json(), error()
            // Print what a message would carry, not the raw value: a string
            // root becomes the content verbatim (message::set_mapped).
            sf::message out(doc);
            if (out.set_mapped(in_.run(input, ctx)))
                std::cout << out.as_bytes() << "\n";
            else
                std::cout << doc << "\n";
        } catch (const sf::eval_error& e) {
            std::cerr << "mapping failed: " << e.what() << "\n";
            return 1;
        }
        return 0;
    }

    // Deliberately byte-compatible with `redpanda-connect blobl -i <file>`, so
    // the two can be diffed directly. The output contract is that tool's,
    // observed rather than assumed:
    //
    //   * one line of stdout per document that maps successfully;
    //   * a document whose root is `deleted()` produces an EMPTY line;
    //   * a document whose mapping errors produces NO line, and a
    //     "failed to execute map: ..." line on stderr instead.
    //
    // Those three rules are what let tools/run_reference_diff.sh compare whole
    // batches rather than launching a process per document.
    if (cmd == "blobl") {
        std::istream* in = &std::cin;
        std::ifstream file;
        for (int i = 3; i < argc; ++i) {
            if ((!std::strcmp(argv[i], "-i") || !std::strcmp(argv[i], "--input")) &&
                i + 1 < argc) {
                file.open(argv[++i]);
                if (!file) {
                    std::cerr << "cannot open " << argv[i] << "\n";
                    return 1;
                }
                in = &file;
            }
        }
        // One context for the whole stream, with variables cleared per document.
        // That is what the runtime's mapping processor does and what the
        // reference does: `counter()` is documented as keeping its state across
        // messages, while a `let` must not leak from one into the next.
        sf::exec_ctx ctx;
        std::string line;
        while (std::getline(*in, line)) {
            if (line.empty()) continue;
            try {
                ctx.vars.clear();
                const sf::message input_msg(line);
                ctx.msg = &input_msg;
                // `this` resolves lazily, so a mapping that never touches it
                // still runs on input that is not valid JSON.
                sf::value parsed;
                bool structured = true;
                try { parsed = sf::parse_json(line); }
                catch (const sf::eval_error&) { structured = false; }
                ctx.this_v = structured ? &parsed : nullptr;

                sf::value result = in_.run_ctx(ctx);
                if (result.is_deleted()) { std::cout << "\n"; continue; }
                sf::message out(line);
                if (out.set_mapped(std::move(result))) std::cout << out.as_bytes() << "\n";
                else                                   std::cout << line << "\n";
            } catch (const sf::eval_error& e) {
                std::cerr << "failed to execute map: " << e.what() << "\n";
            }
        }
        return 0;
    }

    if (cmd == "bench") {
        long n = 1'000'000;
        for (int i = 3; i < argc; ++i)
            if (!std::strcmp(argv[i], "-n") && i + 1 < argc) n = std::atol(argv[++i]);

        sf::exec_ctx ctx;
        sf::value input = sf::value::object();
        input.set("n", sf::value(int64_t{0}));

        auto t0 = std::chrono::steady_clock::now();
        uint64_t sink = 0;
        for (long i = 0; i < n; ++i) {
            input.set("n", sf::value(static_cast<int64_t>(i)));
            sf::value out = in_.run(input, ctx);
            sink += (out.type() == sf::vtype::object)
                ? static_cast<uint64_t>(out.obj().size())
                : static_cast<uint64_t>(out.type());
        }
        auto t1 = std::chrono::steady_clock::now();
        double secs = std::chrono::duration<double>(t1 - t0).count();
        std::fprintf(stdout, "interpreted  %ld msgs  %.3f s  %.0f msg/s  (sink=%llu)\n",
                     n, secs, n / secs, static_cast<unsigned long long>(sink));
        return 0;
    }

    return usage();
}
