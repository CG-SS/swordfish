// Driver for the compiled half of the benchmark. Links the generated mapping
// function against the same libswordfish the interpreter uses, so the only
// difference measured is compiled-vs-interpreted dispatch.
#include <swordfish/runtime.hh>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>

namespace sf::gen { sf::value blobl_0(sf::exec_ctx&); }

int main(int argc, char** argv) {
    long n = 1'000'000;
    for (int i = 1; i < argc; ++i)
        if (!std::strcmp(argv[i], "-n") && i + 1 < argc) n = std::atol(argv[++i]);

    sf::exec_ctx ctx;
    sf::value input = sf::value::object();
    input.set("n", sf::value(int64_t{0}));
    ctx.this_v = &input;

    auto t0 = std::chrono::steady_clock::now();
    uint64_t sink = 0;
    for (long i = 0; i < n; ++i) {
        input.set("n", sf::value(static_cast<int64_t>(i)));
        sf::value out = sf::gen::blobl_0(ctx);
        sink += (out.type() == sf::vtype::object)
                ? static_cast<uint64_t>(out.obj().size())
                : static_cast<uint64_t>(out.type());
    }
    auto t1 = std::chrono::steady_clock::now();
    double secs = std::chrono::duration<double>(t1 - t0).count();
    std::fprintf(stdout, "compiled     %ld msgs  %.3f s  %.0f msg/s  (sink=%llu)\n",
                 n, secs, n / secs, static_cast<unsigned long long>(sink));
    return 0;
}
