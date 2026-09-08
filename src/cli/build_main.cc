// swordfish build — YAML in, native binary out.
//
//   swordfish-build config.yaml -o ./pipeline
//   swordfish-build config.yaml --emit-source DIR    write sources and stop
//
// Steps: parse + validate, emit, hash for the cache, compile, link.
#include "swordfish/config/template.hh"
#include "swordfish/config/yaml.hh"
#include "swordfish/codegen/emit_main.hh"

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <system_error>
#include <vector>

#include <openssl/evp.h>
#include "swordfish/kafka/components.hh"

#include "swordfish/cli/commands.hh"

namespace fs = std::filesystem;

namespace {


void write_file(const fs::path& p, const std::string& content) {
    std::ofstream f(p, std::ios::binary);
    if (!f) throw std::runtime_error("cannot write " + p.string());
    f << content;
}

// Single-quote for the shell. Paths reach std::system/popen, so a directory
// containing a space, a quote or a $ would otherwise break the command or, worse,
// execute part of it.
std::string shq(const std::string& s) {
    std::string out = "'";
    for (char c : s) {
        if (c == '\'') out += "'\\''";
        else out += c;
    }
    return out + "'";
}

std::string run_capture(const std::string& cmd) {
    std::string out;
    FILE* p = ::popen(cmd.c_str(), "r");
    if (!p) throw std::runtime_error("failed to run: " + cmd);
    char buf[4096];
    while (fgets(buf, sizeof buf, p)) out += buf;
    ::pclose(p);
    while (!out.empty() && (out.back() == '\n' || out.back() == ' ')) out.pop_back();
    return out;
}

// Content-addressed build cache.
//
// The key covers everything that can change the output: the emitted sources, the
// compiler identity, the exact flags, and the LIBRARIES those flags name.
// Anything omitted from the key would let a stale binary be served after a real
// change, so the flags are hashed in full rather than summarised.
std::string sha256_hex(const std::string& data) {
    unsigned char md[EVP_MAX_MD_SIZE];
    unsigned int len = 0;
    EVP_Digest(data.data(), data.size(), md, &len, EVP_sha256(), nullptr);
    static const char* hex = "0123456789abcdef";
    std::string out;
    out.reserve(len * 2);
    for (unsigned i = 0; i < len; ++i) { out += hex[md[i] >> 4]; out += hex[md[i] & 15]; }
    return out;
}

// The libraries the link flags NAME, identified by size and modification time.
//
// Without these the key is effectively the config alone, so a rebuild after a
// change to libswordfish_rt -- a runtime fix, say -- silently serves the binary
// built against the OLD library. That is compiled mode quietly disagreeing with
// interpreted mode, which is the one outcome the cache must not be able to
// cause. It was found exactly that way: a `workflow` metadata fix landed,
// `swordfish build` reported success, and the binary kept the previous
// behaviour until --no-cache was passed.
//
// Size and mtime rather than content: libswordfish_rt.a is around 100 MB, and
// hashing it on every build would cost more than the compile the cache exists
// to avoid. Libraries resolved from the system's default search path (-lz and
// friends) are NOT covered -- a system library changing under a cached build is
// a case this does not catch, and the remedy for that is --no-cache.
std::string library_identity(const std::string& libs) {
    std::vector<fs::path> search;
    std::vector<std::string> names, explicit_paths;
    std::istringstream in(libs);
    for (std::string tok; in >> tok;) {
        if (tok.rfind("-L", 0) == 0)      search.emplace_back(tok.substr(2));
        else if (tok.rfind("-l", 0) == 0) names.push_back(tok.substr(2));
        else if (tok.find(".a") != std::string::npos ||
                 tok.find(".so") != std::string::npos)
            explicit_paths.push_back(tok);
    }

    std::string out;
    const auto note = [&out](const fs::path& p) {
        std::error_code ec;
        const auto size = fs::file_size(p, ec);
        if (ec) return false;
        const auto when = fs::last_write_time(p, ec);
        if (ec) return false;
        out += p.string() + ":" + std::to_string(size) + ":" +
               std::to_string(when.time_since_epoch().count()) + "\n";
        return true;
    };

    for (const auto& p : explicit_paths) note(p);
    for (const auto& n : names)
        for (const auto& dir : search)
            // Static first, matching the linker's own preference where both
            // exist; the first hit is enough to identify the build.
            if (note(dir / ("lib" + n + ".a")) || note(dir / ("lib" + n + ".so"))) break;
    return out;
}

fs::path cache_dir() {
    if (const char* x = std::getenv("SWORDFISH_CACHE_DIR")) return fs::path(x);
    if (const char* x = std::getenv("XDG_CACHE_HOME")) return fs::path(x) / "swordfish";
    if (const char* h = std::getenv("HOME")) return fs::path(h) / ".cache" / "swordfish";
    return fs::temp_directory_path() / "swordfish-cache";
}

int usage() {
    std::cerr <<
        "usage: swordfish build <config.yaml> [-o OUT] [--emit-source DIR]\n"
        "                       [-O0|-O2|-O3] [-j N] [--no-cache]\n"
        "                       [-t <templates>]...\n";
    return 2;
}

} // namespace

static int sf_cli_main(int argc, char** argv) {
    // Connectors in their own libraries register explicitly: a static
    // library's registrations are dropped by the linker otherwise.
    sf::kafka::register_components();
    if (argc < 2) return usage();
    std::string config_path, out_path = "./pipeline", emit_dir, opt = "-O2";
    int jobs = 4;
    bool no_cache = false;

    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "-o" && i + 1 < argc) out_path = argv[++i];
        else if (a == "--emit-source" && i + 1 < argc) emit_dir = argv[++i];
        else if (a == "-j" && i + 1 < argc) jobs = std::atoi(argv[++i]);
        else if (a.rfind("-O", 0) == 0) opt = a;
        else if (a == "--no-cache") no_cache = true;
        // Loaded before the config is read: `build` compiles the EXPANDED
        // config, so a template usage has to resolve here exactly as it does
        // under `run`, or the two backends would disagree about what the same
        // file means -- which is the one thing this project will not have.
        else if ((a == "-t" || a == "--templates") && i + 1 < argc) {
            try {
                sf::tmpl::load_templates(argv[++i]);
            } catch (const std::exception& e) {
                std::cerr << "templates: " << e.what() << "\n";
                return 1;
            }
        }
        else if (a == "-h" || a == "--help") return usage();
        else if (!a.empty() && a[0] == '-') { std::cerr << "unknown flag: " << a << "\n"; return usage(); }
        else config_path = a;
    }
    if (config_path.empty()) return usage();

    sf::codegen::emitted em;
    try {
        auto root = sf::cfg::load_config(config_path);
        auto spec = sf::parse_pipeline(root);
        em = sf::codegen::emit_program(spec, fs::absolute(config_path).string());
    } catch (const std::exception& e) {
        std::cerr << config_path << ": error: " << e.what() << "\n";
        return 1;
    }

    // Where the generated sources land. --emit-source keeps them for inspection;
    // otherwise a build directory beside the output.
    const fs::path dir = emit_dir.empty()
        ? fs::path(out_path + ".build") : fs::path(emit_dir);
    fs::create_directories(dir);
    write_file(dir / "main.cc", em.main_cc);
    for (const auto& u : em.units) write_file(dir / u.name, u.content);

    if (!emit_dir.empty()) {
        std::cout << "wrote " << (em.units.size() + 1) << " files to " << dir << "\n";
        return 0;
    }

    // Flags come from swordfish-config, the same mechanism the docs describe, so
    // generated code is compiled exactly as the library was.
    const char* cfg_env = std::getenv("SWORDFISH_CONFIG");
    const std::string sfconfig = cfg_env ? cfg_env : "./build/swordfish-config";
    std::string cflags, libs;
    try {
        cflags = run_capture(shq(sfconfig) + " --cflags");
        libs   = run_capture(shq(sfconfig) + " --libs");
    } catch (const std::exception& e) {
        std::cerr << "error: " << e.what() << "\n"
                  << "hint: set SWORDFISH_CONFIG to the path of swordfish-config\n";
        return 1;
    }
    if (cflags.empty()) {
        std::cerr << "error: " << sfconfig << " produced no flags\n";
        return 1;
    }

    const std::string cxx = std::getenv("CXX") ? std::getenv("CXX") : "g++";

    // ---- cache lookup -------------------------------------------------------
    std::string key_material = em.main_cc;
    for (const auto& u : em.units) key_material += u.content;
    key_material += "\0" + cflags + "\0" + libs + "\0" + opt + "\0" + cxx;
    key_material += "\0" + run_capture(shq(cxx) + " --version 2>/dev/null | head -1");
    key_material += "\0" + library_identity(libs);
    const std::string key = sha256_hex(key_material);
    const fs::path cached = cache_dir() / key / "pipeline";

    if (!no_cache && fs::exists(cached)) {
        fs::remove(out_path);
        fs::copy_file(cached, out_path);
        fs::permissions(out_path, fs::perms::owner_all | fs::perms::group_read |
                                  fs::perms::group_exec | fs::perms::others_read |
                                  fs::perms::others_exec);
        std::cout << "built " << out_path << " (cached)\n";
        return 0;
    }

    // Translation units are compiled in parallel: main.cc dominates at ~3.5s
    // (Seastar template instantiation) while each transform is under a second,
    // so overlapping them hides the transforms entirely.
    std::cout << "compiling " << (em.units.size() + 1) << " translation units...\n";
    std::vector<std::string> tus{"main.cc"};
    for (const auto& u : em.units) tus.push_back(u.name);

    std::string objects;
    std::ostringstream par;
    for (const auto& tu : tus) {
        const fs::path obj = dir / (tu + ".o");
        objects += (objects.empty() ? "" : " ") + shq(obj.string());
        // The object is REMOVED before its compile is launched. Without this a
        // failed compile leaves the previous run's object in place and the link
        // below succeeds against it -- so `swordfish build` could produce a
        // working binary built from a DIFFERENT config.
        std::error_code rm_ec;
        fs::remove(obj, rm_ec);
        par << cxx << " " << cflags << " " << opt << " -c " << shq((dir / tu).string())
            << " -o " << shq(obj.string()) << " & pids=\"$pids $!\"; "
            << "n=$((n+1)); if [ \"$n\" -ge " << jobs
            << " ]; then for p in $pids; do wait \"$p\" || rc=1; done; pids=; n=0; fi; ";
    }
    // Each PID waited on INDIVIDUALLY. A bare `wait` returns 0 once its children
    // have terminated whatever their status -- `sh -c 'set -e; (exit 3) & wait'`
    // exits 0 -- and `set -e` does not fire on a background job either, so the
    // `!= 0` test below was dead code and a compile error was reported as a
    // successful build.
    par << "for p in $pids; do wait \"$p\" || rc=1; done; exit $rc";
    // `-j N` now BOUNDS the concurrency. It was read and then used only to
    // choose this branch: every translation unit was backgrounded at once and
    // waited for at the end, so `-j 2` and `-j 64` issued the same command.
    // Measured on a 27-unit config with `-j 2`: 31 concurrent cc1plus. A build
    // machine asked for two jobs got as many compilers as the config had units,
    // which is how a `swordfish build` in CI takes the box down.
    //
    // The batching is deliberately simple -- launch N, wait for all N, launch
    // the next N -- rather than a sliding window. It bounds the peak, which is
    // the property that was missing, and a build is a handful of units.
    if (jobs > 1) {
        if (std::system(("pids=; rc=0; n=0; " + par.str()).c_str()) != 0) {
            std::cerr << "error: compilation failed\n";
            return 1;
        }
    } else {
        for (const auto& tu : tus) {
            std::ostringstream c;
            c << cxx << " " << cflags << " " << opt << " -c " << shq((dir / tu).string())
              << " -o " << shq((dir / (tu + ".o")).string());
            if (std::system(c.str().c_str()) != 0) {
                std::cerr << "error: compilation failed\n  " << c.str() << "\n";
                return 1;
            }
        }
    }

    std::ostringstream cmd;
    cmd << cxx << " " << objects << " " << libs << " -o " << shq(out_path);
    const int rc = std::system(cmd.str().c_str());
    if (rc != 0) {
        std::cerr << "error: link failed\n  " << cmd.str() << "\n";
        return 1;
    }
    if (!no_cache) {
        std::error_code ec;
        fs::create_directories(cached.parent_path(), ec);
        fs::copy_file(out_path, cached, fs::copy_options::overwrite_existing, ec);
    }
    std::cout << "built " << out_path << " ("
              << em.units.size() << " transform" << (em.units.size() == 1 ? "" : "s")
              << " compiled)\n";
    return 0;
}

// A top-level catch, because every path below can throw on ordinary user error
// -- an unreadable config, an output directory that cannot be created -- and
// without one those surface as `terminate called after throwing ...` and a core
// dump (SIGABRT, exit 134) rather than a message. Found by an audit:
// `swordfish build cfg.yaml -o /unwritable/path` aborted instead of reporting.
int sf::cli::build_main(int argc, char** argv) {
    try {
        return sf_cli_main(argc, argv);
    } catch (const std::exception& e) {
        std::cerr << "error: " << e.what() << "\n";
        return 1;
    } catch (...) {
        std::cerr << "error: unknown failure\n";
        return 1;
    }
}
