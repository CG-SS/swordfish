// `swordfish` — the single front end.
//
// The five per-command executables (swordfish-run, swordfish-build,
// swordfish-test, sfconfig, sfslice) remain as thin wrappers over the same
// entry points, because the gates and benchmarks invoke them by name.
//
// The command NAMES follow redpanda-connect's where they overlap, because the
// point of this project is that a Redpanda Connect user's muscle memory keeps
// working: `swordfish run config.yaml` and `swordfish lint config.yaml` take
// the same shape as theirs, including the positional config file. `build` is
// ours alone, and is the whole reason the project exists.
//
// A command the reference has and swordfish does not is a NAMED error, not an
// unknown one — the same distinction the scanners and the Bloblang methods
// make, and for the same reason: "swordfish has not built this yet" and "you
// made a typo" are different problems with different next steps.

#include "swordfish/cli/commands.hh"

#include <cstring>
#include <iostream>
#include <string>
#include <vector>

#ifndef SWORDFISH_VERSION
#define SWORDFISH_VERSION "0.0.0-dev"
#endif
#ifndef SWORDFISH_BUILT
#define SWORDFISH_BUILT "unknown"
#endif

namespace sf::cli {

const char* version() { return SWORDFISH_VERSION; }
const char* built()   { return SWORDFISH_BUILT; }

} // namespace sf::cli

namespace {

struct command {
    const char* name;
    int (*run)(int, char**);
    const char* summary;
    // Whether the command's own parser understands `-t <templates>`. A leading
    // `-t` is forwarded only to these: `test_main` and `slice_main` have no such
    // option, and handing them the flag made `swordfish -t f blobl 'root = 1'`
    // fail with "mapping parse failed" -- blobl read `-t` as the mapping.
    bool takes_templates = true;
};

const command commands[] = {
    {"run",   sf::cli::run_main,    "run a pipeline from a config file"},
    {"build", sf::cli::build_main,  "compile a config into a native binary"},
    {"lint",  sf::cli::config_main, "check a config and report every problem"},
    {"list",  sf::cli::config_main, "list the components this build implements"},
    {"echo",  sf::cli::config_main, "print a config with ${VAR} resolved"},
    {"create", sf::cli::config_main, "print a starting config for named components"},
    {"test",  sf::cli::test_main,   "run a config's unit tests", false},
    {"streams", sf::cli::streams_main, "run many pipelines with a REST API"},
    {"blobl", sf::cli::slice_main,  "execute a mapping over documents", false},
    // Developer tooling rather than part of the documented surface, but both are
    // reachable today through sfconfig and would otherwise be lost behind the
    // front end.
    {"transforms", sf::cli::config_main, "print the C++ a config's processors compile to"},
    {"emit",       sf::cli::config_main, "print the C++ a config's input and output compile to"},
    {"template",   sf::cli::config_main, "lint config templates and run their tests"},
};

// Commands redpanda-connect has that swordfish has not built. Named here so the
// error can say which of the two mistakes was made; both are planned scope
// rather than something declined.
const char* const unbuilt_reference[] = {"dry-run"};

// Commands SWORDFISH's own plan describes that do not exist yet. `inspect` is
// not a redpanda-connect command at all -- it reports the provenance of a
// COMPILED binary (the config hash it was built from, what compiled, what fell
// back to the interpreter), which is a question only this project has. It was
// listed above as a reference command until 2026-09-08, which told anyone who
// typed it something untrue.
const char* const unbuilt_ours[] = {"inspect"};

int usage(std::ostream& to, int rc) {
    to << "usage: swordfish [-t <templates>] <command> [args]\n\n";
    for (const auto& c : commands)
        to << "  " << c.name << std::string(11 - std::strlen(c.name), ' ')
           << c.summary << "\n";
    // `-t` is documented here as well as in each command's own --help, because
    // it may now appear before the command and a reader who only sees the
    // subcommand list would not know that.
    to << "\n  -t, --templates <file>   import config templates; may be repeated,\n"
          "                           and may come before or after the command\n"
          "  --version    print the version and build stamp\n"
          "  --help       print this message\n\n"
          "Every command also accepts --help.\n";
    return rc;
}

} // namespace

int main(int argc, char** argv) {
    if (argc < 2) return usage(std::cerr, 2);

    // A global flag may come BEFORE the command, because it may for
    // redpanda-connect: `redpanda-connect -t templates/*.yaml lint config.yaml`
    // is the form its own documentation uses. Only the trailing position worked
    // here, so that command line failed with "'-t' is not a command" -- and the
    // whole point of the front end is that a command line written for the
    // reference runs unchanged. The flags are collected and handed to the
    // command after its name, which is where every subcommand's parser already
    // looks for them, so nothing downstream has to know they moved.
    std::vector<char*> globals;
    int at = 1;
    while (at < argc) {
        const std::string a = argv[at];
        if (a != "-t" && a != "--templates") break;
        if (at + 1 >= argc) {
            std::cerr << "swordfish: '" << a << "' needs a template file\n";
            return usage(std::cerr, 2);
        }
        globals.push_back(argv[at]);
        globals.push_back(argv[at + 1]);
        at += 2;
    }
    if (at >= argc) return usage(std::cerr, 2);
    const std::string cmd = argv[at];

    // `--version` and `--help` answer without dispatching, so a `-t` before
    // them would be collected and then quietly dropped. Named instead, for the
    // same reason `blobl` names it: a flag that does nothing is worse than one
    // that is refused.
    if (!globals.empty() && (cmd == "--version" || cmd == "-v" || cmd == "version"
                             || cmd == "--help" || cmd == "-h" || cmd == "help")) {
        std::cerr << "swordfish: '" << cmd << "' does not use templates; drop the -t flag\n";
        return 2;
    }

    if (cmd == "--version" || cmd == "-v" || cmd == "version") {
        std::cout << "swordfish " << sf::cli::version()
                  << " (built " << sf::cli::built() << ")\n";
        return 0;
    }
    // `--help` before a command; `swordfish run --help` belongs to `run`.
    if (cmd == "--help" || cmd == "-h" || cmd == "help") return usage(std::cout, 0);

    for (const auto& c : commands) {
        if (cmd != c.name) continue;
        // argv is rewritten rather than shifted: each command was a main() and
        // still reads argv[0] as its own name -- seastar::app_template prints it
        // in usage and error messages -- so it has to say what the user typed,
        // not "swordfish". config_main and slice_main are the exception: each
        // dispatches on argv[1] itself, so their subcommand word stays in place.
        const bool keep_subcommand =
            c.run == sf::cli::config_main || c.run == sf::cli::slice_main;
        // When the subcommand word stays in argv[1], argv[0] must NOT also
        // carry it: config_main prints `argv[0] << " " << cmd`, which read
        // "swordfish echo echo <config.yaml>".
        std::string name = keep_subcommand ? std::string("swordfish")
                                           : "swordfish " + cmd;
        // A command that cannot use the flag is told so rather than handed it.
        // The reference accepts `-t` everywhere and ignores it where it means
        // nothing; swordfish names it instead, because a flag that silently
        // does nothing is the failure mode this project does not permit -- and
        // the realistic reference command lines (`-t ... lint`, `run`,
        // `streams`, `create`) all reach commands that do use it.
        if (!globals.empty() && !c.takes_templates) {
            std::cerr << "swordfish: '" << cmd
                      << "' does not use templates; drop the -t flag\n";
            return 2;
        }
        std::vector<char*> args;
        args.push_back(name.data());
        if (keep_subcommand) args.push_back(argv[at]);
        // Hoisted flags go before the command's own arguments: config_main
        // stops scanning for them at the first non-flag word, so appending
        // them after a config path would leave them unread.
        for (char* g : globals) args.push_back(g);
        for (int i = at + 1; i < argc; ++i) args.push_back(argv[i]);
        return c.run(static_cast<int>(args.size()), args.data());
    }

    for (const char* u : unbuilt_reference) {
        if (cmd != u) continue;
        std::cerr << "swordfish: '" << cmd
                  << "' is a redpanda-connect command that swordfish has not "
                     "built yet\n";
        return usage(std::cerr, 2);
    }
    for (const char* u : unbuilt_ours) {
        if (cmd != u) continue;
        std::cerr << "swordfish: '" << cmd
                  << "' is a planned swordfish command that is not built yet; "
                     "redpanda-connect has no such command either\n";
        return usage(std::cerr, 2);
    }

    std::cerr << "swordfish: '" << cmd << "' is not a command\n";
    return usage(std::cerr, 2);
}
