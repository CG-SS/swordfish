// The subcommands behind the `swordfish` front end.
//
// Each of these was a `main()` in its own executable -- swordfish-run,
// swordfish-build, swordfish-test, sfconfig, sfslice -- so `swordfish run`,
// `swordfish build` and `swordfish lint` had nothing to dispatch through, and
// `swordfish --version` had nowhere to live.
//
// They are entry points now rather than mains, so the front end and the old
// per-command executables call the SAME function. The old names are kept as
// three-line wrappers: the shell gates, ctest and bench/ all invoke them, and a
// rename would have been a change to the test harness disguised as a change to
// the CLI.
#pragma once

namespace sf::cli {

// Each takes argv as its own command sees it: argv[0] is the command's name,
// and argv[1..] are its arguments. The front end rewrites argv[0] to
// "swordfish <cmd>" so a usage line printed from inside says the invocation the
// user actually typed.
//
// The two that build a seastar::app_template -- run and test -- must be the only
// one called in a process, because each starts a reactor.
int run_main(int argc, char** argv);
int build_main(int argc, char** argv);
int test_main(int argc, char** argv);
int streams_main(int argc, char** argv);
int config_main(int argc, char** argv);   // lint, list, transforms
int slice_main(int argc, char** argv);    // blobl

// The version and build stamp reported by `swordfish --version` and by the
// observability endpoint, which must agree: they are the same two macros, and
// they were only ever compiled into the runtime library before.
const char* version();
const char* built();

} // namespace sf::cli
