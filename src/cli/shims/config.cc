// sfconfig — kept as a wrapper over the shared entry point.
//
// The command itself lives in src/cli/, called by both this binary and the
// unified `swordfish` front end, so the two cannot drift. This name survives
// because the gates, ctest and bench/ all invoke it; renaming them would have
// been a change to the test harness dressed up as a change to the CLI.
#include "swordfish/cli/commands.hh"

int main(int argc, char** argv) { return sf::cli::config_main(argc, argv); }
