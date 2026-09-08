// The `avro` scanner, kept in its own translation unit.
//
// Two reasons it is not in scanners.cc with the other eleven. It is the only
// scanner that needs a third-party library, and that library is optional: a
// build without fmt installed has no avro-cpp, and scanners.cc must still
// compile. And it is the only one that carries a decoder's worth of state, so
// putting it beside the framing scanners would bury them.
#pragma once

#include "swordfish/runtime/scanner.hh"

namespace sf {

// True when this binary was built with avro-cpp. `scanner_kinds()` consults it,
// so a build without Avro reports `avro` as unimplemented rather than accepting
// the config and failing on the first byte.
bool avro_scanner_available();

// Throws the "not built" error when avro_scanner_available() is false.
scanner_ptr make_avro_scanner(bool raw_json);

} // namespace sf
