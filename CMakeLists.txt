cmake_minimum_required(VERSION 3.20)
project(swordfish CXX)

# C++23, pinned deliberately.
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
# Abseil ships ~100 interdependent static archives, so a hand-written link line
# is order-sensitive and fragile. Building libswordfish shared bakes the whole
# transitive closure in, and `swordfish-config` (below) hands out the one flag
# set that generated code needs.
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()
set(CMAKE_CXX_FLAGS_RELEASE "-O2 -g")

# ---- third-party ------------------------------------------------------------
# Built from source in-tree rather than linked prebuilt: both are C++, and we pin
# GCC 16 / C++23 deliberately. Compiling them with
# the same compiler and flags removes any ABI mismatch.

# simdjson ships an amalgamation; that pair IS the distribution.
add_library(simdjson STATIC third_party/simdjson/singleheader/simdjson.cpp)
target_include_directories(simdjson SYSTEM PUBLIC third_party/simdjson/singleheader)

# Abseil must be added before RE2: RE2's `if(NOT TARGET absl::base)` then finds
# these targets instead of looking for an installed copy.
set(ABSL_PROPAGATE_CXX_STD ON  CACHE BOOL "" FORCE)
set(ABSL_ENABLE_INSTALL    OFF CACHE BOOL "" FORCE)
set(ABSL_BUILD_TESTING     OFF CACHE BOOL "" FORCE)
set(BUILD_TESTING          OFF CACHE BOOL "" FORCE)
add_subdirectory(third_party/abseil-cpp EXCLUDE_FROM_ALL)

# RE2 specifically, not std::regex: Go's `regexp` IS RE2, so it is the only
# choice that keeps Bloblang's ~40 regex methods semantically identical.
set(RE2_BUILD_TESTING OFF CACHE BOOL "" FORCE)
# We vendor RE2 rather than install it, and its export set would otherwise
# reference Abseil targets that ABSL_ENABLE_INSTALL=OFF keeps out of any export.
set(RE2_INSTALL      OFF CACHE BOOL "" FORCE)
add_subdirectory(third_party/re2 EXCLUDE_FROM_ALL)

# Apache Avro C++ (vendored; see third_party/avro-cpp/PROVENANCE.md). Needed for
# the schema_registry decode. Only the library is built: the codegen tool and
# the test suite are off, which also leaves the removed test/ and jsonschemas/
# directories unreferenced.
#
# Gated on fmt being installed. Avro's find_package(fmt) falls back to a git
# clone, which fails on a machine without network -- better to skip Avro with a
# clear message than to fail the whole configure. Run ./install_packages.sh.
option(SWORDFISH_ENABLE_AVRO "Build the vendored Apache Avro C++ library" ON)
if (SWORDFISH_ENABLE_AVRO)
  find_package(fmt QUIET)
  if (fmt_FOUND)
    set(AVRO_BUILD_EXECUTABLES OFF CACHE BOOL "" FORCE)
    set(AVRO_BUILD_TESTS       OFF CACHE BOOL "" FORCE)
    set(AVRO_BUILD_SHARED      OFF CACHE BOOL "" FORCE)
    set(AVRO_BUILD_STATIC      ON  CACHE BOOL "" FORCE)
    set(AVRO_USE_BOOST         OFF CACHE BOOL "" FORCE)
    add_subdirectory(third_party/avro-cpp EXCLUDE_FROM_ALL)
    # Its headers are SYSTEM headers to everything that consumes them. avro-cpp's
    # NodeConcepts.hh trips clang's -Wdeprecated-copy-with-user-provided-copy
    # five times, and a warning in a vendored dependency is noise that hides the
    # ones that matter -- the same reason the other third_party trees are
    # excluded from the warning counts.
    get_target_property(SF_AVRO_INCLUDES avrocpp_s INTERFACE_INCLUDE_DIRECTORIES)
    if (SF_AVRO_INCLUDES)
      set_target_properties(avrocpp_s PROPERTIES
          INTERFACE_SYSTEM_INCLUDE_DIRECTORIES "${SF_AVRO_INCLUDES}")
    endif ()
    set(SWORDFISH_HAVE_AVRO ON)
  else ()
    message(STATUS
      "avro-cpp: skipped -- fmt not found. Install fmt-devel (./install_packages.sh) "
      "to enable it. Only the schema_registry/avro components need it.")
  endif ()
endif ()

# Catch2, from the amalgamation. Test-only, so it is deliberately NOT linked
# into libswordfish.
add_library(catch2 STATIC third_party/catch2/catch_amalgamated.cpp)
target_include_directories(catch2 SYSTEM PUBLIC third_party/catch2)

set(YAML_CPP_BUILD_TESTS   OFF CACHE BOOL "" FORCE)
set(YAML_CPP_BUILD_TOOLS   OFF CACHE BOOL "" FORCE)
set(YAML_CPP_BUILD_CONTRIB OFF CACHE BOOL "" FORCE)
set(YAML_CPP_FORMAT_SOURCE OFF CACHE BOOL "" FORCE)
add_subdirectory(third_party/yaml-cpp EXCLUDE_FROM_ALL)

add_library(swordfish SHARED
  src/value.cc
  src/methods.cc
  src/methods_string.cc
  src/methods_number.cc
  src/methods_structured.cc
  src/methods_time.cc
  src/functions.cc
  src/methods_parse.cc
  src/methods_crypto.cc
  src/methods_jwt.cc
  src/methods_compress.cc
  src/methods_msgpack.cc
  src/methods_dynamic.cc
  src/methods_bcrypt.cc
  src/methods_schema.cc
  src/blowfish_tables.cc
  src/case_tables.cc
  src/codecs.cc
  src/blobl/method_registry.cc
  src/json.cc
  src/runtime.cc
  src/message.cc
  src/regex.cc
  src/config/yaml.cc
  src/config/template.cc
  src/config/spec.cc
  src/blobl/parser.cc
  src/blobl/names.cc
  src/blobl/interp.cc
  src/blobl/emit.cc)

# The Kafka client is its own library so a pipeline that does not use Kafka
# links neither it nor the four compression libraries it needs -- the same
# dead-component elimination the generated build driver does for connectors.
#
# protocol.cc is generated from the official Kafka schemas by
# tools/gen_kafka_protocol.py, and checked in rather than generated at build
# time: it needs Python, it changes only when a schema does, and a reviewer
# should see the diff.
add_library(swordfish_kafka STATIC
  src/kafka/generated/messages.cc
  src/kafka/records.cc)
target_include_directories(swordfish_kafka PUBLIC src/kafka/generated)
# The compression libraries come from `swordfish` now: Bloblang's compress() and
# decompress() need the same codecs, so they live in the base library and the
# Kafka batch codec calls them.
target_link_libraries(swordfish_kafka PUBLIC swordfish)
target_include_directories(swordfish PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
# libcrypto: the Bloblang catalogue includes hashing, AES and JWT, so the core
# library needs a crypto provider even when nothing networked is linked. OpenSSL
# is already the project's choice for TLS (§ 9.4) and 3.x is Apache-2.0.
find_package(OpenSSL REQUIRED COMPONENTS Crypto)

# The compression codecs. Bloblang's compress()/decompress() cover seven
# algorithms, so these belong to the base library rather than to Kafka, which
# was simply the first caller.
find_package(PkgConfig REQUIRED)
pkg_check_modules(ZLIB  REQUIRED IMPORTED_TARGET zlib)
pkg_check_modules(LZ4   REQUIRED IMPORTED_TARGET liblz4)
pkg_check_modules(ZSTD  REQUIRED IMPORTED_TARGET libzstd)
pkg_check_modules(BZIP2 IMPORTED_TARGET bzip2)
target_link_libraries(swordfish PRIVATE simdjson yaml-cpp re2::re2 OpenSSL::Crypto
                      PkgConfig::ZLIB PkgConfig::LZ4 PkgConfig::ZSTD snappy bz2)
target_compile_options(swordfish PRIVATE -Wall -Wextra -Wno-unused-parameter)

add_executable(sfslice src/cli/main.cc src/cli/shims/slice.cc)
target_link_libraries(sfslice PRIVATE swordfish)



# ---- Seastar runtime ---------------------------------------------------------
# Optional so the config and Bloblang work still builds on a machine without
# Seastar installed. seastar.pc omits the TLS backend libraries, so ssl/crypto
# are added explicitly.
find_package(PkgConfig)
if (PkgConfig_FOUND)
  pkg_check_modules(SEASTAR QUIET seastar)
endif ()
if (SEASTAR_FOUND)
  add_library(swordfish_rt
    src/runtime/transaction.cc
    src/runtime/transform.cc
    src/runtime/stream.cc
    src/runtime/cache.cc
    src/runtime/observe.cc
    src/testing/unit_test.cc
    src/runtime/batching.cc
    src/runtime/rate_limit.cc
    src/runtime/build_stream.cc
    src/runtime/parse_pipeline.cc
    src/components/basic.cc
    src/components/scanners.cc
    src/components/scanner_avro.cc
    src/components/registry.cc
  src/components/builtin_docs.cc
    src/components/processor_defs.cc
    src/components/io.cc
    src/components/outputs.cc
    src/components/http.cc
    src/components/socket.cc
    src/components/processors.cc
    src/codegen/emit_main.cc)
  # The STATIC_ variants are required: libseastar.a's dependencies live in
  # seastar.pc's Libs.private, which the non-static flags omit. And ssl/crypto
  # are added by hand because seastar.pc does not list its TLS backend at all.
  # seastar.pc lists -lgnutls unconditionally, even when Seastar was built with
  # Seastar_GNUTLS=OFF and Seastar_OPENSSL=ON -- which it is here, and
  # libseastar.a proves it: zero GnuTLS symbols, 75 OpenSSL ones. Left in, the
  # flag puts a NEEDED entry for an LGPL library into every binary
  # `swordfish build` produces, for code that is never called. Since those
  # binaries are redistributed by USERS, that is exactly the obligation the
  # OpenSSL decision was made to avoid.
  list(REMOVE_ITEM SEASTAR_STATIC_LDFLAGS "-lgnutls")
  target_compile_options(swordfish_rt PUBLIC ${SEASTAR_STATIC_CFLAGS})
  # Reported by /version. The build stamp is UTC and ISO 8601, matching the
  # shape redpanda-connect reports so a scraper can parse both the same way.
  string(TIMESTAMP SF_BUILT "%Y-%m-%dT%H:%M:%SZ" UTC)
  # Written ONCE and used by both the runtime library (which serves /version)
  # and the `swordfish` front end (which prints --version). Two literals would
  # have been two things to remember to change, and the failure -- the endpoint
  # and the CLI disagreeing about which build this is -- is the kind nobody
  # notices until they are debugging something else.
  set(SF_VERSION_DEFS SWORDFISH_VERSION="0.1.0-dev" SWORDFISH_BUILT="${SF_BUILT}")
  target_compile_definitions(swordfish_rt PRIVATE ${SF_VERSION_DEFS})
  target_link_libraries(swordfish_rt PUBLIC swordfish ${SEASTAR_STATIC_LDFLAGS} ssl crypto)
  # The `avro` scanner is the one component that needs a third-party library,
  # and that library is optional: without fmt there is no avro-cpp, and the
  # scanner compiles down to a named "this build has no Avro support" error.
  # SWORDFISH_HAVE_AVRO is PUBLIC because scanners.cc consults
  # avro_scanner_available() to decide whether to offer the kind at all.
  if (SWORDFISH_HAVE_AVRO)
    target_link_libraries(swordfish_rt PUBLIC avrocpp_s)
    target_compile_definitions(swordfish_rt PUBLIC SWORDFISH_HAVE_AVRO)
    set(SF_AVRO_LIBS " ${CMAKE_BINARY_DIR}/third_party/avro-cpp/libavrocpp_s.a -lfmt")
  else ()
    set(SF_AVRO_LIBS "")
  endif ()

  # The Kafka client needs the reactor, so it sits on this side of the split
  # while the codecs (which do not) stay in swordfish_kafka. Same reasoning as
  # swordfish / swordfish_rt: a target that only parses configs should not have
  # to link a network stack.
  add_library(swordfish_kafka_rt
    src/kafka/client.cc
    src/kafka/cluster.cc
    src/kafka/consumer_group.cc
    src/kafka/consumer.cc
    src/kafka/group_membership.cc
    src/kafka/producer.cc
    src/kafka/components.cc)
  target_compile_options(swordfish_kafka_rt PUBLIC ${SEASTAR_STATIC_CFLAGS})
  target_link_libraries(swordfish_kafka_rt PUBLIC swordfish_kafka swordfish_rt)

  add_executable(swordfish-run src/cli/run_main.cc src/cli/shims/run.cc)
  target_link_libraries(swordfish-run PRIVATE swordfish_rt swordfish_kafka_rt)

  add_executable(swordfish-build src/cli/build_main.cc src/cli/shims/build.cc)
  target_link_libraries(swordfish-build PRIVATE swordfish_rt swordfish_kafka_rt)

  # sfconfig lints against the component registry, which lives with the
  # components, so it links the runtime library.
  add_executable(swordfish-test src/cli/test_main.cc src/cli/shims/test.cc)
  target_link_libraries(swordfish-test PRIVATE swordfish_kafka_rt swordfish_rt)

  add_executable(sfconfig src/cli/config_main.cc src/cli/shims/config.cc)

  # `swordfish` -- the single front end, and the only place `swordfish --version`
  # can live. It links the
  # same entry points the five per-command executables call, so the two can
  # never disagree about what a command does.
  #
  # sfslice's sources come in directly: it is the one command that does not need
  # the reactor, and it is not in a library.
  # The target is swordfish_cli, not swordfish: the SHARED LIBRARY already owns
  # that target name, and CMake target names are a single flat namespace.
  # OUTPUT_NAME is what decides the file on disk, and that is `swordfish`.
  add_executable(swordfish_cli src/cli/swordfish_main.cc
                               src/cli/run_main.cc
                               src/cli/build_main.cc
                               src/cli/test_main.cc
                               src/cli/config_main.cc
                               src/cli/streams_main.cc
                               src/cli/main.cc)
  set_target_properties(swordfish_cli PROPERTIES OUTPUT_NAME swordfish)
  target_link_libraries(swordfish_cli PRIVATE swordfish_rt swordfish_kafka_rt)
  # The same stamp the runtime library and the /version endpoint report. Without
  # it `swordfish --version` would say 0.0.0-dev while /version said the truth.
  target_compile_definitions(swordfish_cli PRIVATE ${SF_VERSION_DEFS})
  target_link_libraries(sfconfig PRIVATE swordfish_rt swordfish_kafka_rt)


  # The runtime suite uses Seastar's own test framework (Boost.Test based), not
  # Catch2: these cases are coroutines running inside the reactor.
  pkg_check_modules(SEASTAR_TESTING QUIET seastar-testing)
  add_executable(sfrttest tests/test_runtime.cc)
  target_compile_options(sfrttest PRIVATE ${SEASTAR_TESTING_STATIC_CFLAGS})
  target_link_libraries(sfrttest PRIVATE swordfish_rt ${SEASTAR_TESTING_STATIC_LDFLAGS})
  add_test(NAME runtime COMMAND sfrttest -- --smp 2 --memory 512M --overprovisioned)

  # Integration against a REAL broker. Skipped, not failed, when none is
  # reachable: the address comes from SWORDFISH_KAFKA_BROKER, and a developer
  # without a broker still gets a green suite.
  add_executable(sfkafkaitest tests/test_kafka_integration.cc)
  target_compile_options(sfkafkaitest PRIVATE ${SEASTAR_TESTING_STATIC_CFLAGS})
  target_link_libraries(sfkafkaitest PRIVATE swordfish_kafka_rt
                        ${SEASTAR_TESTING_STATIC_LDFLAGS})
  # --smp 2 deliberately: the per-shard assignment cache was once a shared
  # static, which only a multi-shard run can catch.
  add_test(NAME kafka_integration
           COMMAND sfkafkaitest -- --smp 2 --memory 512M --overprovisioned)

  # The chaos member, driven by tools/run_kafka_chaos.sh. Deliberately NOT a
  # ctest entry: it is meaningless without the three-broker cluster that script
  # stands up, and a ctest case that quietly passed without one would be worse
  # than no case at all -- that is exactly how kafka_integration can report
  # green having tested nothing.
  add_executable(sfkafkachaos tests/test_kafka_chaos.cc)
  target_compile_options(sfkafkachaos PRIVATE ${SEASTAR_STATIC_CFLAGS})
  target_link_libraries(sfkafkachaos PRIVATE swordfish_kafka_rt)
  message(STATUS "seastar: found -- runtime targets enabled")
else ()
  message(STATUS "seastar: not found -- runtime targets skipped "
                 "(set PKG_CONFIG_PATH to seastar's build directory)")
endif ()

# ---- precompiled header for generated code ---------------------------------
# Built with the EXACT flags swordfish-config hands out: GCC silently ignores a
# .gch whose flags differ from the consumer's, which would cost the benefit
# without any diagnostic.
string(REPLACE ";" " " SF_SEASTAR_CFLAGS "${SEASTAR_STATIC_CFLAGS}")
string(REPLACE ";" " " SF_SEASTAR_LIBS   "${SEASTAR_STATIC_LDFLAGS}")
set(SF_SEASTAR_LIBS "${SF_SEASTAR_LIBS} -lssl -lcrypto")

# A precompiled header was tried here and removed. Measured on the generated
# main.cc: 3.65s with the PCH against 3.55s without. `-fsyntax-only` suggested a
# 2.4s win, but that measures parsing, which a PCH caches; the real cost is
# EMITTING the Seastar template instantiations, which it cannot.

# swordfish-config: what `swordfish build` uses to compile generated code.
#
# The Kafka libraries come BEFORE swordfish_rt: a static archive only yields the
# members that something already unresolved needs, so a library must be listed
# ahead of the ones it depends on. They are listed unconditionally for now --
# per-config link lines are what the codegen_entry table is for, and this is the
# placeholder until dead-connector elimination lands.
file(GENERATE
  OUTPUT ${CMAKE_BINARY_DIR}/swordfish-config
  CONTENT "#!/bin/sh
# Generated by CMake. Flags for compiling and linking swordfish-generated code.
case \"$1\" in
  --cflags) echo \"-std=c++23 -I${CMAKE_CURRENT_SOURCE_DIR}/include ${SF_SEASTAR_CFLAGS}\" ;;
  --libs)   echo \"-L${CMAKE_BINARY_DIR} -lswordfish_kafka_rt -lswordfish_kafka -lswordfish_rt -lswordfish -lz -llz4 -lzstd -lsnappy -lbz2 -lcrypto${SF_AVRO_LIBS} -Wl,-rpath,${CMAKE_BINARY_DIR} ${SF_SEASTAR_LIBS}\" ;;
  *) echo \"usage: swordfish-config --cflags|--libs\" >&2; exit 2 ;;
esac
"
  FILE_PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE
                   WORLD_READ WORLD_EXECUTE)
enable_testing()
add_executable(sftest tests/test_all.cc)
target_link_libraries(sftest PRIVATE swordfish catch2)
add_test(NAME unit COMMAND sftest)

add_executable(sfconformance tests/test_conformance.cc)
target_link_libraries(sfconformance PRIVATE swordfish catch2)
# Fixtures are looked up relative to the source tree.
add_test(NAME conformance COMMAND sfconformance
         WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

# The differential gate: every fixture that parses is run through the
# interpreter AND through compiled code, and the two answers are compared. It is
# a two-stage build -- sfdiffgen writes a .cc which is then compiled -- so it is
# driven by tools/run_differential.sh rather than by a single target.
add_executable(sfdiffgen tools/gen_differential.cc)
target_link_libraries(sfdiffgen PRIVATE swordfish)
add_test(NAME differential
         COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/tools/run_differential.sh
                 $<TARGET_FILE:sfdiffgen> ${CMAKE_CURRENT_BINARY_DIR}
         WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

add_executable(sfkafkatest tests/test_kafka.cc)
# The assignment strategies live in the Seastar-side library (they sit next to
# the group protocol that uses them) but are pure arithmetic, so this Catch2
# suite exercises them without a reactor.
if (SEASTAR_FOUND)
  target_link_libraries(sfkafkatest PRIVATE swordfish_kafka_rt catch2)
else ()
  target_link_libraries(sfkafkatest PRIVATE swordfish_kafka catch2)
endif ()
add_test(NAME kafka COMMAND sfkafkatest)

add_executable(sfconfigtest tests/test_config.cc)
# The config suite covers the component registry, which lives with the
# components, so it links the runtime library when one was built.
if (SEASTAR_FOUND)
  target_link_libraries(sfconfigtest PRIVATE swordfish_rt catch2)
else ()
  target_link_libraries(sfconfigtest PRIVATE swordfish catch2)
endif ()
add_test(NAME config COMMAND sfconfigtest)

# Pipeline behaviour pinned in the Benthos unit-test format and run by our own
# `swordfish test`. These assert on batch STRUCTURE, which the other gates
# cannot see: stdout flattens batches, so nothing else here distinguishes one
# batch of four from two batches of two. Every fixture is also verified against
# redpanda-connect, so it pins the reference's behaviour rather than ours.
# Inputs are only observable by running a pipeline, so they are checked by a
# script rather than by `swordfish test`. It compares against redpanda-connect
# whenever that binary is present, so the expectations are the reference's.
if (TARGET swordfish-run)
  add_test(NAME inputs
           COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_inputs_check.sh
                   $<TARGET_FILE:swordfish-run>)
  # Outputs, for the same reason: broker/switch/fallback/reject are only
  # observable by running a pipeline, and this compares against the reference
  # whenever it is present.
  add_test(NAME outputs
           COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_outputs_check.sh
                   $<TARGET_FILE:swordfish-run>)
  # The networked connectors need a server running while a client talks to it,
  # which no single-process test can express. Where redpanda-connect is present
  # this runs OUR client against ITS server and the reverse, which is the only
  # evidence the wire format matches rather than our encoder agreeing with our
  # own decoder.
  add_test(NAME http
           COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_http_check.sh
                   $<TARGET_FILE:swordfish-run>)
  add_test(NAME socket
           COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_socket_check.sh
                   $<TARGET_FILE:swordfish-run>)
  # Rate limits are observable only in TIME, which no other gate measures: every
  # check there would pass with the throttling removed. This one asserts on
  # elapsed milliseconds, so it needs a generous timeout of its own.
  add_test(NAME rate_limit
           COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_rate_limit_check.sh
                   $<TARGET_FILE:swordfish-run>)
  set_tests_properties(rate_limit PROPERTIES TIMEOUT 1200)
  # The document-level blocks that are not components. Several of them were in
  # the root schema and read by nobody, so this gate insists that each is either
  # OBSERVED to change behaviour or refused by name -- "it lints clean" is what
  # the broken state also did.
  # Every scanner, compared against redpanda-connect on the same bytes. A
  # scanner decides where one message ends and the next begins, so an error
  # changes the SHAPE of the data rather than losing a feature -- and an
  # expectation written by whoever wrote the scanner would agree with it.
  add_test(NAME scanners
           COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_scanners_check.sh
                   $<TARGET_FILE:swordfish-run> ../redpanda-connect $<TARGET_FILE:sfconfig>)
  set_tests_properties(scanners PROPERTIES TIMEOUT 1800)
  # The `avro` scanner, which has enough of its own surface to warrant a gate of
  # its own: two output formats, two metadata fields, four container codecs and
  # a set of deliberately malformed containers. Its fixtures are generated
  # rather than committed as binaries, so what each one tests is readable.
  add_test(NAME avro
           COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_avro_check.sh
                   $<TARGET_FILE:swordfish-run> ../redpanda-connect $<TARGET_FILE:sfconfig>)
  set_tests_properties(avro PROPERTIES TIMEOUT 1800)
  # The `buffer` block. A buffer changes WHEN the source is acknowledged rather
  # than what comes out, so this gate's core case is whether the process
  # terminates, not what it printed.
  add_test(NAME buffer
           COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_buffer_check.sh
                   $<TARGET_FILE:swordfish-run> ../redpanda-connect $<TARGET_FILE:sfconfig>)
  set_tests_properties(buffer PROPERTIES TIMEOUT 1800)
  # `cache_resources`. A cache is observable only through which messages survive
  # `dedupe`, which is what makes each option testable: a cap-1 LRU lets every
  # repeat through, an init_values key suppresses its first occurrence.
  add_test(NAME cache
           COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_cache_check.sh
                   $<TARGET_FILE:swordfish-run> ../redpanda-connect $<TARGET_FILE:sfconfig>)
  set_tests_properties(cache PROPERTIES TIMEOUT 1800)
  # The `swordfish` front end. Its commands each have their own gate; what this
  # one checks is that dispatching through the front end reaches the SAME code,
  # and the two things only the front end does -- `--version` and
  # telling an unbuilt command apart from an unknown one.
  add_test(NAME cli COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_cli_check.sh
                            $<TARGET_FILE:swordfish_cli>)
  set_tests_properties(cli PROPERTIES TIMEOUT 1800)
  # `swordfish streams`. Its API shapes are compared against redpanda-connect's
  # on the same configs, because a dashboard written against the reference's
  # streams API is meant to keep working; what cannot be compared -- uptimes and
  # counters -- is checked structurally instead.
  add_test(NAME streams COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_streams_check.sh
                                $<TARGET_FILE:swordfish_cli> ../redpanda-connect)
  set_tests_properties(streams PROPERTIES TIMEOUT 1800)
  # Config templates. A template is a component defined by a Bloblang mapping,
  # so what matters is that the config it PRODUCES behaves identically in both
  # implementations -- and that the same file compiles to a binary that behaves
  # the same way again.
  add_test(NAME templates COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_templates_check.sh
                                  $<TARGET_FILE:swordfish_cli> ../redpanda-connect)
  set_tests_properties(templates PROPERTIES TIMEOUT 1800)
  # `swordfish echo` and the YAML writer under it, which the streams API also
  # uses to report a stream's config. The core case round-trips every real
  # config in the tree: what the writer produces must parse back to what it was
  # given, quoting included.
  add_test(NAME echo COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_echo_check.sh
                             $<TARGET_FILE:swordfish_cli> $<TARGET_FILE:sfconfig>
                             ../redpanda-connect)
  set_tests_properties(echo PROPERTIES TIMEOUT 900)
  # Per-component metrics. The claim is a compatibility one -- every series
  # swordfish exports must be one the reference also exports, with the same
  # name, path and label -- so the gate scrapes both and compares identities.
  add_test(NAME metrics COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_metrics_check.sh
                                $<TARGET_FILE:swordfish_cli> ../redpanda-connect)
  set_tests_properties(metrics PROPERTIES TIMEOUT 900)
  # `swordfish list`, and the property that keeps it honest: what it shows must
  # be what the parser accepts. The built-ins are documented from the same table
  # the parser takes its field names from, and this gate checks the pairing by
  # linting a config for every field the listing advertises.
  add_test(NAME list COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_list_check.sh
                             $<TARGET_FILE:swordfish_cli> ../redpanda-connect)
  set_tests_properties(list PROPERTIES TIMEOUT 900)
  # `swordfish create`, whose one real obligation is that what it prints is a
  # config swordfish will ACCEPT -- the first version emitted `scanner` and the
  # deprecated `codec` together, which the parser refuses. Every component this
  # build implements is scaffolded and linted, and the ones with no required
  # field are run, since linting is the weaker claim.
  add_test(NAME create COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_create_check.sh
                               $<TARGET_FILE:swordfish_cli> ../redpanda-connect)
  set_tests_properties(create PROPERTIES TIMEOUT 900)
  add_test(NAME engine_config
           COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_engine_config_check.sh
                   $<TARGET_FILE:swordfish-run> ../redpanda-connect $<TARGET_FILE:sfconfig>)
  set_tests_properties(engine_config PROPERTIES TIMEOUT 1200)
endif ()

if (TARGET swordfish-test)
  add_test(NAME pipelines
           COMMAND swordfish-test --targets
                   ${CMAKE_CURRENT_SOURCE_DIR}/tests/fixtures/pipelines
                   --smp 1 --memory 512M --overprovisioned)
  set_tests_properties(pipelines PROPERTIES
      ENVIRONMENT "SWORDFISH_CONFIG=${CMAKE_BINARY_DIR}/swordfish-config")
endif ()

# ---- warnings on every first-party target ------------------------------------
# Previously only `swordfish` carried these, so the Kafka libraries, the runtime
# libraries and every executable compiled with no warnings enabled at all --
# which is most of the codebase. Applied here at the end rather than at each
# definition so a target added later is caught by the loop instead of being
# silently omitted. Third-party targets are deliberately excluded: their
# warnings are not ours to fix and would bury ours.
set(SF_FIRST_PARTY
    swordfish swordfish_kafka swordfish_rt swordfish_kafka_rt
    sfslice swordfish-run swordfish-build sfconfig swordfish-test
    sftest sfconformance sfkafkatest sfconfigtest sfrttest
    sfkafkaitest sfkafkachaos sfdiffgen)
foreach(t IN LISTS SF_FIRST_PARTY)
  if(TARGET ${t})
    target_compile_options(${t} PRIVATE -Wall -Wextra -Wno-unused-parameter $ENV{SF_EXTRA_WARN})
    # SF_EXTRA_FLAGS lets an audit build add sanitizers to FIRST-PARTY targets
    # only. Putting -fsanitize in CMAKE_CXX_FLAGS instead fails the build:
    # Abseil's hash_policy_traits.h has a constexpr that GCC will not accept
    # under instrumentation. Sanitizing only our own translation units is the
    # thing we actually want, and it sidesteps that entirely.
    if(NOT "$ENV{SF_EXTRA_FLAGS}" STREQUAL "")
      target_compile_options(${t} PRIVATE $ENV{SF_EXTRA_FLAGS})
      target_link_options(${t} PRIVATE $ENV{SF_EXTRA_FLAGS})
    endif()
  endif()
endforeach()
