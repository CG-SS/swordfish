# Swordfish

Swordfish runs [Redpanda Connect](https://docs.redpanda.com/redpanda-connect/) /
[Benthos](https://github.com/benthosdev/benthos) YAML pipelines on
[Seastar](https://seastar.io/), and can compile them into standalone native
binaries.

The same configuration works two ways:

- `swordfish run config.yaml` interprets it, the way the original does.
- `swordfish build config.yaml -o ./pipeline` emits C++, compiles it, and
  produces an executable that parses no YAML at run time.

Both paths share one Bloblang implementation and one component set, so they
agree on what a configuration means.

**Status: 0.1.0-dev.** Usable and tested, but not yet at a tagged release. The
component set below is what this build implements; anything outside it is
rejected by name rather than silently ignored.

---

## Example

`example.yaml`:

```yaml
input:
  stdin:
    scanner: { lines: {} }

pipeline:
  processors:
    - mapping: |
        root.id    = this.id
        root.name  = this.name.uppercase()
        root.total = this.items.sum()

output:
  stdout:
    codec: lines
```

Check it, then run it:

```console
$ swordfish lint example.yaml        # silent, exit 0, when there is nothing to say

$ printf '%s\n' '{"id":1,"name":"ada","items":[3,4,5]}' \
                '{"id":2,"name":"grace","items":[10,20]}' \
    | swordfish run example.yaml
{"id":1,"name":"ADA","total":12}
{"id":2,"name":"GRACE","total":30}
```

Or compile it and run the binary, with the same input and the same result:

```console
$ swordfish build example.yaml -o ./pipeline
compiling 3 translation units...
built ./pipeline (2 transforms compiled)

$ printf '%s\n' '{"id":1,"name":"ada","items":[3,4,5]}' \
                '{"id":2,"name":"grace","items":[10,20]}' \
    | ./pipeline
{"id":1,"name":"ADA","total":12}
{"id":2,"name":"GRACE","total":30}
```

`./pipeline` is self-contained: it reads no configuration file and needs no
Swordfish installation.

### Starting from nothing

`swordfish create` writes a configuration skeleton for named components, with
every field at its default and the required ones marked:

```console
$ swordfish create 'kafka/mapping/stdout'
input:
  kafka:
    seed_brokers: []   # A list of broker addresses to connect to. Required, unless given under its legacy alias `addresses`.
    topics: []   # (required) A list of topics to consume from.
    consumer_group: ''   # An optional consumer group to consume as. Without one, every partition is read and nothing is committed.
    ...
pipeline:
  processors:
    - mapping:
        mapping: ''   # A Bloblang mapping applied to each message.
output:
  stdout:
    codec: "lines"   # How messages are framed on the way out. `lines` is what swordfish writes; every other value is refused by name.
```

`swordfish list` prints every component this build implements, with its fields.

---

## Commands

| Command | What it does |
|---|---|
| `run` | Run a pipeline from a config file |
| `build` | Compile a config into a native binary |
| `lint` | Check a config and report every problem |
| `list` | List the components this build implements, with their fields |
| `echo` | Print a config with `${VAR}` resolved |
| `create` | Print a starting config for named components |
| `test` | Run a config's unit tests (Benthos test-file format) |
| `streams` | Run many pipelines at once, with a REST API |
| `blobl` | Execute a Bloblang mapping over documents |
| `template` | Lint config templates and run their tests |
| `transforms`, `emit` | Print the C++ a config compiles to |

Command names and shapes follow Redpanda Connect's where they overlap, so
`swordfish run config.yaml` and `swordfish lint config.yaml` take the config
positionally as `rpk connect` does. `-t/--templates` may appear before or after
the command and may be repeated.

Run-time flags are Seastar's: `--smp`, `--memory`, `--overprovisioned` and the
rest.

---

## What is implemented

**Inputs** — `generate`, `stdin`, `file`, `broker`, `sequence`, `kafka`,
`http_client`, `http_server`, `socket`, `socket_server`, `websocket`

**Outputs** — `stdout`, `drop`, `file`, `broker`, `switch`, `kafka`,
`http_client`, `http_server`, `socket`, `websocket`, `sync_response`

**Processors** — `mapping`, `mutation`, `bloblang`, `branch`, `switch`,
`workflow`, `try`, `catch`, `for_each`, `group_by`, `split`, `archive`,
`unarchive`, `compress`, `decompress`, `dedupe`, `bounds_check`, `insert_part`,
`select_parts`, `rate_limit`, `sleep`, `log`, `noop`, `cpp`

**Scanners** — `lines`, `csv`, `chunker`, `to_the_end`, `json_array`,
`json_documents`, `re_match`, `skip_bom`, `tar`, `decompress`, `avro`

**Caches** — `memory`, `file`, `lru` &nbsp;•&nbsp; **Buffers** — `memory`
&nbsp;•&nbsp; **Rate limits** — `local`

Also supported: config templates, resources (`input_resources`,
`output_resources`, `processor_resources`, `cache_resources`,
`rate_limit_resources`), batching policies, `${VAR}` interpolation, an HTTP
endpoint serving `/metrics` in Prometheus text format, and `shutdown_timeout`,
`shutdown_delay` and `error_handling.strict`.

The `cpp` processor is Swordfish's own: it takes C++ statements (`body`, with
optional extra `includes`) that are compiled into the pipeline alongside the
Bloblang, with `self`, `root` and `ctx` in scope.

Anything not listed is a named error at lint time, with a line number. A field
that exists in Redpanda Connect but is not implemented here is rejected by name
rather than accepted and ignored.

---

## Building from source

Requires Linux, a C++23 compiler (tested with GCC 16), CMake 3.20 or newer, and
Seastar built and discoverable through `pkg-config`.

Library dependencies: OpenSSL (`libcrypto`), zlib, liblz4, libzstd, and
optionally bzip2 and `{fmt}` (which enables the Avro scanner). Everything else —
simdjson, Abseil, RE2, yaml-cpp, Catch2, avro-cpp — is vendored under
`third_party/` and built in-tree.

```console
$ ./patches/apply.sh /path/to/seastar-master   # idempotent; needed once per Seastar tree

$ PKG_CONFIG_PATH=/path/to/seastar/build \
    cmake -S . -B build
$ cmake --build build -j
```

`patches/` holds every local change to a dependency as a file, with the reason
recorded in `patches/README.md`. The trees under `third_party/` ship with their
patches already applied; Seastar is external, so its one patch has to be applied
to whichever checkout you build against. `apply.sh` is idempotent and skips any
patch already present.

The result is `build/swordfish`, plus `swordfish-run`, `swordfish-build`,
`swordfish-test`, `sfconfig` and `sfslice`, which are thin wrappers over the
same entry points.

Seastar must be built with its OpenSSL TLS backend, so that binaries produced by
`swordfish build` carry no LGPL notice obligation to whoever redistributes them.

## Tests

```console
$ ctest --test-dir build --output-on-failure
```

Twenty-five suites: Catch2 unit tests, a Seastar runtime suite, a Bloblang
conformance corpus, a differential harness that requires the interpreter and the
compiled binary to produce byte-identical output, and shell gates for the
connectors, the scanners, the CLI and the config commands.

Three things the shell gates expect:

- **Build into `build/` inside the checkout.** Several gates resolve helper
  binaries at `build/…` relative to the source root, so an out-of-tree build
  directory makes them fail on a missing file rather than on anything real.
- **A `redpanda-connect` binary one level above the checkout**, if you want the
  comparisons. Most gates compare against it when it is there and skip that part
  when it is not; `avro` and `metrics` require it and exit rather than run a
  weaker version of themselves.
- **Benthos and Redpanda Connect checkouts alongside**, for the `echo` gate,
  which round-trips every config in `benthos-main/config`,
  `connect-main/config` and `tests/fixtures` through the YAML writer. Without
  them it finds 12 files instead of 117 and fails rather than declare a
  twelve-file corpus a pass.

With none of those present, expect 24 of the 25 suites to pass and the rest to
report what they are missing. They fail loudly instead of skipping quietly,
which is deliberate: a suite that reports green while testing nothing is worse
than one that says it could not run.

`tools/run_cppcheck.sh` runs static analysis; `bench/` holds the benchmark
drivers.

## Licence

Apache-2.0. See [`LICENSE`](LICENSE) and [`NOTICE`](NOTICE).

Bloblang semantics, component behaviour and configuration schemas are derived
from Benthos (Apache-2.0). Redpanda Connect behaviour is derived from its public
documentation only.

Swordfish is an independent project and is not affiliated with, endorsed by, or
sponsored by Redpanda Data. "Redpanda Connect" and "Benthos" are the marks of
their respective owners and are used here only to describe compatibility.
