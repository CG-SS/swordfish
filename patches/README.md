# Patches to vendored dependencies

Every local modification to a dependency lives here as a file, and nowhere else.
Nothing is edited in a dependency's tree without a patch in this directory
recording it.

The reason is the next upgrade. When Seastar is refreshed, whoever does it has to
be able to see the complete list of what this project changed and decide, for
each one, whether it still applies or has been fixed upstream. An edit made
directly in `seastar-master/` is invisible to that person: it is silently lost
when the tree is replaced, or silently kept when upstream has since done it
better.

## The rule

**Prefer a fix on our own side.** A patch is a fork, and a fork is a cost paid at
every upgrade. Seastar was pristine until 2026-09-06 and the project had been
deliberate about keeping it so: where `seastar.pc` lists `-lgnutls`
unconditionally even in an OpenSSL build, `CMakeLists.txt` strips the flag with
`list(REMOVE_ITEM SEASTAR_STATIC_LDFLAGS "-lgnutls")` rather than touching the
`.pc` file. That is the shape to reach for first.

Patch only when the dependency genuinely offers no other route, and say so in the
patch's own header: what it changes, why the project needs it, and what would let
it be dropped.

## Applying them

Patches live in a subdirectory named for the dependency, and `apply.sh` takes
the dependency's tree and applies only that subdirectory's patches:

```
./patches/apply.sh ../seastar-master     # idempotent; skips what is applied
cd ../seastar-master/build && ninja
```

`apply.sh` checks each patch with `patch --dry-run -R` first, so re-running it on
an already-patched tree is a no-op rather than a mess. Rebuilding Seastar is
cheap: that patch touches three files and ninja rebuilds twelve steps.

**The two dependencies are consumed differently, and it matters here.** Seastar
is external — `pkg_check_modules(SEASTAR QUIET seastar)` against a build
directory outside this repository — so its patch has to be applied to that tree
before it takes effect, and re-applied whenever the tree is replaced. avro-cpp
is vendored **inside** `third_party/`, so its patch is already applied: the file
in `patches/avro-cpp/` is a RECORD of an edit that is in the tree, kept so that
the next upgrade can see it. Running

```
./patches/apply.sh third_party/avro-cpp
```

should therefore always say "already applied", and it is worth running after a
re-vendor to find out that it does not.

## What is patched, and why

| Patch | Dependency | Why it could not be done on our side |
|---|---|---|
| `seastar/0001-httpd-expose-client-disconnect-to-handlers.patch` | Seastar | An `http_server` output streaming to a client that disconnects kept taking batches from the shared queue and acking them as delivered, because a peer's FIN leaves the socket writable and every write still succeeds. Seastar's own read fiber discovers the disconnect — it is what ends the read loop — but `httpd::connection` holds its `connected_socket` privately and its only friend is `http_server_tester`, which exposes listeners rather than connections. There is no route from a handler to that information. The patch makes it observable and changes nothing else. |
| `avro-cpp/0002-generic-bound-recursion-and-declared-counts.patch` | avro-cpp | `GenericReader::read` recurses once per level of nesting with no limit, and sizes an array or map from a count the FILE declares before reading an element. An OCF file is untrusted input for the `avro` scanner: a 400 KB file nested 200,000 deep **segfaulted the process**, and a 167-byte file declaring an array of 2^40 ints asked for terabytes. Neither is preventable from our side — the recursion and the allocation are both inside the dependency, and a datum's nesting depth is not knowable without decoding it, so there is nothing to check before handing the bytes over. Found by the 2026-09-07 audit; `redpanda-connect` refuses both files cleanly. |

## On dropping one

If a Seastar upgrade makes a patch unnecessary — upstream exposes the same thing,
or the code it touches is gone — delete the patch file and the row above in the
same change, and say in the commit which upstream change replaced it. A patch
that no longer applies is not a merge conflict to be forced through; it is a
question about whether the project still needs it.
