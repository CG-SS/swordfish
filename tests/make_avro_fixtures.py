#!/usr/bin/env python3
"""Writes the Avro OCF fixtures that tests/test_avro_check.sh compares against.

Hand-rolled rather than produced with an Avro library, for two reasons. There is
no Python Avro package on the build host, and more importantly a generator that
only ever emits what one library happens to write would never exercise the
container's alternative encodings -- a metadata map written as a negative-count
block, a file with no `avro.codec` key at all, a deliberately corrupt sync
marker. Those are the cases the scanner has to get right and the ones a
round-trip test cannot reach.

The output is deterministic: the same bytes every run, so a fixture change shows
up as a diff rather than as a mysterious gate failure.
"""

import os
import struct
import sys
import zlib
from compression import zstd

SYNC = bytes(range(16))


# ---- Avro binary primitives ------------------------------------------------

def zz(n: int) -> bytes:
    """A `long`: zigzag, then base-128 varint."""
    u = (n << 1) ^ (n >> 63) if n < 0 else (n << 1)
    out = bytearray()
    while True:
        b = u & 0x7F
        u >>= 7
        if u:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def blob(b: bytes) -> bytes:
    return zz(len(b)) + b


def st(s: str) -> bytes:
    return blob(s.encode())


def array(items):
    """A block-encoded array or map: a count, the items, then a zero."""
    if not items:
        return zz(0)
    return zz(len(items)) + b"".join(items) + zz(0)


# ---- codecs ----------------------------------------------------------------

def snappy_literal(data: bytes) -> bytes:
    """A valid snappy block that stores everything as literals.

    Snappy's format allows a compressor to emit no back-references at all, so
    this is a legitimate stream rather than a fake one -- the scanner's decoder
    has no idea it was produced without a match search.
    """
    out = bytearray()
    n = len(data)
    while True:                      # the uncompressed length, as a plain varint
        b = n & 0x7F
        n >>= 7
        out.append(b | 0x80 if n else b)
        if not n:
            break
    i = 0
    while i < len(data):
        chunk = data[i:i + 65536]
        i += len(chunk)
        run = len(chunk) - 1
        if run < 60:
            out.append(run << 2)
        elif run < 256:
            out.append(60 << 2)
            out.append(run)
        else:
            out.append(61 << 2)
            out += struct.pack("<H", run)
        out += chunk
    return bytes(out)


def encode_block(body: bytes, codec: str) -> bytes:
    if codec == "null":
        return body
    if codec == "deflate":
        c = zlib.compressobj(9, zlib.DEFLATED, -15)
        return c.compress(body) + c.flush()
    if codec == "snappy":
        return snappy_literal(body) + struct.pack(">I", zlib.crc32(body) & 0xFFFFFFFF)
    if codec == "zstandard":          # the spec's name; `zstd` is not legal here
        return zstd.compress(body)
    raise ValueError(codec)


# ---- the container ---------------------------------------------------------

def header(schema: str, codec: str, *, negative_count=False, omit_codec=False) -> bytes:
    """The OCF header. `negative_count` writes the metadata map the other legal
    way: a negative entry count followed by the block's byte size."""
    pairs = [st("avro.schema") + blob(schema.encode())]
    if not omit_codec:
        pairs.append(st("avro.codec") + blob(codec.encode()))
    if negative_count:
        body = b"".join(pairs)
        meta = zz(-len(pairs)) + zz(len(body)) + body + zz(0)
    else:
        meta = array(pairs)
    return b"Obj\x01" + meta + SYNC


def ocf(path: str, schema: str, blocks, codec="null", **kw) -> None:
    """`blocks` is a list of lists of encoded datums -- one list per OCF block,
    so a fixture can force the scanner round the block loop more than once."""
    out = [header(schema, codec, **kw)]
    for datums in blocks:
        body = b"".join(datums)
        out.append(zz(len(datums)) + blob(encode_block(body, codec)) + SYNC)
    write(path, b"".join(out))


def write(path: str, data: bytes) -> None:
    with open(path, "wb") as f:
        f.write(data)
    print(f"  {os.path.basename(path):<24} {len(data):>9} bytes")


# ---- the fixtures ----------------------------------------------------------

FLAT = ('{"type":"record","name":"Flat","namespace":"sf.test","fields":['
        '{"name":"a","type":["null","string"]},'
        '{"name":"b","type":"long"},'
        '{"name":"c","type":{"type":"enum","name":"Colour","symbols":["RED","GREEN"]}}]}')

FLAT_DATUMS = [
    zz(0) + zz(7) + zz(0),
    zz(1) + st("hello") + zz(-3) + zz(1),
    zz(1) + st("") + zz(0) + zz(0),
]

WIDE = ('{"type":"record","name":"Wide","fields":['
        '{"name":"d","type":"double"},'
        '{"name":"f","type":"float"},'
        '{"name":"by","type":"bytes"},'
        '{"name":"fx","type":{"type":"fixed","name":"F4","size":4}},'
        '{"name":"m","type":{"type":"map","values":"int"}},'
        '{"name":"ar","type":{"type":"array","items":"long"}},'
        '{"name":"bo","type":"boolean"},'
        '{"name":"big","type":"long"},'
        '{"name":"u","type":["null","bytes","double"]}]}')


def wide(d, f, by, fx, m, ar, bo, big, u):
    out = struct.pack("<d", d) + struct.pack("<f", f) + blob(by) + fx
    out += array([st(k) + zz(v) for k, v in m.items()])
    out += array([zz(v) for v in ar])
    out += (b"\x01" if bo else b"\x00") + zz(big)
    if u is None:
        out += zz(0)
    elif isinstance(u, bytes):
        out += zz(1) + blob(u)
    else:
        out += zz(2) + struct.pack("<d", u)
    return out


# Float values chosen to straddle every branch of strconv's 'g' format: the
# positional/exponential cutoffs at 1e-4 and 1e6, a negative zero, the float32
# maximum, and a value whose 17-digit form differs from its shortest form.
# Every map here has at most ONE key. The reference writes a map in Go's
# randomised iteration order, so a two-key map has no reproducible byte form to
# compare against; maporder.avro below covers that case separately.
WIDE_DATUMS = [
    wide(0.1, 1.5, b"\x00\xffA\x7f", b"\xde\xad\xbe\xef", {"k": 3},
         [1, -2, 3], True, 9223372036854775807, None),
    wide(1e21, -0.0, b"", b"\x00\x00\x00\x00", {}, [], False,
         -9223372036854775808, b"\xc3("),
    wide(-1.5e-7, 3.4028235e38, b"hi", b"abcd", {"one": 1}, [0], True, 0, 2.5),
    wide(1e6, 1e-5, b"\x01", b"\x00\x01\x02\x03", {"a": 0}, [7], False, -1, 0.0),
    wide(1e-4, 123456.0, b"\xff", b"\xff\xff\xff\xff", {"b": 2}, [-1], True, 1, -0.0),
    wide(0.0001220703125, 0.5, b"\x7f", b"\x20\x21\x22\x23", {}, [1, 2], False, 42, 1e-45),
]

ESC = ('{"type":"record","name":"E","fields":['
       '{"name":"st","type":"string"},{"name":"by","type":"bytes"}]}')

ESC_DATUMS = [
    st('a"b\\c/d<e>f&g\nh\ti') + blob(bytes([0x22, 0x5C, 0x2F, 0x3C, 0x3E, 0x26,
                                             0x0A, 0x09, 0x7F, 0x80, 0x1F])),
    st("héllo → 世界 \U0001F600") + blob(bytes(range(0x20))),
    # U+2028 and U+2029 escape; the runes around them do not.
    st(" ­ x ​﻿͸\U000e0001　") + blob(b""),
    st("") + blob(b""),
]

UNION = ('{"type":"record","name":"Top","namespace":"a.b","fields":['
         '{"name":"u","type":["null",'
         '{"type":"record","name":"Inner","fields":[{"name":"x","type":"int"}]},'
         '{"type":"enum","name":"E","symbols":["P","Q"]},'
         '{"type":"fixed","name":"F","size":2}]},'
         '{"name":"dt","type":{"type":"int","logicalType":"date"}},'
         '{"name":"tsm","type":{"type":"long","logicalType":"timestamp-millis"}},'
         '{"name":"uu","type":{"type":"string","logicalType":"uuid"}},'
         '{"name":"dec","type":{"type":"bytes","logicalType":"decimal",'
         '"precision":5,"scale":2}}]}')

UNION_TAIL = (zz(19000) + zz(1700000000000) +
              st("0f1b2c3d-1234-5678-9abc-def012345678") + blob(b"\x04\xd2"))

UNION_DATUMS = [
    zz(1) + zz(7) + UNION_TAIL,
    zz(2) + zz(1) + UNION_TAIL,
    zz(3) + b"\xaa\xbb" + UNION_TAIL,
    zz(0) + UNION_TAIL,
]

# A self-referential schema: `next` names the record being defined, which
# avro-cpp represents as a symbolic placeholder the walker has to follow.
RECURSIVE = ('{"type":"record","name":"Node","namespace":"tree","fields":['
             '{"name":"v","type":"int"},{"name":"next","type":["null","Node"]}]}')

RECURSIVE_DATUMS = [
    zz(1) + zz(1) + zz(2) + zz(1) + zz(3) + zz(0),   # 1 -> 2 -> 3 -> null
    zz(9) + zz(0),                                   # 9 -> null
]

# Containers of containers, which the walker recurses through with a different
# node at each level.
NESTED = ('{"type":"record","name":"N","fields":['
          '{"name":"rows","type":{"type":"array","items":'
          '{"type":"record","name":"Row","fields":['
          '{"name":"k","type":"string"},'
          '{"name":"vals","type":{"type":"map","values":["null","double"]}}]}}},'
          '{"name":"grid","type":{"type":"map","values":'
          '{"type":"array","items":{"type":"array","items":"int"}}}}]}')


def nested_row(k, vals):
    return st(k) + array([st(name) +
                          (zz(0) if v is None else zz(1) + struct.pack("<d", v))
                          for name, v in vals])


NESTED_DATUMS = [
    array([nested_row("one", [("x", 1.5)]),
           nested_row("two", [])]) +
    array([st("g") + array([array([zz(1), zz(2)]), array([])])]),
    array([]) + array([]),
]


def main(out_dir):
    os.makedirs(out_dir, exist_ok=True)
    j = lambda n: os.path.join(out_dir, n)

    for codec in ("null", "deflate", "snappy", "zstandard"):
        ocf(j(f"flat_{codec}.avro"), FLAT, [FLAT_DATUMS], codec=codec)

    ocf(j("wide.avro"), WIDE, [WIDE_DATUMS])
    ocf(j("escapes.avro"), ESC, [ESC_DATUMS])
    ocf(j("unions.avro"), UNION, [UNION_DATUMS])
    ocf(j("recursive.avro"), RECURSIVE, [RECURSIVE_DATUMS])
    ocf(j("nested.avro"), NESTED, [NESTED_DATUMS])

    # The container's shape, independent of the data: several blocks, the
    # metadata map written with a negative count, and no `avro.codec` key.
    ocf(j("multiblock.avro"), FLAT, [FLAT_DATUMS] * 5, codec="deflate")
    ocf(j("negcount.avro"), FLAT, [FLAT_DATUMS], negative_count=True)
    ocf(j("nocodec.avro"), FLAT, [FLAT_DATUMS], omit_codec=True)

    # Large enough to arrive as many read chunks, so the scanner's incremental
    # path is what produces the output rather than one lucky whole-file feed.
    # Deliberately uncompressed: it is the file's SIZE ON DISK that decides how
    # many chunks the input reads it in, and deflate would shrink a repetitive
    # payload back down to a single one.
    # FLAT's first field is a UNION, so a datum starts with the branch index.
    # Omitting it made every datum here malformed -- the string's 200-byte
    # length was read as union index 200 -- and BOTH implementations refused the
    # file, which the gate then compared as two identical empty outputs and
    # called agreement. The chunked path this fixture exists to exercise was not
    # being exercised at all.
    big = [zz(1) + st("x" * 200) + zz(i) + zz(i % 2) for i in range(4000)]
    ocf(j("large.avro"), FLAT, [big[i:i + 500] for i in range(0, len(big), 500)])

    # Multi-key maps, in keys whose sorted order is NOT their encounter order.
    # Checked on its own terms rather than against the reference: see
    # test_avro_check.sh.
    ocf(j("maporder.avro"), WIDE, [[
        wide(0.0, 0.0, b"", b"\x00\x00\x00\x00",
             {"zebra": 1, "apple": 2, "mango": 3}, [], True, 0, None),
        wide(0.0, 0.0, b"", b"\x00\x00\x00\x00",
             {"b": 1, "a": 2}, [], False, 1, None),
    ]])

    # Hostile containers. Every one of these was a live defect found by the
    # 2026-09-07 audit, and each has to stay REFUSED rather than crashing,
    # exhausting memory or -- worst of the three -- quietly losing records.
    E = '{"type":"record","name":"E","fields":[]}'
    B = '{"type":"record","name":"B","fields":[{"name":"b","type":"bytes"}]}'
    AR = ('{"type":"record","name":"A","fields":['
          '{"name":"a","type":{"type":"array","items":"int"}}]}')
    U = ('{"type":"record","name":"U","fields":['
         '{"name":"u","type":["null","string"]}]}')
    REC = ('{"type":"record","name":"N","fields":['
           '{"name":"v","type":"int"},{"name":"next","type":["null","N"]}]}')

    def one_block(schema, count, body, codec="null"):
        return header(schema, codec) + zz(count) + blob(encode_block(body, codec)) + SYNC

    # A union branch index past the end of the union.
    write(j("bad_union_index.avro"), one_block(U, 1, zz(9)))
    # A count no file can honour: an EMPTY record costs zero bytes per datum, so
    # the claim is free to make and used to be answered with a std::bad_alloc.
    write(j("bad_count_bomb.avro"), one_block(E, 10**15, b""))
    # An array whose declared element count is terabytes. Before the avro-cpp
    # patch this was allocated before a single element was read.
    write(j("bad_array_count.avro"), one_block(AR, 1, zz(2**40) + zz(1)))
    # A `bytes` field longer than the whole container.
    write(j("bad_bytes_length.avro"), one_block(B, 1, zz(2**40)))
    # 200,000 levels of nesting through a self-referential schema. This one
    # SEGFAULTED the process, which is why it is here.
    deep = b"".join(zz(1) + zz(1) for _ in range(200000)) + zz(1) + zz(0)
    write(j("bad_deep_nesting.avro"), one_block(REC, 1, deep))
    # The quiet one: a block that declares one datum and holds three. This was
    # accepted, and the two extra records were dropped with a zero exit status.
    write(j("bad_short_count.avro"),
          one_block('{"type":"record","name":"T","fields":[{"name":"n","type":"long"}]}',
                    1, b"".join(zz(n) for n in (1, 2, 3))))

    # A sound block followed by a corrupt one. Two behaviours meet here and both
    # were wrong at first: the sound block's records must still be DELIVERED
    # (throwing straight out discarded them), and the corrupt block must
    # contribute NOTHING (appending as it decoded leaked its first record).
    T = '{"type":"record","name":"T","fields":[{"name":"n","type":"long"}]}'
    three = b"".join(zz(n) for n in (1, 2, 3))
    write(j("partial_then_bad.avro"),
          header(T, "null")
          + zz(3) + blob(three) + SYNC
          + zz(1) + blob(three) + SYNC)

    # Non-finite floats, which are legal Avro and have no JSON syntax. NaN is
    # `null` and infinity is `1e999`; writing `null` for both lost the sign.
    NF = ('{"type":"record","name":"F","fields":['
          '{"name":"d","type":"double"},{"name":"f","type":"float"}]}')
    nf = [struct.pack("<d", d) + struct.pack("<f", f)
          for d, f in ((float("nan"), float("nan")),
                       (float("inf"), float("inf")),
                       (float("-inf"), float("-inf")))]
    ocf(j("nonfinite.avro"), NF, [nf])

    # An empty stream: zero messages, not an error.
    write(j("empty.avro"), b"")

    # Malformed inputs. Each has to be REFUSED, and the scanner names which part
    # of the container is wrong rather than reporting a generic parse failure.
    whole = header(FLAT, "null") + zz(len(FLAT_DATUMS)) + \
        blob(b"".join(FLAT_DATUMS)) + SYNC
    write(j("bad_truncated_header.avro"), header(FLAT, "null")[:20])
    write(j("bad_truncated_block.avro"), whole[:len(whole) - 20])
    write(j("bad_sync.avro"), whole[:len(whole) - 16] + bytes(16))
    write(j("bad_magic.avro"), b"NOPE" + whole[4:])
    # Two different mistakes: a codec that is not an Avro codec at all, and one
    # that IS in the spec but that neither implementation reads. Both have to be
    # refused by name rather than read as if the block were uncompressed.
    write(j("bad_codec.avro"), header(FLAT, "lzo") + zz(1) + blob(b"") + SYNC)
    write(j("bad_codec_spec.avro"), header(FLAT, "bzip2") + zz(1) + blob(b"") + SYNC)
    write(j("bad_schema.avro"), header("{not json", "null") + zz(0) + blob(b"") + SYNC)


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else
         os.path.join(os.path.dirname(__file__), "fixtures", "avro"))
