#!/usr/bin/env python3
"""Sorts the entries inside each `"m":{...}` field of the lines it is given.

Used by test_avro_check.sh for one fixture only. An Avro map has no reproducible
key order in the reference -- it writes one in Go's randomised map iteration
order -- so a byte comparison of a multi-key map is a coin flip. Normalising the
order out of BOTH sides turns "the only difference is the order" into something
that can actually be asserted: every value, escape and float is still compared
byte-for-byte, and only the order is discarded.

The fixture's map values are plain integers, chosen so that splitting on commas
is exact rather than an approximation of JSON parsing.
"""
import re
import sys


def sort_map(m):
    return '"m":{' + ",".join(sorted(m.group(1).split(","))) + "}"


for path in sys.argv[1:]:
    with open(path, encoding="utf-8") as f:
        for line in f:
            sys.stdout.write(re.sub(r'"m":\{([^}]*)\}', sort_map, line))
