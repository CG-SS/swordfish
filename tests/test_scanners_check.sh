#!/usr/bin/env bash
# The scanners, every one compared against redpanda-connect on the same bytes.
#
# A scanner decides where one message ends and the next begins, so getting it
# wrong changes the SHAPE of the data rather than losing a feature -- which is
# why an unimplemented scanner is a named error here and why these are compared
# against the reference rather than against expectations written by the same
# person who wrote the scanner.
#
# Two rules this file follows, both learned the hard way on this project:
#   - the reference's output is the expectation, and a reference that produced
#     NOTHING is a failure rather than agreement;
#   - output is sorted before comparison, because the reference pipelines
#     messages concurrently and emits them out of order on a fast source.
set -uo pipefail
cd "$(dirname "$0")/.."
SF=${1:-build/swordfish-run}
RC=${2:-../redpanda-connect}
LINT=${3:-build/sfconfig}
export SWORDFISH_CONFIG="$PWD/build/swordfish-config"
WORK=$(mktemp -d /tmp/sfscan.XXXXXX)
SEA_ARGS="--smp 1 --memory 512M --overprovisioned"
trap 'rm -rf "$WORK"' EXIT
trap 'rm -rf "$WORK"; exit 143' INT TERM

fail=0
note() { echo "ok   $1"; }
bad()  { echo "FAIL $1: $2" >&2; fail=1; }

# ---- the corpus ---------------------------------------------------------------
printf 'name,age\nada,36\nbob,7\n'                > "$WORK/t.csv"
printf 'na"me,age\n"ada,x",36\n'                  > "$WORK/quoted.csv"
printf '{"a":1}{"a":2}\n{"a":3}\n'                > "$WORK/t.jsonl"
printf '[{"x":1}, {"x":2}, 3, "s", true, null]\n' > "$WORK/t.jsonarr"
printf 'abcdefghij'                               > "$WORK/t.bin"
printf '\xEF\xBB\xBFhello\nworld\n'               > "$WORK/t.bom"
# The other four marks the reference knows, plus the two shapes that used to be
# mishandled: a DOUBLED mark (the second is data, not a mark) and a file that is
# nothing but a mark.
printf '\xEF\xBB\xBF\xEF\xBB\xBFhello\n'          > "$WORK/t.bom2"
printf '\xFE\xFFhello\n'                        > "$WORK/t.bom16be"
printf '\xFF\xFE\x00\x00hello\n'                > "$WORK/t.bom32le"
printf '\xEF\xBB\xBF'                            > "$WORK/t.bomonly"
printf '12:00:00 a\nmore a\n12:00:01 b\n'         > "$WORK/t.log"
printf 'one\ntwo\n\nthree\n'                      > "$WORK/blank.txt"
gzip -c "$WORK/t.jsonl" > "$WORK/t.jsonl.gz"
mkdir -p "$WORK/td"
printf 'one\n' > "$WORK/td/a.txt"; printf 'two\n' > "$WORK/td/b.txt"
tar -C "$WORK/td" -cf "$WORK/t.tar" a.txt b.txt 2>/dev/null
cp "$WORK/t.csv" "$WORK/sw1.csv"; cp "$WORK/t.jsonl" "$WORK/sw2.jsonl"

# <name> <file(s)> <scanner block, already indented six spaces>
cmp_ref() {
  local name=$1 paths=$2 scanner=$3 sf ref
  cat > "$WORK/c.yaml" <<YAML
input:
  file:
    paths: [ $paths ]
    scanner:
$scanner
pipeline:
  processors:
    - mapping: 'root.body = content().string()'
output: { stdout: {} }
YAML
  # shellcheck disable=SC2086
  sf=$(timeout 60 "$SF" --config "$WORK/c.yaml" $SEA_ARGS 2>/dev/null | sort | tr '\n' '|')
  if [ ! -x "$RC" ]; then
    [ -n "$sf" ] && note "$name (no reference to compare against)" \
                 || bad "$name" "swordfish produced nothing"
    return
  fi
  ref=$(timeout 60 "$RC" run --log.level off "$WORK/c.yaml" 2>/dev/null | sort | tr '\n' '|')
  if [ -z "$ref" ]; then
    bad "$name" "the reference produced nothing, so it did not run this config"; return
  fi
  [ "$sf" = "$ref" ] && note "$name" \
                     || bad "$name" "swordfish '$sf' reference '$ref'"
}

# `content().string()` above is what makes the framing visible: it puts each
# message's exact bytes into a field, so a scanner that split in the wrong place
# differs even when the messages happen to reassemble into the same stream.
cmp_ref "csv splits rows and keys them by the header" "$WORK/t.csv" \
        '      csv: {}'
cmp_ref "csv without a header row yields arrays" "$WORK/t.csv" \
        '      csv: { parse_header_row: false }'
cmp_ref "csv honours a custom delimiter" "$WORK/t.csv" \
        '      csv: { custom_delimiter: ";" }'
cmp_ref "csv lazy_quotes accepts a bare quote" "$WORK/quoted.csv" \
        '      csv: { lazy_quotes: true }'
cmp_ref "json_documents splits concatenated documents" "$WORK/t.jsonl" \
        '      json_documents: {}'
cmp_ref "json_array yields one message per element" "$WORK/t.jsonarr" \
        '      json_array: {}'
# Both JSON scanners RE-SERIALISE what they parse rather than handing back the
# source bytes, which is what the reference does: whitespace goes, keys come out
# sorted, and `1.50` becomes `1.5`. Emitting the span verbatim also meant a
# structurally-balanced but invalid token -- `notjson` between two documents --
# went out as a message where the reference reports an error.
printf '{"b": 1, "a": 2, "num": 1.50}\n'          > "$WORK/messy.json"
printf '[1,2]  [3,4]\n'                           > "$WORK/twoarrays.json"
printf '[ {"x": 1,  "y":2} , 3 ]\n'               > "$WORK/messyarr.json"
cmp_ref "json_documents re-serialises what it parses" "$WORK/messy.json" \
        '      json_documents: {}'
cmp_ref "json_array re-serialises each element" "$WORK/messyarr.json" \
        '      json_array: {}'
# Consecutive arrays. Latching on the first `]` buffered the rest for ever and
# reported nothing, so `[1,2] [3,4]` gave two messages against the reference's
# four.
cmp_ref "json_array reads consecutive arrays" "$WORK/twoarrays.json" \
        '      json_array: {}'
cmp_ref "chunker cuts fixed-size blocks" "$WORK/t.bin" \
        '      chunker: { size: 3 }'
cmp_ref "skip_bom strips the mark and delegates" "$WORK/t.bom" \
        '      skip_bom: { into: { lines: {} } }'
# The reference strips EXACTLY ONE mark, longest-first, out of five. Stripping
# them in a loop swallowed a doubled mark whole -- which is what concatenating
# two BOM-prefixed files produces -- and knowing only the UTF-8 one left two or
# four stray bytes at the head of a UTF-16/32 file.
cmp_ref "skip_bom strips exactly one mark, not every one" "$WORK/t.bom2" \
        '      skip_bom: { into: { lines: {} } }'
cmp_ref "skip_bom knows the UTF-16 mark" "$WORK/t.bom16be" \
        '      skip_bom: { into: { lines: {} } }'
cmp_ref "skip_bom knows the UTF-32 mark" "$WORK/t.bom32le" \
        '      skip_bom: { into: { lines: {} } }'
cmp_ref "re_match starts a segment at each match" "$WORK/t.log" \
        "      re_match:
        pattern: '(?m)^\\d\\d:\\d\\d:\\d\\d'"
# Two details of the reference's split function that swordfish had wrong:
# matches are NON-OVERLAPPING, and the region before the first match is a
# message rather than something to discard. The second one lost the whole stream
# when nothing matched at all.
printf 'aXaXa\n'                            > "$WORK/overlap.txt"
printf 'junk before\nMARK one\nMARK two\n'  > "$WORK/leading.txt"
printf 'nothing matches here\n'             > "$WORK/nomatch.txt"
cmp_ref "re_match matches do not overlap" "$WORK/overlap.txt" \
        "      re_match: { pattern: 'aXa' }"
cmp_ref "re_match emits the text before the first match" "$WORK/leading.txt" \
        "      re_match: { pattern: '(?m)^MARK' }"
cmp_ref "re_match with no match at all yields the stream" "$WORK/nomatch.txt" \
        "      re_match: { pattern: '(?m)^MARK' }"
cmp_ref "tar walks the archive member by member" "$WORK/t.tar" \
        '      tar: {}'
cmp_ref "decompress inflates before the child scanner sees it" "$WORK/t.jsonl.gz" \
        '      decompress: { algorithm: gzip, into: { lines: {} } }'
cmp_ref "lines takes a custom delimiter" "$WORK/t.csv" \
        '      lines: { custom_delimiter: "," }'
cmp_ref "lines omit_empty drops blank lines" "$WORK/blank.txt" \
        '      lines: { omit_empty: true }'
cmp_ref "to_the_end reads the whole source as one" "$WORK/t.jsonl" \
        '      to_the_end: {}'
# `switch` picks per SOURCE, so it needs two files that want different scanners.
cmp_ref "switch chooses a scanner per file name" "\"$WORK/sw*.csv\", \"$WORK/sw*.jsonl\"" \
        "      switch:
        - re_match_name: '\\.csv\$'
          scanner: { csv: {} }
        - scanner: { json_documents: {} }"

# tar_name and csv_row are the metadata the reference documents; a scanner that
# produced the right bytes with the wrong metadata would pass every check above.
meta_case() {  # <name> <file> <scanner> <meta key>
  local sf ref
  cat > "$WORK/m.yaml" <<YAML
input:
  file:
    paths: [ $2 ]
    scanner:
$3
pipeline:
  processors:
    - mapping: 'root = { "k": meta("$4") }'
output: { stdout: {} }
YAML
  # shellcheck disable=SC2086
  sf=$(timeout 60 "$SF" --config "$WORK/m.yaml" $SEA_ARGS 2>/dev/null | sort | tr '\n' '|')
  if [ ! -x "$RC" ]; then
    [ -n "$sf" ] && note "$1" || bad "$1" "swordfish produced nothing"; return
  fi
  ref=$(timeout 60 "$RC" run --log.level off "$WORK/m.yaml" 2>/dev/null | sort | tr '\n' '|')
  [ -n "$ref" ] && [ "$sf" = "$ref" ] && note "$1" \
                || bad "$1" "swordfish '$sf' reference '$ref'"
}
meta_case "tar sets tar_name on each member" "$WORK/t.tar" '      tar: {}' tar_name
# Every ENTRY is a message, not just the regular files, and a path over 100 bytes
# survives whole. Dropping directories turned an archive of three files in two
# directories into three messages against the reference's six, and reading only
# the 100-byte `name` field truncated any longer path to its tail -- both silent.
mkdir -p "$WORK/td2/sub"
printf 'one\n' > "$WORK/td2/a.txt"; printf 'two\n' > "$WORK/td2/sub/b.txt"
LONGDIR=$(python3 -c "print('d'*80)"); LONGLEAF=$(python3 -c "print('e'*60)")
mkdir -p "$WORK/td2/$LONGDIR"; printf 'long\n' > "$WORK/td2/$LONGDIR/$LONGLEAF"
( cd "$WORK/td2" && tar -cf "$WORK/t2.tar" . 2>/dev/null )
meta_case "tar emits a message for a directory too" "$WORK/t2.tar" '      tar: {}' tar_name
cmp_ref  "tar keeps a path longer than the name field" "$WORK/t2.tar" '      tar: {}'
# GNU long-name entries carry the name in the BODY of a preceding 'L' record,
# which is a different mechanism from the ustar prefix field above.
mkdir -p "$WORK/td3"
printf 'hi\n' > "$WORK/td3/plain.txt"
VERYLONG=$(python3 -c "print('f'*250)")
printf 'verylong\n' > "$WORK/td3/$VERYLONG"
( cd "$WORK/td3" && tar --format=gnu -cf "$WORK/t3.tar" . 2>/dev/null )
meta_case "tar reads a GNU long-name entry" "$WORK/t3.tar" '      tar: {}' tar_name
# The numeric fields, read the way Go's archive/tar reads them. Two of these
# shapes were BROKEN BY an earlier fix to the same function: rejecting a field
# that is nothing but padding threw away every archive containing a directory,
# and base-256 -- what GNU and star write for a member at or above 8 GiB -- was
# never decoded at all. A false "invalid tar header" costs the whole archive,
# not one member, which is why `b256big` carries an ordinary member behind the
# base-256 one.
python3 - "$WORK" <<'PYEOF'
import os, sys
D = os.path.join(sys.argv[1], "tars"); os.makedirs(D, exist_ok=True)
def hdr(name, size_field, typeflag=b'0'):
    h = bytearray(512)
    h[0:len(name)] = name
    h[100:108] = b'0000644\x00'; h[108:116] = b'0000000\x00'; h[116:124] = b'0000000\x00'
    h[124:136] = size_field; h[136:148] = b'14000000000\x00'
    h[148:156] = b' ' * 8; h[156:157] = typeflag
    h[257:263] = b'ustar\x00'; h[263:265] = b'00'
    h[148:156] = (b'%06o\x00 ' % sum(h))
    return bytes(h)
def member(name, payload, size_field=None, typeflag=b'0'):
    sf = size_field if size_field is not None else (b'%011o\x00' % len(payload))
    return hdr(name, sf, typeflag) + payload + b'\x00' * ((512 - len(payload) % 512) % 512)
p = b'hello base256\n'
b256 = b'\x80' + b'\x00' * 10 + bytes([len(p)])
open(D + "/b256.tar", "wb").write(member(b"b256.txt", p, b256) + b'\x00' * 1024)
open(D + "/b256big.tar", "wb").write(
    member(b"b256.txt", p, b256) + member(b"two.txt", b"second\n") + b'\x00' * 1024)
open(D + "/nulsize.tar", "wb").write(member(b"nulsize.txt", b"", b'\x00' * 12) + b'\x00' * 1024)
open(D + "/nuldir.tar", "wb").write(
    member(b"dir/", b"", b'\x00' * 12, b'5') + member(b"dir/f.txt", b"hello") + b'\x00' * 1024)
PYEOF
meta_case "tar decodes a base-256 size field"        "$WORK/tars/b256.tar"    '      tar: {}' tar_name
cmp_ref   "and does not lose the members behind it"  "$WORK/tars/b256big.tar" '      tar: {}'
cmp_ref   "a size field of nothing but NULs is zero" "$WORK/tars/nulsize.tar" '      tar: {}'
cmp_ref   "a directory written that way keeps its archive" "$WORK/tars/nuldir.tar" '      tar: {}'
meta_case "csv numbers its rows in csv_row"  "$WORK/t.csv" '      csv: {}'  csv_row

# ---- cases where the right answer is "nothing", or "an error" ------------------

# A file that is nothing but a byte-order mark. cmp_ref cannot express this: it
# treats an empty reference run as "the reference did not run the config", which
# is exactly the answer here.
cat > "$WORK/bomonly.yaml" <<YAML
input:
  file:
    paths: [ $WORK/t.bomonly ]
    scanner:
      skip_bom: { into: { lines: {} } }
output: { stdout: {} }
YAML
# shellcheck disable=SC2086
sf_out=$(timeout 60 "$SF" --config "$WORK/bomonly.yaml" $SEA_ARGS 2>/dev/null)
if [ -n "$sf_out" ]; then
  bad "a file that is only a byte-order mark yields no message" "got '$sf_out'"
elif [ -x "$RC" ]; then
  rc_out=$(timeout 60 "$RC" run --log.level off "$WORK/bomonly.yaml" 2>/dev/null)
  [ -z "$rc_out" ] && note "a file that is only a byte-order mark yields no message" \
                   || bad "a file that is only a byte-order mark yields no message" \
                          "the reference produced '$rc_out'"
else
  note "a file that is only a byte-order mark yields no message"
fi

# `max_buffer_size` bounds the TOKEN, not just the unterminated residue, and the
# boundary is `>=`: Go's bufio.Scanner needs room for the token and its
# delimiter, so a token of exactly max_buffer_size is already "token too long".
# Checking only the residue made the setting inert for any token that arrived
# whole inside one read.
mbs_case() {  # <name> <length> <max> <expect: ok|err>
  local name=$1 n=$2 mx=$3 want=$4 got
  python3 -c "open('$WORK/mbs.txt','w').write('x'*$n + '\n')"
  cat > "$WORK/mbs.yaml" <<YAML
input:
  file:
    paths: [ $WORK/mbs.txt ]
    scanner:
      lines: { max_buffer_size: $mx }
output: { stdout: {} }
YAML
  # Judged on OUTPUT, not on log text: the reference is run with `--log.level
  # off` everywhere in this file, which hides the very "token too long" line a
  # grep would look for -- an earlier version of this check passed for that
  # reason alone. A refused token means no message; an accepted one means one.
  # shellcheck disable=SC2086
  [ -n "$(timeout 60 "$SF" --config "$WORK/mbs.yaml" $SEA_ARGS 2>/dev/null)" ] && got=ok || got=err
  if [ "$got" != "$want" ]; then
    bad "$name" "expected $want, got $got"; return
  fi
  if [ -x "$RC" ]; then
    local rgot
    [ -n "$(timeout 60 "$RC" run --log.level off "$WORK/mbs.yaml" 2>/dev/null)" ] && rgot=ok || rgot=err
    [ "$rgot" = "$got" ] || { bad "$name" "reference says $rgot, swordfish $got"; return; }
  fi
  note "$name"
}
mbs_case "a line one under max_buffer_size is a message"      99  100 ok
mbs_case "a line of exactly max_buffer_size is too long"     100  100 err
mbs_case "a line over max_buffer_size is too long"           101  100 err
mbs_case "the token is measured even when it arrives whole"  500  100 err

# ---- refusals -----------------------------------------------------------------
refuses() {  # <name> <yaml> <expected substring>
  local out
  printf '%s\n' "$2" > "$WORK/r.yaml"
  out=$(timeout 30 "$LINT" lint "$WORK/r.yaml" 2>&1)
  case "$out" in
    *"$3"*) note "$1" ;;
    *)      bad "$1" "expected '$3', got '$out'" ;;
  esac
}
BASE_IN="input: { file: { paths: [ $WORK/t.csv ], scanner:"
refuses "chunker without a size is refused" \
  "$BASE_IN { chunker: {} } } }
output: { stdout: {} }" "requires a \`size\` above zero"
refuses "re_match without a pattern is refused" \
  "$BASE_IN { re_match: {} } } }
output: { stdout: {} }" "requires a \`pattern\`"
refuses "a bad re_match pattern is a config error, not a runtime one" \
  "$BASE_IN { re_match: { pattern: '(' } } } }
output: { stdout: {} }" "bad pattern"
refuses "an unknown decompress algorithm is named" \
  "$BASE_IN { decompress: { algorithm: rot13 } } } }
output: { stdout: {} }" "is not one of gzip"
refuses "an empty switch is refused" \
  "$BASE_IN { switch: [] } } }
output: { stdout: {} }" "non-empty list of candidates"
refuses "a switch candidate without a scanner is refused" \
  "$BASE_IN { switch: [ { re_match_name: 'x' } ] } } }
output: { stdout: {} }" "needs a \`scanner\`"
refuses "a multi-character csv delimiter is refused" \
  "$BASE_IN { csv: { custom_delimiter: '::' } } } }
output: { stdout: {} }" "single character"

# A `switch` that matches nothing REJECTS the source rather than guessing one,
# which is what the reference documents. Observed at run time, since it depends
# on the name of the data rather than on the config.
cat > "$WORK/nomatch.yaml" <<YAML
input:
  file:
    paths: [ $WORK/t.csv ]
    scanner:
      switch:
        - re_match_name: '\.nothing\$'
          scanner: { lines: {} }
output: { stdout: {} }
YAML
# shellcheck disable=SC2086
out=$(timeout 30 "$SF" --config "$WORK/nomatch.yaml" $SEA_ARGS 2>&1)
case "$out" in
  *"no candidate matched"*) note "a switch with no matching candidate rejects the source" ;;
  *) bad "a switch with no matching candidate rejects the source" \
         "$(echo "$out" | tail -2 | tr '\n' ' ')" ;;
esac

# ---- the compiled path ---------------------------------------------------------
#
# A scanner is emitted as a spec that generated code hands to the same factory,
# so this proves the emitted spec round-trips -- including a nested `switch`.
if [ -x build/swordfish-build ]; then
  cat > "$WORK/comp.yaml" <<YAML
input:
  file:
    paths: [ "$WORK/sw*.csv", "$WORK/sw*.jsonl" ]
    scanner:
      switch:
        - re_match_name: '\.csv\$'
          scanner: { csv: { parse_header_row: true } }
        - scanner: { json_documents: {} }
output: { stdout: {} }
YAML
  # shellcheck disable=SC2086
  interp=$(timeout 60 "$SF" --config "$WORK/comp.yaml" $SEA_ARGS 2>/dev/null | sort | tr '\n' '|')
  if timeout 900 build/swordfish-build "$WORK/comp.yaml" -o "$WORK/comp.bin" \
        >"$WORK/build.log" 2>&1; then
    # shellcheck disable=SC2086
    compiled=$(timeout 60 "$WORK/comp.bin" $SEA_ARGS 2>/dev/null | sort | tr '\n' '|')
    [ -n "$interp" ] && [ "$interp" = "$compiled" ] \
      && note "a compiled binary scans identically, nested switch included" \
      || bad "a compiled binary scans identically, nested switch included" \
             "interpreted '$interp' compiled '$compiled'"
  else
    bad "a compiled binary scans identically, nested switch included" \
        "swordfish build failed: $(tail -3 "$WORK/build.log" | tr '\n' ' ')"
  fi
fi

# ---- malformed input: strict where the reference is strict ---------------------------
#
# Found by fuzzing under UBSan: none of these produced undefined behaviour, but
# several produced a MESSAGE where the reference reports an error -- swordfish
# emitting malformed data as if it were valid, which is the silent-corruption
# class this project treats as unacceptable.
# Built with the `tar` COMMAND, not python's tarfile: tarfile writes a PAX header
# block first, so the first member's data does not begin until byte 1024 and a
# 600-byte truncation cuts before any member is complete. The plain USTAR layout
# is what makes "data present, padding missing" expressible at all. `good.tar`
# above is the tarfile-written one, so the PAX layout is covered too.
tar -C "$WORK/td" -cf "$WORK/ustar.tar" a.txt 2>/dev/null
python3 - "$WORK" <<'PY'
import sys
W = sys.argv[1]
raw = open(W + "/ustar.tar", "rb").read()
open(W + "/trunc.tar", "wb").write(raw[:600])                     # data present, padding not
h = bytearray(raw[:512]); h[124:136] = b"zzzzzzzzzzz\0"
open(W + "/junk.tar", "wb").write(bytes(h) + b"\0" * 512)          # size not octal
h2 = bytearray(raw[:512]); h2[148:156] = b"999999\0 "
open(W + "/badsum.tar", "wb").write(bytes(h2) + raw[512:])        # checksum wrong
open(W + "/unterm.jsonl", "w").write('{"a":[1,2,{"b":')           # truncated value
open(W + "/ragged.csv", "w").write("a,b\n1,2,3\n4\n")            # wrong field counts
import tarfile
with tarfile.open(W + "/paxed.tar", "w", format=tarfile.PAX_FORMAT) as t:
    t.add(W + "/td/a.txt", arcname="a.txt")
PY

malformed() {  # <name> <file> <scanner> <expected swordfish output>
  cat > "$WORK/mal.yaml" <<YAML
input:
  file:
    paths: [ $2 ]
    scanner:
$3
output: { stdout: {} }
YAML
  local got ref
  # shellcheck disable=SC2086
  got=$(timeout 30 "$SF" --config "$WORK/mal.yaml" $SEA_ARGS 2>/dev/null | tr '\n' '|')
  if [ "$got" != "$4" ]; then
    echo "FAIL $1: expected '$4' got '$got'" >&2; fail=1; return
  fi
  if [ -x "$RC" ]; then
    ref=$(timeout 30 "$RC" run --log.level off "$WORK/mal.yaml" 2>/dev/null | tr '\n' '|')
    if [ "$ref" != "$got" ]; then
      echo "FAIL $1: reference produced '$ref', swordfish '$got'" >&2; fail=1; return
    fi
  fi
  echo "ok   $1"
}

# A member whose DATA is present but whose 512-byte padding was cut off is still
# a member; requiring the padding dropped it where the reference delivers it.
malformed "a truncated tar still yields its complete members" \
          "$WORK/trunc.tar" '      tar: {}' 'one|'
# A non-octal size field used to be read as zero, which emitted an EMPTY message.
malformed "a tar size field that is not octal is refused" \
          "$WORK/junk.tar" '      tar: {}' ''
# The header checksum was never verified, so a corrupt header was walked as sound.
malformed "a tar header with a bad checksum is refused" \
          "$WORK/badsum.tar" '      tar: {}' ''
malformed "a valid tar is unaffected" \
          "$WORK/t.tar" '      tar: {}' 'one|two|'
# A PAX-prefixed archive -- what python and modern GNU tar write for long names
# or timestamps -- is walked correctly too: the PAX block is a member type with
# no payload a message could be, and is skipped.
malformed "a PAX-prefixed archive is walked, not mistaken for corruption" \
          "$WORK/paxed.tar" '      tar: {}' 'one|'
# `{"a":[1,2,{"b":` went out as a message. A trailing BARE SCALAR is complete at
# end of stream; something that opened a brace and never closed it is not.
malformed "a truncated JSON value is refused, not emitted" \
          "$WORK/unterm.jsonl" '      json_documents: {}' ''
# Go's encoding/csv requires every record to have the first record's field count.
# This padded short rows and truncated long ones, producing plausible wrong data.
malformed "a ragged csv is refused after the records before it" \
          "$WORK/ragged.csv" '      csv: { parse_header_row: false }' '["a","b"]|'
malformed "continue_on_error maps what is there without padding" \
          "$WORK/ragged.csv" '      csv: { continue_on_error: true }' \
          '{"a":"1","b":"2"}|{"a":"4"}|'

# ---- a scanner the component does not read -------------------------------------------
#
# lift_scanners removes EVERY key named `scanner` before spec_of sees the body, so
# spec_of can never call one unrecognised. Without an accepted-path list, a
# scanner on a component that has none was accepted and silently dropped.
refuses "a scanner on a component that has none is refused" \
  "input: { generate: { count: 1, interval: 0s, mapping: 'root = 1' } }
output: { socket: { network: tcp, address: '127.0.0.1:9', scanner: { csv: {} } } }" \
  "output.socket has no \`scanner\` field"
refuses "a scanner at the wrong path is refused" \
  "input: { http_client: { url: 'http://127.0.0.1:9/x', scanner: { csv: {} } } }
output: { drop: {} }" \
  "has no \`scanner\` at 'scanner'"

exit $fail
