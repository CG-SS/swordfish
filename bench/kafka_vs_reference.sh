#!/usr/bin/env bash
# Swordfish against
# the real Redpanda Connect, through a real 3-broker cluster.
#
#   bench/kafka_vs_reference.sh [path-to-kafka] [path-to-redpanda-connect]
#
# WHY THIS MEASURES CPU AT A FIXED RATE RATHER THAN PEAK THROUGHPUT
#
# The original criterion asked for throughput within 20% of `rpk connect`. On
# this hardware that question cannot be answered, and worse, it answers itself
# the wrong way: three brokers at RF=3 cost 66.5 CPU-seconds per million
# messages, against 37.5 for the reference's best configuration and 4.5 for
# compiled Swordfish. The broker costs more CPU than either client. Run flat
# out, both implementations saturate the broker, converge on ITS ceiling, and
# land within a few percent of each other -- a pass that says nothing about
# either client.
#
# So: hold the rate at a level both sustain comfortably, and measure what each
# CLIENT spends to keep up. The client is a separate process, so broker CPU
# never enters its accounting. What is left is client efficiency, which is what
# the criterion was always trying to establish.
#
# The same fairness rules as bench/vs_reference.sh apply, plus one more:
#
#   * The two configs CANNOT be byte-identical -- Swordfish's input takes
#     `seed_brokers`, the reference takes `addresses`. That makes the output
#     diff more important, not less: both are made to consume the same seeded
#     topic and their results are compared before any timing is reported.
#   * NOTHING runs unbounded. Every invocation is wrapped, and the trap kills
#     the process tree on EXIT, INT and TERM (§ 8.1).
set -uo pipefail
cd "$(dirname "$0")/.."
SF_ROOT=$PWD

KAFKA=${1:-../kafka_2.13-4.3.1}
RC=${2:-../redpanda-connect}
[ -d "$KAFKA" ] || { echo "no kafka distribution at $KAFKA" >&2; exit 2; }
[ -x "$RC" ]    || { echo "no redpanda-connect binary at $RC" >&2; exit 2; }
source tools/kafka_env.sh 2>/dev/null || true
export SWORDFISH_CONFIG="$PWD/build/swordfish-config"

# Produced at a HELD rate while each client consumes live, rather than draining
# a pre-seeded topic flat out. Two reasons, and the second is the one that
# matters: a flat-out drain is broker-limited, and a broker-starved Seastar
# reactor keeps polling -- burning CPU with no work to show for it -- while Go
# blocks on epoll and does not. Measuring CPU-per-message under starvation would
# penalise Swordfish for waiting. Held below the broker ceiling, neither client
# waits and what is left is the cost of the work itself.
# A SWEEP, not one rate. Redpanda Connect has fixed per-poll overhead it
# amortises better as load rises -- measured at 15k/s it spends 164 CPU-s per
# million, at 22k/s only 128 -- so a single low rate would report its worst case
# and flatter Swordfish. The same principle as measuring it batched in
# bench/vs_reference.sh: each implementation is credited with its BEST result
# across the sweep. (Swordfish is nearly rate-insensitive here, 9.2 vs 9.3,
# because its cost is per-message work rather than per-poll overhead.)
RATES=${RATES:-"5000 10000 20000"}
DURATION=${DURATION:-30}     # seconds of production per implementation
GRACE=${GRACE:-15}           # seconds allowed afterwards to drain and commit
PARTITIONS=${PARTITIONS:-8}
CORES=${CORES:-4}            # cores given to each client
RUN_TIMEOUT=${RUN_TIMEOUT:-900}
# YAML list of the seed brokers. Built by iteration: the obvious sed over the
# comma-separated form matches empty strings between separators and produces
# `"localhost:19092","" "localhost:19093"`, which parses as extra empty entries.
broker_list() {
  local out="" b; local IFS=','
  for b in $BOOT; do out="${out}${out:+, }\"$b\""; done
  printf '%s' "$out"
}
SETUP_TIMEOUT=${SETUP_TIMEOUT:-300}

WORK=$(mktemp -d /tmp/sfkbench.XXXXXX)
BROKERS=()
exec 3>&2

kill_tree() {
  local p=$1 c
  # Children before parent: killing a wrapper alone re-parents what it bounded.
  for c in $(pgrep -P "$p" 2>/dev/null); do kill_tree "$c"; done
  kill -9 "$p" 2>/dev/null
}
cleanup() {
  local rc=$? c
  trap - EXIT INT TERM
  for c in $(pgrep -P $$ 2>/dev/null); do kill_tree "$c"; done
  for p in "${BROKERS[@]:-}"; do [ -n "$p" ] && kill_tree "$p"; done
  [ "${KEEP:-0}" = 1 ] && echo "logs kept in $WORK" >&3 || rm -rf "$WORK"
  exit $rc
}
trap cleanup EXIT INT TERM

bounded() {  # bounded <seconds> <what> <command...>
  local secs=$1 what=$2; shift 2
  timeout -k 10 "$secs" "$@"
  local rc=$?
  if [ "$rc" -eq 124 ] || [ "$rc" -eq 137 ]; then
    echo "HUNG: $what exceeded ${secs}s and was killed" >&3; return 99
  fi
  return $rc
}

t0=$SECONDS
phase() { printf '[%4ss] %s\n' "$((SECONDS-t0))" "$1"; }

# ---- a real 3-broker cluster -------------------------------------------------
BOOT=""
CID=$("$KAFKA/bin/kafka-storage.sh" random-uuid)
for i in 0 1 2; do
  P=$((19092+i)); C=$((29092+i))
  BOOT="${BOOT}${BOOT:+,}localhost:$P"
  cat > "$WORK/b$i.properties" <<EOF
process.roles=broker,controller
node.id=$i
controller.quorum.voters=0@localhost:29092,1@localhost:29093,2@localhost:29094
listeners=PLAINTEXT://localhost:$P,CONTROLLER://localhost:$C
inter.broker.listener.name=PLAINTEXT
controller.listener.names=CONTROLLER
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
advertised.listeners=PLAINTEXT://localhost:$P
log.dirs=$WORK/d$i
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2
group.initial.rebalance.delay.ms=0
EOF
  "$KAFKA/bin/kafka-storage.sh" format -t "$CID" -c "$WORK/b$i.properties" >/dev/null 2>&1
  "$KAFKA/bin/kafka-server-start.sh" "$WORK/b$i.properties" > "$WORK/b$i.log" 2>&1 &
  BROKERS+=($!)
done

# Retried: the brokers answer before the controller will accept RF=3.
created=""
for _ in $(seq 1 40); do
  "$KAFKA/bin/kafka-topics.sh" --bootstrap-server "$BOOT" --create --topic src \
      --partitions "$PARTITIONS" --replication-factor 3 >/dev/null 2>&1 && { created=yes; break; }
  sleep 2
done
[ -n "$created" ] || { echo "could not create the source topic" >&2; exit 1; }
phase "cluster up: 3 brokers, RF=3, $PARTITIONS partitions"

# ---- the payload --------------------------------------------------------------
# JSON, because the mapping reads a field. kafka-producer-perf-test's
# --record-size generates random letters, which the mapping cannot parse: both
# implementations then pass the bytes through unchanged, agree perfectly, and
# the comparison measures nothing but the consume path. Padded to ~200 bytes so
# the byte rate matches the broker-tax figure this benchmark is calibrated
# against.
python3 - "$WORK/payload.json" <<'PYEOF'
import sys, json
pad = "x" * 160
with open(sys.argv[1], "w") as f:
    for i in range(2000):
        f.write(json.dumps({"n": i, "pad": pad}) + "\n")
PYEOF
PAYLOAD=(--payload-file "$WORK/payload.json" --payload-delimiter '\n')

# ---- a small fixed corpus, for the correctness check only --------------------
CHECK=2000
bounded "$SETUP_TIMEOUT" "seeding" "$KAFKA/bin/kafka-producer-perf-test.sh" \
    --topic src --num-records "$CHECK" "${PAYLOAD[@]}" --throughput -1 \
    --producer-props bootstrap.servers="$BOOT" acks=1 >/dev/null 2>&1 ||
    { echo "seeding failed" >&2; exit 1; }
phase "seeded $CHECK JSON messages for the correctness check"

# ---- the two configs ---------------------------------------------------------
# Semantically identical, syntactically different because the schemas are.
# `drop` on both sides: this measures consume + map, not the broker's produce
# path, which would otherwise dominate and is the same cost for both anyway.
MAPPING='root = this
root.sq = this.n * this.n'

sf_cfg() {  # sf_cfg <group>
  cat > "$WORK/sf.yaml" <<EOF
input:
  kafka:
    seed_brokers: [ $(broker_list) ]
    topics: [ "src" ]
    consumer_group: "$1"
    start_from_oldest: true
pipeline:
  processors:
    - mapping: |
$(printf '%s\n' "$MAPPING" | sed 's/^/        /')
output:
  drop: {}
EOF
}
rc_cfg() {  # rc_cfg <group>
  cat > "$WORK/rc.yaml" <<EOF
input:
  kafka:
    addresses: [ $(broker_list) ]
    topics: [ "src" ]
    consumer_group: "$1"
    start_from_oldest: true
pipeline:
  processors:
    - mapping: |
$(printf '%s\n' "$MAPPING" | sed 's/^/        /')
output:
  drop: {}
EOF
}

# ---- correctness before timing ----------------------------------------------
# A throughput number for two programs doing different work is worse than no
# number, and here the configs are not even the same text -- so this is the only
# thing establishing that they do the same job.
#
# A Kafka input never terminates: it consumes what is there and then waits for
# more. So each side is read under a bounded window rather than to completion,
# and a timeout is the EXPECTED outcome, not a failure.
sf_cfg "chk-sf"; rc_cfg "chk-rc"
sed -i 's/^  drop: {}/  stdout: {}/' "$WORK/sf.yaml" "$WORK/rc.yaml"
timeout -k 5 60 $SF_ROOT/build/swordfish-run --config "$WORK/sf.yaml" \
    --smp 1 --memory 1G --overprovisioned > "$WORK/sf.raw" 2>"$WORK/sf.err"
timeout -k 5 60 "$RC" run --disable-telemetry --log.level off \
    "$WORK/rc.yaml" > "$WORK/rc.raw" 2>"$WORK/rc.err"
grep '^{' "$WORK/sf.raw" | sort > "$WORK/sf.chk"
grep '^{' "$WORK/rc.raw" | sort > "$WORK/rc.chk"
n_sf=$(wc -l < "$WORK/sf.chk"); n_rc=$(wc -l < "$WORK/rc.chk")
if [ "$n_sf" -lt "$CHECK" ] || [ "$n_rc" -lt "$CHECK" ]; then
  echo "correctness check read too little (swordfish $n_sf, reference $n_rc of $CHECK)" >&2
  echo "--- swordfish stderr ---" >&2; tail -5 "$WORK/sf.err" >&2
  echo "--- reference stderr ---" >&2; tail -5 "$WORK/rc.err" >&2
  exit 1
fi
if ! diff -q "$WORK/sf.chk" "$WORK/rc.chk" >/dev/null; then
  echo "outputs differ; refusing to report a number" >&2
  diff "$WORK/sf.chk" "$WORK/rc.chk" | head -6 >&2; exit 1
fi
phase "correctness: identical output on $CHECK messages, both implementations"
echo

# ---- measured runs -----------------------------------------------------------
# Each implementation gets its OWN topic and group, so the second is not handed
# whatever the first one's production left behind.
#
# Only the CLIENT's CPU is measured. The brokers are separate processes, so
# their 66.5 CPU-s/1M never enters these figures.
consumed() {  # consumed <group> -- sum of committed offsets
  "$KAFKA/bin/kafka-consumer-groups.sh" --bootstrap-server "$BOOT" \
      --describe --group "$1" 2>/dev/null |
    awk '$4 ~ /^[0-9]+$/ { n += $4 } END { print n+0 }'
}

run_one() {  # run_one <label> <cfgfn> <command...>
  local label=$1 cfgfn=$2; shift 2
  local topic="bench-${label}" group="g-${label}"

  bounded 120 "create $topic" "$KAFKA/bin/kafka-topics.sh" --bootstrap-server "$BOOT" \
      --create --topic "$topic" --partitions "$PARTITIONS" --replication-factor 3 \
      >/dev/null 2>&1 || { echo "could not create $topic" >&3; return 1; }

  "$cfgfn" "$group"
  # sf_cfg/rc_cfg always write sf.yaml/rc.yaml; the sweep needs one per rate.
  mv "$WORK/${cfgfn%_cfg}.yaml" "$WORK/${label}.yaml"
  # Point the config at this run's own topic.
  sed -i "s/topics: \[ \"src\" \]/topics: [ \"$topic\" ]/" "$WORK/${label}.yaml"

  rm -f "$WORK/t"
  /usr/bin/time -f "%e %U %S" -o "$WORK/t" "$@" >/dev/null 2>"$WORK/$label.run.err" &
  local tpid=$!
  sleep 5                                   # let the group form before producing

  bounded "$RUN_TIMEOUT" "$label load" "$KAFKA/bin/kafka-producer-perf-test.sh" \
      --topic "$topic" --num-records $((RATE * DURATION)) "${PAYLOAD[@]}" \
      --throughput "$RATE" \
      --producer-props bootstrap.servers="$BOOT" acks=1 > "$WORK/$label.prod" 2>&1
  sleep "$GRACE"                            # drain and commit

  # SIGINT the CLIENT, not the `time` wrapper: killing the wrapper would leave
  # the client re-parented and lose the measurement with it.
  local c
  for c in $(pgrep -P "$tpid" 2>/dev/null); do kill -INT "$c" 2>/dev/null; done
  for _ in $(seq 30); do kill -0 "$tpid" 2>/dev/null || break; sleep 1; done
  kill_tree "$tpid" 2>/dev/null
  wait "$tpid" 2>/dev/null

  local got; got=$(consumed "$group")
  [ -s "$WORK/t" ] || { echo "no measurement written for $label" >&3; return 1; }
  [ "$got" -gt 0 ] || { echo "$label consumed nothing" >&3; return 1; }
  local w c_ offered=$((RATE * DURATION))
  w=$(awk '{printf "%.2f", $1}' "$WORK/t")
  c_=$(awk '{printf "%.2f", $2 + $3}' "$WORK/t")
  # Rate is derived from the WALL time actually spent, not from DURATION. A
  # client that falls behind keeps consuming through the grace period and past
  # it, so dividing by DURATION would report a rate it never achieved.
  local per; per=$(awk -v c="$c_" -v n="$got" 'BEGIN{printf "%.1f", c/n*1000000}')
  local kept=yes
  [ "$got" -lt $((offered * 99 / 100)) ] && kept=no
  printf '%-6s %-10s %14s %8s %9s %11s %8s\n' "$RATE" "${label%%[0-9]*}" \
      "$got/$offered" "$w" "$c_" "$per" "$kept"
  echo "${label%%[0-9]*} $RATE $per $kept" >> "$WORK/results"
  # The premise of this benchmark is that BOTH clients keep up, so that what is
  # compared is the cost of the same work rather than one implementation's
  # behaviour under saturation. If one falls behind, the comparison is not
  # apples-to-apples and saying so is more useful than printing the number.
}

printf '%-6s %-10s %14s %8s %9s %11s %8s\n' rate impl consumed/offered "wall(s)" "cpu(s)" "cpu-s/1M" "kept up"
: > "$WORK/results"
for RATE in $RATES; do
  run_one "sf$RATE" sf_cfg $SF_ROOT/build/swordfish-run --config "$WORK/sf$RATE.yaml" \
      --smp "$CORES" --memory 2G --overprovisioned || exit 1
  run_one "rc$RATE" rc_cfg env GOMAXPROCS=$CORES "$RC" run --disable-telemetry \
      --log.level off "$WORK/rc$RATE.yaml" || exit 1
done

# Each implementation is credited with its best (lowest) cost among the rates it
# actually sustained. A rate it could not keep up with says something useful
# about its ceiling, but its cost there describes saturation, not the work.
best() { awk -v k="$1" '$1 ~ "^"k && $4 == "yes" { if (b == "" || $3+0 < b+0) b = $3 } END { print (b == "" ? "n/a" : b) }' "$WORK/results"; }
sf_best=$(best sf); rc_best=$(best rc)
echo
echo "sf = swordfish (interpreted), rc = redpanda-connect"
echo "${DURATION}s per rate, $CORES cores each, 3-broker RF=3 cluster, ~200-byte JSON"
echo "client CPU only; broker CPU (66.5 s/1M here) is excluded by construction"
echo
echo "best sustained cost:  swordfish ${sf_best} cpu-s/1M   redpanda-connect ${rc_best} cpu-s/1M"
if [ "$sf_best" != "n/a" ] && [ "$rc_best" != "n/a" ]; then
  awk -v a="$sf_best" -v b="$rc_best" 'BEGIN{printf "ratio: swordfish uses %.1fx less CPU per message at each implementation\x27s best\n", b/a}'
else
  echo "one implementation sustained no rate in the sweep; widen RATES downward"
  exit 1
fi
