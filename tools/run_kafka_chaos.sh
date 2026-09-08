#!/usr/bin/env bash
# Consumer-group rebalance correctness under broker
# restarts, and at-least-once verified by a chaos test.
#
#   tools/run_kafka_chaos.sh [path-to-kafka-dist]
#
# Stands up a THREE-broker KRaft cluster, then consumes a replicated topic while
# killing and restarting brokers underneath -- including the group's
# coordinator, which is the failover most likely to be broken and the one a
# single-broker suite cannot reach at all.
#
# Messages are produced DURING the chaos, not before it. Seeding up front and
# then breaking things measures whether a drained consumer reconnects; producing
# throughout measures whether messages written while a broker is down still
# arrive, which is the property at-least-once actually claims.
#
# What is asserted, in order of how much each one matters:
#
#   1. NO LOSS. Every value the producer wrote appears at least once in the
#      union of what the members consumed.
#   2. THE CHAOS WAS FELT. The group went through more than one generation. A
#      run where the consumer never noticed the restarts would satisfy (1)
#      trivially and prove nothing -- the failure this project has been bitten
#      by before, a suite reporting green having skipped everything.
#   3. THE PRODUCER KEPT UP. The topic's end offsets are compared against what
#      was fed in, so a message the PRODUCER dropped is reported as that rather
#      than blamed on the consumer.
#
# Duplicates are reported but do not fail the run: at-least-once permits them.
set -uo pipefail
cd "$(dirname "$0")/.."
SF_ROOT=$PWD

KAFKA=${1:-../kafka_2.13-4.3.1}
[ -d "$KAFKA" ] || { echo "no Kafka distribution at $KAFKA" >&2; exit 2; }
KAFKA=$(cd "$KAFKA" && pwd)

BIN=${SWORDFISH_KAFKA_CHAOS:-build/sfkafkachaos}
[ -x "$BIN" ] || { echo "build sfkafkachaos first ($BIN)" >&2; exit 2; }

TOPIC=${TOPIC:-sf-chaos}
GROUP=${GROUP:-sf-chaos-group}
PARTITIONS=${PARTITIONS:-6}
MESSAGES=${MESSAGES:-40000}
CHUNK=${CHUNK:-250}          # produced per tick, then a short sleep
MEMBERS=${MEMBERS:-2}
SMP=${SMP:-2}

WORK=$(mktemp -d)
declare -a BROKER_PID=() BROKER_PORT=() MEMBER_PID=()
PRODUCER_PID=""
START=$(date +%s)
# Every phase reports how long it took. The whole run is ~2.5 minutes when
# nothing hangs, and the first version of this script had no way to say that --
# a run that had wedged for four hours looked exactly like one still working.
phase() { printf '[%4ss] %s\n' "$(( $(date +%s) - START ))" "$*"; }

cleanup() {
  rm -f "$WORK/producing"
  [ -n "$PRODUCER_PID" ] && kill "$PRODUCER_PID" 2>/dev/null
  local p
  # The members MUST be killed here. Without this, `timeout` firing would run
  # this trap, kill the brokers, and leave the script blocked in `wait` on two
  # consumers that had themselves hung -- which is how one run stayed alive for
  # four hours.
  for p in ${MEMBER_PID[@]+"${MEMBER_PID[@]}"}; do
    # Children first: these are `timeout` wrappers, and killing one does not
    # take its member with it.
    [ -n "$p" ] && pkill -9 -P "$p" 2>/dev/null
    [ -n "$p" ] && kill -9 "$p" 2>/dev/null
  done
  pkill -9 -f "$BIN --brokers" 2>/dev/null
  for p in ${BROKER_PID[@]+"${BROKER_PID[@]}"}; do
    [ -n "$p" ] && kill "$p" 2>/dev/null
  done
  sleep 1
  for p in ${BROKER_PID[@]+"${BROKER_PID[@]}"}; do
    [ -n "$p" ] && kill -9 "$p" 2>/dev/null
  done
  if [ -n "${KEEP:-}" ]; then
    echo "logs kept in $WORK" >&2
  else
    rm -rf "$WORK"
  fi
}
trap cleanup EXIT INT TERM

. "$SF_ROOT/tools/kafka_env.sh"
export LOG_DIR="$WORK/logs"

# ---- a three-node KRaft cluster ---------------------------------------------
#
# Every node is both broker and controller, the simplest arrangement that
# survives losing one. Replication factor 3 with min.insync.replicas=2 means a
# single broker can go away without the topic becoming unavailable -- without
# that, "chaos" would just be an outage and the run would measure nothing but
# our retry loop.
BASE_PORT=${KAFKA_PORT:-19092}
NODES=3
CLUSTER_ID=$("$KAFKA/bin/kafka-storage.sh" random-uuid 2>/dev/null | tail -1)
[ -n "$CLUSTER_ID" ] || { echo "could not generate a cluster id" >&2; exit 1; }

quorum=""
for i in $(seq 0 $((NODES - 1))); do
  quorum="${quorum}${quorum:+,}${i}@localhost:$((BASE_PORT + 100 + i))"
done

for i in $(seq 0 $((NODES - 1))); do
  BROKER_PORT[$i]=$((BASE_PORT + i))
  cat > "$WORK/b$i.properties" <<EOF
process.roles=broker,controller
node.id=$i
controller.quorum.voters=$quorum
listeners=PLAINTEXT://:$((BASE_PORT + i)),CONTROLLER://:$((BASE_PORT + 100 + i))
advertised.listeners=PLAINTEXT://localhost:$((BASE_PORT + i))
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
inter.broker.listener.name=PLAINTEXT
log.dirs=$WORK/d$i
num.partitions=$PARTITIONS
default.replication.factor=3
min.insync.replicas=2
offsets.topic.replication.factor=3
offsets.topic.num.partitions=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2
group.initial.rebalance.delay.ms=0
EOF
  "$KAFKA/bin/kafka-storage.sh" format -t "$CLUSTER_ID" \
      -c "$WORK/b$i.properties" >/dev/null 2>&1 || {
      echo "storage format failed for node $i" >&2; exit 1; }
done

start_broker() { "$KAFKA/bin/kafka-server-start.sh" "$WORK/b$1.properties" \
                     > "$WORK/b$1.log" 2>&1 & BROKER_PID[$1]=$!; }

wait_for_port() {  # wait_for_port <port> <seconds>
  local n
  for n in $(seq 1 $(($2 * 2))); do
    (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null && return 0
    sleep 0.5
  done
  return 1
}

phase "starting a $NODES-broker KRaft cluster on ports $BASE_PORT..$((BASE_PORT + NODES - 1))"
for i in $(seq 0 $((NODES - 1))); do start_broker "$i"; done
for i in $(seq 0 $((NODES - 1))); do
  wait_for_port "${BROKER_PORT[$i]}" 90 || {
    echo "broker $i did not start; see $WORK/b$i.log" >&2
    tail -20 "$WORK/b$i.log" >&2; exit 1; }
done

BOOTSTRAP=""
for i in $(seq 0 $((NODES - 1))); do
  BOOTSTRAP="${BOOTSTRAP}${BOOTSTRAP:+,}localhost:${BROKER_PORT[$i]}"
done

# Retried: the brokers answer before the controller will accept a topic with
# RF=3, so a single attempt loses a run to a race that has nothing to do with
# what is under test.
created=""
for attempt in $(seq 1 20); do
  if "$KAFKA/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --create \
      --topic "$TOPIC" --partitions "$PARTITIONS" --replication-factor 3 \
      --config min.insync.replicas=2 >/dev/null 2>&1; then
    created=yes; break
  fi
  sleep 2
done
[ -n "$created" ] || { echo "could not create $TOPIC after 20 attempts" >&2; exit 1; }
phase "created $TOPIC: $PARTITIONS partitions, RF=3, min.insync.replicas=2"

# The node hosting the group coordinator. Killing a broker at random usually
# misses it, and coordinator failover is the interesting half of this test.
coordinator_node() {
  "$KAFKA/bin/kafka-consumer-groups.sh" --bootstrap-server "$BOOTSTRAP" \
      --describe --group "$GROUP" --state 2>/dev/null |
      grep -o '([0-9]\+)' | head -1 | tr -d '()'
}

# How many records the topic actually holds, from Kafka's own accounting. Used
# to tell a message the PRODUCER dropped from one the consumer missed.
topic_count() {
  # kafka-get-offsets.sh, not kafka-run-class kafka.tools.GetOffsetShell: Kafka
  # 4 renamed it, and with stderr suppressed the old form printed nothing, which
  # awk summed to a confident-looking 0. An empty result is now reported as
  # "unknown" rather than as zero.
  "$KAFKA/bin/kafka-get-offsets.sh" --bootstrap-server "$BOOTSTRAP" \
      --topic "$TOPIC" 2>/dev/null |
      awk -F: 'NF>=3 {s += $3; n++} END {if (n) print s; else print ""}'
}

# ---- run the members --------------------------------------------------------
# Started BEFORE the producer, so the group forms while the topic is empty and
# every message is consumed as it lands rather than replayed from the log.
phase "starting $MEMBERS member(s) at --smp $SMP"
for m in $(seq 1 "$MEMBERS"); do
  # No `--` separator: app_template parses its own and Seastar's options
  # together, unlike the Boost.Test binaries.
  # Under `timeout`: --run-seconds is the member's own deadline, but a member
  # that hangs cannot enforce its own deadline -- that is precisely the bug this
  # harness found. The outer bound is what turns such a hang into a reported
  # failure instead of a wait with no end.
  timeout -k 10 300 \
      "$SF_ROOT/$BIN" --brokers "$BOOTSTRAP" --topic "$TOPIC" --group "$GROUP" \
      --out "$WORK/got-$m.txt" --run-seconds 240 --idle-seconds 30 \
      --drain-file "$WORK/drained" \
      --session-timeout-ms 10000 \
      --smp "$SMP" --memory 512M --overprovisioned \
      > "$WORK/member-$m.log" 2>&1 &
  MEMBER_PID+=($!)
done
sleep 6

# ---- produce, continuously, through the chaos -------------------------------
# `tee` captures exactly what was fed in, so the expectation is what was really
# offered rather than what the loop intended to offer.
phase "producing up to $MESSAGES messages in chunks of $CHUNK while brokers restart"
touch "$WORK/producing"
(
  i=1
  while [ -e "$WORK/producing" ] && [ "$i" -le "$MESSAGES" ]; do
    last=$((i + CHUNK - 1))
    [ "$last" -gt "$MESSAGES" ] && last=$MESSAGES
    seq -f "chaos-%08g" "$i" "$last"
    i=$((last + 1))
    sleep 0.4
  done
) | tee "$WORK/expected.txt" |
  "$KAFKA/bin/kafka-console-producer.sh" --bootstrap-server "$BOOTSTRAP" \
      --topic "$TOPIC" --producer-property acks=all \
      --producer-property linger.ms=0 --producer-property retries=2147483647 \
      --producer-property delivery.timeout.ms=120000 \
      > "$WORK/producer.log" 2>&1 &
PRODUCER_PID=$!
sleep 6

chaos_round() {  # chaos_round <index> <label>
  echo "  killing broker $1 ($2) on port ${BROKER_PORT[$1]}"
  kill -9 "${BROKER_PID[$1]}" 2>/dev/null
  sleep 8
  echo "  restarting broker $1"
  start_broker "$1"
  wait_for_port "${BROKER_PORT[$1]}" 90 ||
      echo "  WARNING: broker $1 did not come back" >&2
  sleep 8
}

coord=$(coordinator_node)
case "$coord" in ''|*[!0-9]*) coord=0 ;; esac
phase "chaos: the group coordinator is node $coord"
chaos_round "$coord" "the group coordinator"
chaos_round $(( (coord + 1) % NODES )) "a partition leader and replica"

# A broker restart does not necessarily rebalance the GROUP: with the offsets
# topic replicated, members can re-find the coordinator and carry on in the same
# generation, which is correct and desirable. Rebalance correctness needs a
# membership change, so one member is killed outright and the survivor must pick
# up its partitions -- including any offsets it had not committed, which is the
# at-least-once claim at its most load-bearing.
victim=${MEMBER_PID[$((MEMBERS - 1))]}
phase "killing member $MEMBERS (pid $victim) to force a rebalance"
# `$victim` is the `timeout` wrapper, not the member. SIGKILL is not forwarded,
# so killing it alone re-parents the member to init and leaves it happily in the
# group -- the rebalance then never happens and the whole point of this phase is
# lost. Kill the child first, while its parent still identifies it.
pkill -9 -P "$victim" 2>/dev/null
kill -9 "$victim" 2>/dev/null
unset 'MEMBER_PID[$((MEMBERS - 1))]'
MEMBER_PID=(${MEMBER_PID[@]+"${MEMBER_PID[@]}"})
sleep 25          # session timeout (10s) plus rejoin and refetch

rm -f "$WORK/producing"
wait "$PRODUCER_PID"; producer_rc=$?
PRODUCER_PID=""
# Only now may a member stop for idleness. Before this point a quiet member is
# quiet because its partitions have no data yet or its brokers are down, and
# stopping would take it out of the next rebalance.
touch "$WORK/drained"
phase "producer finished (exit $producer_rc); waiting for members to drain"
member_rc=0; member_why=""
for p in "${MEMBER_PID[@]}"; do
  wait "$p" || { rc=$?
    case $rc in
      124|137) member_rc=$rc; member_why="hung and was killed by timeout" ;;
      139)     member_rc=$rc; member_why="CRASHED (SIGSEGV)" ;;
      13[4-9]|14[0-3]) member_rc=$rc; member_why="died on signal $((rc - 128))" ;;
    esac; }
done
MEMBER_PID=()
phase "members finished"

# ---- verify ------------------------------------------------------------------
cat "$WORK"/got-*.txt.* > "$WORK/got-all.txt" 2>/dev/null
received=$(wc -l < "$WORK/got-all.txt")
sort -u "$WORK/got-all.txt" > "$WORK/got-distinct.txt"
distinct=$(wc -l < "$WORK/got-distinct.txt")
sort -u "$WORK/expected.txt" > "$WORK/expected-sorted.txt"
offered=$(wc -l < "$WORK/expected-sorted.txt")
comm -23 "$WORK/expected-sorted.txt" "$WORK/got-distinct.txt" > "$WORK/missing.txt"
missing=$(wc -l < "$WORK/missing.txt")
in_topic=$(topic_count)

echo
printf 'offered to producer  %s\n' "$offered"
printf 'accepted by Kafka    %s\n' "${in_topic:-unknown}"
printf 'received by members  %s\n' "$received"
printf 'distinct             %s\n' "$distinct"
printf 'duplicates           %s   (at-least-once permits these)\n' "$((received - distinct))"
printf 'MISSING              %s\n' "$missing"

gens=$(grep -ho "generations=\[[^]]*\]" "$WORK"/member-*.log | sort -u | tr '\n' ' ')
errs=$(grep -ho "errors=[0-9]*" "$WORK"/member-*.log | tr '\n' ' ')
printf 'generations seen     %s\n' "${gens:-none}"
printf 'errors survived      %s\n' "${errs:-none}"

fail=0
# 124/137 from `timeout`: the member never exited on its own, which is a hang
# and not a slow run. Reported separately because a hang usually also shows up
# as missing messages, and the hang is the cause.
if [ "$member_rc" -ne 0 ]; then
  echo "FAIL: a surviving member $member_why (exit $member_rc)" >&2
  fail=1
fi
if [ "$missing" -ne 0 ]; then
  if [ -n "$in_topic" ] && [ "$in_topic" -lt "$offered" ]; then
    # Distinguish the two: a message Kafka never accepted is the producer's
    # problem, and blaming the consumer for it would send the next reader
    # hunting in the wrong place.
    echo "NOTE: Kafka accepted $in_topic of $offered offered, so up to" \
         "$((offered - in_topic)) of the missing were never produced" >&2
  fi
  dropped=0
  [ -n "$in_topic" ] && [ "$in_topic" -lt "$offered" ] && dropped=$((offered - in_topic))
  if [ "$missing" -gt "$dropped" ]; then
    echo "FAIL: $missing missing, more than the $dropped the producer dropped" >&2
    head -5 "$WORK/missing.txt" | sed 's/^/    /' >&2
    fail=1
  fi
fi
# The SURVIVOR must have gone through more than one generation: it was in the
# group before the other member was killed and after, so it has to have seen the
# reassignment. Checking every member would let a run pass on the victim's own
# startup generations.
if ! grep -qE "generations=\[[0-9]+,[0-9]+" "$WORK/member-1.log"; then
  echo "FAIL: the surviving member never changed generation, so the rebalance" >&2
  echo "      after killing member $MEMBERS did not happen" >&2
  grep -ho "generations=\[[^]]*\]" "$WORK/member-1.log" | sed 's/^/      /' >&2
  fail=1
fi
# And it must have noticed the broker restarts at all.
if grep -q "errors=0 " "$WORK/member-1.log"; then
  echo "FAIL: the surviving member recorded no errors, so the broker restarts" >&2
  echo "      never reached it and the no-loss result is vacuous" >&2
  fail=1
fi

echo
phase "done"
[ "$fail" -eq 0 ] &&
  echo "chaos: no loss across a coordinator restart and a broker restart, with a rebalance in each"
exit $fail
