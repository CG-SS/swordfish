#!/usr/bin/env bash
# Does the consumer group's COMMITTED OFFSET end up where it should?
#
#   tools/run_kafka_commit_check.sh [path-to-kafka-dist]
#
# The unit tests around offset_tracker are cheap and were, for a while, all
# green while two critical defects sat in the consumer: they exercised a
# partition whose offsets started at 0 and were dense, which is the one shape in
# which the broken model happened to work. Both defects are invisible without a
# real broker and a real group:
#
#   RESUME   A group that comes back and resumes from a committed offset must go
#            on committing. It committed nothing, for ever, and replayed the same
#            records on every restart.
#   HOLE     A partition's offsets are not dense. Compaction and transaction
#            markers leave gaps, and the commit must step over them. It froze one
#            past the gap and the lag grew without bound.
#
# Each scenario asserts on `kafka-consumer-groups --describe` -- the broker's own
# view -- rather than on swordfish's logs, and each is run against the reference
# too when one is present, so "correct" is not this implementation's opinion.
set -uo pipefail
cd "$(dirname "$0")/.."
SF_ROOT=$PWD

KAFKA=${1:-../kafka_2.13-4.3.1}
[ -d "$KAFKA" ] || { echo "no Kafka distribution at $KAFKA" >&2; exit 2; }
KAFKA=$(cd "$KAFKA" && pwd)

SF=${SWORDFISH_RUN:-build/swordfish-run}
[ -x "$SF" ] || { echo "build swordfish-run first ($SF)" >&2; exit 2; }
RC=${REDPANDA_CONNECT:-$SF_ROOT/../redpanda-connect}

WORK=$(mktemp -d)
BROKER_PID=""
cleanup() {
  if [ -n "$BROKER_PID" ] && kill -0 "$BROKER_PID" 2>/dev/null; then
    kill "$BROKER_PID" 2>/dev/null || true
    for _ in $(seq 1 20); do kill -0 "$BROKER_PID" 2>/dev/null || break; sleep 0.5; done
    kill -9 "$BROKER_PID" 2>/dev/null || true
  fi
  rm -rf "$WORK"
}
trap cleanup EXIT INT TERM

. "$SF_ROOT/tools/kafka_env.sh"
export LOG_DIR="$WORK/logs"

PORT=${KAFKA_PORT:-9092}
CPORT=$((PORT + 1))

# REFUSE to run if something is already listening. A leftover broker from an
# earlier run answers on the same port with the same topic names, and the new
# broker's failure to bind is just a line in a log nobody reads -- so the whole
# suite silently measures the wrong data. That happened: a run reported a
# committed offset of 30 on a topic this script had put ten records in, because
# the topic already held twenty from a previous run's broker that was still up.
# A stale broker survives `pkill -f kafka-server-start` because the start script
# execs a JVM, so the process is `java ... kafka.Kafka` and the pattern misses
# it; and a `kill -9` on this script skips the cleanup trap that would have
# stopped it. Both are easy to do, so the check is here rather than in the
# advice.
if (exec 3<>"/dev/tcp/127.0.0.1/$PORT") 2>/dev/null; then
  exec 3>&-
  echo "something is already listening on 127.0.0.1:$PORT." >&2
  echo "This script must own its broker: a leftover one has the same topic" >&2
  echo "names and would silently answer with another run's data." >&2
  echo "Find it with:  ps -eo pid,args | grep '[k]afka.Kafka'" >&2
  echo "or run against another port with KAFKA_PORT=<n>." >&2
  exit 2
fi

sed -e "s|^log.dirs=.*|log.dirs=$WORK/data|" \
    -e "s|:9093|:$CPORT|g" -e "s|:9092|:$PORT|g" \
    "$KAFKA/config/server.properties" > "$WORK/server.properties"
CLUSTER_ID=$("$KAFKA/bin/kafka-storage.sh" random-uuid 2>/dev/null | tail -1)
"$KAFKA/bin/kafka-storage.sh" format -t "$CLUSTER_ID" \
    -c "$WORK/server.properties" --standalone >/dev/null
"$KAFKA/bin/kafka-server-start.sh" "$WORK/server.properties" > "$WORK/broker.log" 2>&1 &
BROKER_PID=$!
for i in $(seq 1 60); do
  (exec 3<>"/dev/tcp/127.0.0.1/$PORT") 2>/dev/null && break
  [ "$i" = 60 ] && { echo "broker did not start; see $WORK/broker.log" >&2
                     tail -20 "$WORK/broker.log" >&2; exit 1; }
  sleep 1
done
BOOTSTRAP=localhost:$PORT
echo "broker up on $BOOTSTRAP"

fail=0
note() { echo "ok   $1"; }
bad()  { echo "FAIL $1: $2" >&2; fail=1; }

produce() {  # produce <topic> <line>...
  local t=$1; shift
  printf '%s\n' "$@" | \
    "$KAFKA/bin/kafka-console-producer.sh" --bootstrap-server "$BOOTSTRAP" \
      --topic "$t" >/dev/null 2>&1
}

# CURRENT-OFFSET and LAG for one partition, from the broker. Prints
# "<offset> <lag>", or "none" when the group has no row for the partition --
# which is itself a result worth distinguishing from a committed 0.
group_pos() {  # group_pos <group> <topic>
  local out
  out=$("$KAFKA/bin/kafka-consumer-groups.sh" --bootstrap-server "$BOOTSTRAP" \
          --describe --group "$1" 2>/dev/null \
        | awk -v t="$2" '$2 == t { print $4, $6 }' | head -1)
  [ -n "$out" ] && echo "$out" || echo "none"
}

# Runs one consumer to completion and stops it with SIGTERM, so the shutdown
# commit runs. `timeout` bounds a consumer that never settles.
consume() {  # consume <kind> <topic> <group> <seconds> <outfile>
  local kind=$1 topic=$2 group=$3 secs=$4 out=$5 pid comp
  # swordfish registers the modern franz-go-shaped consumer under the name
  # `kafka`; the reference has it under `kafka_franz` and keeps `kafka` for the
  # older sarama-shaped one, whose broker list is `addresses` rather than
  # `seed_brokers`. The FIELDS are the same, so only the component name differs.
  comp=kafka
  [ "$kind" = rc ] && comp=kafka_franz
  cat > "$WORK/c.yaml" <<YAML
input:
  $comp:
    seed_brokers: [ $BOOTSTRAP ]
    topics: [ $topic ]
    consumer_group: $group
    start_from_oldest: true
output:
  file: { path: $out, codec: lines }
YAML
  if [ "$kind" = sf ]; then
    "$SF" --config "$WORK/c.yaml" --smp 1 -m 512M >"$WORK/$kind.log" 2>&1 &
  else
    "$RC" run --log.level off "$WORK/c.yaml" >"$WORK/$kind.log" 2>&1 &
  fi
  pid=$!
  sleep "$secs"
  kill -TERM "$pid" 2>/dev/null
  for _ in $(seq 1 40); do kill -0 "$pid" 2>/dev/null || break; sleep 0.5; done
  kill -9 "$pid" 2>/dev/null
  wait "$pid" 2>/dev/null
}

# ---- RESUME: a restarted group must go on committing -------------------------
#
# Run 1 reads offsets 0..9 and commits 10. Run 2 resumes at 10, reads 10..19 and
# must commit 20. It used to commit nothing at all on run 2 -- the tracker
# advanced a run from offset 0 that could never close once the partition started
# anywhere else -- so every restart replayed the same records for ever.
resume_case() {  # resume_case <kind> <suffix>
  local kind=$1 t=resume$2 g=gresume$2 pos1 pos2 n2
  "$KAFKA/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --create \
      --topic "$t" --partitions 1 --replication-factor 1 >/dev/null 2>&1
  produce "$t" A-0 A-1 A-2 A-3 A-4 A-5 A-6 A-7 A-8 A-9
  consume "$kind" "$t" "$g" 12 "$WORK/$kind.r1"
  pos1=$(group_pos "$g" "$t")
  produce "$t" B-10 B-11 B-12 B-13 B-14 B-15 B-16 B-17 B-18 B-19
  consume "$kind" "$t" "$g" 12 "$WORK/$kind.r2"
  pos2=$(group_pos "$g" "$t")
  n2=$(wc -l < "$WORK/$kind.r2" 2>/dev/null || echo 0)
  echo "$pos1|$pos2|$n2"
}

# ---- HOLE: transaction markers leave gaps the commit must step over ----------
#
# A committed transaction ends with a CONTROL BATCH -- a marker Kafka writes into
# the log that occupies an offset but is not data. Three transactions of two
# records each therefore produce a log ending at offset 9 whose DATA offsets are
# 0,1,3,4,6,7, with markers at 2, 5 and 8.
#
# That is two defects in one fixture:
#   * the markers must not reach the pipeline (they are a 6-byte struct that
#     arrived as a message whose body is `000000000000`, mapped, acked and
#     written to the output: 9 delivered where the reference delivered 6);
#   * and the holes they leave must not freeze the commit.
#
# Markers are used rather than log compaction because their offsets are exact.
# Compaction is asynchronous and the cleaner declined to run at all here within
# ten minutes, which makes a compaction-based fixture either slow and flaky or
# silently vacuous.
TXJAVA=$WORK/TxProduce.java
cat > "$TXJAVA" <<'JAVA'
import org.apache.kafka.clients.producer.*;
import java.util.Properties;

// Three committed transactions of two records each. Each commit writes a control
// batch, so the log ends at offset 9 with data at 0,1,3,4,6,7.
public class TxProduce {
    public static void main(String[] args) {
        String bootstrap = args[0], topic = args[1];
        Properties p = new Properties();
        p.put("bootstrap.servers", bootstrap);
        p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        p.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        p.put("transactional.id", "sf-commit-check-tx");
        p.put("enable.idempotence", "true");
        try (Producer<String, String> pr = new KafkaProducer<>(p)) {
            pr.initTransactions();
            for (int t = 1; t <= 3; t++) {
                pr.beginTransaction();
                for (int i = 1; i <= 2; i++)
                    pr.send(new ProducerRecord<>(topic, null, "tx" + t + "-" + i));
                pr.commitTransaction();
            }
        }
    }
}
JAVA
KAFKA_CP=$KAFKA/libs/*
if ! javac -cp "$KAFKA_CP" -d "$WORK" "$TXJAVA" >"$WORK/javac.log" 2>&1; then
  echo "FAIL could not build the transactional producer:" >&2
  tail -5 "$WORK/javac.log" >&2
  exit 1
fi

hole_case() {  # hole_case <kind> <suffix>
  local kind=$1 t=tx$2 g=gtx$2 pos n end
  "$KAFKA/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --create \
      --topic "$t" --partitions 1 --replication-factor 1 >/dev/null 2>&1
  java -cp "$WORK:$KAFKA_CP" TxProduce "$BOOTSTRAP" "$t" >"$WORK/tx.$kind.log" 2>&1 || {
    echo "producer failed:" >&2; tail -5 "$WORK/tx.$kind.log" >&2; }
  # The broker's own view of where the log ends, so the assertion below is
  # against Kafka rather than against an assumption about marker placement.
  end=$("$KAFKA/bin/kafka-get-offsets.sh" \
          --bootstrap-server "$BOOTSTRAP" --topic "$t" 2>/dev/null \
        | awk -F: '{print $3}' | head -1)
  consume "$kind" "$t" "$g" 12 "$WORK/$kind.h"
  pos=$(group_pos "$g" "$t")
  n=$(wc -l < "$WORK/$kind.h" 2>/dev/null || echo 0)
  echo "$end|$pos|$n"
}

# ---- BOUNCE: the broker dying must not end consumption ------------------------
#
# poll() throwing propagated out of read_batch, and stream::input_loop catches
# everything, logs "input layer failed" and leaves the loop for good. Measured:
# `kill -9` on the broker ended consumption for the LIFE OF THE PROCESS -- the
# pipeline reported a clean in=3 out=3 and exited -- and it never resumed when
# the broker returned. The reference survives the same kill and goes on.
#
# So this asserts on two things a log line cannot fake: the consumer is still
# ALIVE after the broker is killed, and it delivers records produced AFTER the
# broker came back.
restart_broker() {
  kill -9 "$BROKER_PID" 2>/dev/null
  wait "$BROKER_PID" 2>/dev/null
  for _ in $(seq 1 40); do
    (exec 3<>"/dev/tcp/127.0.0.1/$PORT") 2>/dev/null || break
    sleep 0.5
  done
  "$KAFKA/bin/kafka-server-start.sh" "$WORK/server.properties" \
      >> "$WORK/broker.log" 2>&1 &
  BROKER_PID=$!
  local i
  for i in $(seq 1 90); do
    (exec 3<>"/dev/tcp/127.0.0.1/$PORT") 2>/dev/null && break
    sleep 1
  done
}

bounce_case() {  # bounce_case <kind> <suffix>
  local kind=$1 t=bounce$2 g=gbounce$2 comp pid alive_after_kill alive_after_restart
  "$KAFKA/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --create \
      --topic "$t" --partitions 1 --replication-factor 1 >/dev/null 2>&1
  produce "$t" E-1 E-2 E-3
  comp=kafka
  [ "$kind" = rc ] && comp=kafka_franz
  cat > "$WORK/b.yaml" <<YAML
input:
  $comp:
    seed_brokers: [ $BOOTSTRAP ]
    topics: [ $t ]
    consumer_group: $g
    start_from_oldest: true
output:
  file: { path: $WORK/$kind.b, codec: lines }
YAML
  if [ "$kind" = sf ]; then
    "$SF" --config "$WORK/b.yaml" --smp 1 -m 512M >"$WORK/$kind.b.log" 2>&1 &
  else
    "$RC" run --log.level off "$WORK/b.yaml" >"$WORK/$kind.b.log" 2>&1 &
  fi
  pid=$!
  sleep 10                                   # let it read E-1..E-3
  restart_broker
  kill -0 "$pid" 2>/dev/null && alive_after_kill=yes || alive_after_kill=no
  produce "$t" F-4 F-5 F-6
  sleep 20                                   # let it notice and catch up
  kill -0 "$pid" 2>/dev/null && alive_after_restart=yes || alive_after_restart=no
  kill -TERM "$pid" 2>/dev/null
  for _ in $(seq 1 40); do kill -0 "$pid" 2>/dev/null || break; sleep 0.5; done
  kill -9 "$pid" 2>/dev/null
  wait "$pid" 2>/dev/null
  # Written to a file, NOT echoed. This function restarts the broker and assigns
  # BROKER_PID, and a `$(bounce_case ...)` would run it in a subshell where that
  # assignment is lost -- leaving the restarted broker with nothing tracking it,
  # so the cleanup trap kills the pid of a broker that is already dead and the
  # live one survives the script. That exact leak, from an earlier run, is why
  # this script now refuses to start when the port is busy.
  echo "$alive_after_kill|$alive_after_restart|$(sort "$WORK/$kind.b" 2>/dev/null | tr '\n' ',')" \
    > "$WORK/bounce.$kind"
}

# ---- ACKS 0: a produce nobody answers must not wait for an answer -------------
#
# Kafka sends NOTHING back when acks is 0. The request was still sent as one that
# waits for a response: it parked for the whole 30s request timeout, marked the
# connection dead, failed every pending request and threw, and the retry rewrote
# the same records. Three messages became nine in the topic -- three physical
# copies -- the process never terminated, and stderr carried not one diagnostic
# line. `acks` is an advertised, lintable field, so a config both linters passed
# reached all of that.
acks0_case() {  # acks0_case <acks> <topic>
  local acks=$1 t=$2 rc n
  "$KAFKA/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --create \
      --topic "$t" --partitions 1 --replication-factor 1 >/dev/null 2>&1
  cat > "$WORK/a.yaml" <<YAML
input:
  generate:
    count: 3
    interval: ""
    mapping: 'root = "hello-" + count("$t").string()'
output:
  kafka:
    seed_brokers: [ $BOOTSTRAP ]
    topic: $t
    acks: $acks
YAML
  # 45s is past the 30s request timeout the broken version parked on, so a run
  # that hits it is reported as a hang rather than waited out.
  timeout -s KILL 45 "$SF" --config "$WORK/a.yaml" --smp 1 -m 512M \
      >"$WORK/a.$t.log" 2>&1
  rc=$?
  n=$("$KAFKA/bin/kafka-get-offsets.sh" --bootstrap-server "$BOOTSTRAP" \
        --topic "$t" 2>/dev/null | awk -F: '{print $3}' | head -1)
  echo "$rc|${n:-?}"
}
echo "--- acks: 0 ---"
a0=$(acks0_case 0 acks0)
a1=$(acks0_case -1 acksall)
echo "swordfish: acks=0 -> exit ${a0%%|*}, ${a0##*|} record(s) in the topic"
echo "swordfish: acks=-1 -> exit ${a1%%|*}, ${a1##*|} record(s) in the topic"
case "$a0" in
  '0|3') note "acks: 0 terminates and writes each record once" ;;
  137\|*|124\|*)
    bad "acks: 0 terminates and writes each record once" \
        "the run never terminated (exit ${a0%%|*}); ${a0##*|} record(s) reached the topic" ;;
  *) bad "acks: 0 terminates and writes each record once" \
         "want exit 0 with 3 records, got '$a0'" ;;
esac
[ "$a1" = '0|3' ] && note "and acks: -1 is unaffected" \
                  || bad "and acks: -1 is unaffected" "want '0|3', got '$a1'"

echo "--- broker bounce ---"
bounce_case sf sf
sf_bounce=$(cat "$WORK/bounce.sf")
echo "swordfish: alive_after_kill=${sf_bounce%%|*} alive_after_restart=$(echo "$sf_bounce" | cut -d'|' -f2) got=$(echo "$sf_bounce" | cut -d'|' -f3)"
if [ -x "$RC" ]; then
  bounce_case rc rc
  rc_bounce=$(cat "$WORK/bounce.rc")
  echo "reference: alive_after_kill=${rc_bounce%%|*} alive_after_restart=$(echo "$rc_bounce" | cut -d'|' -f2) got=$(echo "$rc_bounce" | cut -d'|' -f3)"
fi
case "$sf_bounce" in
  'yes|yes|E-1,E-2,E-3,F-4,F-5,F-6,')
    note "the consumer survives a broker restart and resumes" ;;
  no\|*)  bad "the consumer survives a broker restart and resumes" \
              "it was already gone when the broker was killed: '$sf_bounce'" ;;
  *)      bad "the consumer survives a broker restart and resumes" \
              "want 'yes|yes|E-1,E-2,E-3,F-4,F-5,F-6,', got '$sf_bounce'" ;;
esac

echo "--- resume ---"
sf_resume=$(resume_case sf sf)
echo "swordfish: run1=${sf_resume%%|*} $(echo "$sf_resume" | cut -d'|' -f2) delivered_run2=$(echo "$sf_resume" | cut -d'|' -f3)"
if [ -x "$RC" ]; then
  rc_resume=$(resume_case rc rc)
  echo "reference: run1=${rc_resume%%|*} $(echo "$rc_resume" | cut -d'|' -f2) delivered_run2=$(echo "$rc_resume" | cut -d'|' -f3)"
fi
case "$sf_resume" in
  '10 0|20 0|10')
    note "a restarted group resumes and goes on committing (10/0 then 20/0)" ;;
  *) bad "a restarted group resumes and goes on committing" \
         "want '10 0|20 0|10', got '$sf_resume'" ;;
esac

echo "--- transaction markers ---"
sf_hole=$(hole_case sf sf)
sf_end=${sf_hole%%|*}
echo "swordfish: log_end=$sf_end pos=$(echo "$sf_hole" | cut -d'|' -f2) delivered=$(echo "$sf_hole" | cut -d'|' -f3)"
if [ -x "$RC" ]; then
  rc_hole=$(hole_case rc rc)
  echo "reference: log_end=${rc_hole%%|*} pos=$(echo "$rc_hole" | cut -d'|' -f2) delivered=$(echo "$rc_hole" | cut -d'|' -f3)"
fi
if [ "$sf_end" != 9 ]; then
  bad "transaction markers are skipped and do not freeze the commit" \
      "the log ends at '$sf_end', not 9, so the three markers were not written and" \
      "the scenario proves nothing"
else
  # Commit 8 with LAG 1, not 9 with LAG 0. The last thing in the log is a marker
  # and nobody consumes it, so a transactional topic sits one behind its log end
  # for ever. That is the reference's answer too, which is the point of asking it
  # rather than asserting the number that looks tidier.
  want='8 1|6'
  [ -x "$RC" ] && want=$(echo "$rc_hole" | cut -d'|' -f2,3)
  case "$(echo "$sf_hole" | cut -d'|' -f2,3)" in
    "$want") note "transaction markers are skipped and do not freeze the commit ($want)" ;;
    *) bad "transaction markers are skipped and do not freeze the commit" \
           "want '$want', got '$(echo "$sf_hole" | cut -d'|' -f2,3)'" ;;
  esac
fi
# And the six must be the actual records, not markers that happen to number six.
if [ -s "$WORK/sf.h" ]; then
  got=$(sort "$WORK/sf.h" | tr '\n' ',')
  [ "$got" = 'tx1-1,tx1-2,tx2-1,tx2-2,tx3-1,tx3-2,' ] \
    && note "and the delivered records are the transaction bodies, not the markers" \
    || bad "and the delivered records are the transaction bodies, not the markers" \
           "got '$got'"
fi

echo
[ "$fail" -eq 0 ] && echo "kafka commit check: the committed offset lands where the broker says it should"
exit $fail
