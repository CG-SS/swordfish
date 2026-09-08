#!/usr/bin/env bash
# Integration test against a real Kafka broker.
#
#   tools/run_kafka_itest.sh [path-to-kafka-dist]
#
# Starts a single-node KRaft broker, creates the fixture topic, seeds it through
# Kafka's OWN console producer (so the decoder is tested against bytes Kafka
# authored), runs sfkafkaitest, and shuts the broker down again. Everything
# lands under a scratch directory that is removed on exit.
#
# Without this, `ctest -R kafka_integration` still passes -- it skips when
# nothing is listening on SWORDFISH_KAFKA_BROKER.
set -euo pipefail
cd "$(dirname "$0")/.."
SF_ROOT=$PWD

KAFKA=${1:-../kafka_2.13-4.3.1}
[ -d "$KAFKA" ] || { echo "no Kafka distribution at $KAFKA" >&2; exit 2; }
KAFKA=$(cd "$KAFKA" && pwd)

BIN=${SWORDFISH_KAFKA_ITEST:-build/sfkafkaitest}
[ -x "$BIN" ] || { echo "build sfkafkaitest first ($BIN)" >&2; exit 2; }

WORK=$(mktemp -d)
BROKER_PID=""
# `kill %1` does not reliably reach a JVM started earlier in the script, and a
# leaked broker holds its ports and breaks the NEXT run. Track the pid, give it
# a chance to shut down cleanly, then insist.
cleanup() {
  if [ -n "$BROKER_PID" ] && kill -0 "$BROKER_PID" 2>/dev/null; then
    kill "$BROKER_PID" 2>/dev/null || true
    for _ in $(seq 1 20); do
      kill -0 "$BROKER_PID" 2>/dev/null || break
      sleep 0.5
    done
    kill -9 "$BROKER_PID" 2>/dev/null || true
  fi
  rm -rf "$WORK"
}
trap cleanup EXIT INT TERM

# The JDK on this image has been shipped broken in two different ways; the
# probing and the workarounds live in one place, shared with the chaos harness.
. "$SF_ROOT/tools/kafka_env.sh"

export LOG_DIR="$WORK/logs"

# Both listeners move together: the broker port AND the KRaft controller port,
# or a second run collides with the first on 9093 while looking like a broker
# start failure.
PORT=${KAFKA_PORT:-9092}
CPORT=$((PORT + 1))
sed -e "s|^log.dirs=.*|log.dirs=$WORK/data|" \
    -e "s|:9093|:$CPORT|g" \
    -e "s|:9092|:$PORT|g" \
    "$KAFKA/config/server.properties" > "$WORK/server.properties"

echo "formatting storage in $WORK/data"
CLUSTER_ID=$("$KAFKA/bin/kafka-storage.sh" random-uuid 2>/dev/null | tail -1)
"$KAFKA/bin/kafka-storage.sh" format -t "$CLUSTER_ID" \
    -c "$WORK/server.properties" --standalone >/dev/null

echo "starting broker on localhost:$PORT"
"$KAFKA/bin/kafka-server-start.sh" "$WORK/server.properties" > "$WORK/broker.log" 2>&1 &
BROKER_PID=$!

for i in $(seq 1 60); do
  if (exec 3<>"/dev/tcp/127.0.0.1/$PORT") 2>/dev/null; then break; fi
  [ "$i" = 60 ] && { echo "broker did not start; see $WORK/broker.log" >&2;
                     tail -20 "$WORK/broker.log" >&2; exit 1; }
  sleep 1
done

BOOTSTRAP=localhost:$PORT
"$KAFKA/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --create \
    --topic sf-test --partitions 2 --replication-factor 1 >/dev/null 2>&1

# Seeded through Kafka's own producer, compressed: this is what
# kafka_reads_what_kafkas_own_producer_wrote looks for.
printf 'from-kafkas-own-producer\n' | \
  "$KAFKA/bin/kafka-console-producer.sh" --bootstrap-server "$BOOTSTRAP" \
      --topic sf-test --producer-property compression.type=snappy >/dev/null 2>&1

echo "running $BIN"
SWORDFISH_KAFKA_BROKER=$BOOTSTRAP "$SF_ROOT/$BIN" --log_level=message \
    -- --smp 2 --memory 512M --overprovisioned
