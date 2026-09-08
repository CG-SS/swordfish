#!/usr/bin/env python3
"""Re-extract the vendored Kafka protocol schemas from a Kafka distribution.

The schemas live in `common/message/*.json` inside `kafka-clients-<v>.jar`. They
are Apache-2.0 and are vendored verbatim; see third_party/kafka-protocol/README.md.

    tools/vendor_kafka_schemas.py <path-to-kafka-dist>
"""
import pathlib
import sys
import zipfile

# The APIs an at-least-once consumer and producer need. Adding one here and
# re-running is the whole cost of supporting it.
APIS = [
    "ApiVersions", "Metadata", "Fetch", "Produce", "ListOffsets",
    "FindCoordinator", "JoinGroup", "SyncGroup", "Heartbeat", "LeaveGroup",
    "OffsetCommit", "OffsetFetch", "SaslHandshake", "SaslAuthenticate",
    "DescribeGroups",
]

# Schemas with "type": "data" rather than "request"/"response": embedded blobs
# that travel INSIDE another message's bytes field. The consumer group protocol
# puts a member's subscription and its assignment into JoinGroup and SyncGroup
# this way, so they are as much a part of the wire format as any request.
DATA = ["ConsumerProtocolSubscription", "ConsumerProtocolAssignment"]

ROOT = pathlib.Path(__file__).resolve().parents[1]
OUT = ROOT / "third_party/kafka-protocol/message"


def main(dist):
    jars = sorted(pathlib.Path(dist).glob("libs/kafka-clients-*.jar"))
    if not jars:
        sys.exit(f"no kafka-clients jar under {dist}/libs")
    z = zipfile.ZipFile(jars[-1])
    wanted = {f"{a}{s}.json" for a in APIS for s in ("Request", "Response")}
    wanted |= {f"{d}.json" for d in DATA}
    OUT.mkdir(parents=True, exist_ok=True)
    n = 0
    for name in z.namelist():
        base = name.rsplit("/", 1)[-1]
        if name.startswith("common/message/") and base in wanted:
            (OUT / base).write_bytes(z.read(name))
            n += 1
    print(f"{n} schemas -> {OUT.relative_to(ROOT)}  (from {jars[-1].name})")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "../kafka_2.13-4.3.1")
