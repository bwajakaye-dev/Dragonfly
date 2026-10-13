"""Print the as-run log: every switchboard cut, replayed from the beginning.

The log lives only in the ``dragonfly.asrun`` topic (7-day retention). This
reader assigns itself every partition at the earliest offset, prints until
it reaches the end, and exits (or keeps tailing with --follow). It never
commits offsets, so it can be re-run any number of times.

    python scripts/asrun_log.py
    python scripts/asrun_log.py --follow

The table goes to stdout with print() on purpose: it is this tool's output
(and can be redirected to a file), not diagnostics. Errors use logging.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import uuid
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from confluent_kafka import (  # noqa: E402
    OFFSET_BEGINNING,
    Consumer,
    KafkaError,
    KafkaException,
    Message,
    TopicPartition,
)

from common.config import TOPIC_ASRUN, default_brokers  # noqa: E402
from common.log import setup_logging  # noqa: E402

log = logging.getLogger("asrun_log")

METADATA_TIMEOUT_S = 5.0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments, falling back to DRAGONFLY_* environment variables."""
    p = argparse.ArgumentParser(description="Print every switchboard cut from dragonfly.asrun.")
    p.add_argument(
        "--brokers", default=default_brokers(), help="bootstrap servers (env DRAGONFLY_BROKERS)"
    )
    p.add_argument("--follow", action="store_true", help="keep printing new cuts as they happen")
    return p.parse_args(argv)


def format_event(msg: Message) -> str:
    """One table row for an as-run record; malformed records are shown, not hidden."""
    try:
        event = json.loads(msg.value() or b"")
        ts = datetime.fromisoformat(event["ts"]).astimezone().strftime("%Y-%m-%d %H:%M:%S.%f")
        return (
            f"{ts[:-3]}  {event.get('from_cam') or '-':>8} -> {event['to_cam']:<8}  "
            f"{event.get('switchboard', '?')}"
        )
    except (ValueError, KeyError, TypeError) as exc:
        return f"(offset {msg.offset()}: unreadable record: {exc})"


def main(argv: list[str] | None = None) -> int:
    """Entry point; returns a process exit code."""
    args = parse_args(argv)
    setup_logging()
    consumer = Consumer(
        {
            "bootstrap.servers": args.brokers,
            "group.id": f"dragonfly-asrun-reader-{uuid.uuid4().hex[:8]}",
            "enable.auto.commit": False,
            "logger": logging.getLogger("rdkafka"),
        }
    )
    try:
        try:
            topic = consumer.list_topics(TOPIC_ASRUN, timeout=METADATA_TIMEOUT_S).topics[
                TOPIC_ASRUN
            ]
        except KafkaException:
            log.error("cannot reach Kafka at %s (is the cluster running?)", args.brokers)
            return 1
        if topic.error is not None:
            log.error("topic %s not found; run: python scripts/create_topics.py", TOPIC_ASRUN)
            return 1

        partitions = [TopicPartition(TOPIC_ASRUN, p, OFFSET_BEGINNING) for p in topic.partitions]
        remaining: dict[int, int] = {}
        for tp in partitions:
            low, high = consumer.get_watermark_offsets(tp, timeout=METADATA_TIMEOUT_S)
            if high > low:
                remaining[tp.partition] = high
        consumer.assign(partitions)

        print(f"{'time (local)':<23}  {'from':>8}    {'to':<8}  switchboard")
        count = 0
        while remaining or args.follow:
            msg = consumer.poll(0.5)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.warning("Kafka error: %s", msg.error())
                continue
            print(format_event(msg), flush=True)
            count += 1
            end = remaining.get(msg.partition())
            if end is not None and msg.offset() + 1 >= end:
                del remaining[msg.partition()]
        print(f"-- {count} cut(s)")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
