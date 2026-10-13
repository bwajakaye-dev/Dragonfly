"""Create all Dragonfly topics (safe to run repeatedly).

Local 3-broker cluster:  python scripts/create_topics.py
Single-broker cloud VM:  python scripts/create_topics.py --brokers <vm-ip>:9092 \
                             --replication-factor 1 --min-isr 1
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from confluent_kafka import KafkaException  # noqa: E402
from confluent_kafka.admin import AdminClient  # noqa: E402

from common.config import default_brokers, env_int  # noqa: E402
from common.topics import check_brokers, ensure_topics  # noqa: E402

log = logging.getLogger("create_topics")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments, falling back to DRAGONFLY_* environment variables."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--brokers",
        default=default_brokers(),
        help="bootstrap servers (env DRAGONFLY_BROKERS)",
    )
    parser.add_argument(
        "--replication-factor",
        type=int,
        default=env_int("DRAGONFLY_REPLICATION_FACTOR", 3),
        help="replicas per partition: 3 locally, 1 on the single-broker VM",
    )
    parser.add_argument(
        "--min-isr",
        type=int,
        default=env_int("DRAGONFLY_MIN_ISR", 2),
        help="min.insync.replicas per topic: 2 locally, 1 on the VM",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Entry point; returns a process exit code."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    args = parse_args(argv)
    if not 1 <= args.min_isr <= args.replication_factor:
        log.error("--min-isr must be between 1 and --replication-factor")
        return 2

    admin = AdminClient({"bootstrap.servers": args.brokers})
    try:
        live = check_brokers(admin)
    except KafkaException:
        log.error("cannot reach Kafka at %s (is the cluster running?)", args.brokers)
        return 1
    if live < args.replication_factor:
        log.error(
            "replication factor %d needs %d brokers, but only %d are up",
            args.replication_factor,
            args.replication_factor,
            live,
        )
        return 1

    try:
        results = ensure_topics(admin, args.replication_factor, args.min_isr)
    except KafkaException as exc:
        log.error("topic creation failed: %s", exc)
        return 1
    for name, status in sorted(results.items()):
        log.info("%-22s %s", name, status)
    return 0


if __name__ == "__main__":
    sys.exit(main())
