"""Central configuration: broker list, topic names, topic configs, defaults.

Every value can be overridden by a CLI argument; when the argument is absent,
the corresponding ``DRAGONFLY_*`` environment variable is used, and finally the
hard-coded default below. No secrets live in this file.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

# --- Brokers -----------------------------------------------------------------

DEFAULT_BROKERS = "localhost:9092,localhost:9094,localhost:9096"


def env_str(name: str, default: str) -> str:
    """Return environment variable ``name`` or ``default`` if unset/empty."""
    value = os.environ.get(name, "").strip()
    return value or default


def env_int(name: str, default: int) -> int:
    """Return environment variable ``name`` parsed as int, or ``default``.

    Raises:
        ValueError: if the variable is set but is not an integer.
    """
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"environment variable {name}={raw!r} is not an integer") from exc


def default_brokers() -> str:
    """Bootstrap servers: ``DRAGONFLY_BROKERS`` or the local 3-broker cluster."""
    return env_str("DRAGONFLY_BROKERS", DEFAULT_BROKERS)


# --- Topics ------------------------------------------------------------------

TOPIC_FEEDS = "dragonfly.feeds"
TOPIC_REGISTRY = "dragonfly.registry"
TOPIC_HEARTBEAT = "dragonfly.heartbeat"
TOPIC_ASRUN = "dragonfly.asrun"
TOPIC_HEALTH = "dragonfly.health"

# Frames larger than this are split into chunks. Kept below Kafka's default
# 1 MB message limit so no broker or client size settings must be raised
# for normal (<= 1080p) frames.
MAX_CHUNK_BYTES = 900_000

# Per-topic message cap on the feeds topic: one chunk plus headers, with margin.
FEEDS_MAX_MESSAGE_BYTES = 1_048_576

_MINUTE_MS = 60_000
_HOUR_MS = 60 * _MINUTE_MS
_DAY_MS = 24 * _HOUR_MS


@dataclass(frozen=True)
class TopicSpec:
    """Desired shape of one Kafka topic."""

    name: str
    partitions: int
    config: dict[str, str] = field(default_factory=dict)


TOPIC_SPECS: tuple[TopicSpec, ...] = (
    TopicSpec(
        TOPIC_FEEDS,
        partitions=3,
        config={
            "retention.ms": str(5 * _MINUTE_MS),
            "max.message.bytes": str(FEEDS_MAX_MESSAGE_BYTES),
            # JPEG is already compressed; keep whatever the producer sends.
            "compression.type": "producer",
        },
    ),
    TopicSpec(
        TOPIC_REGISTRY,
        partitions=3,
        config={"cleanup.policy": "compact"},
    ),
    TopicSpec(
        TOPIC_HEARTBEAT,
        partitions=3,
        config={"retention.ms": str(_MINUTE_MS)},
    ),
    TopicSpec(
        TOPIC_ASRUN,
        partitions=1,  # single partition => one total order of cuts
        config={"retention.ms": str(7 * _DAY_MS)},
    ),
    TopicSpec(
        TOPIC_HEALTH,
        partitions=3,
        config={"retention.ms": str(_DAY_MS)},
    ),
)

# --- Media defaults ----------------------------------------------------------

DEFAULT_JPEG_QUALITY = 80
