"""Kafka client factories, so every process uses the same, documented settings.

Two producer profiles exist because the topics need different guarantees:

* **media** (frames, heartbeats): ``acks=1``, no retries beyond a short
  timeout. A lost frame is replaced by the next one 33 ms later, so waiting
  for every replica would only add latency. This is at-most-once in spirit;
  the rare duplicate from a retry is discarded by the switchboard's
  per-camera sequence check.
* **control** (registry, as-run, health): ``acks=all`` with idempotence. With
  ``min.insync.replicas=2`` a record is on two brokers before it counts as
  sent, so camera state and the cut log survive a broker crash.
"""

from __future__ import annotations

import atexit
import logging
import socket
import sys
import time
from collections.abc import Callable
from typing import Any

from confluent_kafka import Consumer, KafkaError, Producer

from common.config import FEEDS_MAX_MESSAGE_BYTES

log = logging.getLogger("kafka")
_RDKAFKA_LOG = logging.getLogger("rdkafka")

# Largest single fetch per partition; must exceed the biggest message.
CONSUMER_MAX_PARTITION_FETCH_BYTES = 8 * 1024 * 1024

ErrorCallback = Callable[[KafkaError], None]


class ErrorLogger:
    """Default ``error_cb``: report cluster-level problems, at most every 10 s.

    librdkafka reconnects on its own; these callbacks are informational.
    """

    INTERVAL_S = 10.0

    def __init__(self, name: str) -> None:
        self._name = name
        self._last: dict[int, float] = {}

    def __call__(self, err: KafkaError) -> None:
        """Log ``err`` unless the same error code was logged recently."""
        now = time.monotonic()
        if now - self._last.get(err.code(), float("-inf")) < self.INTERVAL_S:
            return
        self._last[err.code()] = now
        if err.code() == KafkaError._ALL_BROKERS_DOWN:
            log.warning("%s: all Kafka brokers unreachable; retrying", self._name)
        else:
            log.debug("%s: %s", self._name, err)


_timer_resolution_set = False


def _ensure_fine_timer() -> None:
    """On Windows, ask for 1 ms timer resolution for this process.

    librdkafka's network threads wait on the system timer, which on Windows
    ticks every 15.6 ms by default. Two ticks (31.2 ms) beat against a 30 fps
    frame interval (33.3 ms), so about half of all frames arrived in pairs
    and the switchboard showed ~18 fps from a 30 fps camera. With 1 ms
    resolution frames arrive evenly. Released automatically at exit.
    """
    global _timer_resolution_set
    if _timer_resolution_set or sys.platform != "win32":
        return
    import ctypes

    winmm = ctypes.WinDLL("winmm")
    if winmm.timeBeginPeriod(1) == 0:  # TIMERR_NOERROR
        atexit.register(winmm.timeEndPeriod, 1)
        _timer_resolution_set = True


def _base(brokers: str, client_id: str, error_cb: ErrorCallback | None) -> dict[str, Any]:
    _ensure_fine_timer()
    return {
        "bootstrap.servers": brokers,
        "client.id": f"{client_id}@{socket.gethostname()}",
        "logger": _RDKAFKA_LOG,
        "error_cb": error_cb or ErrorLogger(client_id),
    }


def media_producer(brokers: str, client_id: str, error_cb: ErrorCallback | None = None) -> Producer:
    """Producer for video frames and heartbeats (low latency, at-most-once)."""
    return Producer(
        {
            **_base(brokers, client_id, error_cb),
            "acks": "1",
            "compression.type": "none",  # JPEG is already compressed
            "linger.ms": 5,
            "message.max.bytes": FEEDS_MAX_MESSAGE_BYTES,
            # Bound the local send queue (64 MB). When the network can't keep
            # up, produce() raises BufferError and the frame is dropped rather
            # than queued, so latency never compounds.
            "queue.buffering.max.kbytes": 64 * 1024,
            # A frame still undelivered after 5 s is worthless; fail it.
            "message.timeout.ms": 5000,
        }
    )


def control_producer(
    brokers: str, client_id: str, error_cb: ErrorCallback | None = None
) -> Producer:
    """Producer for registry, as-run and health records (durable)."""
    return Producer(
        {
            **_base(brokers, client_id, error_cb),
            "acks": "all",
            "enable.idempotence": True,
            "message.timeout.ms": 30000,
        }
    )


def latest_consumer(
    brokers: str, client_id: str, group: str, error_cb: ErrorCallback | None = None
) -> Consumer:
    """Consumer that always starts at the live edge.

    Offsets are never committed, so a restarted switchboard does not replay
    minutes of stale video; with no committed offset, ``auto.offset.reset``
    puts it at the end of every partition.
    """
    return Consumer(
        {
            **_base(brokers, client_id, error_cb),
            "group.id": group,
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
            "max.partition.fetch.bytes": CONSUMER_MAX_PARTITION_FETCH_BYTES,
            "fetch.wait.max.ms": 10,
        }
    )
