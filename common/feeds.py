"""Switchboard receive side: Kafka -> per-camera state, on a background thread.

The :class:`FeedReceiver` thread owns the Kafka consumer and is the only
writer of camera state. The display thread reads that state through
:meth:`FeedReceiver.snapshot`, which copies it under a lock. Nothing here
touches OpenCV windows.

Liveness comes from three topics:
    * ``dragonfly.feeds``      frames; a camera is LIVE while frames arrive
    * ``dragonfly.heartbeat``  1 Hz pulses; tell "alive but no video" apart
                               from "transmitter gone"
    * ``dragonfly.registry``   online/offline records; a clean shutdown
                               shows immediately instead of after a timeout

Latency cannot compound: messages are consumed in batches and only the
newest complete frame per camera in each batch is decoded. If the
switchboard falls behind (a slow machine, or a burst after a broker outage)
it skips the backlog instead of replaying it.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum

import numpy as np
from confluent_kafka import (
    OFFSET_BEGINNING,
    Consumer,
    KafkaError,
    KafkaException,
    Message,
    TopicPartition,
)

from common.config import TOPIC_FEEDS, TOPIC_HEARTBEAT, TOPIC_REGISTRY
from common.frames import (
    FrameMeta,
    MalformedFrame,
    Reassembler,
    decode_jpeg,
    parse_headers,
    pick_reduction,
)

log = logging.getLogger("switchboard.feeds")

CONSUME_BATCH = 500
CONSUME_TIMEOUT_S = 0.1
HOUSEKEEPING_INTERVAL_S = 0.5
CONNECTION_PROBE_INTERVAL_S = 2.0
CONNECTION_PROBE_TIMEOUT_S = 1.5
# Heartbeats arrive every second, so a live transmitter's last one is never
# much older than that.
HEARTBEAT_TIMEOUT_S = 1.5
# A restarted transmitter begins again at seq 0. We accept the lower seq only
# if the camera has been silent this long; a restart (open file, connect to
# Kafka) always takes longer, while a second live transmitter misconfigured
# with the same camera id never goes silent and is ignored instead.
RESTART_SILENCE_S = 1.0
# Lower seqs by more than this while the camera is live indicate a duplicate id.
DUPLICATE_SEQ_GAP = 30
DUPLICATE_WARN_INTERVAL_S = 10.0


class Status(Enum):
    """What a camera tile should show."""

    LIVE = "live"
    NO_SIGNAL = "no_signal"


def accept_seq(new_seq: int, stored_seq: int, silent_s: float) -> bool:
    """Decide whether a frame should replace the stored one for its camera.

    Kafka keeps one camera's frames in order (one partition per key), so a
    newer frame always has a higher seq. A lower seq is either a duplicate
    delivery (retry), or a restarted transmitter whose counter began at 0;
    only the latter is preceded by silence.
    """
    return new_seq > stored_seq or silent_s >= RESTART_SILENCE_S


def camera_status(
    frame_age_s: float, heartbeat_age_s: float, offline: bool, stale_s: float
) -> tuple[Status, str]:
    """Classify a camera from how long ago it was last heard from.

    Returns:
        The status and a short human-readable reason for NO SIGNAL.
    """
    if offline:
        return Status.NO_SIGNAL, "stopped (offline)"
    if frame_age_s < stale_s:
        return Status.LIVE, ""
    if heartbeat_age_s < HEARTBEAT_TIMEOUT_S:
        return Status.NO_SIGNAL, "transmitter alive, no video"
    return Status.NO_SIGNAL, f"lost {frame_age_s:.0f} s ago"


def should_evict(frame_age_s: float, heartbeat_age_s: float, evict_s: float) -> bool:
    """A camera is removed once neither frames nor heartbeats arrived for ``evict_s``."""
    return min(frame_age_s, heartbeat_age_s) >= evict_s


@dataclass
class CameraState:
    """Newest frame and bookkeeping for one camera (written by the receive thread only)."""

    camera_id: str
    source_name: str
    seq: int
    frame: np.ndarray
    latency_ms: float
    last_frame: float  # time.monotonic() of the last accepted frame
    last_heartbeat: float
    full_size: tuple[int, int]  # source (width, height) before reduced decoding
    offline: bool = False
    arrivals: deque[float] = field(default_factory=lambda: deque(maxlen=120))
    duplicate_warned_at: float = float("-inf")
    # Health window (read and reset by take_health).
    window_frames: int = 0  # frames decoded and shown
    window_skipped: int = 0  # complete frames skipped because a newer one was waiting
    window_gaps: int = 0  # frames that never arrived (lost in transit or dropped by sender)
    window_latency_sum: float = 0.0
    window_latency_max: float = 0.0

    def fps(self, now: float) -> float:
        """Frames shown during the last second."""
        return float(sum(1 for t in self.arrivals if t > now - 1.0))


@dataclass(frozen=True)
class CameraView:
    """Immutable snapshot of a camera handed from the receive thread to the display."""

    camera_id: str
    source_name: str
    seq: int
    frame: np.ndarray
    latency_ms: float
    fps: float
    status: Status
    reason: str


@dataclass(frozen=True)
class HealthSample:
    """Per-camera metrics for one reporting window."""

    camera_id: str
    status: Status
    fps: float
    frames: int
    skipped: int
    gaps: int
    latency_avg_ms: float
    latency_max_ms: float


@dataclass
class Counters:
    """Receive-thread statistics."""

    frames: int = 0
    skipped: int = 0
    malformed: int = 0
    undecodable: int = 0
    stale: int = 0
    evicted: int = 0


@dataclass
class _Pending:
    """Newest complete frame for one camera within the current batch."""

    meta: FrameMeta
    payload: bytes
    received_ns: int
    superseded: int = 0


class FeedReceiver(threading.Thread):
    """Background thread: Kafka -> validated, decoded, latest-only camera state."""

    def __init__(
        self,
        make_consumer: Callable[[Callable[[KafkaError], None]], Consumer],
        stop_event: threading.Event,
        stale_s: float = 2.0,
        evict_s: float = 15.0,
        display_size: tuple[int, int] = (1280, 720),
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        """Create the receiver.

        Args:
            make_consumer: builds the Kafka consumer given the ``error_cb`` to
                install, so connection errors reach this receiver.
            stop_event: set by the display thread to end :meth:`run`.
            stale_s: frame silence after which a camera shows NO SIGNAL.
            evict_s: frame *and* heartbeat silence after which it is removed.
            display_size: largest box a frame is drawn into (the program
                window); frames are decoded no larger than needed to fill it.
            clock: monotonic time source (replaceable in tests).
        """
        super().__init__(name="feed-receiver", daemon=True)
        self.consumer = make_consumer(self.on_error)
        self._stop_event = stop_event
        self._stale_s = stale_s
        self._evict_s = evict_s
        self._display_size = display_size
        self._clock = clock
        self._lock = threading.Lock()
        self._cameras: dict[str, CameraState] = {}
        self._reassembler = Reassembler(max_age_s=1.0)
        self.counters = Counters()
        # Connection state. While disconnected, camera ages are frozen so the
        # display keeps showing the last state instead of NO SIGNAL/eviction.
        self._connected = False
        self._brokers = 0
        self._outage_started: float | None = clock()
        self._next_probe = 0.0
        self._next_housekeeping = 0.0

    # --- Kafka callbacks (run inside consume(), on this thread) ---------------

    def on_error(self, err: KafkaError) -> None:
        """``error_cb`` for the consumer: notice a total outage immediately."""
        if err.code() == KafkaError._ALL_BROKERS_DOWN:
            self._set_connected(False, 0)

    def _on_assign(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        # Feeds and heartbeats start at the live edge (auto.offset.reset), but
        # the compacted registry is read from the beginning so we learn every
        # camera's last known state.
        for tp in partitions:
            if tp.topic == TOPIC_REGISTRY:
                tp.offset = OFFSET_BEGINNING
        consumer.assign(partitions)

    # --- receive thread --------------------------------------------------------

    def run(self) -> None:
        """Consume until the stop event is set. Never raises on bad input."""
        self.consumer.subscribe(
            [TOPIC_FEEDS, TOPIC_HEARTBEAT, TOPIC_REGISTRY], on_assign=self._on_assign
        )
        while not self._stop_event.is_set():
            try:
                # Wait for the first message, then take whatever else is
                # already buffered without waiting: a batch is "everything
                # that piled up while we were busy", never an artificial delay.
                first = self.consumer.poll(CONSUME_TIMEOUT_S)
                batch = [first] if first is not None else []
                if first is not None:
                    batch += self.consumer.consume(CONSUME_BATCH, 0)
            except KafkaException as exc:
                log.warning("consumer error: %s", exc)
                batch = []
            self.process_batch(batch)
            now = self._clock()
            if now >= self._next_probe:
                self._probe_connection()
                self._next_probe = self._clock() + CONNECTION_PROBE_INTERVAL_S
            if now >= self._next_housekeeping:
                self._housekeeping(now)
                self._next_housekeeping = now + HOUSEKEEPING_INTERVAL_S

    def process_batch(self, batch: list[Message]) -> None:
        """Handle one batch: control messages immediately, then decode only the
        newest complete frame per camera."""
        newest: dict[str, _Pending] = {}
        for msg in batch:
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.debug("Kafka error: %s", msg.error())
                continue
            if not self._connected:
                self._set_connected(True, self._brokers)
            topic = msg.topic()
            if topic == TOPIC_FEEDS:
                self._collect_frame(msg, newest)
            elif topic == TOPIC_HEARTBEAT:
                self._handle_heartbeat(msg)
            elif topic == TOPIC_REGISTRY:
                self._handle_registry(msg)
        for pending in newest.values():
            self._store_frame(pending)

    def _collect_frame(self, msg: Message, newest: dict[str, _Pending]) -> None:
        """Validate and reassemble; remember the frame if it is the newest so far."""
        try:
            meta = parse_headers(msg.headers())
            payload = self._reassembler.add(meta, msg.value() or b"")
        except MalformedFrame as exc:
            self.counters.malformed += 1
            log.warning(
                "dropped malformed message (partition %d offset %d): %s",
                msg.partition(),
                msg.offset(),
                exc,
            )
            return
        if payload is None:
            return  # waiting for more chunks
        pending = _Pending(meta, payload, time.time_ns())
        previous = newest.get(meta.camera_id)
        if previous is not None:
            # Same camera => same partition => in order: this one is newer.
            pending.superseded = previous.superseded + 1
            self.counters.skipped += 1
        newest[meta.camera_id] = pending

    def _store_frame(self, pending: _Pending) -> None:
        meta = pending.meta
        now = self._clock()
        # This thread is the only writer of self._cameras, so reading it here
        # without the lock is safe; the check runs before the (costly) decode.
        state = self._cameras.get(meta.camera_id)
        if state is not None and not accept_seq(meta.seq, state.seq, now - state.last_frame):
            self.counters.stale += 1
            if (
                state.seq - meta.seq > DUPLICATE_SEQ_GAP
                and now - state.duplicate_warned_at > DUPLICATE_WARN_INTERVAL_S
            ):
                state.duplicate_warned_at = now
                log.warning(
                    "camera %s: ignoring frames from a second transmitter using the same "
                    "camera id (seq %d while live at %d)",
                    meta.camera_id,
                    meta.seq,
                    state.seq,
                )
            return

        reduce = pick_reduction(*state.full_size, *self._display_size) if state else 1
        image = decode_jpeg(pending.payload, reduce)
        if image is None:
            self.counters.undecodable += 1
            log.warning("dropped undecodable JPEG from %s seq %d", meta.camera_id, meta.seq)
            return
        full_size = (image.shape[1] * reduce, image.shape[0] * reduce)

        latency_ms = (pending.received_ns - meta.capture_ts_ns) / 1e6
        with self._lock:
            if state is None:
                log.info("new camera %s (%s)", meta.camera_id, meta.source_name)
                state = CameraState(
                    meta.camera_id,
                    meta.source_name,
                    meta.seq,
                    image,
                    latency_ms,
                    last_frame=now,
                    last_heartbeat=now,
                    full_size=full_size,
                )
                self._cameras[meta.camera_id] = state
            else:
                silent = now - state.last_frame
                if meta.seq < state.seq:
                    log.info(
                        "camera %s restarted (seq %d -> %d)", meta.camera_id, state.seq, meta.seq
                    )
                else:
                    state.window_gaps += max(0, meta.seq - state.seq - 1 - pending.superseded)
                if state.offline and silent >= RESTART_SILENCE_S:
                    state.offline = False  # came back after a clean stop
                if silent >= self._stale_s:
                    log.info("camera %s recovered after %.1f s", meta.camera_id, silent)
                state.seq = meta.seq
                state.frame = image
                state.full_size = full_size
                state.source_name = meta.source_name
                state.latency_ms = latency_ms
                state.last_frame = now
                state.last_heartbeat = max(state.last_heartbeat, now)
            state.arrivals.append(now)
            state.window_frames += 1
            state.window_skipped += pending.superseded
            state.window_latency_sum += latency_ms
            state.window_latency_max = max(state.window_latency_max, latency_ms)
        self.counters.frames += 1

    def _handle_heartbeat(self, msg: Message) -> None:
        camera_id = (msg.key() or b"").decode("utf-8", errors="replace")
        state = self._cameras.get(camera_id)
        if state is not None:
            with self._lock:
                state.last_heartbeat = self._clock()

    def _handle_registry(self, msg: Message) -> None:
        camera_id = (msg.key() or b"").decode("utf-8", errors="replace")
        try:
            status = json.loads(msg.value() or b"")["status"]
        except (ValueError, KeyError, TypeError):
            log.warning("ignoring malformed registry record for %r", camera_id)
            return
        state = self._cameras.get(camera_id)
        if state is None:
            return
        with self._lock:
            if status == "offline" and not state.offline:
                state.offline = True
                log.info("camera %s went offline (clean shutdown)", camera_id)
            elif status == "online":
                state.offline = False

    def _probe_connection(self) -> None:
        """Ask the cluster for metadata; reachable brokers prove we are connected."""
        try:
            metadata = self.consumer.list_topics(TOPIC_FEEDS, timeout=CONNECTION_PROBE_TIMEOUT_S)
        except KafkaException:
            self._set_connected(False, 0)
            return
        self._set_connected(True, len(metadata.brokers))

    def _set_connected(self, connected: bool, brokers: int) -> None:
        now = self._clock()
        with self._lock:
            self._brokers = brokers
            if connected == self._connected:
                return
            self._connected = connected
            if not connected:
                self._outage_started = now
                log.warning("broker connection lost; showing last state and retrying")
                return
            # Reconnected: shift every camera's clock by the outage length so
            # the outage itself does not count as camera silence.
            outage = now - self._outage_started if self._outage_started is not None else 0.0
            self._outage_started = None
            for state in self._cameras.values():
                state.last_frame += outage
                state.last_heartbeat += outage
        log.info("connected to Kafka (%d brokers)", brokers)

    def _housekeeping(self, now: float) -> None:
        """Bounded memory: drop stale chunk assemblies and long-silent cameras."""
        self._reassembler.evict_stale()
        if not self._connected:
            return
        with self._lock:
            gone = [
                cid
                for cid, s in self._cameras.items()
                if should_evict(now - s.last_frame, now - s.last_heartbeat, self._evict_s)
            ]
            for cid in gone:
                del self._cameras[cid]
        for cid in gone:
            self.counters.evicted += 1
            log.info("camera %s evicted after %.0f s of silence", cid, self._evict_s)

    # --- called from other threads ---------------------------------------------

    def _ages(self, state: CameraState, now: float) -> tuple[float, float]:
        # During an outage time stands still for every camera.
        ref = self._outage_started if self._outage_started is not None else now
        return max(0.0, ref - state.last_frame), max(0.0, ref - state.last_heartbeat)

    def snapshot(self) -> list[CameraView]:
        """Copy the current per-camera state, sorted by camera id.

        Frames are shared, not copied: they are never mutated after being
        stored, only replaced, so the display can read them outside the lock.
        """
        now = self._clock()
        views = []
        with self._lock:
            for s in self._cameras.values():
                frame_age, hb_age = self._ages(s, now)
                status, reason = camera_status(frame_age, hb_age, s.offline, self._stale_s)
                views.append(
                    CameraView(
                        camera_id=s.camera_id,
                        source_name=s.source_name,
                        seq=s.seq,
                        frame=s.frame,
                        latency_ms=s.latency_ms,
                        fps=s.fps(now) if status is Status.LIVE else 0.0,
                        status=status,
                        reason=reason,
                    )
                )
        return sorted(views, key=lambda v: v.camera_id)

    def connection(self) -> tuple[bool, int]:
        """``(connected, live broker count)`` as of the last probe."""
        with self._lock:
            return self._connected, self._brokers

    def take_health(self) -> list[HealthSample]:
        """Return per-camera metrics since the last call and start a new window."""
        now = self._clock()
        samples = []
        with self._lock:
            for s in self._cameras.values():
                frame_age, hb_age = self._ages(s, now)
                status, _ = camera_status(frame_age, hb_age, s.offline, self._stale_s)
                avg = s.window_latency_sum / s.window_frames if s.window_frames else 0.0
                samples.append(
                    HealthSample(
                        camera_id=s.camera_id,
                        status=status,
                        fps=s.fps(now) if status is Status.LIVE else 0.0,
                        frames=s.window_frames,
                        skipped=s.window_skipped,
                        gaps=s.window_gaps,
                        latency_avg_ms=round(avg, 1),
                        latency_max_ms=round(s.window_latency_max, 1),
                    )
                )
                s.window_frames = 0
                s.window_skipped = 0
                s.window_gaps = 0
                s.window_latency_sum = 0.0
                s.window_latency_max = 0.0
        return samples
