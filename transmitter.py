"""Dragonfly transmitter: plays one video file as a live camera into Kafka.

Each frame is JPEG-encoded and produced to ``dragonfly.feeds`` keyed by
camera id, so all frames of one camera land on one partition, in order.
Metadata travels in Kafka headers (see ``common/frames.py``).

Example:
    python transmitter.py --camera-id cam1 --file sample_videos/cam1.mp4 --max-width 1280
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
import socket
import sys
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from types import FrameType
from typing import Any

import cv2
import numpy as np
from confluent_kafka import KafkaError, KafkaException, Message, Producer

from common.clients import control_producer, media_producer
from common.config import (
    DEFAULT_JPEG_QUALITY,
    TOPIC_FEEDS,
    TOPIC_HEARTBEAT,
    TOPIC_REGISTRY,
    default_brokers,
    env_int,
    env_str,
)
from common.frames import encode_jpeg, frame_messages, resize_max_width
from common.log import setup_logging
from common.pacing import Pacer

log = logging.getLogger("transmitter")

BROKER_CHECK_TIMEOUT_S = 5.0
FLUSH_TIMEOUT_S = 5.0
HEARTBEAT_INTERVAL_S = 1.0
STATS_INTERVAL_S = 1.0
MAX_SKIP_PER_FRAME = 1


class StartupError(Exception):
    """A problem that should stop the transmitter with a one-line message."""


@dataclass
class Stats:
    """Counters for the once-per-second stats line."""

    delivered: int = 0  # Kafka messages acknowledged by the broker
    failed: int = 0  # messages that failed delivery or were dropped locally
    skipped: int = 0  # frames skipped by the pacer to stay real-time
    frames_window: int = 0  # frames produced since the last stats line
    bytes_window: int = 0  # JPEG bytes produced since the last stats line


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments, falling back to DRAGONFLY_* environment variables."""
    p = argparse.ArgumentParser(description="Play a video file as a live Dragonfly camera.")
    p.add_argument("--camera-id", required=True, help="unique camera name, e.g. cam1")
    p.add_argument("--file", required=True, type=Path, help="video file to play")
    p.add_argument(
        "--brokers", default=default_brokers(), help="bootstrap servers (env DRAGONFLY_BROKERS)"
    )
    p.add_argument("--fps", type=float, default=None, help="send rate (default: file's native fps)")
    p.add_argument(
        "--max-width",
        type=int,
        default=env_int("DRAGONFLY_MAX_WIDTH", 0) or None,
        help="downscale frames wider than this (env DRAGONFLY_MAX_WIDTH)",
    )
    p.add_argument(
        "--jpeg-quality",
        type=int,
        default=env_int("DRAGONFLY_JPEG_QUALITY", DEFAULT_JPEG_QUALITY),
        help=f"JPEG quality 1-100 (default {DEFAULT_JPEG_QUALITY})",
    )
    p.add_argument(
        "--loop",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="rewind at end of file (default: --loop)",
    )
    p.add_argument("--start-sec", type=float, default=0.0, help="start this far into the file")
    p.add_argument(
        "--verbose",
        action="store_true",
        default=env_str("DRAGONFLY_VERBOSE", "") == "1",
        help="debug logging, including librdkafka",
    )
    args = p.parse_args(argv)
    if not 1 <= args.jpeg_quality <= 100:
        p.error("--jpeg-quality must be between 1 and 100")
    if args.fps is not None and args.fps <= 0:
        p.error("--fps must be positive")
    if args.max_width is not None and args.max_width < 16:
        p.error("--max-width must be at least 16")
    if args.start_sec < 0:
        p.error("--start-sec must not be negative")
    return args


class Transmitter:
    """Reads frames from a file, paces them, and publishes them to Kafka."""

    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.camera_id: str = args.camera_id
        self.key = self.camera_id.encode()
        self.source_name = Path(args.file).name
        self.stop_event = threading.Event()
        self.stats = Stats()
        self.seq = 0  # monotonic across loops; never reset
        self.loops = 0
        self.cap: cv2.VideoCapture | None = None
        self.media: Producer | None = None
        self.control: Producer | None = None
        self.fps = 0.0
        self.resolution = ""

    # --- startup -------------------------------------------------------------

    def open_video(self) -> np.ndarray:
        """Open the file, seek to --start-sec, and return the first frame.

        Raises:
            StartupError: if the file is missing, unreadable, or yields no frame.
        """
        path: Path = self.args.file
        if not path.is_file():
            raise StartupError(f"video file not found: {path}")
        cap = cv2.VideoCapture(str(path))
        if not cap.isOpened():
            raise StartupError(f"cannot open video file (unsupported format?): {path}")
        if self.args.start_sec:
            cap.set(cv2.CAP_PROP_POS_MSEC, self.args.start_sec * 1000)
        ok, frame = cap.read()
        if not ok or frame is None:
            cap.release()
            raise StartupError(
                f"video file yields no frames (corrupt, or --start-sec too large): {path}"
            )
        self.cap = cap

        native_fps = cap.get(cv2.CAP_PROP_FPS)
        self.fps = self.args.fps or (native_fps if native_fps > 0 else 30.0)
        src_h, src_w = frame.shape[:2]
        frame = resize_max_width(frame, self.args.max_width)
        out_h, out_w = frame.shape[:2]
        self.resolution = f"{out_w}x{out_h}"
        scaled = f" -> {self.resolution}" if (out_w, out_h) != (src_w, src_h) else ""
        frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        log.info(
            "camera %s | file %s | %dx%d%s | %.2f fps | %d frames",
            self.camera_id,
            path,
            src_w,
            src_h,
            scaled,
            self.fps,
            frame_count,
        )
        return frame

    def connect(self) -> None:
        """Create producers and confirm the cluster answers and has the feeds topic.

        Raises:
            StartupError: if no broker answers or the topics were not created.
        """
        client_id = f"transmitter-{self.camera_id}"
        self.media = media_producer(self.args.brokers, client_id)
        self.control = control_producer(self.args.brokers, client_id)
        try:
            metadata = self.media.list_topics(timeout=BROKER_CHECK_TIMEOUT_S)
        except KafkaException as exc:
            raise StartupError(
                f"cannot reach Kafka at {self.args.brokers} (is the cluster running?)"
            ) from exc
        if TOPIC_FEEDS not in metadata.topics:
            raise StartupError(
                f"topic {TOPIC_FEEDS} does not exist; run: python scripts/create_topics.py"
            )
        log.info("connected to Kafka (%d brokers)", len(metadata.brokers))

    # --- Kafka callbacks / records ------------------------------------------

    def _on_delivery(self, err: KafkaError | None, _msg: Message) -> None:
        if err is None:
            self.stats.delivered += 1
        else:
            self.stats.failed += 1
            log.debug("delivery failed: %s", err)

    def _publish_registry(self, record: dict[str, Any]) -> None:
        assert self.control is not None
        payload = json.dumps({"camera_id": self.camera_id, **record}).encode()
        self.control.produce(TOPIC_REGISTRY, key=self.key, value=payload)
        self.control.poll(0)

    def _publish_heartbeat(self) -> None:
        assert self.media is not None
        payload = json.dumps(
            {"camera_id": self.camera_id, "seq": self.seq, "ts_ns": time.time_ns()}
        ).encode()
        try:
            self.media.produce(TOPIC_HEARTBEAT, key=self.key, value=payload)
        except BufferError:
            log.debug("send queue full; heartbeat dropped")

    # --- main loop -------------------------------------------------------------

    def _read_frame(self) -> np.ndarray | None:
        """Read the next frame, rewinding at end of file when looping."""
        assert self.cap is not None
        ok, frame = self.cap.read()
        if ok:
            return frame
        if not self.args.loop:
            return None
        self.cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
        self.loops += 1
        log.info("end of file; looping (loop %d)", self.loops)
        ok, frame = self.cap.read()
        return frame if ok else None

    def _skip_frames(self, count: int) -> None:
        """Advance the file ``count`` frames without converting them to images.

        ``grab()`` skips the costly colour conversion but the video decoder
        still has to decode each frame, so callers keep ``count`` small.
        """
        assert self.cap is not None
        for _ in range(count):
            if not self.cap.grab():
                if not self.args.loop:
                    return
                self.cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
                self.loops += 1
        self.stats.skipped += count

    def _send_frame(self, frame: np.ndarray) -> None:
        assert self.cap is not None and self.media is not None
        frame = resize_max_width(frame, self.args.max_width)
        jpeg = encode_jpeg(frame, self.args.jpeg_quality)
        messages = frame_messages(
            jpeg,
            camera_id=self.camera_id,
            seq=self.seq,
            capture_ts_ns=time.time_ns(),
            media_pos_ms=int(self.cap.get(cv2.CAP_PROP_POS_MSEC)),
            source_name=self.source_name,
        )
        for value, headers in messages:
            try:
                self.media.produce(
                    TOPIC_FEEDS,
                    key=self.key,
                    value=value,
                    headers=headers,
                    on_delivery=self._on_delivery,
                )
            except BufferError:
                # Local queue full: the network is slower than the camera.
                # Drop the rest of this frame instead of blocking.
                self.stats.failed += 1
                break
        self.seq += 1
        self.stats.frames_window += 1
        self.stats.bytes_window += len(jpeg)

    def _print_stats(self, elapsed: float) -> None:
        s = self.stats
        fps = s.frames_window / elapsed if elapsed > 0 else 0.0
        kb = s.bytes_window / s.frames_window / 1024 if s.frames_window else 0.0
        print(
            f"[{self.camera_id}] fps {fps:5.1f} | {kb:6.1f} KB/frame | seq {self.seq} | "
            f"delivered {s.delivered} | failed {s.failed} | skipped {s.skipped}",
            flush=True,
        )
        s.frames_window = 0
        s.bytes_window = 0

    def run(self, first_frame: np.ndarray) -> None:
        """Publish frames until stopped, then shut down cleanly."""
        assert self.media is not None
        self._publish_registry(
            {
                "status": "online",
                "file": str(self.args.file),
                "source_name": self.source_name,
                "resolution": self.resolution,
                "fps": round(self.fps, 3),
                "host": socket.gethostname(),
                "started_at": datetime.now(UTC).isoformat(timespec="seconds"),
            }
        )

        pacer = Pacer(self.fps)
        now = time.perf_counter()
        next_heartbeat = now
        last_stats = now
        pending: np.ndarray | None = first_frame  # already read during startup
        try:
            while not self.stop_event.is_set():
                skipped = pacer.wait()
                if skipped:
                    # Behind schedule: drop a missed frame so the picture stays
                    # close to real time. Skipping is not free (the H.264
                    # decoder must still decode every frame it passes), so
                    # catch up by at most MAX_SKIP_PER_FRAME and forgive the
                    # rest; otherwise an overloaded sender spirals to ~0 fps.
                    pending = None
                    self._skip_frames(min(skipped, MAX_SKIP_PER_FRAME))
                    if skipped > MAX_SKIP_PER_FRAME:
                        pacer.resync()
                frame = pending if pending is not None else self._read_frame()
                pending = None
                if frame is None:
                    log.info("end of file reached (--no-loop)")
                    break
                self._send_frame(frame)
                self.media.poll(0)

                now = time.perf_counter()
                if now >= next_heartbeat:
                    self._publish_heartbeat()
                    next_heartbeat = now + HEARTBEAT_INTERVAL_S
                if now - last_stats >= STATS_INTERVAL_S:
                    self._print_stats(now - last_stats)
                    last_stats = now
        finally:
            self.shutdown()

    def shutdown(self) -> None:
        """Send the offline record, flush both producers, release the file."""
        log.info("shutting down")
        if self.control is not None:
            self._publish_registry(
                {
                    "status": "offline",
                    "host": socket.gethostname(),
                    "stopped_at": datetime.now(UTC).isoformat(timespec="seconds"),
                }
            )
        for name, producer in (("media", self.media), ("control", self.control)):
            if producer is None:
                continue
            remaining = producer.flush(FLUSH_TIMEOUT_S)
            if remaining:
                log.warning("%s producer: %d messages not delivered before exit", name, remaining)
        if self.cap is not None:
            self.cap.release()
        s = self.stats
        log.info(
            "sent %d frames | delivered %d msgs | failed %d | skipped %d",
            self.seq,
            s.delivered,
            s.failed,
            s.skipped,
        )


def install_signal_handlers(stop_event: threading.Event) -> None:
    """Turn SIGINT/SIGTERM (and SIGBREAK on Windows) into a graceful stop."""

    def handler(signum: int, _frame: FrameType | None) -> None:
        log.info("received %s; stopping", signal.Signals(signum).name)
        stop_event.set()

    for name in ("SIGINT", "SIGTERM", "SIGBREAK"):
        sig = getattr(signal, name, None)
        if sig is not None:
            signal.signal(sig, handler)


def main(argv: list[str] | None = None) -> int:
    """Entry point; returns a process exit code."""
    args = parse_args(argv)
    setup_logging(args.verbose)
    tx = Transmitter(args)
    install_signal_handlers(tx.stop_event)
    try:
        first_frame = tx.open_video()
        tx.connect()
    except StartupError as exc:
        log.error("%s", exc)
        if tx.cap is not None:
            tx.cap.release()
        return 1
    tx.run(first_frame)
    return 0


if __name__ == "__main__":
    sys.exit(main())
