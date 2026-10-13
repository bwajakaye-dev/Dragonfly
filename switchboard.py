"""Dragonfly switchboard: receives camera feeds from Kafka and displays them.

Threading model:
    * A background **receive thread** (``common.feeds.FeedReceiver``) owns the
      Kafka consumer. It validates, reassembles and decodes frames and keeps
      the newest frame per camera, plus liveness from heartbeats and the
      registry, in a dict guarded by a lock.
    * The **main thread** (this module) owns every OpenCV window call
      (imshow, waitKey, destroyAllWindows). It takes a quick snapshot under
      the lock and renders outside it, so it never blocks on Kafka.

Only the latest frame per camera is kept: a new frame overwrites the old one
and nothing is queued, so a slow display can never build up latency.

Windows:
    * Multiview: a grid of every camera with id, source, latency and fps;
      the on-air camera has a red border; silent cameras show NO SIGNAL.
    * Program: the on-air camera at full size.
Keys 1-9 cut to that tile (logged to ``dragonfly.asrun``); q quits.

Example:
    python switchboard.py --max-width 1280
"""

from __future__ import annotations

import argparse
import json
import logging
import socket
import sys
import threading
import time
from datetime import UTC, datetime

import cv2
import numpy as np
from confluent_kafka import KafkaError, Message, Producer

from common import render
from common.clients import control_producer, latest_consumer
from common.config import (
    TOPIC_ASRUN,
    TOPIC_HEALTH,
    default_brokers,
    env_int,
    env_str,
)
from common.feeds import CameraView, FeedReceiver, Status
from common.log import setup_logging

log = logging.getLogger("switchboard")

DISPLAY_INTERVAL_MS = 15  # ~60 Hz refresh; frames arrive at <= 60 fps
HEALTH_INTERVAL_S = 5.0
FLUSH_TIMEOUT_S = 5.0
PROGRAM_WINDOW = "Dragonfly - Program"
MULTIVIEW_WINDOW = "Dragonfly - Multiview"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments, falling back to DRAGONFLY_* environment variables."""
    p = argparse.ArgumentParser(description="Display Dragonfly camera feeds from Kafka.")
    p.add_argument(
        "--brokers", default=default_brokers(), help="bootstrap servers (env DRAGONFLY_BROKERS)"
    )
    p.add_argument(
        "--group",
        default=env_str("DRAGONFLY_GROUP", f"dragonfly-switchboard-{socket.gethostname()}"),
        help="consumer group id (env DRAGONFLY_GROUP)",
    )
    p.add_argument(
        "--stale-ms",
        type=int,
        default=env_int("DRAGONFLY_STALE_MS", 2000),
        help="show NO SIGNAL after this long without frames (default 2000)",
    )
    p.add_argument(
        "--evict-s",
        type=float,
        default=env_int("DRAGONFLY_EVICT_S", 15),
        help="remove a camera after this long without frames or heartbeats (default 15)",
    )
    p.add_argument(
        "--max-width",
        type=int,
        default=env_int("DRAGONFLY_SWITCHBOARD_WIDTH", 1280),
        help="window width in pixels; height is 16:9 (default 1280)",
    )
    p.add_argument(
        "--grid",
        default=env_str("DRAGONFLY_GRID", "auto"),
        help="multiview layout: 'auto' or COLSxROWS such as 3x2 (default auto)",
    )
    p.add_argument(
        "--verbose",
        action="store_true",
        default=env_str("DRAGONFLY_VERBOSE", "") == "1",
        help="debug logging, including librdkafka",
    )
    args = p.parse_args(argv)
    if args.max_width < 320:
        p.error("--max-width must be at least 320")
    if args.stale_ms < 100:
        p.error("--stale-ms must be at least 100")
    if args.evict_s * 1000 <= args.stale_ms:
        p.error("--evict-s must be longer than --stale-ms")
    try:
        args.grid = render.parse_grid(args.grid)
    except ValueError as exc:
        p.error(str(exc))
    return args


class Switchboard:
    """Main-thread side: renders both windows, handles keys, publishes cuts and health."""

    def __init__(
        self, args: argparse.Namespace, receiver: FeedReceiver, producer: Producer
    ) -> None:
        self.receiver = receiver
        self.producer = producer
        self.group: str = args.group
        self.fixed_grid: tuple[int, int] | None = args.grid
        self.width: int = args.max_width
        self.height = self.width * 9 // 16
        self.program: str | None = None  # camera id currently on air
        self.cuts = 0
        # Rendered images reused until what they show changes.
        self._tile_cache: dict[str, tuple[tuple[object, ...], np.ndarray]] = {}
        self._program_cache: tuple[tuple[object, ...], np.ndarray] | None = None
        self._next_health = time.monotonic() + HEALTH_INTERVAL_S

    # --- cuts ------------------------------------------------------------------

    def cut(self, slot: int, views: list[CameraView]) -> None:
        """Put the camera in multiview ``slot`` (1-based) on air and log it to as-run."""
        if not 1 <= slot <= len(views):
            log.info("no camera on key %d", slot)
            return
        target = views[slot - 1].camera_id
        if target == self.program:
            return
        previous, self.program = self.program, target
        self.cuts += 1
        event = {
            "ts": datetime.now(UTC).isoformat(timespec="milliseconds"),
            "ts_ns": time.time_ns(),
            "from_cam": previous,
            "to_cam": target,
            "switchboard": socket.gethostname(),
            "group": self.group,
        }
        try:
            self.producer.produce(
                TOPIC_ASRUN, value=json.dumps(event).encode(), on_delivery=self._on_delivery
            )
        except BufferError:
            log.warning("as-run queue full; cut %s -> %s not logged", previous, target)
        log.info("CUT %s -> %s", previous or "(none)", target)

    @staticmethod
    def _on_delivery(err: KafkaError | None, msg: Message) -> None:
        if err is not None:
            log.warning("%s record not stored: %s", msg.topic(), err)

    # --- health ----------------------------------------------------------------

    def publish_health(self) -> None:
        """Send one record per camera to ``dragonfly.health`` (non-blocking)."""
        ts = datetime.now(UTC).isoformat(timespec="seconds")
        for sample in self.receiver.take_health():
            record = {
                "ts": ts,
                "camera_id": sample.camera_id,
                "status": sample.status.value,
                "fps": sample.fps,
                "frames": sample.frames,
                "skipped": sample.skipped,
                "gaps": sample.gaps,
                "latency_avg_ms": sample.latency_avg_ms,
                "latency_max_ms": sample.latency_max_ms,
                "on_air": sample.camera_id == self.program,
                "switchboard": socket.gethostname(),
            }
            try:
                self.producer.produce(
                    TOPIC_HEALTH,
                    key=sample.camera_id.encode(),
                    value=json.dumps(record).encode(),
                    on_delivery=self._on_delivery,
                )
            except BufferError:
                log.debug("health queue full; sample for %s dropped", sample.camera_id)

    # --- rendering ---------------------------------------------------------------

    def _program_view(self, views: list[CameraView]) -> CameraView | None:
        if self.program is None:
            live = [v for v in views if v.status is Status.LIVE]
            if live:
                # Nothing on air yet: take the first live camera (not logged as a cut).
                self.program = live[0].camera_id
                log.info("program defaulted to %s", self.program)
        return next((v for v in views if v.camera_id == self.program), None)

    def render_program(self, view: CameraView | None, connected: bool) -> np.ndarray:
        """Full-size output of the on-air camera, or a card explaining why there is none."""
        cache_key = (
            (view.camera_id, view.seq, view.status, view.reason) if view else self.program,
            connected,
        )
        if self._program_cache is not None and self._program_cache[0] == cache_key:
            return self._program_cache[1]

        if view is None and self.program is None:
            img = render.card(self.width, self.height, "WAITING FOR FEEDS", "start a transmitter")
        elif view is None:
            img = render.card(
                self.width, self.height, "NO SIGNAL", f"{self.program} is gone - press 1-9 to cut"
            )
        elif view.status is not Status.LIVE:
            img = render.card(
                self.width, self.height, "NO SIGNAL", f"{view.camera_id}: {view.reason}"
            )
        else:
            img = render.fit(view.frame, self.width, self.height)
            render.draw_label(
                img,
                [
                    f"PGM  {view.camera_id}  |  {view.source_name}",
                    f"latency {view.latency_ms:6.1f} ms   {view.fps:4.1f} fps   seq {view.seq}",
                ],
            )
        if not connected:
            render.draw_banner(img, "BROKER DISCONNECTED - showing last state, retrying")
        self._program_cache = (cache_key, img)
        return img

    def _render_tile(self, view: CameraView, slot: int, tile_w: int, tile_h: int) -> np.ndarray:
        key = str(slot) if slot <= 9 else "-"
        if view.status is Status.LIVE:
            tile = render.fit(view.frame, tile_w, tile_h)
            stats = f"{view.latency_ms:.0f} ms  {view.fps:.0f} fps"
        else:
            tile = render.card(tile_w, tile_h, "NO SIGNAL", view.reason)
            stats = view.reason
        render.draw_label(
            tile, [f"[{key}] {view.camera_id} | {view.source_name}", stats], scale=0.45
        )
        if view.camera_id == self.program:
            render.draw_border(tile, render.RED, thickness=6)
        return tile

    def render_multiview(
        self, views: list[CameraView], connected: bool, brokers: int
    ) -> np.ndarray:
        """Grid of every camera, numbered for the cut keys, program in red."""
        cols, rows = render.grid_shape(len(views), self.fixed_grid)
        tile_w = self.width // cols
        tile_h = tile_w * 9 // 16
        tiles = []
        for slot, view in enumerate(views, start=1):
            # Frames arrive at ~30 fps but we redraw at ~60 Hz: reuse a tile
            # until anything it shows changes.
            cache_key = (
                view.seq,
                slot,
                view.camera_id == self.program,
                tile_w,
                view.status,
                view.reason,
            )
            cached = self._tile_cache.get(view.camera_id)
            if cached is not None and cached[0] == cache_key:
                tiles.append(cached[1])
                continue
            tile = self._render_tile(view, slot, tile_w, tile_h)
            self._tile_cache[view.camera_id] = (cache_key, tile)
            tiles.append(tile)
        # Bounded memory: forget tiles of cameras that are no longer listed.
        for gone in self._tile_cache.keys() - {v.camera_id for v in views}:
            del self._tile_cache[gone]
        if not tiles:
            tiles.append(render.card(tile_w, tile_h, "NO CAMERAS"))

        grid = render.compose_grid(tiles, cols, rows)
        live = sum(1 for v in views if v.status is Status.LIVE)
        if connected:
            footer = render.canvas(grid.shape[1], 32, (30, 30, 30))
            status = (
                f"PGM: {self.program or '-'}   cams: {live}/{len(views)} live   "
                f"cuts: {self.cuts}   Kafka: {brokers} broker(s)"
            )
            hidden = len(views) - cols * rows
            if hidden > 0:
                status += f"   ({hidden} not shown)"
        else:
            # Red footer rather than a banner, so no tile label is covered.
            footer = render.canvas(grid.shape[1], 32, render.RED)
            status = "BROKER DISCONNECTED - showing last state, retrying"
        render.draw_footer(footer, status, "1-9 cut   q quit")
        return np.vstack([grid, footer])

    # --- main loop ---------------------------------------------------------------

    def run(self) -> None:
        """Render and handle keys until the user quits."""
        cv2.namedWindow(MULTIVIEW_WINDOW, cv2.WINDOW_AUTOSIZE)
        cv2.namedWindow(PROGRAM_WINDOW, cv2.WINDOW_AUTOSIZE)
        while True:
            views = self.receiver.snapshot()
            connected, brokers = self.receiver.connection()
            program_view = self._program_view(views)
            cv2.imshow(MULTIVIEW_WINDOW, self.render_multiview(views, connected, brokers))
            cv2.imshow(PROGRAM_WINDOW, self.render_program(program_view, connected))

            now = time.monotonic()
            if now >= self._next_health:
                self.publish_health()
                self._next_health = now + HEALTH_INTERVAL_S
            self.producer.poll(0)  # serve delivery callbacks; never blocks

            key = cv2.waitKey(DISPLAY_INTERVAL_MS) & 0xFF
            if key in (ord("q"), 27):  # q or Esc
                return
            if ord("1") <= key <= ord("9"):
                self.cut(key - ord("0"), views)
            if any(
                cv2.getWindowProperty(w, cv2.WND_PROP_VISIBLE) < 1
                for w in (MULTIVIEW_WINDOW, PROGRAM_WINDOW)
            ):
                return  # a window was closed with the mouse


def main(argv: list[str] | None = None) -> int:
    """Entry point; returns a process exit code."""
    args = parse_args(argv)
    setup_logging(args.verbose)

    stop_event = threading.Event()
    receiver = FeedReceiver(
        lambda error_cb: latest_consumer(args.brokers, "switchboard", args.group, error_cb),
        stop_event,
        stale_s=args.stale_ms / 1000,
        evict_s=args.evict_s,
        display_size=(args.max_width, args.max_width * 9 // 16),
    )
    producer = control_producer(args.brokers, "switchboard")
    receiver.start()
    log.info("brokers %s | group %s | keys: 1-9 cut, q quit", args.brokers, args.group)

    try:
        Switchboard(args, receiver, producer).run()
    except KeyboardInterrupt:
        log.info("interrupted")
    finally:
        log.info("shutting down")
        stop_event.set()
        receiver.join(timeout=5)
        receiver.consumer.close()
        remaining = producer.flush(FLUSH_TIMEOUT_S)
        if remaining:
            log.warning("%d as-run/health records not delivered before exit", remaining)
        cv2.destroyAllWindows()
        c = receiver.counters
        log.info(
            "frames %d | malformed %d | undecodable %d | stale %d | evicted %d",
            c.frames,
            c.malformed,
            c.undecodable,
            c.stale,
            c.evicted,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
