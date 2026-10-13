"""Tests for the switchboard receive side, driven by fake messages and a fake clock."""

from __future__ import annotations

import json
import threading

import numpy as np
from confluent_kafka import KafkaError

from common.config import TOPIC_FEEDS, TOPIC_HEARTBEAT, TOPIC_REGISTRY
from common.feeds import (
    HEARTBEAT_TIMEOUT_S,
    RESTART_SILENCE_S,
    FeedReceiver,
    Status,
    accept_seq,
    camera_status,
    should_evict,
)
from common.frames import encode_jpeg, frame_messages

JPEG = encode_jpeg(np.zeros((36, 64, 3), dtype=np.uint8))


class FakeClock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


class FakeMessage:
    """Just enough of confluent_kafka.Message for the receiver."""

    def __init__(self, topic: str, key: bytes | None, value: bytes | None, headers=None) -> None:
        self._topic, self._key, self._value, self._headers = topic, key, value, headers

    def topic(self) -> str:
        return self._topic

    def key(self) -> bytes | None:
        return self._key

    def value(self) -> bytes | None:
        return self._value

    def headers(self):
        return self._headers

    def error(self) -> None:
        return None

    def partition(self) -> int:
        return 0

    def offset(self) -> int:
        return 0


class FakeKafkaError:
    def __init__(self, code: int) -> None:
        self._code = code

    def code(self) -> int:
        return self._code


def frame_msgs(camera_id: str, seq: int, payload: bytes = JPEG) -> list[FakeMessage]:
    return [
        FakeMessage(TOPIC_FEEDS, camera_id.encode(), value, headers)
        for value, headers in frame_messages(payload, camera_id, seq, 0, 0, f"{camera_id}.mp4")
    ]


def make_receiver(clock: FakeClock) -> FeedReceiver:
    rx = FeedReceiver(lambda _cb: None, threading.Event(), 2.0, 15.0, clock=clock)  # type: ignore[arg-type,return-value]
    rx._set_connected(True, 3)
    return rx


def send(rx: FeedReceiver, msgs: list[FakeMessage]) -> None:
    """Deliver messages one per batch, like a receiver that keeps up."""
    for m in msgs:
        rx.process_batch([m])  # type: ignore[list-item]


def statuses(rx: FeedReceiver) -> dict[str, tuple[Status, str]]:
    return {v.camera_id: (v.status, v.reason) for v in rx.snapshot()}


# --- pure rules ----------------------------------------------------------------


def test_accept_seq_rules() -> None:
    assert accept_seq(11, 10, silent_s=0.03)
    assert not accept_seq(10, 10, silent_s=0.03)  # duplicate delivery
    assert not accept_seq(62, 15544, silent_s=0.03)  # second live transmitter, same id
    assert accept_seq(0, 15544, silent_s=RESTART_SILENCE_S)  # restart after silence


def test_camera_status_rules() -> None:
    assert camera_status(0.5, 0.5, offline=False, stale_s=2.0) == (Status.LIVE, "")
    assert camera_status(3.0, 0.5, offline=False, stale_s=2.0)[1] == "transmitter alive, no video"
    assert camera_status(4.0, HEARTBEAT_TIMEOUT_S, offline=False, stale_s=2.0) == (
        Status.NO_SIGNAL,
        "lost 4 s ago",
    )
    assert camera_status(0.1, 0.1, offline=True, stale_s=2.0)[0] is Status.NO_SIGNAL


def test_should_evict_needs_both_frames_and_heartbeats_silent() -> None:
    assert not should_evict(20.0, 0.5, evict_s=15.0)  # heartbeats still arriving
    assert should_evict(20.0, 16.0, evict_s=15.0)


# --- receiver behaviour --------------------------------------------------------


def test_camera_goes_no_signal_then_recovers() -> None:
    clock = FakeClock()
    rx = make_receiver(clock)
    send(rx, frame_msgs("cam1", 0))
    assert statuses(rx)["cam1"][0] is Status.LIVE

    clock.now += 5.0  # transmitter crashed: no frames, no heartbeats
    assert statuses(rx)["cam1"] == (Status.NO_SIGNAL, "lost 5 s ago")

    send(rx, frame_msgs("cam1", 0))  # restarted: seq back at 0 after silence
    assert statuses(rx)["cam1"][0] is Status.LIVE


def test_heartbeat_distinguishes_alive_without_video() -> None:
    clock = FakeClock()
    rx = make_receiver(clock)
    send(rx, frame_msgs("cam1", 0))
    clock.now += 5.0
    send(rx, [FakeMessage(TOPIC_HEARTBEAT, b"cam1", b"{}")])
    assert statuses(rx)["cam1"] == (Status.NO_SIGNAL, "transmitter alive, no video")


def test_offline_registry_record_shows_immediately() -> None:
    clock = FakeClock()
    rx = make_receiver(clock)
    send(rx, frame_msgs("cam1", 0))
    offline = json.dumps({"camera_id": "cam1", "status": "offline"}).encode()
    send(rx, [FakeMessage(TOPIC_REGISTRY, b"cam1", offline)])
    assert statuses(rx)["cam1"] == (Status.NO_SIGNAL, "stopped (offline)")


def test_silent_camera_is_evicted_but_heartbeating_one_is_kept() -> None:
    clock = FakeClock()
    rx = make_receiver(clock)
    send(rx, frame_msgs("dead", 0) + frame_msgs("alive", 0))
    for _ in range(16):
        clock.now += 1.0
        send(rx, [FakeMessage(TOPIC_HEARTBEAT, b"alive", b"{}")])
    rx._housekeeping(clock.now)
    assert set(statuses(rx)) == {"alive"}
    assert rx.counters.evicted == 1


def test_disconnect_freezes_ages_and_prevents_eviction() -> None:
    clock = FakeClock()
    rx = make_receiver(clock)
    send(rx, frame_msgs("cam1", 0))
    rx.on_error(FakeKafkaError(KafkaError._ALL_BROKERS_DOWN))  # type: ignore[arg-type]
    assert rx.connection() == (False, 0)

    clock.now += 60.0  # a long outage
    rx._housekeeping(clock.now)
    assert statuses(rx)["cam1"][0] is Status.LIVE  # last state kept, not evicted

    rx._set_connected(True, 3)
    assert statuses(rx)["cam1"][0] is Status.LIVE  # outage does not count as silence
    clock.now += 3.0
    assert statuses(rx)["cam1"][0] is Status.NO_SIGNAL  # normal timing resumes


def test_malformed_and_corrupt_messages_are_dropped_not_raised() -> None:
    clock = FakeClock()
    rx = make_receiver(clock)
    send(
        rx,
        [
            FakeMessage(TOPIC_FEEDS, b"x", b"jpeg", None),  # no headers
            *frame_msgs("cam1", 0, payload=b"not a jpeg"),
            FakeMessage(TOPIC_REGISTRY, b"cam1", b"{not json"),
            FakeMessage(TOPIC_HEARTBEAT, None, None),
        ],
    )
    assert rx.counters.malformed == 1
    assert rx.counters.undecodable == 1
    assert statuses(rx) == {}


def test_health_window_counts_gaps_and_resets() -> None:
    clock = FakeClock()
    rx = make_receiver(clock)
    for seq in (0, 1, 2, 5, 6):  # seqs 3 and 4 lost
        send(rx, frame_msgs("cam1", seq))
    (sample,) = rx.take_health()
    assert (sample.frames, sample.gaps) == (5, 2)
    (sample,) = rx.take_health()
    assert (sample.frames, sample.gaps) == (0, 0)


def test_backlog_batch_decodes_only_newest_frame_per_camera() -> None:
    """A burst (e.g. after an outage) is skipped, not replayed, so latency cannot compound."""
    clock = FakeClock()
    rx = make_receiver(clock)
    backlog = [m for seq in range(100) for m in frame_msgs("cam1", seq)]
    backlog += [m for seq in range(50) for m in frame_msgs("cam2", seq)]
    rx.process_batch(backlog)  # type: ignore[arg-type]
    views = {v.camera_id: v.seq for v in rx.snapshot()}
    assert views == {"cam1": 99, "cam2": 49}
    assert rx.counters.frames == 2  # two decodes, not 150
    assert rx.counters.skipped == 148
    samples = {s.camera_id: (s.skipped, s.gaps) for s in rx.take_health()}
    assert samples == {"cam1": (99, 0), "cam2": (49, 0)}  # skipped is not "lost"
