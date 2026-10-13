"""Tests for the deadline-based Pacer."""

from __future__ import annotations

import time

import pytest

from common.pacing import Pacer


class FakeTime:
    """Deterministic clock whose sleep() advances time instantly."""

    def __init__(self) -> None:
        self.now = 100.0

    def clock(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


def test_rejects_non_positive_fps() -> None:
    with pytest.raises(ValueError):
        Pacer(0)


def test_no_drift_with_constant_work() -> None:
    """Per-frame work must not accumulate into the frame period."""
    t = FakeTime()
    pacer = Pacer(30, clock=t.clock, sleep=t.sleep)
    start = t.now
    for _ in range(300):
        t.now += 0.010  # 10 ms of "encode + produce" per frame
        assert pacer.wait() == 0
    # The first frame is released immediately, so 300 frames span 299 periods.
    assert t.now - start == pytest.approx(299 / 30, abs=1e-6)


def test_skips_frames_when_more_than_one_behind() -> None:
    t = FakeTime()
    pacer = Pacer(10, clock=t.clock, sleep=t.sleep)  # 100 ms period
    pacer.wait()  # frame 0 at t=0
    t.now += 0.35  # stall to t=350 ms: frames due at 100/200/300 ms are all overdue
    assert pacer.wait() == 2  # send one of them, skip the other two
    # Back on schedule afterwards: the next frame needs a short sleep, no skip.
    assert pacer.wait() == 0


def test_resync_forgets_backlog() -> None:
    t = FakeTime()
    pacer = Pacer(10, clock=t.clock, sleep=t.sleep)
    pacer.wait()
    t.now += 5.0  # badly behind
    pacer.resync()
    assert pacer.wait() == 0  # no skip burst after resync
    assert pacer.wait() == 0


def test_one_frame_late_is_not_skipped() -> None:
    t = FakeTime()
    pacer = Pacer(10, clock=t.clock, sleep=t.sleep)
    pacer.wait()
    t.now += 0.15  # 50 ms late: within one period
    assert pacer.wait() == 0


def test_real_clock_holds_target_fps() -> None:
    fps = 50.0
    duration_s = 2.0
    pacer = Pacer(fps)
    frames = 0
    start = time.perf_counter()
    while time.perf_counter() - start < duration_s:
        pacer.wait()
        frames += 1
    measured = frames / (time.perf_counter() - start)
    assert measured == pytest.approx(fps, rel=0.05)
