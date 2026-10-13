"""Deadline-based frame pacing.

``time.sleep(1 / fps)`` drifts: every iteration adds the encode/produce time
on top of the sleep. Instead we keep an absolute deadline on the
``perf_counter`` clock and advance it by exactly one period per frame, so the
average rate stays locked to the target. When the loop falls more than one
frame behind, the pacer reports how many frames to skip rather than trying
to catch up by bursting.
"""

from __future__ import annotations

import time
from collections.abc import Callable


class Pacer:
    """Hold a loop to ``fps`` iterations per second using absolute deadlines."""

    def __init__(
        self,
        fps: float,
        clock: Callable[[], float] = time.perf_counter,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if fps <= 0:
            raise ValueError(f"fps must be positive, got {fps}")
        self.period = 1.0 / fps
        self._clock = clock
        self._sleep = sleep
        self._deadline = clock()

    def wait(self) -> int:
        """Block until the next frame is due.

        Returns:
            Number of frames the caller should skip because the loop is
            running more than one frame behind schedule (0 when on time).
        """
        lag = self._clock() - self._deadline
        skipped = 0
        if lag > self.period:
            skipped = int(lag // self.period)
            self._deadline += skipped * self.period
        elif lag < 0:
            self._sleep(-lag)
        self._deadline += self.period
        return skipped

    def resync(self) -> None:
        """Forget any backlog: the next frame is due one period from now.

        Used when catching up would cost more than it saves, so a slow
        sender settles at the rate it can sustain instead of spiralling.
        """
        self._deadline = self._clock() + self.period
