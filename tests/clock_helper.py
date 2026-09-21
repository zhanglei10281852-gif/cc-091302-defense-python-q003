"""可手动步进的墙上时钟，用于确定性测试有效期与重启恢复。"""

from __future__ import annotations

from src.models import Clock


class ManualClock(Clock):
    def __init__(self, start: float = 1_000_000.0):
        self._t = start

    def now(self) -> float:
        return self._t

    def advance(self, seconds: float) -> float:
        self._t += seconds
        return self._t
