from __future__ import annotations
import threading
from collections import defaultdict


class Metrics:
    def __init__(self):
        self._lock = threading.Lock()
        self._counts: dict[str, int] = defaultdict(int)
        self.decisions: list[dict] = []  # safety-controller / pacing decision log

    def incr(self, key: str, n: int = 1) -> None:
        with self._lock:
            self._counts[key] += n

    def snapshot(self) -> dict[str, int]:
        with self._lock:
            return dict(self._counts)

    def log_decision(self, record: dict) -> None:
        with self._lock:
            self.decisions.append(record)
