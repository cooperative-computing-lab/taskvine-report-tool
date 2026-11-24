"""Small in-process operational counters for the report service."""

from __future__ import annotations

import threading
import time


class RuntimeMetrics:
    def __init__(self, request_threads: int, pending_requests: int):
        self.request_threads = request_threads
        self.pending_requests = pending_requests
        self.started_monotonic = time.monotonic()
        self._lock = threading.Lock()
        self._active = 0
        self._peak_active = 0
        self._completed = 0
        self._errors = 0

    def request_started(self) -> None:
        with self._lock:
            self._active += 1
            self._peak_active = max(self._peak_active, self._active)

    def request_finished(self, status_code: int) -> None:
        with self._lock:
            self._active = max(0, self._active - 1)
            self._completed += 1
            if status_code >= 500:
                self._errors += 1

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "uptime_seconds": round(time.monotonic() - self.started_monotonic, 3),
                "active_requests": self._active,
                "peak_active_requests": self._peak_active,
                "completed_requests": self._completed,
                "server_errors": self._errors,
                "request_threads": self.request_threads,
                "pending_request_capacity": self.pending_requests,
                "total_request_capacity": self.request_threads + self.pending_requests,
            }
