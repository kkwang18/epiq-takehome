# src/content_intake/stub/state.py
import hashlib
import threading
from random import Random


class StubState:
    def __init__(
        self,
        latency_mode: str = "fixed",
        latency_ms: int = 150,
        latency_jitter_min_ms: int = 50,
        latency_jitter_max_ms: int = 250,
        latency_seed: int = 20260803,
        failure_every_n: int = 7,
        failure_status: int = 500,
        in_flight_capacity: int = 2,
    ):
        self._lock = threading.Lock()
        self._configure(
            latency_mode, latency_ms, latency_jitter_min_ms, latency_jitter_max_ms,
            latency_seed, failure_every_n, failure_status, in_flight_capacity,
        )
        self._reset_counters()

    def _configure(self, latency_mode, latency_ms, jitter_min, jitter_max, latency_seed,
                   failure_every_n, failure_status, in_flight_capacity):
        self.latency_mode = latency_mode
        self.latency_ms = latency_ms
        self.latency_jitter_min_ms = jitter_min
        self.latency_jitter_max_ms = jitter_max
        self.latency_seed = latency_seed
        self.failure_every_n = failure_every_n
        self.failure_status = failure_status
        self.in_flight_capacity = in_flight_capacity
        self._jitter_rng = Random(latency_seed)

    def _reset_counters(self):
        self._billed_calls = 0
        self._current_in_flight = 0
        self._max_in_flight = 0
        self._server_error_calls = 0
        self._over_capacity_calls = 0
        self._billed_call_index = 0

    def next_latency_ms(self) -> float:
        with self._lock:
            if self.latency_mode == "fixed":
                return self.latency_ms
            return self._jitter_rng.uniform(self.latency_jitter_min_ms, self.latency_jitter_max_ms)

    def enter_call(self) -> bool:
        with self._lock:
            if self._current_in_flight >= self.in_flight_capacity:
                return False
            self._current_in_flight += 1
            self._max_in_flight = max(self._max_in_flight, self._current_in_flight)
            return True

    def exit_call(self) -> None:
        with self._lock:
            self._current_in_flight = max(0, self._current_in_flight - 1)

    def bill(self) -> None:
        with self._lock:
            self._billed_calls += 1
            self._billed_call_index += 1

    def should_fail_this_billed_call(self) -> bool:
        with self._lock:
            if self.failure_every_n <= 0:
                return False
            return self._billed_call_index % self.failure_every_n == 0

    def bill_and_check_failure(self) -> bool:
        """Atomic bill()+should_fail_this_billed_call() pair. The HTTP handler (Task 9)
        must use this instead of calling the two separately — under concurrent requests,
        two independently-locked calls are not atomic as a pair, so one thread's check can
        read an index another thread's bill() already advanced past, corrupting the
        1-in-N failure count (not just which item fails, which EXT-REQ-2 permits, but the
        count, which it does not)."""
        with self._lock:
            self._billed_calls += 1
            self._billed_call_index += 1
            if self.failure_every_n <= 0:
                return False
            return self._billed_call_index % self.failure_every_n == 0

    def record_server_error(self) -> None:
        with self._lock:
            self._server_error_calls += 1

    def record_over_capacity(self) -> None:
        with self._lock:
            self._over_capacity_calls += 1

    def annotate(self, content: bytes) -> dict:
        digest = hashlib.sha256(content).hexdigest()
        rng = Random(digest)
        return {"sha256": digest, "result": rng.random(), "labels": [w for w in ["a", "b", "c"] if rng.random() > 0.5]}

    def stats(self) -> dict:
        with self._lock:
            return {
                "billed_calls": self._billed_calls,
                "current_in_flight": self._current_in_flight,
                "max_in_flight": self._max_in_flight,
                "server_error_calls": self._server_error_calls,
                "over_capacity_calls": self._over_capacity_calls,
            }

    def reset(self, patch: dict | None) -> None:
        with self._lock:
            self._reset_counters()
            if patch:
                for key, value in patch.items():
                    if hasattr(self, key):
                        setattr(self, key, value)
                self._jitter_rng = Random(self.latency_seed)
            else:
                self._jitter_rng = Random(self.latency_seed)
