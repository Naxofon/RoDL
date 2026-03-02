from __future__ import annotations

import asyncio
import hashlib
import time
from collections import deque
from contextlib import asynccontextmanager

from .logging_utils import get_logger


def _now() -> float:
    return time.monotonic()


def token_fingerprint(token: str) -> str:
    """Short, non-reversible token fingerprint for logging and keys."""
    return hashlib.sha1(token.encode("utf-8")).hexdigest()[:8]


class GlobalRateLimiter:
    """
    Global sliding-window limiter: max N requests total across all clients per window.
    This enforces the Yandex Direct API limit of 20 requests per 10 seconds globally.
    """

    def __init__(self, max_requests: int, window_seconds: float) -> None:
        self.max_requests = max_requests
        self.window = window_seconds
        self._request_times: deque[float] = deque()
        self._lock: asyncio.Lock | None = None
        self._lock_loop: asyncio.AbstractEventLoop | None = None

    def _get_lock(self) -> asyncio.Lock:
        """Lazily create lock in the current event loop, recreating if loop changed."""
        try:
            current_loop = asyncio.get_running_loop()
        except RuntimeError:
            current_loop = None

        if self._lock is None or self._lock_loop is not current_loop:
            self._lock = asyncio.Lock() if current_loop else None
            self._lock_loop = current_loop
        return self._lock

    async def acquire(self, client_id: str | None = None) -> float:
        """
        Wait until a request can be made without exceeding the global rate limit.

        Args:
            client_id: Client identifier for logging purposes

        Returns:
            float: seconds spent waiting (0 if no wait was required).
        """
        waited_total = 0.0
        client_label = client_id or "unknown"

        while True:
            async with self._get_lock():
                now = _now()
                while self._request_times and now - self._request_times[0] > self.window:
                    self._request_times.popleft()

                if len(self._request_times) < self.max_requests:
                    self._request_times.append(now)
                    return waited_total

                wait = max(0.01, self.window - (now - self._request_times[0]))
                waited_total += wait
                get_logger().debug(
                    "%s: GLOBAL throttling — limit %d/%ss reached, waiting %.2fs (total requests in window: %d)",
                    client_label,
                    self.max_requests,
                    self.window,
                    wait,
                    len(self._request_times),
                )
            await asyncio.sleep(wait)


class PerAdvertiserRateLimiter:
    """Sliding-window limiter: max N requests per advertiser per window."""

    def __init__(self, max_requests: int, window_seconds: float) -> None:
        self.max_requests = max_requests
        self.window = window_seconds
        self._windows: dict[str, deque[float]] = {}
        self._lock: asyncio.Lock | None = None
        self._lock_loop: asyncio.AbstractEventLoop | None = None

    def _get_lock(self) -> asyncio.Lock:
        """Lazily create lock in the current event loop, recreating if loop changed."""
        try:
            current_loop = asyncio.get_running_loop()
        except RuntimeError:
            current_loop = None

        if self._lock is None or self._lock_loop is not current_loop:
            self._lock = asyncio.Lock() if current_loop else None
            self._lock_loop = current_loop
        return self._lock

    async def acquire(self, advertiser: str) -> float:
        """
        Wait until the advertiser can make a reports request.

        Returns:
            float: seconds spent waiting (0 if no wait was required).
        """
        key = advertiser or "unknown"
        waited_total = 0.0

        while True:
            async with self._get_lock():
                now = _now()
                window = self._windows.setdefault(key, deque())
                while window and now - window[0] > self.window:
                    window.popleft()

                if len(window) < self.max_requests:
                    window.append(now)
                    return waited_total

                wait = max(0.01, self.window - (now - window[0]))
                waited_total += wait
                get_logger().info(
                    "%s: throttling reports — limit %d/%ss reached, waiting %.2fs",
                    key,
                    self.max_requests,
                    self.window,
                    wait,
                )
            await asyncio.sleep(wait)


class OfflineReportTracker:
    """
    Protect against the offline queue limit (max 5 per user).

    We pessimistically reserve a slot before each reports request; the slot
    is released once the request finishes (200/201/202/error). This keeps the
    number of in-flight offline reports per token at or below the API limit.
    """

    def __init__(self, max_inflight: int = 5) -> None:
        self.max_inflight = max_inflight
        self._semaphores: dict[str, asyncio.Semaphore] = {}
        self._semaphore_loops: dict[str, asyncio.AbstractEventLoop] = {}
        self._inflight: dict[str, int] = {}
        self._lock: asyncio.Lock | None = None
        self._lock_loop: asyncio.AbstractEventLoop | None = None

    def _get_lock(self) -> asyncio.Lock:
        """Lazily create lock in the current event loop, recreating if loop changed."""
        try:
            current_loop = asyncio.get_running_loop()
        except RuntimeError:
            current_loop = None

        if self._lock is None or self._lock_loop is not current_loop:
            self._lock = asyncio.Lock() if current_loop else None
            self._lock_loop = current_loop
        return self._lock

    async def _get_sem(self, key: str) -> asyncio.Semaphore:
        try:
            current_loop = asyncio.get_running_loop()
        except RuntimeError:
            current_loop = None

        async with self._get_lock():
            if key not in self._semaphores or self._semaphore_loops.get(key) is not current_loop:
                self._semaphores[key] = asyncio.Semaphore(self.max_inflight)
                self._semaphore_loops[key] = current_loop
            return self._semaphores[key]

    @asynccontextmanager
    async def reserve(self, token: str, *, owner: str | None = None):
        user_key = token_fingerprint(token)
        sem = await self._get_sem(user_key)

        started = _now()
        await sem.acquire()
        waited = _now() - started

        async with self._get_lock():
            self._inflight[user_key] = self._inflight.get(user_key, 0) + 1
            current = self._inflight[user_key]

        if waited > 0.01:
            get_logger().debug(
                "%s: offline reports queue saturated (waited %.2fs, now %d/%d in-flight)",
                owner or user_key,
                waited,
                current,
                self.max_inflight,
            )
        else:
            get_logger().debug(
                "%s: offline slot acquired (%d/%d in-flight)",
                owner or user_key,
                current,
                self.max_inflight,
            )

        try:
            yield current
        finally:
            async with self._get_lock():
                self._inflight[user_key] = max(self._inflight.get(user_key, 1) - 1, 0)
                current = self._inflight[user_key]
            sem.release()
            get_logger().debug(
                "%s: offline slot released (%d/%d remain)",
                owner or user_key,
                current,
                self.max_inflight,
            )
