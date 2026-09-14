import asyncio
import contextlib
import hashlib
import json
import time
from collections import OrderedDict
from typing import Any, Optional


class CacheEntry:
    """Represents a cached response with TTL and task tracking."""

    def __init__(
        self,
        value: Any = None,
        error: Optional[Exception] = None,
        task: Optional[asyncio.Task] = None,
    ):
        self.value = value
        self.error = error
        self.task = task
        self.timestamp = time.time()
        self.waiters = 0

    def is_expired(self, ttl: float) -> bool:
        """Check if the cache entry has exceeded its TTL."""
        return (time.time() - self.timestamp) > ttl

    def get_result(self):
        """Get the cached result or raise the cached error."""
        if self.error:
            raise self.error
        return self.value


class ResponseCache:
    """Simple LRU cache with TTL for async function responses.

    Features:
    - Fixed TTL (10 seconds by default)
    - LRU eviction at max size (100 entries by default)
    - Concurrent request deduplication (singleflight)
    - Error caching to prevent retry storms
    """

    def __init__(self, ttl: float = 10.0, max_size: int = 100):
        self.ttl = ttl
        self.max_size = max_size
        self._cache: OrderedDict[str, CacheEntry] = OrderedDict()
        self._lock = asyncio.Lock()

    def make_cache_key(self, *args, **kwargs) -> str:
        """Generate a stable cache key from function arguments."""
        sorted_kwargs = sorted(kwargs.items())
        key_data = {"args": args, "kwargs": sorted_kwargs}
        key_json = json.dumps(key_data, sort_keys=True, default=str)
        return hashlib.sha256(key_json.encode()).hexdigest()

    async def get_or_fetch(self, key: str, fetch_func, *args, **kwargs) -> Any:
        """Get cached result or fetch new one, with concurrent request deduplication.

        Args:
            key: Cache key
            fetch_func: Async function to call if cache miss
            *args, **kwargs: Arguments to pass to fetch_func

        Returns:
            The cached or freshly fetched result
        """
        async with self._lock:
            entry = self._cache.get(key)
            if entry is not None and entry.task is not None and entry.task.done():
                # Its starter was cancelled before it could settle the entry.
                self._settle(key, entry)
                entry = self._cache.get(key)
            if entry is not None and entry.task is None:
                if not entry.is_expired(self.ttl):
                    self._cache.move_to_end(key)
                    return entry.get_result()
                del self._cache[key]
                entry = None
            if entry is None:
                task = asyncio.create_task(fetch_func(*args, **kwargs))
                entry = CacheEntry(task=task)
                self._cache[key] = entry
                task.add_done_callback(lambda _: self._settle(key, entry))

                # LRU eviction
                while len(self._cache) > self.max_size:
                    self._cache.popitem(last=False)
            entry.waiters += 1

        # Shielded, so one cancelled caller does not cancel the fetch the others
        # (and the cache) wait on. The last caller to leave cancels it instead: a
        # fetch must not outlive its callers, nor its loop.
        try:
            return await asyncio.shield(entry.task)
        finally:
            entry.waiters -= 1
            if entry.waiters == 0 and not entry.task.done():
                entry.task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await asyncio.wait({entry.task})

    def _settle(self, key: str, entry: CacheEntry) -> None:
        """Keep what a finished fetch produced, or drop it when it was cancelled."""
        if self._cache.get(key) is not entry or not entry.task.done():
            return
        if entry.task.cancelled():
            del self._cache[key]
        elif entry.task.exception() is not None:
            self._cache[key] = CacheEntry(error=entry.task.exception())
        else:
            self._cache[key] = CacheEntry(value=entry.task.result())

    def clear(self):
        """Clear all cached entries."""
        self._cache.clear()
