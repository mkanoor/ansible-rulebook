"""
Memory-Mapped Event Storage with Reference Counting (Anonymous mmap)

Alternative to SharedMemory using anonymous memory mapping.
Provides the same API but with automatic cleanup (no orphan segments).

Key differences from SharedMemory:
- Uses anonymous mmap (no /dev/shm files)
- Automatic cleanup on process exit (no orphans possible)
- Single-process only (cannot share across processes)
- Same GC benefits (events stored outside Python heap)

Usage:
    # Initialize at startup with 100MB limit
    store = MmapEventStore.get_instance(max_size_mb=100)

    # Add event (blocks if near threshold)
    event_uuid = await store.add_event(event)

    # Get event by reference (zero-copy)
    event_ref = store.get_event_lazy(event_uuid)

    # Remove event (decrements refcount)
    await store.remove_event(event_uuid)

    # No cleanup needed - auto-cleaned when process exits!
"""

import asyncio
import json
import logging
import mmap
import sys
import time
from collections.abc import MutableMapping
from typing import Any, Dict, Optional

from ansible_rulebook.conf import settings
from ansible_rulebook.event_store import EventStore

logger = logging.getLogger(__name__)


class LazyEventDict(MutableMapping):
    """
    Lazy dict-like wrapper for events in mmap.

    Holds a memoryview reference to the event in mmap and only
    deserializes to a Python dict when first accessed. This minimizes
    memory copies and GC pressure.

    Identical to the SharedMemory version.
    """

    def __init__(self, memoryview_ref: memoryview, event_uuid: str):
        """
        Initialize lazy event wrapper.

        Args:
            memoryview_ref: memoryview reference to event in mmap
            event_uuid: UUID of the event (for logging)
        """
        self._memoryview = memoryview_ref
        self._event_uuid = event_uuid
        self._deserialized = None  # Cached deserialized dict
        self._accessed = False

    def _ensure_deserialized(self):
        """Deserialize event on first access."""
        if self._deserialized is None:
            try:
                event_json = bytes(self._memoryview).decode("utf-8")
                self._deserialized = json.loads(event_json)
                if not self._accessed:
                    self._accessed = True
                    logger.debug(
                        "[LAZY EVENT] Deserialized event %s on first access (size: %d bytes)",
                        self._event_uuid[:8]
                        if len(self._event_uuid) > 8
                        else self._event_uuid,
                        len(self._memoryview),
                    )
            except (ValueError, BufferError) as e:
                # mmap was closed (e.g., during hot reload) - return empty dict
                logger.warning(
                    "[LAZY EVENT] Failed to deserialize event %s: %s (mmap likely closed)",
                    self._event_uuid[:8]
                    if len(self._event_uuid) > 8
                    else self._event_uuid,
                    str(e),
                )
                self._deserialized = {}

    def __getitem__(self, key):
        self._ensure_deserialized()
        return self._deserialized[key]

    def __setitem__(self, key, value):
        raise TypeError("Events are read-only and cannot be modified")

    def __delitem__(self, key):
        raise TypeError("Events are read-only and cannot be modified")

    def __iter__(self):
        self._ensure_deserialized()
        return iter(self._deserialized)

    def __len__(self):
        self._ensure_deserialized()
        return len(self._deserialized)

    def __repr__(self):
        if self._deserialized is None:
            return f"<LazyEventDict uuid={self._event_uuid[:8]}... (not yet deserialized)>"
        return f"<LazyEventDict uuid={self._event_uuid[:8]}... {repr(self._deserialized)}>"

    def __str__(self):
        self._ensure_deserialized()
        return str(self._deserialized)

    def get(self, key, default=None):
        self._ensure_deserialized()
        return self._deserialized.get(key, default)

    def keys(self):
        self._ensure_deserialized()
        return self._deserialized.keys()

    def values(self):
        self._ensure_deserialized()
        return self._deserialized.values()

    def items(self):
        self._ensure_deserialized()
        return self._deserialized.items()

    def to_dict(self):
        """Get the deserialized dict (for cases that need actual dict)."""
        self._ensure_deserialized()
        return self._deserialized

    def copy(self):
        """
        Return self since LazyEventDict is read-only and immutable.

        This method is needed for compatibility with substitute_variables()
        which calls value.copy() on dicts. Since events are read-only,
        we can safely return self instead of creating a new copy.

        Returns:
            self: The same LazyEventDict instance
        """
        return self

    def __reduce__(self):
        """
        Support for pickling LazyEventDict.

        Since memoryview objects cannot be pickled, we deserialize the event
        and return a plain dict when pickling is needed (e.g., when sending
        events to queues or across process boundaries).

        Returns:
            tuple: (dict, (dict_data,)) for unpickling as a plain dict
        """
        self._ensure_deserialized()
        return (dict, (self._deserialized,))


class MmapEventStore(EventStore):
    """
    Singleton class for storing events in anonymous mmap with reference counting.

    Uses anonymous memory mapping (RAM only, no file) to store events,
    avoiding Python heap allocation and GC issues. Events are accessed
    by reference (memoryview) to prevent copying.

    Key benefits over SharedMemory:
    - Automatic cleanup (no orphan segments)
    - No /dev/shm pollution
    - Same performance and GC benefits

    Memory layout:
    - Index: dictionary mapping UUID -> (offset, size, refcount)
    - Data: actual event payloads (JSON serialized) in mmap
    """

    _instance: Optional["MmapEventStore"] = None
    _instance_lock = asyncio.Lock()

    def __init__(
        self,
        max_size_mb: int = 100,
        threshold_pct: float = 90.0,
        sleep_duration: float = 0.5,
        max_retries: int = 10,
    ):
        """
        Initialize mmap event store.

        Args:
            max_size_mb: Maximum size in megabytes for the mmap segment
            threshold_pct: Threshold percentage (0-100) to trigger backpressure
            sleep_duration: Seconds to sleep when near threshold before retry
            max_retries: Maximum number of retries when waiting for space
        """
        if MmapEventStore._instance is not None:
            raise RuntimeError(
                "MmapEventStore is a singleton. Use get_instance() instead."
            )

        self._max_size_bytes = max_size_mb * 1024 * 1024
        self.__threshold_pct = (
            threshold_pct  # Private variable accessed via property
        )
        self._sleep_duration = sleep_duration
        self._max_retries = max_retries

        # Thread-safe lock for add/remove operations
        self._lock = asyncio.Lock()

        # Event index: {event_uuid: {'offset': int, 'size': int, 'refcount': int}}
        self._event_index: Dict[str, Dict[str, int]] = {}

        # Current offset in mmap (where next event will be written)
        self._current_offset = 0

        # Free space tracking (for compaction/reuse)
        self._free_blocks: list = []  # [(offset, size), ...]

        # Statistics
        self._total_adds = 0
        self._total_removes = 0
        self._total_blocked = 0
        self._peak_usage_bytes = 0

        # Backpressure tracking (shared across all concurrent add_event calls)
        self._backpressure_active = False
        self._backpressure_start_time = 0
        self._backpressure_last_log_time = 0

        # Create anonymous memory mapping (RAM only, no file backing)
        # -1 means anonymous mapping (no file descriptor)
        self._mmap = mmap.mmap(-1, self._max_size_bytes)

        logger.info(
            "[MMAP STORE] Created anonymous memory mapping with size=%d bytes (%.2f MB)",
            self._max_size_bytes,
            max_size_mb,
        )
        logger.info(
            "[MMAP STORE] Initialized with threshold=%.1f%%, sleep=%.2fs, max_retries=%d",
            threshold_pct,
            sleep_duration,
            max_retries,
        )
        logger.info(
            "[MMAP STORE] Using anonymous mmap - auto cleanup on exit, no orphan segments"
        )

    @classmethod
    async def get_instance(
        cls,
        max_size_mb: int = 100,
        threshold_pct: float = 90.0,
        sleep_duration: float = 0.5,
        max_retries: int = 10,
    ) -> "MmapEventStore":
        """
        Get or create the singleton instance.

        Args:
            max_size_mb: Maximum size in MB (only used on first creation)
            threshold_pct: Threshold percentage for backpressure
            sleep_duration: Sleep duration when waiting for space
            max_retries: Max retries when waiting for space

        Returns:
            MmapEventStore singleton instance
        """
        if cls._instance is None:
            async with cls._instance_lock:
                if cls._instance is None:
                    cls._instance = cls(
                        max_size_mb=max_size_mb,
                        threshold_pct=threshold_pct,
                        sleep_duration=sleep_duration,
                        max_retries=max_retries,
                    )
        return cls._instance

    async def add_event(
        self, event: Dict[str, Any], caller: str = "unknown"
    ) -> str:
        """
        Add event to mmap with reference counting.

        If the event UUID already exists, increments the reference count.
        If near threshold, uses exponential backoff retry until space is available.

        Retry behavior (configured via settings):
        - Initial wait: settings.shared_memory_initial_wait (default: 0.5s)
        - Exponentially increases: 1s, 2s, 4s, 8s, 16s, 32s, ...
        - Max per iteration: settings.shared_memory_max_wait_per_iteration (default: 60s)
        - Total max wait: settings.shared_memory_max_total_wait (default: 3600s / 1 hour)

        Args:
            event: Event dictionary with event["meta"]["uuid"] and JSON-serializable data
            caller: Name of the calling function (for logging)

        Returns:
            event_uuid: UUID of the added event

        Raises:
            KeyError: If event missing meta.uuid
            MemoryError: If unable to allocate space after max wait time
        """
        try:
            event_uuid = event["meta"]["uuid"]
        except KeyError:
            raise KeyError("Event missing meta.uuid - cannot store")

        # Manually acquire lock (so we can release it during sleep)
        await self._lock.acquire()
        try:
            # Check if event already exists (increment refcount)
            if event_uuid in self._event_index:
                self._event_index[event_uuid]["refcount"] += 1
                logger.debug(
                    "[MMAP STORE] Incremented refcount for event %s (refcount=%d)",
                    event_uuid,
                    self._event_index[event_uuid]["refcount"],
                )
                return event_uuid

            # Serialize event to JSON bytes
            event_json = json.dumps(event)
            event_bytes = event_json.encode("utf-8")
            event_size = len(event_bytes)

            # Check space availability with exponential backoff retry logic
            # Retry for up to max_total_wait with exponential backoff (configured in settings)
            start_time = time.time()
            max_total_wait = settings.shared_memory_max_total_wait
            current_wait = settings.shared_memory_initial_wait
            max_wait_per_iteration = (
                settings.shared_memory_max_wait_per_iteration
            )
            log_interval = 30  # Log every 30 seconds

            while not self._has_space_for(event_size):
                elapsed = time.time() - start_time

                if elapsed >= max_total_wait:
                    raise MemoryError(
                        f"Unable to allocate {event_size} bytes after waiting {elapsed:.1f} seconds. "
                        f"Current usage: {self.get_used_space_bytes()}/{self._max_size_bytes} bytes "
                        f"({self.get_usage_percent():.1f}%). "
                        f"Mmap threshold not clearing - check for event leaks or increase memory size."
                    )

                # Log only when backpressure first activates (globally) or every 30 seconds
                current_time = time.time()
                if not self._backpressure_active:
                    # First task to hit backpressure - log it
                    self._backpressure_active = True
                    self._backpressure_start_time = current_time
                    self._backpressure_last_log_time = current_time
                    logger.warning(
                        "[MMAP STORE:%s] Backpressure activated - threshold reached (%.1f%%), "
                        "blocking new event additions (available %d bytes, need %d bytes)",
                        caller.upper(),
                        self.get_usage_percent(),
                        self.get_available_space_bytes(),
                        event_size,
                    )
                elif (
                    current_time - self._backpressure_last_log_time
                    >= log_interval
                ):
                    # Periodic update
                    self._backpressure_last_log_time = current_time
                    logger.warning(
                        "[MMAP STORE:%s] Still under backpressure - elapsed %.1fs "
                        "(usage: %.1f%%, available %d bytes, %d tasks blocked)",
                        caller.upper(),
                        current_time - self._backpressure_start_time,
                        self.get_usage_percent(),
                        self.get_available_space_bytes(),
                        self._total_blocked,
                    )

                self._total_blocked += 1

                # Release lock while sleeping to allow removals
                self._lock.release()
                await asyncio.sleep(current_wait)
                await self._lock.acquire()

                # Exponential backoff: 0.5s -> 1s -> 2s -> 4s -> 8s -> ... -> 60s (max)
                current_wait = min(current_wait * 2, max_wait_per_iteration)

            # If space became available, check if we should log and reset backpressure flag
            if self._backpressure_active and self._has_space_for(event_size):
                # Check if this is the last waiting task (only we have the lock now)
                # We'll reset the flag - if other tasks still need to wait, they'll reactivate it
                self._backpressure_active = False
                logger.info(
                    "[MMAP STORE:%s] Backpressure released after %.1fs "
                    "(usage: %.1f%%, available %d bytes)",
                    caller.upper(),
                    time.time() - self._backpressure_start_time,
                    self.get_usage_percent(),
                    self.get_available_space_bytes(),
                )

            # Find space (reuse free block or append to end)
            offset = self._find_space_for(event_size)

            # Write to mmap
            self._write_to_mmap(offset, event_bytes)

            # Update index
            self._event_index[event_uuid] = {
                "offset": offset,
                "size": event_size,
                "refcount": 1,
            }

            # Update offset if we appended to end
            if offset == self._current_offset:
                self._current_offset += event_size

            # Update statistics
            self._total_adds += 1
            current_usage = self.get_used_space_bytes()
            if current_usage > self._peak_usage_bytes:
                self._peak_usage_bytes = current_usage

            logger.debug(
                "[MMAP STORE] Added event %s at offset %d (size=%d bytes, refcount=1, "
                "usage=%d/%d bytes, %.1f%%)",
                event_uuid,
                offset,
                event_size,
                current_usage,
                self._max_size_bytes,
                self.get_usage_percent(),
            )

            return event_uuid
        finally:
            # Always release lock
            self._lock.release()

    async def remove_event(self, event_uuid: str) -> bool:
        """
        Decrement reference count and remove event if zero.

        Args:
            event_uuid: UUID of event to remove

        Returns:
            True if event was deleted (refcount reached 0), False if still referenced

        Raises:
            KeyError: If event UUID not found
        """
        async with self._lock:
            if event_uuid not in self._event_index:
                raise KeyError(f"Event {event_uuid} not found in store")

            # Decrement refcount
            self._event_index[event_uuid]["refcount"] -= 1
            refcount = self._event_index[event_uuid]["refcount"]

            if refcount < 0:
                logger.error(
                    "[MMAP STORE] Refcount went negative for event %s! "
                    "This indicates a bug in reference counting.",
                    event_uuid,
                )
                # Fix it
                self._event_index[event_uuid]["refcount"] = 0
                refcount = 0

            if refcount == 0:
                # Last reference - remove the event
                event_info = self._event_index[event_uuid]
                offset = event_info["offset"]
                size = event_info["size"]

                # Add to free blocks for reuse
                self._free_blocks.append((offset, size))
                self._free_blocks.sort()  # Keep sorted by offset for coalescing

                # Coalesce adjacent free blocks
                self._coalesce_free_blocks()

                # Remove from index
                del self._event_index[event_uuid]

                self._total_removes += 1

                logger.debug(
                    "[MMAP STORE] Removed event %s (refcount=0, freed %d bytes, "
                    "usage=%d/%d bytes, %.1f%%)",
                    event_uuid,
                    size,
                    self.get_used_space_bytes(),
                    self._max_size_bytes,
                    self.get_usage_percent(),
                )

                return True
            else:
                logger.debug(
                    "[MMAP STORE] Decremented refcount for event %s (refcount=%d)",
                    event_uuid,
                    refcount,
                )
                return False

    def get_event_lazy(self, event_uuid: str) -> LazyEventDict:
        """
        Get event as lazy dict wrapper (zero-copy, deserializes on first access).

        This is the preferred method for accessing events as it:
        - Holds memoryview reference (no copy until accessed)
        - Only deserializes when event data is actually used
        - Provides dict-like interface for Jinja2/variable substitution
        - Minimizes GC pressure

        Args:
            event_uuid: UUID of event to retrieve

        Returns:
            LazyEventDict wrapper

        Raises:
            KeyError: If event UUID not found
        """
        if event_uuid not in self._event_index:
            raise KeyError(f"Event {event_uuid} not found in store")

        event_info = self._event_index[event_uuid]
        offset = event_info["offset"]
        size = event_info["size"]

        # Get memoryview reference (zero-copy)
        event_view = memoryview(self._mmap[offset : offset + size])

        # Wrap in lazy dict
        return LazyEventDict(event_view, event_uuid)

    def get_event_sync(self, event_uuid: str) -> Dict[str, Any]:
        """
        Get event as dictionary (synchronous version).

        NOTE: This copies data from mmap to Python heap.
        For better performance, use get_event_lazy() instead.

        Args:
            event_uuid: UUID of event to retrieve

        Returns:
            Event dictionary

        Raises:
            KeyError: If event UUID not found
        """
        if event_uuid not in self._event_index:
            raise KeyError(f"Event {event_uuid} not found in store")

        event_info = self._event_index[event_uuid]
        offset = event_info["offset"]
        size = event_info["size"]

        # Read from mmap (copies bytes)
        event_bytes = bytes(self._mmap[offset : offset + size])
        event_json = event_bytes.decode("utf-8")
        return json.loads(event_json)

    def get_available_space_bytes(self) -> int:
        """Get available space in bytes."""
        return self._max_size_bytes - self.get_used_space_bytes()

    def get_available_space_mb(self) -> float:
        """Get available space in megabytes."""
        return self.get_available_space_bytes() / (1024 * 1024)

    def get_used_space_bytes(self) -> int:
        """Get used space in bytes (sum of all event sizes)."""
        total = sum(info["size"] for info in self._event_index.values())
        return total

    def get_used_space_mb(self) -> float:
        """Get used space in megabytes."""
        return self.get_used_space_bytes() / (1024 * 1024)

    def get_usage_percent(self) -> float:
        """Get usage as percentage of max size."""
        return (self.get_used_space_bytes() / self._max_size_bytes) * 100

    def is_near_threshold(self) -> bool:
        """Check if usage is near threshold."""
        return self.get_usage_percent() >= self._threshold_pct

    @property
    def _threshold_pct(self) -> float:
        """Get the threshold percentage for backpressure."""
        return self.__threshold_pct

    def get_stats(self) -> Dict[str, Any]:
        """Get storage statistics."""
        return {
            "max_size_bytes": self._max_size_bytes,
            "max_size_mb": self._max_size_bytes / (1024 * 1024),
            "used_bytes": self.get_used_space_bytes(),
            "used_mb": self.get_used_space_mb(),
            "available_bytes": self.get_available_space_bytes(),
            "available_mb": self.get_available_space_mb(),
            "usage_percent": self.get_usage_percent(),
            "threshold_percent": self._threshold_pct,
            "near_threshold": self.is_near_threshold(),
            "current_events": len(self._event_index),
            "peak_usage_bytes": self._peak_usage_bytes,
            "peak_usage_mb": self._peak_usage_bytes / (1024 * 1024),
            "total_adds": self._total_adds,
            "total_removes": self._total_removes,
            "total_blocked": self._total_blocked,
            "free_blocks": len(self._free_blocks),
        }

    def get_detailed_diagnostics(self) -> Dict[str, Any]:
        """Get detailed diagnostics for debugging memory issues."""
        # Calculate memory accounting
        accounted_bytes = sum(
            info["size"] for info in self._event_index.values()
        )
        free_blocks_bytes = sum(size for _, size in self._free_blocks)

        # Get refcount distribution
        refcount_dist = {}
        for info in self._event_index.values():
            rc = info["refcount"]
            refcount_dist[rc] = refcount_dist.get(rc, 0) + 1

        return {
            **self.get_stats(),
            # Memory accounting
            "accounted_bytes": accounted_bytes,
            "accounted_mb": accounted_bytes / (1024 * 1024),
            "free_blocks_bytes": free_blocks_bytes,
            "free_blocks_mb": free_blocks_bytes / (1024 * 1024),
            "current_offset": self._current_offset,
            "current_offset_mb": self._current_offset / (1024 * 1024),
            "unaccounted_bytes": self._current_offset
            - accounted_bytes
            - free_blocks_bytes,
            "unaccounted_mb": (
                self._current_offset - accounted_bytes - free_blocks_bytes
            )
            / (1024 * 1024),
            # Reference counting
            "refcount_distribution": refcount_dist,
            "total_references": sum(
                info["refcount"] for info in self._event_index.values()
            ),
            # Potential issues
            "potential_leak": self._total_adds != self._total_removes,
            "leak_count": self._total_adds - self._total_removes
            if self._total_adds != self._total_removes
            else 0,
            # Index overhead
            "event_index_size": len(self._event_index),
            "event_index_overhead_est_kb": sys.getsizeof(self._event_index)
            / 1024,
            "free_blocks_overhead_est_kb": sys.getsizeof(self._free_blocks)
            / 1024,
        }

    def print_diagnostics(self):
        """Print detailed diagnostics to logger for debugging."""
        diag = self.get_detailed_diagnostics()

        logger.info("=" * 60)
        logger.info("MMAP EVENT STORE DIAGNOSTICS")
        logger.info("=" * 60)
        logger.info(f"Max size: {diag['max_size_mb']:.2f} MB")
        logger.info(f"Current offset: {diag['current_offset_mb']:.2f} MB")
        logger.info("")
        logger.info(f"Current events: {diag['current_events']}")
        logger.info(f"Total adds: {diag['total_adds']}")
        logger.info(f"Total removes: {diag['total_removes']}")
        logger.info(f"Total references: {diag['total_references']}")
        logger.info("")
        logger.info(
            f"Accounted memory: {diag['accounted_mb']:.2f} MB ({diag['current_events']} events)"
        )
        logger.info(
            f"Free blocks: {diag['free_blocks']} blocks, {diag['free_blocks_mb']:.2f} MB"
        )
        logger.info(f"Unaccounted: {diag['unaccounted_mb']:.2f} MB")
        logger.info("")
        logger.info(
            f"Usage: {diag['usage_percent']:.1f}% ({diag['used_mb']:.2f} MB / {diag['max_size_mb']:.2f} MB)"
        )
        logger.info(f"Peak usage: {diag['peak_usage_mb']:.2f} MB")
        logger.info(f"Total blocked: {diag['total_blocked']}")
        logger.info("")
        logger.info("Refcount distribution:")
        for rc, count in sorted(diag["refcount_distribution"].items()):
            logger.info(f"  refcount={rc}: {count} events")
        logger.info("")

        if diag["potential_leak"]:
            logger.warning(
                f"⚠️  POTENTIAL LEAK: {diag['leak_count']} events not removed"
            )
            logger.warning(
                f"   Adds: {diag['total_adds']}, Removes: {diag['total_removes']}"
            )

        if diag["current_events"] > 0:
            logger.warning(
                f"⚠️  UNRELEASED EVENTS: {diag['current_events']} events still in memory"
            )

        logger.info("=" * 60)

    async def cleanup_all(self):
        """Clear all events (useful for testing/shutdown)."""
        async with self._lock:
            count = len(self._event_index)
            self._event_index.clear()
            self._free_blocks.clear()
            self._current_offset = 0

            logger.info(
                "[MMAP STORE] Cleaned up all events (removed %d events)", count
            )

    def close(self):
        """
        Close the mmap.

        Call this during shutdown to clean up resources.
        Note: mmap auto-closes when process exits, but explicit is better.
        """
        try:
            self._mmap.close()
            logger.info("[MMAP STORE] Closed memory mapping")
        except Exception as e:
            logger.warning("[MMAP STORE] Error closing mmap: %s", e)

    def __del__(self):
        """Cleanup on garbage collection."""
        try:
            self.close()
        except:
            pass

    # Private helper methods

    def _has_space_for(self, size: int) -> bool:
        """Check if there's space for the given size considering threshold."""
        projected_usage = self.get_used_space_bytes() + size
        projected_pct = (projected_usage / self._max_size_bytes) * 100
        return projected_pct < self._threshold_pct

    def _find_space_for(self, size: int) -> int:
        """
        Find space for event (reuse free block or append to end).

        Returns:
            Offset where event should be written
        """
        # Try to find suitable free block
        for i, (offset, block_size) in enumerate(self._free_blocks):
            if block_size >= size:
                # Reuse this block
                self._free_blocks.pop(i)

                # If block is larger, add remainder back to free list
                if block_size > size:
                    remainder_offset = offset + size
                    remainder_size = block_size - size
                    self._free_blocks.append(
                        (remainder_offset, remainder_size)
                    )
                    self._free_blocks.sort()

                logger.debug(
                    "[MMAP STORE] Reusing free block at offset %d (size=%d, needed=%d)",
                    offset,
                    block_size,
                    size,
                )

                return offset

        # No suitable free block, append to end
        return self._current_offset

    def _write_to_mmap(self, offset: int, data: bytes):
        """Write data to mmap at offset."""
        size = len(data)
        self._mmap[offset : offset + size] = data

    def _coalesce_free_blocks(self):
        """Merge adjacent free blocks to reduce fragmentation."""
        if len(self._free_blocks) < 2:
            return

        coalesced = []
        i = 0
        while i < len(self._free_blocks):
            offset, size = self._free_blocks[i]

            # Check if next block is adjacent
            while i + 1 < len(self._free_blocks):
                next_offset, next_size = self._free_blocks[i + 1]
                if offset + size == next_offset:
                    # Merge
                    size += next_size
                    i += 1
                else:
                    break

            coalesced.append((offset, size))
            i += 1

        if len(coalesced) < len(self._free_blocks):
            logger.debug(
                "[MMAP STORE] Coalesced %d free blocks into %d",
                len(self._free_blocks),
                len(coalesced),
            )
            self._free_blocks = coalesced


# Convenience function for global singleton access


async def get_mmap_store(
    max_size_mb: int = 100,
    threshold_pct: float = 90.0,
    sleep_duration: float = 0.5,
    max_retries: int = 10,
) -> MmapEventStore:
    """
    Get or create the global MmapEventStore singleton.

    Args:
        max_size_mb: Maximum size in MB (only used on first creation)
        threshold_pct: Threshold percentage for backpressure
        sleep_duration: Sleep duration when waiting for space
        max_retries: Max retries when waiting for space

    Returns:
        MmapEventStore singleton instance
    """
    return await MmapEventStore.get_instance(
        max_size_mb=max_size_mb,
        threshold_pct=threshold_pct,
        sleep_duration=sleep_duration,
        max_retries=max_retries,
    )
