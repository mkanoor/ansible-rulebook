"""
Event Store Abstract Base Class

Defines the interface for event storage implementations.
Implementations can use SharedMemory, mmap, or other backing stores.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict


class EventStore(ABC):
    """
    Abstract base class for event storage implementations.

    Defines the common interface that all event store implementations must provide.
    This allows easy switching between different backends (SharedMemory, mmap, etc.)
    without changing the calling code.
    """

    @abstractmethod
    async def add_event(
        self, event: Dict[str, Any], caller: str = "unknown"
    ) -> str:
        """
        Add event to storage with reference counting.

        Args:
            event: Event dictionary with event["meta"]["uuid"] and JSON-serializable data
            caller: Name of the calling function (for logging)

        Returns:
            event_uuid: UUID of the added event

        Raises:
            KeyError: If event missing meta.uuid
            MemoryError: If unable to allocate space
        """
        pass

    @abstractmethod
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
        pass

    @abstractmethod
    def get_event_lazy(self, event_uuid: str):
        """
        Get event as lazy dict wrapper (zero-copy, deserializes on first access).

        Args:
            event_uuid: UUID of event to retrieve

        Returns:
            LazyEventDict wrapper

        Raises:
            KeyError: If event UUID not found
        """
        pass

    @abstractmethod
    def get_event_sync(self, event_uuid: str) -> Dict[str, Any]:
        """
        Get event as dictionary (synchronous version).

        NOTE: This copies data to Python heap.
        For better performance, use get_event_lazy() instead.

        Args:
            event_uuid: UUID of event to retrieve

        Returns:
            Event dictionary

        Raises:
            KeyError: If event UUID not found
        """
        pass

    @abstractmethod
    def get_available_space_bytes(self) -> int:
        """Get available space in bytes."""
        pass

    @abstractmethod
    def get_available_space_mb(self) -> float:
        """Get available space in megabytes."""
        pass

    @abstractmethod
    def get_used_space_bytes(self) -> int:
        """Get used space in bytes."""
        pass

    @abstractmethod
    def get_used_space_mb(self) -> float:
        """Get used space in megabytes."""
        pass

    @abstractmethod
    def get_usage_percent(self) -> float:
        """Get usage as percentage of max size."""
        pass

    @abstractmethod
    def is_near_threshold(self) -> bool:
        """Check if usage is near threshold."""
        pass

    @abstractmethod
    def get_stats(self) -> Dict[str, Any]:
        """Get storage statistics."""
        pass

    @abstractmethod
    def get_detailed_diagnostics(self) -> Dict[str, Any]:
        """Get detailed diagnostics for debugging memory issues."""
        pass

    @abstractmethod
    def print_diagnostics(self):
        """Print detailed diagnostics to logger for debugging."""
        pass

    @abstractmethod
    async def cleanup_all(self):
        """Clear all events (useful for testing/shutdown)."""
        pass

    @abstractmethod
    def close(self):
        """
        Close and cleanup the event store.

        Call this during shutdown to clean up resources.
        """
        pass

    @property
    @abstractmethod
    def _threshold_pct(self) -> float:
        """Get the threshold percentage for backpressure."""
        pass
