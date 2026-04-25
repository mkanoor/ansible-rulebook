"""
Event Store Manager

Global singleton access to the active event store instance.
This module provides a unified interface to access the event store
regardless of which backend (SharedMemory or mmap) was initialized.
"""

from typing import Optional

from ansible_rulebook.event_store import EventStore

# Global reference to the active event store
_active_store: Optional[EventStore] = None


def set_event_store(store: EventStore):
    """
    Set the active event store instance.

    This should be called once during initialization in engine.py.

    Args:
        store: The event store instance (SharedMemoryEventStore or MmapEventStore)
    """
    global _active_store
    _active_store = store


def get_event_store() -> EventStore:
    """
    Get the active event store instance.

    This function returns whichever backend (SharedMemory or mmap) was
    initialized by engine.py. Use this in rule_set_runner.py and other
    modules that need access to the event store.

    Returns:
        The active event store instance

    Raises:
        RuntimeError: If no event store has been initialized
    """
    if _active_store is None:
        raise RuntimeError(
            "Event store not initialized. Call set_event_store() "
            "from engine.py first."
        )
    return _active_store


def reset_event_store():
    """
    Reset the event store (useful for testing and hot reload).

    This clears:
    - The global active store reference
    - MmapEventStore singleton instance
    - SharedMemoryEventStore singleton instance

    Safe to call even if stores haven't been initialized.
    """
    global _active_store
    _active_store = None

    # Reset both singleton implementations
    try:
        from ansible_rulebook.mmap_event_store import MmapEventStore

        MmapEventStore._instance = None
    except ImportError:
        pass

    try:
        from ansible_rulebook.shared_memory_event_store import (
            SharedMemoryEventStore,
        )

        SharedMemoryEventStore._instance = None
    except ImportError:
        pass
