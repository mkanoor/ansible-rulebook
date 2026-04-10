import asyncio
import ctypes
import gc
import os
import platform
import random

import psutil

from ansible_rulebook.conf import settings

DEFAULT_MAX_MEMORY = 200 * 1024 * 1024


def malloc_trim_if_available():
    """
    Force glibc to return freed memory to OS (Linux only).
    This can significantly reduce RSS after garbage collection.
    """
    if platform.system() == "Linux":
        try:
            libc = ctypes.CDLL("libc.so.6")
            freed = libc.malloc_trim(0)
            return freed
        except Exception as e:
            # Not available (musl, or other libc)
            return 0
    return 0


async def measure_baseline(location: str):
    """
    Measure baseline memory and objects after aggressive GC.
    Can be called from multiple locations to track memory at different points.

    Args:
        location: String identifier for where this measurement is taken from
                 (e.g., "before_run_ruleset", "before_drain_source_queue")

    Returns:
        tuple: (memory_bytes, object_count)
    """
    process = psutil.Process()

    print(f"[BASELINE] Measuring at: {location}")

    # Aggressive GC
    gc.collect(0)
    gc.collect(1)
    gc.collect(2)

    # Force memory return to OS (Linux only)
    malloc_trim_if_available()

    # Give OS time to reclaim
    await asyncio.sleep(0.1)

    # Measure
    memory_bytes = process.memory_info().rss
    object_count = len(gc.get_objects())

    print(
        f"[BASELINE] {location}: "
        f"{memory_bytes/1024**2:.1f}MB, {object_count} objects"
    )

    return memory_bytes, object_count


def get_cgroup_memory_limit():
    try:
        # Path for cgroups v2
        with open("/sys/fs/cgroup/memory.max", "r") as f:
            limit = f.read().strip()
            if limit == "max":
                return None  # No limit set
            return int(limit)
    except FileNotFoundError:
        # Fallback for older cgroups v1
        try:
            with open("/sys/fs/cgroup/memory/memory.limit_in_bytes", "r") as f:
                return int(f.read().strip())
        except FileNotFoundError:
            return None


LIMIT_BYTES = get_cgroup_memory_limit() or DEFAULT_MAX_MEMORY
EVENT_SIZE = 90 * 1024  # 90KB (update based on actual event size)
# Use 75% for events, leaving 25% for Python overhead
MAX_SLOTS = int((LIMIT_BYTES * 0.75) / EVENT_SIZE)

global_action_semaphore = asyncio.Semaphore(MAX_SLOTS)

# Leak detection tracking
_baseline_memory = None
_baseline_objects = None
_idle_samples = []  # Track (memory, objects) when tasks=0


async def event_queue_speed_breaker():
    global _baseline_memory, _baseline_objects, _idle_samples
    process = psutil.Process(os.getpid())

    while True:
        current_usage = process.memory_info().rss
        active_tasks = MAX_SLOTS - global_action_semaphore._value

        # Get GC stats
        gc_count = gc.get_count()  # (gen0, gen1, gen2) object counts
        gc_stats = gc.get_stats()
        total_objects = len(gc.get_objects())

        print(f"Tasks: {active_tasks} Memory: {current_usage}")
        print(
            f"GC generations: gen0={gc_count[0]}, gen1={gc_count[1]}, gen2={gc_count[2]}"
        )
        print(
            f"GC stats - gen0: {gc_stats[0]['collections']} collections, gen2: {gc_stats[2]['collections']} collections"
        )
        print(f"Total tracked objects: {total_objects}")

        # LEAK DETECTION: Track baseline when idle (0 tasks)
        if active_tasks == 0:
            if _baseline_memory is None:
                # Use the measure_baseline function
                _baseline_memory, _baseline_objects = await measure_baseline(
                    "idle_with_0_tasks"
                )
            else:
                # Check if memory/objects growing when idle
                _idle_samples.append((current_usage, total_objects))
                if len(_idle_samples) >= 5:
                    mem_growth = (current_usage - _baseline_memory) / 1024**2
                    obj_growth = total_objects - _baseline_objects
                    print(
                        f"[LEAK CHECK] Growth since baseline: Memory +{mem_growth:.1f}MB, Objects +{obj_growth}"
                    )

                    # Show top object types if significant growth
                    if mem_growth > 50 or obj_growth > 10000:
                        print(f"[WARNING] Possible memory leak detected!")
                        from collections import Counter

                        obj_types = Counter(
                            type(obj).__name__ for obj in gc.get_objects()
                        )
                        print(
                            f"[LEAK CHECK] Top object types: {obj_types.most_common(10)}"
                        )

                    _idle_samples = []  # Reset

        # Apply backpressure when:
        # 1. Semaphore is locked (max slots reached based on EVENT_SIZE), OR
        # 2. Memory exceeds limit (safety valve for incorrect EVENT_SIZE estimation)
        if global_action_semaphore.locked() or (current_usage > LIMIT_BYTES):
            # Trigger your YAML-defined randomized sleep
            wait_time = random.uniform(settings.sleep_min, settings.sleep_max)
            print(
                "Backpressure active. Usage: "
                f"Currently processing {active_tasks} events. "
                f"Memory usage {current_usage/1024**2:.1f}MB. Sleeping..."
            )

            # Aggressive GC before sleeping
            objects_before = len(gc.get_objects())
            collected_0 = gc.collect(0)  # Collect generation 0
            collected_1 = gc.collect(1)  # Collect generation 1
            collected_2 = gc.collect(2)  # Collect generation 2
            objects_after = len(gc.get_objects())

            print(
                f"GC collected: gen0={collected_0}, gen1={collected_1}, gen2={collected_2}"
            )
            print(
                f"Objects: {objects_before} -> {objects_after} (freed {objects_before - objects_after})"
            )

            await asyncio.sleep(wait_time)

            # Force return memory to OS (Linux only)
            mem_before_trim = process.memory_info().rss
            malloc_trim_if_available()

            # Brief pause to allow OS to reclaim memory
            await asyncio.sleep(0.1)

            # Check memory after GC + malloc_trim
            current_usage_after_gc = process.memory_info().rss
            mem_freed_by_trim = (
                mem_before_trim - current_usage_after_gc
            ) / 1024**2
            print(
                f"After GC+trim: Memory: {current_usage_after_gc} "
                f"({current_usage_after_gc/1024**2:.1f}MB), "
                f"GC freed: {(current_usage - mem_before_trim)/1024**2:.1f}MB, "
                f"malloc_trim freed: {mem_freed_by_trim:.1f}MB"
            )

            # Re-check if we can exit backpressure
            if not global_action_semaphore.locked() and (
                current_usage_after_gc <= LIMIT_BYTES
            ):
                print("Resources freed, exiting backpressure")
                break

            continue
        else:
            break
