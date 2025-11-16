#  Copyright 2025 Red Hat, Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import asyncio
import logging
from typing import Optional

from psycopg import AsyncConnection, OperationalError

logger = logging.getLogger(__name__)


class LeaderElection:
    """PostgreSQL-based leader election using advisory locks.

    This class implements a distributed leader election mechanism where
    multiple workers compete to become the leader. Only one worker can hold
    the leadership at any given time. Workers poll at regular intervals to
    attempt leadership acquisition.

    The class provides asyncio.Event objects that can be passed to customer
    scripts:
    - leader_event: Set when this worker is leader, cleared when follower
    - follower_event: Set when this worker is follower, cleared when leader
    - shutdown_event: Set when election is stopping (for graceful shutdown)

    Args:
        dsn: PostgreSQL connection string
            (https://www.postgresql.org/docs/current/libpq-connect.html)
        ha_uuid: UUID string for HA coordination and state persistence
            (gets hashed internally to PostgreSQL advisory lock ID)
        poll_interval: Interval in seconds to poll for leadership
            (default: 5.0)
        worker_id: Optional identifier for this worker
            (for logging purposes)

    Example:
        >>> election = LeaderElection(
        ...     dsn="postgresql://user:pass@localhost/dbname",
        ...     ha_uuid="550e8400-e29b-41d4-a716-446655440000",
        ...     poll_interval=5.0,
        ...     worker_id="worker-1"
        ... )
        >>> # Pass events to customer scripts
        >>> async with election:
        ...     await asyncio.gather(
        ...         election.start(),
        ...         customer_script_1(
        ...             election.leader_event, election.follower_event
        ...         ),
        ...         customer_script_2(
        ...             election.leader_event, election.follower_event
        ...         ),
        ...     )
    """

    def __init__(
        self,
        dsn: str,
        ha_uuid: str = "",
        poll_interval: float = 5.0,
        worker_id: Optional[str] = None,
    ):
        self.dsn = dsn
        self.ha_uuid = ha_uuid  # UUID string for HA coordination
        self.poll_interval = poll_interval
        self.worker_id = worker_id or "unknown"

        # Convert UUID to integer hash for PostgreSQL advisory lock
        # PostgreSQL advisory locks use bigint (64-bit signed integer)
        if ha_uuid:
            import hashlib

            hash_value = int(
                hashlib.sha256(ha_uuid.encode()).hexdigest()[:16], 16
            )
            # Convert to signed 64-bit integer
            self._pg_lock_id = (
                hash_value if hash_value < 2**63 else hash_value - 2**64
            )
        else:
            self._pg_lock_id = 0
        self._conn: Optional[AsyncConnection] = None
        self._is_leader = False
        self._running = False

        # Public events that customer scripts can wait on
        self.leader_event = asyncio.Event()  # Set when leader
        self.follower_event = asyncio.Event()  # Set when follower
        self.shutdown_event = asyncio.Event()  # Set when shutting down

        # Start as follower
        self.follower_event.set()

    async def __aenter__(self):
        """Async context manager entry - establishes database connection."""
        try:
            self._conn = await AsyncConnection.connect(
                conninfo=self.dsn,
                autocommit=True,
            )
            logger.info(
                "Worker %s: Connected to PostgreSQL for leader election",
                self.worker_id,
            )
            return self
        except OperationalError as e:
            logger.error(
                "Worker %s: Failed to connect to PostgreSQL: %s",
                self.worker_id,
                str(e),
            )
            raise

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit - releases lock and closes connection."""
        await self.stop()
        if self._conn:
            await self._conn.close()
            logger.info(
                "Worker %s: Disconnected from PostgreSQL",
                self.worker_id,
            )

    async def try_acquire_leadership(self) -> bool:
        """Attempt to acquire leadership using a PostgreSQL advisory lock.

        Returns:
            True if leadership was acquired, False otherwise.

        Raises:
            OperationalError: If database operation fails.
        """
        if not self._conn:
            raise RuntimeError(
                "Not connected to database. Use async context manager."
            )

        try:
            async with self._conn.cursor() as cursor:
                # pg_try_advisory_lock returns True if lock acquired,
                # False otherwise
                await cursor.execute(
                    "SELECT pg_try_advisory_lock(%s)", (self._pg_lock_id,)
                )
                result = await cursor.fetchone()
                acquired = result[0] if result else False

                if acquired and not self._is_leader:
                    self._is_leader = True
                    logger.info(
                        "Worker %s: Successfully acquired leadership "
                        "(ha_uuid=%s)",
                        self.worker_id,
                        self.ha_uuid,
                    )
                elif not acquired and self._is_leader:
                    # This shouldn't normally happen, but handle it
                    self._is_leader = False
                    logger.warning(
                        "Worker %s: Lost leadership unexpectedly",
                        self.worker_id,
                    )

                return acquired
        except OperationalError as e:
            logger.error(
                "Worker %s: Failed to acquire leadership: %s",
                self.worker_id,
                str(e),
            )
            self._is_leader = False
            raise

    async def release_leadership(self) -> bool:
        """Release the leadership lock.

        Returns:
            True if lock was released, False if lock wasn't held.

        Raises:
            OperationalError: If database operation fails.
        """
        if not self._conn:
            raise RuntimeError(
                "Not connected to database. Use async context manager."
            )

        try:
            async with self._conn.cursor() as cursor:
                # pg_advisory_unlock returns True if lock was held
                # and released
                await cursor.execute(
                    "SELECT pg_advisory_unlock(%s)", (self._pg_lock_id,)
                )
                result = await cursor.fetchone()
                released = result[0] if result else False

                if released:
                    logger.info(
                        "Worker %s: Released leadership (ha_uuid=%s)",
                        self.worker_id,
                        self.ha_uuid,
                    )
                    self._is_leader = False

                return released
        except OperationalError as e:
            logger.error(
                "Worker %s: Failed to release leadership: %s",
                self.worker_id,
                str(e),
            )
            raise

    def is_leader(self) -> bool:
        """Check if this worker is currently the leader.

        Returns:
            True if this worker holds the leadership lock.
        """
        return self._is_leader

    async def start(self, on_became_leader=None, on_lost_leadership=None):
        """Start the leader election process.

        This method runs indefinitely, polling for leadership at the
        configured interval. It manages the public events automatically and
        calls the provided callbacks when leadership state changes.

        This method is designed to run as an asyncio task and properly
        handles CancelledError for graceful shutdown.

        Args:
            on_became_leader: Optional async callback function called when
                leadership is acquired (after events are updated).
            on_lost_leadership: Optional async callback function called when
                leadership is lost (after events are updated).

        Example:
            >>> async def on_leader():
            ...     print("I am the leader!")
            >>>
            >>> async def on_follower():
            ...     print("I lost leadership")
            >>>
            >>> # Run as asyncio task
            >>> task = asyncio.create_task(
            ...     election.start(on_became_leader=on_leader,
            ...                    on_lost_leadership=on_follower)
            ... )
            >>> # Later, cancel the task
            >>> task.cancel()
            >>> await task  # Wait for graceful shutdown
        """
        self._running = True
        was_leader = False

        logger.info(
            "Worker %s: Starting leader election (poll_interval=%.1fs)",
            self.worker_id,
            self.poll_interval,
        )

        # Internal callback to manage events and call user callbacks
        async def _internal_became_leader():
            """Internal callback that sets events then calls user callback."""
            # Update events first
            self.leader_event.set()
            self.follower_event.clear()
            logger.debug(
                "Worker %s: State transition - became leader",
                self.worker_id,
            )
            # Then call user callback if provided
            if on_became_leader:
                await on_became_leader()

        async def _internal_lost_leadership():
            """Internal callback that sets events then calls user callback."""
            # Update events first
            self.leader_event.clear()
            self.follower_event.set()
            logger.debug(
                "Worker %s: State transition - lost leadership",
                self.worker_id,
            )
            # Then call user callback if provided
            if on_lost_leadership:
                await on_lost_leadership()

        try:
            while self._running:
                try:
                    is_leader = await self.try_acquire_leadership()

                    # Check for leadership state changes
                    if is_leader and not was_leader:
                        # Became leader
                        await _internal_became_leader()
                        was_leader = True
                    elif not is_leader and was_leader:
                        # Lost leadership
                        await _internal_lost_leadership()
                        was_leader = False

                    # Sleep before next poll
                    await asyncio.sleep(self.poll_interval)

                except OperationalError as e:
                    logger.error(
                        "Worker %s: Database error during polling: %s",
                        self.worker_id,
                        str(e),
                    )
                    # On database error, mark as not leader and retry
                    # after interval
                    if was_leader:
                        await _internal_lost_leadership()
                        was_leader = False
                    self._is_leader = False
                    await asyncio.sleep(self.poll_interval)

        except asyncio.CancelledError:
            logger.info(
                "Worker %s: Leader election task cancelled, cleaning up...",
                self.worker_id,
            )
            # Clean up: release lock and notify if we were leader
            if self._is_leader:
                try:
                    await self.release_leadership()
                    await _internal_lost_leadership()
                except Exception as e:
                    logger.error(
                        "Worker %s: Error during cleanup: %s",
                        self.worker_id,
                        str(e),
                    )
            # Re-raise CancelledError to properly propagate cancellation
            raise
        except Exception as e:
            logger.error(
                "Worker %s: Unexpected error in leader election: %s",
                self.worker_id,
                str(e),
            )
            # Clean up on unexpected errors
            if self._is_leader:
                try:
                    await self.release_leadership()
                    await _internal_lost_leadership()
                except Exception as cleanup_error:
                    logger.error(
                        "Worker %s: Error during error cleanup: %s",
                        self.worker_id,
                        str(cleanup_error),
                    )
            raise
        finally:
            self._running = False
            # Signal shutdown to all waiting tasks
            self.shutdown_event.set()
            logger.info(
                "Worker %s: Leader election stopped",
                self.worker_id,
            )

    async def stop(self):
        """Stop the leader election process and release leadership if held."""
        self._running = False
        if self._is_leader:
            await self.release_leadership()


async def run_leader_election_task(
    dsn: str,
    ha_uuid: str = "",
    poll_interval: float = 5.0,
    worker_id: Optional[str] = None,
    on_became_leader=None,
    on_lost_leadership=None,
) -> None:
    """Run leader election as an asyncio task.

    This is a convenience function that creates a LeaderElection instance,
    manages the database connection, and runs the election loop. It's
    designed to be run as an asyncio task and handles all cleanup properly
    on cancellation.

    Args:
        dsn: PostgreSQL connection string
        ha_uuid: UUID for HA coordination
            (same across all workers in cluster)
        poll_interval: Interval in seconds to poll for leadership
            (default: 5.0)
        worker_id: Optional identifier for this worker
        on_became_leader: Optional async callback when leadership
            is acquired
        on_lost_leadership: Optional async callback when leadership
            is lost

    Example:
        >>> async def main():
        ...     # Create the task
        ...     task = asyncio.create_task(
        ...         run_leader_election_task(
        ...             dsn="postgresql://localhost/mydb",
        ...             ha_uuid="550e8400-e29b-41d4-a716-446655440000",
        ...             poll_interval=5.0,
        ...             worker_id="worker-1"
        ...         )
        ...     )
        ...
        ...     # Do other work...
        ...     await asyncio.sleep(60)
        ...
        ...     # Gracefully shutdown
        ...     task.cancel()
        ...     try:
        ...         await task
        ...     except asyncio.CancelledError:
        ...         print("Leader election task stopped")

    Raises:
        asyncio.CancelledError: When the task is cancelled
            (propagated for cleanup)
        OperationalError: On database connection/operation failures
    """
    election = LeaderElection(
        dsn=dsn,
        ha_uuid=ha_uuid,
        poll_interval=poll_interval,
        worker_id=worker_id,
    )

    async with election:
        await election.start(
            on_became_leader=on_became_leader,
            on_lost_leadership=on_lost_leadership,
        )
