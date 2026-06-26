#  Copyright 2026 Red Hat, Inc.
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
"""
PostgreSQL-based leader election for ansible-rulebook.

This module provides leader election functionality using PostgreSQL
advisory locks. Multiple rulebook instances can coordinate to elect a
single leader that is responsible for executing critical tasks like
starting event sources.

PostgreSQL advisory locks are session-level locks that are
automatically released when the database connection is closed, making
them ideal for leader election.

Example:
    Basic usage with DSN:
        election = LeaderElection(
            dsn="postgresql://user:pass@localhost/db",
            lock_id=12345,
            poll_interval=5.0,
            worker_id="worker-1"
        )

        async with election:
            await election.start()
            if election.is_leader():
                # Start sources
                pass

    Using with callbacks:
        async def on_became_leader():
            print("I am now the leader!")

        async def on_lost_leadership():
            print("I lost leadership!")

        async with election:
            await election.start(on_became_leader, on_lost_leadership)

    Waiting for leadership:
        async with election:
            await election.start()
            await election.wait_for_leadership()
            # This code runs only when we become leader
            print("Starting critical tasks...")
"""

import asyncio
import logging
from typing import Any, Callable, Optional

from psycopg import AsyncConnection, OperationalError

from ansible_rulebook.conf import settings

logger = logging.getLogger(__name__)


class LeaderElectionError(Exception):
    """Base exception for leader election errors."""


class LeaderElection:
    """
    PostgreSQL advisory lock-based leader election.

    This class manages leader election among multiple workers using PostgreSQL
    advisory locks. It continuously attempts to acquire an advisory lock, and
    the worker that holds the lock is considered the leader.

    Attributes:
        dsn: PostgreSQL connection string (DSN format)
        lock_id: Integer identifier for the advisory lock (must be same
            across all workers)
        poll_interval: Seconds between leadership checks (default: 5.0)
        worker_id: Unique identifier for this worker instance
    """

    def __init__(
        self,
        ha_uuid: str,
        dsn: Optional[str] = None,
        postgres_params: Optional[dict[str, Any]] = None,
        poll_interval: float = 5.0,
        worker_id: str = "worker-unknown",
    ):
        """
        Initialize the leader election instance.

        Args:
            ha_uuid: HA activation UUID. This identifies the HA group
                and is used to generate the advisory lock ID. All
                instances with the same ha_uuid compete for leadership.
                REQUIRED.
            dsn: PostgreSQL connection string. Either dsn or
                postgres_params is required.
            postgres_params: PostgreSQL connection parameters dict.
                Either dsn or postgres_params is required.
            poll_interval: Seconds between attempts to acquire/check
                the lock.
            worker_id: Unique identifier for this worker instance
                (for logging).

        Raises:
            ValueError: If ha_uuid is not provided or if neither dsn
                nor postgres_params is provided.
        """
        if not ha_uuid:
            raise ValueError("ha_uuid is required for leader election")

        if dsn is None and postgres_params is None:
            raise ValueError("Either dsn or postgres_params must be provided")

        self.ha_uuid = ha_uuid
        self.lock_id = self._uuid_to_lock_id(ha_uuid)
        self.dsn = dsn or ""
        self.postgres_params = postgres_params or {}
        self.poll_interval = poll_interval
        self.worker_id = worker_id

        logger.info(
            f"Leader election initialized: ha_uuid='{ha_uuid}', "
            f"lock_id={self.lock_id}, worker_id={worker_id}"
        )

        self._conn: Optional[AsyncConnection] = None
        self._is_leader = False
        self._running = False
        self._leader_event = asyncio.Event()
        self._follower_event = asyncio.Event()
        self._follower_event.set()  # Initially not leader

        # Callbacks for state changes
        self._on_became_leader: Optional[Callable[[], Any]] = None
        self._on_lost_leadership: Optional[Callable[[], Any]] = None

    async def __aenter__(self):
        """Async context manager entry."""
        await self._connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()
        return False

    async def _connect(self):
        """Establish connection to PostgreSQL."""
        try:
            self._conn = await AsyncConnection.connect(
                conninfo=self.dsn,
                autocommit=True,
                **self.postgres_params,
            )
            logger.info(
                "[%s] Connected to PostgreSQL for leader election",
                self.worker_id,
            )
        except OperationalError as e:
            logger.error(
                "[%s] Failed to connect to PostgreSQL: %s",
                self.worker_id,
                e,
            )
            raise LeaderElectionError(
                f"Failed to connect to PostgreSQL: {e}"
            ) from e

    async def close(self):
        """Close the database connection and release leadership."""
        self._running = False
        if self._conn and not self._conn.closed:
            # Advisory locks are automatically released when connection closes
            await self._conn.close()
            logger.info(
                "[%s] Closed PostgreSQL connection, released leadership",
                self.worker_id,
            )
        self._is_leader = False
        self._leader_event.clear()
        self._follower_event.set()

    async def start(
        self,
        on_became_leader: Optional[Callable[[], Any]] = None,
        on_lost_leadership: Optional[Callable[[], Any]] = None,
    ):
        """
        Start the leader election loop.

        This method continuously attempts to acquire the advisory lock.
        When the lock is acquired, this instance becomes the leader.
        When the lock is lost (e.g., due to network issues or another
        instance acquiring it), this instance becomes a follower.

        Args:
            on_became_leader: Optional async callback invoked when
                becoming leader
            on_lost_leadership: Optional async callback invoked when
                losing leadership

        Note:
            This method runs indefinitely and should be run as a
            background task.
        """
        self._on_became_leader = on_became_leader
        self._on_lost_leadership = on_lost_leadership
        self._running = True

        while self._running:
            try:
                was_leader = self._is_leader
                is_leader_now = await self._try_acquire_lock()

                if is_leader_now != was_leader:
                    # State changed
                    self._is_leader = is_leader_now

                    if is_leader_now:
                        # Became leader
                        logger.info("[%s] Became LEADER", self.worker_id)
                        settings.leader_enabled = True
                        self._leader_event.set()
                        self._follower_event.clear()
                        if self._on_became_leader:
                            if asyncio.iscoroutinefunction(
                                self._on_became_leader
                            ):
                                await self._on_became_leader()
                            else:
                                self._on_became_leader()
                    else:
                        # Lost leadership
                        logger.info("[%s] Became FOLLOWER", self.worker_id)
                        settings.leader_enabled = False
                        self._leader_event.clear()
                        self._follower_event.set()
                        if self._on_lost_leadership:
                            if asyncio.iscoroutinefunction(
                                self._on_lost_leadership
                            ):
                                await self._on_lost_leadership()
                            else:
                                self._on_lost_leadership()

                await asyncio.sleep(self.poll_interval)

            except asyncio.CancelledError:
                logger.info("[%s] Leader election cancelled", self.worker_id)
                raise
            except Exception as e:
                logger.error(
                    "[%s] Error in leader election loop: %s",
                    self.worker_id,
                    e,
                )
                # On error, assume we're not leader
                if self._is_leader:
                    self._is_leader = False
                    self._leader_event.clear()
                    self._follower_event.set()
                    if self._on_lost_leadership:
                        if asyncio.iscoroutinefunction(
                            self._on_lost_leadership
                        ):
                            await self._on_lost_leadership()
                        else:
                            self._on_lost_leadership()
                await asyncio.sleep(self.poll_interval)

    async def _try_acquire_lock(self) -> bool:
        """
        Try to acquire the PostgreSQL advisory lock.

        Returns:
            True if lock was acquired (we are leader), False otherwise.
        """
        if not self._conn or self._conn.closed:
            logger.warning(
                "[%s] Connection lost, attempting to reconnect...",
                self.worker_id,
            )
            try:
                await self._connect()
            except LeaderElectionError:
                return False

        try:
            cursor = self._conn.cursor()
            # pg_try_advisory_lock returns True if lock acquired,
            # False otherwise. This is a non-blocking call that
            # returns immediately
            result = await cursor.execute(
                "SELECT pg_try_advisory_lock(%s)", (self.lock_id,)
            )
            row = await result.fetchone()

            if row and row[0]:
                # Successfully acquired the lock
                return True
            else:
                # Lock is held by another instance
                return False

        except OperationalError as e:
            logger.warning(
                "[%s] Database error checking lock: %s",
                self.worker_id,
                e,
            )
            return False

    def is_leader(self) -> bool:
        """
        Check if this instance is currently the leader.

        Returns:
            True if this instance holds the advisory lock (is leader).
        """
        return self._is_leader

    async def wait_for_leadership(self):
        """
        Block until this instance becomes the leader.

        This is useful for tasks that should only run when the
        instance is the leader.

        Example:
            await election.wait_for_leadership()
            # Now we are leader, start critical tasks
            while election.is_leader():
                await do_leader_work()
        """
        await self._leader_event.wait()

    async def wait_for_follower_state(self):
        """
        Block until this instance becomes a follower (loses leadership).

        Example:
            await election.wait_for_follower_state()
            # Now we are follower
        """
        await self._follower_event.wait()

    def get_leader_event(self) -> asyncio.Event:
        """
        Get the asyncio.Event that is set when this instance is the leader.

        Returns:
            Event that is set when leader, cleared when follower.
        """
        return self._leader_event

    def get_follower_event(self) -> asyncio.Event:
        """
        Get the asyncio.Event that is set when this instance is a follower.

        Returns:
            Event that is set when follower, cleared when leader.
        """
        return self._follower_event

    @staticmethod
    def _uuid_to_lock_id(ha_uuid: str) -> int:
        """
        Convert HA UUID string to PostgreSQL advisory lock ID.

        Uses SHA256 hash to convert string UUID to a consistent
        64-bit signed integer suitable for PostgreSQL advisory locks.

        Args:
            ha_uuid: The HA activation UUID string

        Returns:
            Integer lock ID for PostgreSQL pg_try_advisory_lock()
        """
        import hashlib

        # Use SHA256 hash and take first 8 bytes as signed 64-bit int
        hash_bytes = hashlib.sha256(ha_uuid.encode()).digest()[:8]
        lock_id = int.from_bytes(hash_bytes, byteorder="big", signed=True)

        return lock_id


def create_leader_election_from_ha_uuid(
    ha_uuid: str,
    worker_id: str,
    variables: dict[str, Any],
    poll_interval: float = 5.0,
) -> Optional[LeaderElection]:
    """
    Create a LeaderElection instance from HA UUID.

    This is the primary way to create leader election for HA mode.
    The presence of ha_uuid indicates that leader election should be enabled.

    Args:
        ha_uuid: HA activation UUID (shared by all instances in the HA group)
        worker_id: Unique identifier for this worker instance
        variables: Dictionary containing database connection variables
        poll_interval: Seconds between leadership checks

    Returns:
        LeaderElection instance if valid PostgreSQL config found,
        None otherwise.

    Example:
        election = create_leader_election_from_ha_uuid(
            ha_uuid="activation-abc-123",
            worker_id="1",
            variables={
                "drools_db_host": "localhost",
                "drools_db_port": 5432,
                "drools_db_name": "eda",
                "drools_db_user": "postgres",
                "drools_db_password": "secret"
            }
        )
    """
    # Check for PostgreSQL configuration
    from ansible_rulebook.persistence import _get_postgres_params

    db_params = _get_postgres_params(variables)
    if db_params is None:
        logger.error(
            "HA mode requires PostgreSQL configuration. "
            "Leader election cannot be enabled without drools_db_* variables."
        )
        return None

    # Build DSN from db_params - use ALL params for consistency
    # with persistence. This ensures SSL settings and other
    # connection params are shared
    dsn_parts = [
        f"host={db_params['host']}",
        f"port={db_params['port']}",
        f"dbname={db_params['database']}",
    ]

    # Add all optional parameters from db_params (user, password,
    # SSL, etc.). This ensures leader election uses the SAME
    # connection config as persistence
    optional_params = [
        "user",
        "password",
        "sslmode",
        "sslpassword",
        "sslrootcert",
        "sslkey",
        "sslcert",
    ]
    for param in optional_params:
        if param in db_params:
            dsn_parts.append(f"{param}={db_params[param]}")

    dsn = " ".join(dsn_parts)

    # Format worker_id same as persistence module
    formatted_worker_id = f"instance-{worker_id or 'default'}"

    logger.info(
        f"Creating leader election for HA group: ha_uuid='{ha_uuid}', "
        f"worker_id={formatted_worker_id}, poll_interval={poll_interval}s"
    )

    return LeaderElection(
        ha_uuid=ha_uuid,
        dsn=dsn,
        poll_interval=poll_interval,
        worker_id=formatted_worker_id,
    )
