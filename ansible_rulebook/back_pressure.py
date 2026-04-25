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
"""Back pressure management for ansible-rulebook.

This module provides back pressure mechanisms to prevent system overload
when actions or reporting queues reach capacity.
"""

import asyncio
import logging
from typing import Optional

from ansible_rulebook.conf import settings
from ansible_rulebook.exception import (
    TimedOutActionsException,
    TimedOutReportingException,
)

logger = logging.getLogger(__name__)


class BackPressureManager:
    """Manages back pressure for actions and reporting queues.

    This class implements back pressure mechanisms to prevent the system
    from being overwhelmed when:
    - Too many actions are running concurrently
    - The reporting queue is full and cannot drain to EDA Server

    Attributes:
        event_log: AsyncIO Queue for reporting/audit events
    """

    def __init__(self, event_log: Optional[asyncio.Queue] = None):
        """Initialize the BackPressureManager.

        Args:
            event_log: Optional asyncio.Queue for reporting events.
                      If None, reporting back pressure is skipped.
        """
        self.event_log = event_log

    async def apply_reporting_back_pressure(self) -> None:
        """Apply back pressure based on reporting queue capacity.

        Blocks event processing if the reporting queue is full, waiting
        for audit events to drain to the EDA Server before allowing
        new events to be processed.

        Raises:
            TimedOutReportingException: If queue doesn't drain within timeout
        """
        if settings.skip_audit_events or self.event_log is None:
            return

        # Skip if queue has no size limit (unbounded or NullQueue)
        if (
            not hasattr(self.event_log, "maxsize")
            or self.event_log.maxsize == 0
        ):
            return

        timeout = settings.max_back_pressure_timeout
        blocked = False
        once = False

        while timeout > 0:
            if not self.event_log.full():
                if blocked:
                    logger.info(
                        "Back pressure due to reporting released after "
                        f"{settings.max_back_pressure_timeout - timeout} "
                        "seconds. Free slots "
                        f"{self.event_log.maxsize - self.event_log.qsize()} "
                        f"Queue capacity {self.event_log.maxsize}"
                    )
                return
            else:
                if not once:
                    logger.info(
                        f"Waiting on {self.event_log.qsize()} "
                        "reporting objects to flush, queue capacity "
                        f"{self.event_log.maxsize}. back pressure applied"
                    )
                    once = True
                blocked = True
                await asyncio.sleep(1)
                timeout -= 1

        # Final check after timeout
        if self.event_log.full():
            raise TimedOutReportingException(
                f"Reporting objects did not drain in "
                f"{settings.max_back_pressure_timeout} seconds hence aborting"
            )

    async def apply_actions_back_pressure(self) -> None:
        """Apply back pressure based on concurrent action capacity.

        Blocks event processing if max concurrent actions limit is reached,
        waiting for running actions to complete before allowing new events
        to be processed.

        Raises:
            TimedOutActionsException: If actions don't complete within timeout
        """
        if settings.max_actions_semaphore is None:
            return

        timeout = settings.max_back_pressure_timeout
        blocked = False
        once = False

        while timeout > 0:
            if settings.max_actions_semaphore._value > 0:
                if blocked:
                    logger.info(
                        "Back pressure released after "
                        f"{settings.max_back_pressure_timeout - timeout} "
                        "seconds. Free slots "
                        f"{settings.max_actions_semaphore._value}"
                    )
                return
            else:
                if not once:
                    logger.info(
                        f"Waiting on {settings.max_concurrent_actions} "
                        "actions to finish, back pressure applied"
                    )
                    once = True
                blocked = True
                await asyncio.sleep(1)
                timeout -= 1

        # Final check after timeout
        if settings.max_actions_semaphore._value > 0:
            return

        raise TimedOutActionsException(
            f"Actions failed to end in {settings.max_back_pressure_timeout} "
            "seconds hence aborting"
        )

    async def apply_all_back_pressure(self) -> None:
        """Apply both actions and reporting back pressure.

        Checks both back pressure mechanisms sequentially before
        allowing event processing to continue.

        Raises:
            TimedOutActionsException: If actions don't complete within timeout
            TimedOutReportingException: If reporting queue doesn't drain
        """
        await self.apply_actions_back_pressure()
        await self.apply_reporting_back_pressure()
