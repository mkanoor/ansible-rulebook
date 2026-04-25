#  Copyright 2022 Red Hat, Inc.
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
from typing import TYPE_CHECKING, Any, Dict

if TYPE_CHECKING:
    from ansible_rulebook.job_template_runner import JobTemplateRunner

logger = logging.getLogger(__name__)


class SharedJobMonitor:
    """Shared monitor that polls multiple jobs in batches.

    This class implements a singleton pattern to ensure only one polling
    loop runs for all jobs, significantly reducing API calls to the controller.

    Instead of N tasks each polling their job individually (N API calls per
    interval), one shared monitor polls all N jobs together, making individual
    requests for each but in a coordinated batch (still N calls but managed
    centrally and easier to optimize further).

    Benefits:
    - Centralized polling logic
    - Easier to add true batch API support later
    - Better error handling across all jobs
    - Reduced overhead from multiple asyncio tasks
    """

    _instance = None
    _lock = asyncio.Lock()

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self._initialized = True
        self._jobs: Dict[str, Dict[str, Any]] = {}
        self._monitor_task = None
        self._jobs_lock = asyncio.Lock()
        self._total_jobs_monitored = 0
        self._monitor_start_count = 0

    async def register_job(
        self, job_url: str, runner: "JobTemplateRunner"
    ) -> asyncio.Future:
        """Register a job for monitoring and return a future that resolves when complete.

        Args:
            job_url: The job URL to monitor
            runner: The JobTemplateRunner instance to use for API calls

        Returns:
            asyncio.Future that resolves to the final job status dict
        """
        # Extract job ID from URL
        job_id = job_url.rstrip("/").split("/")[-1]

        async with self._jobs_lock:
            if job_id in self._jobs:
                # Job already registered, return existing future
                logger.debug(
                    "Job %s already registered, reusing future", job_id
                )
                return self._jobs[job_id]["future"]

            future = asyncio.Future()
            self._jobs[job_id] = {
                "url": job_url,
                "future": future,
                "runner": runner,
                "registered_at": asyncio.get_event_loop().time(),
            }
            self._total_jobs_monitored += 1

            # Start monitor task if not running
            monitor_was_stopped = (
                self._monitor_task is None or self._monitor_task.done()
            )
            if monitor_was_stopped:
                self._monitor_start_count += 1
                self._monitor_task = asyncio.create_task(self._monitor_loop())
                logger.info(
                    "Started shared job monitor loop (cycle #%d) - "
                    "monitoring %d job(s)",
                    self._monitor_start_count,
                    len(self._jobs),
                )
            else:
                logger.debug(
                    "Registered job %s for batch monitoring (%d active jobs)",
                    job_id,
                    len(self._jobs),
                )

        return future

    async def _monitor_loop(self):
        """Main monitoring loop that polls all registered jobs."""
        logger.info("Starting shared job monitor loop")

        while True:
            try:
                # Clean up cancelled/errored futures before polling
                await self._cleanup_stale_jobs()

                async with self._jobs_lock:
                    if not self._jobs:
                        # No more jobs to monitor, exit loop
                        logger.info(
                            "No more jobs to monitor, stopping monitor loop. "
                            "Total monitored this cycle: %d",
                            self._total_jobs_monitored,
                        )
                        self._monitor_task = None
                        break

                    jobs_to_check = list(self._jobs.items())

                # Group jobs by type (regular jobs vs workflow jobs) for batch querying
                regular_jobs = {}
                workflow_jobs = {}

                for job_id, job_info in jobs_to_check:
                    if job_info["future"].done():
                        continue

                    # Determine job type from URL
                    if "workflow_jobs" in job_info["url"]:
                        workflow_jobs[job_id] = job_info
                    else:
                        regular_jobs[job_id] = job_info

                # Determine API path prefix (gateway vs legacy)
                # Use the runner's _api_slug_prefix() to get the correct path
                runner = next(iter(jobs_to_check), (None, {"runner": None}))[
                    1
                ].get("runner")
                if runner:
                    api_prefix = runner._api_slug_prefix()

                    # Batch poll regular jobs
                    if regular_jobs:
                        await self._batch_poll_jobs(
                            regular_jobs, f"{api_prefix}jobs/", "jobs"
                        )

                    # Batch poll workflow jobs
                    if workflow_jobs:
                        await self._batch_poll_jobs(
                            workflow_jobs,
                            f"{api_prefix}workflow_jobs/",
                            "workflow_jobs",
                        )

                # Wait before next poll cycle
                # Use the refresh_delay from one of the runners
                if jobs_to_check:
                    delay = jobs_to_check[0][1]["runner"].refresh_delay
                    await asyncio.sleep(delay)

            except Exception as e:
                logger.error(
                    "Error in shared job monitor loop: %s",
                    str(e),
                    exc_info=True,
                )
                await asyncio.sleep(5)  # Back off on error

    async def _batch_poll_jobs(
        self, jobs: Dict[str, Dict[str, Any]], api_path: str, job_type: str
    ):
        """Poll multiple jobs in a single batch API call.

        Args:
            jobs: Dictionary of job_id -> job_info to poll
            api_path: API endpoint path (e.g., "api/v2/jobs/")
            job_type: Type name for logging ("jobs" or "workflow_jobs")
        """
        if not jobs:
            return

        # Extract job IDs and get runner from first job
        job_ids = list(jobs.keys())
        runner = next(iter(jobs.values()))["runner"]

        try:
            # Use batch API with id__in filter
            id_filter = ",".join(job_ids)
            params = {"id__in": id_filter}

            logger.debug(
                "Batch polling %d %s with single API call",
                len(job_ids),
                job_type,
            )

            result = await runner._get_page_no_retry(api_path, params)

            # Process results and match back to jobs
            processed_count = 0
            for job_data in result.get("results", []):
                job_id = str(job_data["id"])
                if job_id in jobs:
                    job_info = jobs[job_id]

                    if job_data["status"] in runner.JOB_COMPLETION_STATUSES:
                        # Job completed
                        async with self._jobs_lock:
                            if (
                                job_id in self._jobs
                                and not job_info["future"].done()
                            ):
                                job_info["future"].set_result(job_data)
                                del self._jobs[job_id]

                                # Log at appropriate level based on status
                                if job_data["status"] == "successful":
                                    logger.debug(
                                        "Job %s completed successfully "
                                        "(%d jobs remaining)",
                                        job_id,
                                        len(self._jobs),
                                    )
                                elif job_data["status"] in ["failed", "error"]:
                                    logger.error(
                                        "Job %s completed with status: %s "
                                        "(%d jobs remaining)",
                                        job_id,
                                        job_data["status"],
                                        len(self._jobs),
                                    )
                                elif job_data["status"] == "canceled":
                                    logger.warning(
                                        "Job %s was canceled "
                                        "(%d jobs remaining)",
                                        job_id,
                                        len(self._jobs),
                                    )

                                processed_count += 1
                    else:
                        logger.debug(
                            "Job %s still running (status: %s)",
                            job_id,
                            job_data.get("status"),
                        )
                        processed_count += 1

            logger.debug(
                "Batch poll processed %d/%d %s",
                processed_count,
                len(job_ids),
                job_type,
            )

        except Exception as e:
            # Batch polling failed, fall back to individual polling
            logger.warning(
                "Batch polling failed for %s, falling back to individual polls: %s",
                job_type,
                str(e),
            )
            await self._individual_poll_jobs(jobs)

    async def _individual_poll_jobs(self, jobs: Dict[str, Dict[str, Any]]):
        """Fallback: Poll jobs individually if batch polling fails.

        Args:
            jobs: Dictionary of job_id -> job_info to poll
        """
        for job_id, job_info in jobs.items():
            if job_info["future"].done():
                continue

            try:
                runner = job_info["runner"]
                result = await runner._get_page_no_retry(job_info["url"], {})

                if result["status"] in runner.JOB_COMPLETION_STATUSES:
                    # Job completed
                    async with self._jobs_lock:
                        if (
                            job_id in self._jobs
                            and not job_info["future"].done()
                        ):
                            job_info["future"].set_result(result)
                            del self._jobs[job_id]

                            # Log at appropriate level based on status
                            if result["status"] == "successful":
                                logger.debug(
                                    "Job %s completed successfully "
                                    "(%d jobs remaining)",
                                    job_id,
                                    len(self._jobs),
                                )
                            elif result["status"] in ["failed", "error"]:
                                logger.error(
                                    "Job %s completed with status: %s "
                                    "(%d jobs remaining)",
                                    job_id,
                                    result["status"],
                                    len(self._jobs),
                                )
                            elif result["status"] == "canceled":
                                logger.warning(
                                    "Job %s was canceled "
                                    "(%d jobs remaining)",
                                    job_id,
                                    len(self._jobs),
                                )
                else:
                    logger.debug(
                        "Job %s still running (status: %s)",
                        job_id,
                        result.get("status"),
                    )
            except Exception as e:
                # Transient error during polling, will retry next cycle
                logger.debug(
                    "Transient error polling job %s (will retry): %s",
                    job_id,
                    str(e),
                )

    async def _cleanup_stale_jobs(self):
        """Remove jobs with cancelled or errored futures."""
        async with self._jobs_lock:
            stale_jobs = [
                job_id
                for job_id, job_info in self._jobs.items()
                if job_info["future"].done()
                and (
                    job_info["future"].cancelled()
                    or job_info["future"].exception() is not None
                )
            ]

            for job_id in stale_jobs:
                job_info = self._jobs[job_id]
                if job_info["future"].cancelled():
                    logger.debug(
                        "Removing cancelled job %s from monitor", job_id
                    )
                else:
                    logger.warning(
                        "Removing job %s with error from monitor: %s",
                        job_id,
                        job_info["future"].exception(),
                    )
                del self._jobs[job_id]

    async def unregister_job(self, job_url: str):
        """Manually unregister a job (e.g., if cancelled).

        Args:
            job_url: The job URL to unregister
        """
        job_id = job_url.rstrip("/").split("/")[-1]
        async with self._jobs_lock:
            if job_id in self._jobs:
                del self._jobs[job_id]
                logger.debug(
                    "Unregistered job %s (%d jobs remaining)",
                    job_id,
                    len(self._jobs),
                )

    def get_monitored_job_count(self) -> int:
        """Return the number of jobs currently being monitored.

        Returns:
            int: Number of active jobs
        """
        return len(self._jobs)

    def get_stats(self) -> Dict[str, Any]:
        """Get statistics about the monitor.

        Returns:
            dict: Statistics including active jobs, total monitored, and cycle count
        """
        return {
            "active_jobs": len(self._jobs),
            "total_jobs_monitored": self._total_jobs_monitored,
            "monitor_cycles": self._monitor_start_count,
            "monitor_running": self._monitor_task is not None
            and not self._monitor_task.done(),
        }

    def is_healthy(self) -> bool:
        """Check if the monitor is in a healthy state.

        Returns:
            bool: True if healthy, False otherwise
        """
        # Monitor should be running if there are jobs, stopped if no jobs
        has_jobs = len(self._jobs) > 0
        monitor_running = (
            self._monitor_task is not None and not self._monitor_task.done()
        )

        if has_jobs and not monitor_running:
            logger.error(
                "Monitor unhealthy: %d jobs registered but monitor not running",
                len(self._jobs),
            )
            return False

        return True


# Singleton instance
shared_job_monitor = SharedJobMonitor()
