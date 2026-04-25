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

"""Tests for SharedJobMonitor - centralized batch job monitoring."""

import asyncio
import json

import pytest
from aioresponses import aioresponses

from .data.awx_test_data import JOB_1_RUNNING, JOB_1_SLUG, JOB_1_SUCCESSFUL


@pytest.fixture
def new_job_template_runner():
    from ansible_rulebook.job_template_runner import JobTemplateRunner

    obj = JobTemplateRunner(
        host="https://example.com",
        token="DUMMY",
    )
    obj.refresh_delay = 0.0
    return obj


@pytest.mark.asyncio
async def test_shared_monitor_single_job(new_job_template_runner):
    """Test that shared monitor works for a single job."""
    with aioresponses() as mocked:
        # Job is running initially
        mocked.get(
            f"{new_job_template_runner.host}{JOB_1_SLUG}",
            status=200,
            body=json.dumps(JOB_1_RUNNING),
        )
        # Job completes
        mocked.get(
            f"{new_job_template_runner.host}{JOB_1_SLUG}",
            status=200,
            body=json.dumps(JOB_1_SUCCESSFUL),
        )

        # Use shared monitor (default behavior)
        result = await new_job_template_runner.monitor_job(JOB_1_SLUG)
        assert result["status"] == "successful"


@pytest.mark.asyncio
async def test_shared_monitor_multiple_jobs_concurrently(
    new_job_template_runner,
):
    """Test that shared monitor efficiently handles multiple concurrent jobs."""
    from ansible_rulebook.shared_job_monitor import shared_job_monitor

    job_2_slug = "api/v2/jobs/124/"
    job_3_slug = "api/v2/jobs/125/"

    job_2_running = {**JOB_1_RUNNING, "id": 124, "url": job_2_slug}
    job_2_successful = {**JOB_1_SUCCESSFUL, "id": 124, "url": job_2_slug}
    job_3_running = {**JOB_1_RUNNING, "id": 125, "url": job_3_slug}
    job_3_successful = {**JOB_1_SUCCESSFUL, "id": 125, "url": job_3_slug}

    with aioresponses() as mocked:
        # Job 1: running -> successful
        mocked.get(
            f"{new_job_template_runner.host}{JOB_1_SLUG}",
            status=200,
            body=json.dumps(JOB_1_RUNNING),
        )
        mocked.get(
            f"{new_job_template_runner.host}{JOB_1_SLUG}",
            status=200,
            body=json.dumps(JOB_1_SUCCESSFUL),
        )

        # Job 2: running -> successful
        mocked.get(
            f"{new_job_template_runner.host}{job_2_slug}",
            status=200,
            body=json.dumps(job_2_running),
        )
        mocked.get(
            f"{new_job_template_runner.host}{job_2_slug}",
            status=200,
            body=json.dumps(job_2_successful),
        )

        # Job 3: immediately successful
        mocked.get(
            f"{new_job_template_runner.host}{job_3_slug}",
            status=200,
            body=json.dumps(job_3_successful),
        )

        # Monitor all 3 jobs concurrently
        results = await asyncio.gather(
            new_job_template_runner.monitor_job(JOB_1_SLUG),
            new_job_template_runner.monitor_job(job_2_slug),
            new_job_template_runner.monitor_job(job_3_slug),
        )

        assert all(r["status"] == "successful" for r in results)
        assert results[0]["id"] == 123
        assert results[1]["id"] == 124
        assert results[2]["id"] == 125

        # Verify all jobs were cleaned up from monitor
        assert shared_job_monitor.get_monitored_job_count() == 0


@pytest.mark.asyncio
async def test_shared_monitor_handles_duplicate_registration(
    new_job_template_runner,
):
    """Test that registering the same job twice returns the same future."""
    from ansible_rulebook.shared_job_monitor import shared_job_monitor

    with aioresponses() as mocked:
        mocked.get(
            f"{new_job_template_runner.host}{JOB_1_SLUG}",
            status=200,
            body=json.dumps(JOB_1_SUCCESSFUL),
        )

        # Register the same job twice
        future1 = await shared_job_monitor.register_job(
            JOB_1_SLUG, new_job_template_runner
        )
        future2 = await shared_job_monitor.register_job(
            JOB_1_SLUG, new_job_template_runner
        )

        # Should be the same future
        assert future1 is future2

        # Both should resolve to the same result
        result = await future1
        assert result["status"] == "successful"


@pytest.mark.asyncio
async def test_shared_monitor_batch_polling(new_job_template_runner):
    """Test that batch polling uses correct API path for multiple jobs."""
    from ansible_rulebook.shared_job_monitor import shared_job_monitor

    job_2_slug = "api/v2/jobs/124/"
    job_3_slug = "api/v2/jobs/125/"

    # Expected batch API response
    batch_response = {
        "count": 3,
        "results": [
            {**JOB_1_RUNNING, "id": 123},
            {**JOB_1_RUNNING, "id": 124},
            {**JOB_1_SUCCESSFUL, "id": 125},
        ],
    }

    with aioresponses() as mocked:
        # Mock batch polling endpoint
        # Should use api/v2/jobs/ with id__in parameter
        mocked.get(
            f"{new_job_template_runner.host}api/v2/jobs/",
            status=200,
            body=json.dumps(batch_response),
        )

        # Second poll - remaining jobs
        batch_response_2 = {
            "count": 2,
            "results": [
                {**JOB_1_SUCCESSFUL, "id": 123},
                {**JOB_1_SUCCESSFUL, "id": 124},
            ],
        }
        mocked.get(
            f"{new_job_template_runner.host}api/v2/jobs/",
            status=200,
            body=json.dumps(batch_response_2),
        )

        # Monitor all 3 jobs concurrently
        results = await asyncio.gather(
            new_job_template_runner.monitor_job(JOB_1_SLUG),
            new_job_template_runner.monitor_job(job_2_slug),
            new_job_template_runner.monitor_job(job_3_slug),
        )

        assert all(r["status"] == "successful" for r in results)
        assert results[0]["id"] == 123
        assert results[1]["id"] == 124
        assert results[2]["id"] == 125


@pytest.mark.asyncio
async def test_shared_monitor_batch_polling_gateway_path(
    new_job_template_runner,
):
    """Test that batch polling works with gateway URL format."""
    from ansible_rulebook.shared_job_monitor import shared_job_monitor

    # Simulate gateway setup
    new_job_template_runner.host = "https://example.com/api/controller/"
    job_1_slug = "v2/jobs/123/"
    job_2_slug = "v2/jobs/124/"

    batch_response = {
        "count": 2,
        "results": [
            {**JOB_1_SUCCESSFUL, "id": 123, "url": job_1_slug},
            {**JOB_1_SUCCESSFUL, "id": 124, "url": job_2_slug},
        ],
    }

    with aioresponses() as mocked:
        # Should use v2/jobs/ (not api/v2/jobs/) for gateway
        mocked.get(
            f"{new_job_template_runner.host}v2/jobs/",
            status=200,
            body=json.dumps(batch_response),
        )

        # Monitor both jobs
        results = await asyncio.gather(
            new_job_template_runner.monitor_job(job_1_slug),
            new_job_template_runner.monitor_job(job_2_slug),
        )

        assert all(r["status"] == "successful" for r in results)
        assert results[0]["id"] == 123
        assert results[1]["id"] == 124


@pytest.mark.asyncio
async def test_shared_monitor_stats(new_job_template_runner):
    """Test that SharedJobMonitor.get_stats() returns accurate statistics."""
    from ansible_rulebook.shared_job_monitor import shared_job_monitor

    with aioresponses() as mocked:
        mocked.get(
            f"{new_job_template_runner.host}{JOB_1_SLUG}",
            status=200,
            body=json.dumps(JOB_1_SUCCESSFUL),
        )

        # Get initial stats
        initial_stats = shared_job_monitor.get_stats()

        # Register and monitor a job
        result = await new_job_template_runner.monitor_job(JOB_1_SLUG)
        assert result["status"] == "successful"

        # Check final stats
        final_stats = shared_job_monitor.get_stats()
        assert final_stats["active_jobs"] == 0  # Job completed
        assert (
            final_stats["total_jobs_monitored"]
            >= initial_stats["total_jobs_monitored"]
        )
        assert final_stats["monitor_cycles"] >= initial_stats["monitor_cycles"]


@pytest.mark.asyncio
async def test_shared_monitor_get_monitored_job_count(new_job_template_runner):
    """Test that get_monitored_job_count returns correct count."""
    from ansible_rulebook.shared_job_monitor import shared_job_monitor

    job_2_slug = "api/v2/jobs/124/"

    with aioresponses() as mocked:
        # Jobs that stay running for this test
        mocked.get(
            f"{new_job_template_runner.host}{JOB_1_SLUG}",
            status=200,
            body=json.dumps(JOB_1_RUNNING),
            repeat=True,
        )
        mocked.get(
            f"{new_job_template_runner.host}{job_2_slug}",
            status=200,
            body=json.dumps({**JOB_1_RUNNING, "id": 124}),
            repeat=True,
        )

        # Start monitoring (don't await - let them run in background)
        task1 = asyncio.create_task(
            new_job_template_runner.monitor_job(JOB_1_SLUG)
        )
        task2 = asyncio.create_task(
            new_job_template_runner.monitor_job(job_2_slug)
        )

        # Give monitor time to register jobs
        await asyncio.sleep(0.1)

        # Should have 2 active jobs
        count = shared_job_monitor.get_monitored_job_count()
        assert count == 2

        # Cancel tasks to cleanup
        task1.cancel()
        task2.cancel()
        try:
            await task1
        except asyncio.CancelledError:
            pass
        try:
            await task2
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
async def test_shared_monitor_unregister_job(new_job_template_runner):
    """Test manual unregistration of a job."""
    from ansible_rulebook.shared_job_monitor import shared_job_monitor

    with aioresponses() as mocked:
        mocked.get(
            f"{new_job_template_runner.host}{JOB_1_SLUG}",
            status=200,
            body=json.dumps(JOB_1_RUNNING),
            repeat=True,
        )

        # Register job
        future = await shared_job_monitor.register_job(
            JOB_1_SLUG, new_job_template_runner
        )
        assert shared_job_monitor.get_monitored_job_count() == 1

        # Unregister job
        await shared_job_monitor.unregister_job(JOB_1_SLUG)
        assert shared_job_monitor.get_monitored_job_count() == 0


@pytest.mark.asyncio
async def test_shared_monitor_is_healthy(new_job_template_runner):
    """Test health check in various states."""
    from ansible_rulebook.shared_job_monitor import shared_job_monitor

    # Initially healthy (no jobs, no monitor)
    assert shared_job_monitor.is_healthy()

    with aioresponses() as mocked:
        mocked.get(
            f"{new_job_template_runner.host}{JOB_1_SLUG}",
            status=200,
            body=json.dumps(JOB_1_SUCCESSFUL),
        )

        # Monitor a job
        result = await new_job_template_runner.monitor_job(JOB_1_SLUG)
        assert result["status"] == "successful"

        # Still healthy after job completes
        assert shared_job_monitor.is_healthy()


@pytest.mark.asyncio
async def test_shared_monitor_cleanup_cancelled_jobs(new_job_template_runner):
    """Test that cancelled futures are cleaned up properly."""
    from ansible_rulebook.shared_job_monitor import shared_job_monitor

    with aioresponses() as mocked:
        # Job stays running
        mocked.get(
            f"{new_job_template_runner.host}{JOB_1_SLUG}",
            status=200,
            body=json.dumps(JOB_1_RUNNING),
            repeat=True,
        )

        # Start monitoring but cancel immediately
        task = asyncio.create_task(
            new_job_template_runner.monitor_job(JOB_1_SLUG)
        )
        await asyncio.sleep(0.1)  # Let it register

        initial_count = shared_job_monitor.get_monitored_job_count()
        assert initial_count >= 1

        # Cancel the task
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        # Give cleanup time to run
        await asyncio.sleep(0.2)

        # Cancelled job should be cleaned up
        final_count = shared_job_monitor.get_monitored_job_count()
        assert final_count < initial_count


@pytest.mark.asyncio
async def test_shared_monitor_batch_polling_mixed_job_types(
    new_job_template_runner,
):
    """Test batch polling with both regular jobs and workflow jobs."""
    regular_job_slug = "api/v2/jobs/123/"
    workflow_job_slug = "api/v2/workflow_jobs/456/"

    workflow_job_running = {
        "id": 456,
        "status": "running",
        "url": workflow_job_slug,
        "artifacts": {},
    }
    workflow_job_successful = {
        "id": 456,
        "status": "successful",
        "url": workflow_job_slug,
        "artifacts": {"result": "ok"},
    }

    with aioresponses() as mocked:
        # Batch response for regular jobs
        mocked.get(
            f"{new_job_template_runner.host}api/v2/jobs/",
            status=200,
            body=json.dumps(
                {"count": 1, "results": [{**JOB_1_SUCCESSFUL, "id": 123}]}
            ),
        )

        # Batch response for workflow jobs
        mocked.get(
            f"{new_job_template_runner.host}api/v2/workflow_jobs/",
            status=200,
            body=json.dumps(
                {"count": 1, "results": [workflow_job_successful]}
            ),
        )

        # Monitor both types concurrently
        results = await asyncio.gather(
            new_job_template_runner.monitor_job(regular_job_slug),
            new_job_template_runner.monitor_job(workflow_job_slug),
        )

        assert results[0]["id"] == 123
        assert results[0]["status"] == "successful"
        assert results[1]["id"] == 456
        assert results[1]["status"] == "successful"


@pytest.mark.asyncio
async def test_shared_monitor_batch_polling_fallback_on_error(
    new_job_template_runner,
):
    """Test that batch polling falls back to individual polling on error."""
    job_2_slug = "api/v2/jobs/124/"

    with aioresponses() as mocked:
        # First batch poll fails
        mocked.get(
            f"{new_job_template_runner.host}api/v2/jobs/",
            status=500,
            body="Internal Server Error",
        )

        # Fallback to individual polling - job 1
        mocked.get(
            f"{new_job_template_runner.host}{JOB_1_SLUG}",
            status=200,
            body=json.dumps(JOB_1_SUCCESSFUL),
        )

        # Fallback to individual polling - job 2
        mocked.get(
            f"{new_job_template_runner.host}{job_2_slug}",
            status=200,
            body=json.dumps({**JOB_1_SUCCESSFUL, "id": 124}),
        )

        # Should succeed via fallback
        results = await asyncio.gather(
            new_job_template_runner.monitor_job(JOB_1_SLUG),
            new_job_template_runner.monitor_job(job_2_slug),
        )

        assert results[0]["status"] == "successful"
        assert results[1]["status"] == "successful"


@pytest.mark.asyncio
async def test_shared_monitor_empty_batch_results(new_job_template_runner):
    """Test handling of empty batch results (jobs not in response)."""
    with aioresponses() as mocked:
        # First poll: empty results
        mocked.get(
            f"{new_job_template_runner.host}api/v2/jobs/",
            status=200,
            body=json.dumps({"count": 0, "results": []}),
        )

        # Second poll: job appears and completes
        mocked.get(
            f"{new_job_template_runner.host}api/v2/jobs/",
            status=200,
            body=json.dumps(
                {"count": 1, "results": [{**JOB_1_SUCCESSFUL, "id": 123}]}
            ),
        )

        # Should eventually complete
        result = await new_job_template_runner.monitor_job(JOB_1_SLUG)
        assert result["status"] == "successful"


@pytest.mark.asyncio
async def test_monitor_job_with_failed_status_logged(new_job_template_runner):
    """Test that failed jobs are properly logged at ERROR level."""
    job_failed = {
        "id": 123,
        "status": "failed",
        "url": JOB_1_SLUG,
        "artifacts": {},
        "created": "2024-01-01T00:00:00Z",
    }

    with aioresponses() as mocked:
        mocked.get(
            f"{new_job_template_runner.host}{JOB_1_SLUG}",
            status=200,
            body=json.dumps(job_failed),
        )

        result = await new_job_template_runner.monitor_job(JOB_1_SLUG)
        assert result["status"] == "failed"


@pytest.mark.asyncio
async def test_monitor_job_with_canceled_status_logged(
    new_job_template_runner,
):
    """Test that canceled jobs are properly logged at WARNING level."""
    job_canceled = {
        "id": 123,
        "status": "canceled",
        "url": JOB_1_SLUG,
        "artifacts": {},
        "created": "2024-01-01T00:00:00Z",
    }

    with aioresponses() as mocked:
        mocked.get(
            f"{new_job_template_runner.host}{JOB_1_SLUG}",
            status=200,
            body=json.dumps(job_canceled),
        )

        result = await new_job_template_runner.monitor_job(JOB_1_SLUG)
        assert result["status"] == "canceled"
