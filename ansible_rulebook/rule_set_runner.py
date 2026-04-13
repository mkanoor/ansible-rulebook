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
import gc
import logging
import uuid
from collections import defaultdict
from pprint import pformat
from types import MappingProxyType
from typing import Dict, List, Optional, Union, cast

import dpath
import jinja2.exceptions as jinja2_exceptions
from drools import ruleset as lang
from drools.exceptions import (
    MessageNotHandledException,
    MessageObservedException,
)
from drools.ruleset import session_stats

from ansible_rulebook import terminal
from ansible_rulebook.action.control import Control
from ansible_rulebook.action.debug import Debug
from ansible_rulebook.action.helper import (
    FAILED_STATUS,
    STARTED_STATUS,
    SUCCESSFUL_STATUS,
)
from ansible_rulebook.action.metadata import ActionPersistence, Metadata
from ansible_rulebook.action.noop import Noop
from ansible_rulebook.action.pg_notify import PGNotify
from ansible_rulebook.action.post_event import PostEvent
from ansible_rulebook.action.print_event import PrintEvent
from ansible_rulebook.action.retract_fact import RetractFact
from ansible_rulebook.action.run_job_template import RunJobTemplate
from ansible_rulebook.action.run_module import RunModule
from ansible_rulebook.action.run_playbook import RunPlaybook
from ansible_rulebook.action.run_workflow_template import RunWorkflowTemplate
from ansible_rulebook.action.set_fact import SetFact
from ansible_rulebook.action.shutdown import Shutdown as ShutdownAction
from ansible_rulebook.action.sleep import Sleep
from ansible_rulebook.conf import settings
from ansible_rulebook.event_store_manager import get_event_store
from ansible_rulebook.exception import (
    ShutdownException,
    UnsupportedActionException,
)
from ansible_rulebook.messages import Shutdown
from ansible_rulebook.persistence import (
    get_action_a_priori,
    update_action_info,
)
from ansible_rulebook.rule_types import (
    Action,
    ActionContext,
    EngineRuleSetQueuePlan,
    ExecutionStrategy,
)
from ansible_rulebook.rules_parser import parse_hosts
from ansible_rulebook.util import (
    mask_sensitive_variable_values,
    run_at,
    send_session_stats,
    substitute_variables,
)

COMPLETED_STATUSES = [FAILED_STATUS, SUCCESSFUL_STATUS]

logger = logging.getLogger(__name__)

ACTION_CLASSES = {
    "debug": Debug,
    "print_event": PrintEvent,
    "none": Noop,
    "set_fact": SetFact,
    "post_event": PostEvent,
    "retract_fact": RetractFact,
    "shutdown": ShutdownAction,
    "run_playbook": RunPlaybook,
    "run_module": RunModule,
    "run_job_template": RunJobTemplate,
    "run_workflow_template": RunWorkflowTemplate,
    "pg_notify": PGNotify,
    "sleep": Sleep,
}


class RuleSetRunner:
    def __init__(
        self,
        event_log: asyncio.Queue,
        ruleset_queue_plan: EngineRuleSetQueuePlan,
        hosts_facts,
        variables,
        rule_set,
        project_data_file: Optional[str] = None,
        parsed_args=None,
        broadcast_method=None,
    ):
        self.action_loop_task = None
        self.event_log = event_log
        self.ruleset_queue_plan = ruleset_queue_plan
        self.name = ruleset_queue_plan.ruleset.name
        self.rule_set = rule_set
        self.hosts_facts = hosts_facts
        self.variables = variables
        self.project_data_file = project_data_file
        self.parsed_args = parsed_args
        self.shutdown = None
        self.active_actions = set()
        self.broadcast_method = broadcast_method
        self.event_counter = 0
        self.event_store_stats_interval = 1000  # Log stats every N events
        self.display = terminal.Display()
        self.locks = defaultdict(asyncio.Lock)

    async def run_ruleset(self):
        tasks = []
        try:
            prime_facts(self.name, self.hosts_facts)
            task_name = (
                f"action_plan_task:: {self.ruleset_queue_plan.ruleset.name}"
            )
            self.action_loop_task = asyncio.create_task(
                self._drain_actionplan_queue(), name=task_name
            )
            tasks.append(self.action_loop_task)
            task_name = (
                f"source_reader_task:: {self.ruleset_queue_plan.ruleset.name}"
            )
            self.source_loop_task = asyncio.create_task(
                self._drain_source_queue(), name=task_name
            )
            tasks.append(self.source_loop_task)
            await asyncio.wait([self.action_loop_task])
        except asyncio.CancelledError:
            logger.debug("Cancelled error caught in run_ruleset")
            for task in tasks:
                if not task.done():
                    logger.debug("Cancelling (2) task %s", task.get_name())
                    task.cancel()

        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        except asyncio.CancelledError:
            raise

    async def _cleanup(self):
        logger.debug("Cleaning up ruleset %s", self.name)
        if not self.source_loop_task.done():
            self.source_loop_task.cancel()

        if (
            self.active_actions
            and self.shutdown
            and self.shutdown.kind == "graceful"
        ):
            logger.debug("Waiting for active actions to end")
            await asyncio.wait(
                self.active_actions,
                return_when=asyncio.FIRST_EXCEPTION,
                timeout=self.shutdown.delay,
            )

        if self.active_actions:
            logger.info(
                "Cancelling %d active tasks, ruleset %s cleanup",
                len(self.active_actions),
                self.name,
            )
            for task in self.active_actions:
                logger.debug("Cancelling active task %s", task.get_name())
                task.cancel()
        if self.shutdown:
            await self.event_log.put(
                dict(
                    type="Shutdown",
                    message=self.shutdown.message,
                    delay=self.shutdown.delay,
                    source_plugin=self.shutdown.source_plugin,
                    kind=self.shutdown.kind,
                )
            )
        stats = lang.end_session(self.name)
        if self.parsed_args and self.parsed_args.heartbeat > 0:
            await send_session_stats(self.event_log, stats)
        logger.info(pformat(stats))

    async def _check_event_store_backpressure(self, caller: str = "unknown"):
        """
        Check if event store has enough space available before processing next event.

        Applies backpressure with exponential backoff if memory usage is near threshold.
        This prevents queues from overwhelming the system with events
        that cannot be stored in the event store (SharedMemory or mmap).

        Uses different thresholds based on caller to create layered backpressure:
        - SOURCE_QUEUE: Most conservative (threshold - 3%), blocks at genesis
        - ACTIONPLAN_QUEUE: Moderate (threshold - 1%), blocks after Drools
        - Others: Standard threshold

        Uses exponential backoff to handle long-running tasks (configurable via settings):
        - Initial wait: settings.shared_memory_initial_wait (default: 0.5s)
        - Exponentially increases: 1s, 2s, 4s, 8s, 16s, 32s, ...
        - Max per iteration: settings.shared_memory_max_wait_per_iteration (default: 60s)
        - Total max wait: settings.shared_memory_max_total_wait (default: 3600s / 1 hour)
        - Returns when space available or timeout reached

        Args:
            caller: Name of the calling function (for logging)

        Returns:
            None
        """
        store = get_event_store()

        # Use more conservative threshold for earlier stages
        # This ensures SOURCE_QUEUE blocks FIRST, before events enter the pipeline
        if caller == "source_queue":
            # Most conservative: block at threshold - 3% (e.g., 87% when threshold is 90%)
            # This stops events at the genesis point
            effective_threshold = store._threshold_pct - 3.0
        elif caller == "actionplan_queue":
            # Moderate: block at threshold - 1% (e.g., 89% when threshold is 90%)
            # This handles events already in Drools pipeline
            effective_threshold = store._threshold_pct - 1.0
        else:
            # Standard threshold for other callers
            effective_threshold = store._threshold_pct

        current_usage = store.get_usage_percent()

        if current_usage < effective_threshold:
            # Space available, no backpressure needed
            return

        # Exponential backoff configuration (from settings)
        initial_wait = settings.shared_memory_initial_wait
        max_wait_per_iteration = settings.shared_memory_max_wait_per_iteration
        max_total_wait = settings.shared_memory_max_total_wait
        backoff_multiplier = 2  # Double the wait time each iteration

        current_wait = initial_wait
        total_wait = 0
        check_count = 0
        last_log_time = 0
        log_interval = 30  # Log every 30 seconds

        logger.warning(
            "[BACKPRESSURE:%s] Activated - usage %.1f%% >= %.1f%% threshold, "
            "pausing queue processing (available: %.2f MB / %.2f MB)",
            caller.upper(),
            current_usage,
            effective_threshold,
            store.get_available_space_mb(),
            store._max_size_bytes / (1024 * 1024),
        )

        while store.get_usage_percent() >= effective_threshold:
            check_count += 1

            # Check if we've exceeded max total wait time
            if total_wait >= max_total_wait:
                logger.error(
                    "[BACKPRESSURE:%s] Event store still near threshold after "
                    "%.1f seconds (%.1f%%), forcing through to prevent "
                    "deadlock (max wait: %d seconds reached)",
                    caller.upper(),
                    total_wait,
                    store.get_usage_percent(),
                    max_total_wait,
                )
                break

            # Log only every 30 seconds
            if total_wait - last_log_time >= log_interval:
                last_log_time = total_wait
                logger.warning(
                    "[BACKPRESSURE:%s] Still waiting for space - elapsed %.1fs / %ds "
                    "(usage: %.1f%% >= %.1f%% threshold, available: %.2f MB)",
                    caller.upper(),
                    total_wait,
                    max_total_wait,
                    store.get_usage_percent(),
                    effective_threshold,
                    store.get_available_space_mb(),
                )

            # Sleep with current wait time
            await asyncio.sleep(current_wait)
            total_wait += current_wait

            # Calculate next wait time with exponential backoff
            current_wait = min(
                current_wait * backoff_multiplier, max_wait_per_iteration
            )

        # Space became available
        if total_wait > 0:
            logger.info(
                "[BACKPRESSURE:%s] Released after %.1fs - resuming event processing "
                "(usage: %.1f%% < %.1f%% threshold, available: %.2f MB, ready to fetch new events)",
                caller.upper(),
                total_wait,
                store.get_usage_percent(),
                effective_threshold,
                store.get_available_space_mb(),
            )

    async def _handle_shutdown(self):
        logger.info(
            "Ruleset: %s, received shutdown: %s",
            self.name,
            str(self.shutdown),
        )
        if self.shutdown.kind == "now":
            logger.debug(
                "ruleset: %s has issued an immediate shutdown", self.name
            )
            self.action_loop_task.cancel()
        elif (
            self.ruleset_queue_plan.plan.queue.empty()
            and not self.active_actions
        ):
            logger.debug("ruleset: %s shutdown no pending work", self.name)
            self.action_loop_task.cancel()
        else:
            logger.debug(
                "ruleset: %s waiting %f for shutdown",
                self.name,
                self.shutdown.delay,
            )
            await asyncio.sleep(self.shutdown.delay)
            if not self.action_loop_task.done():
                self.action_loop_task.cancel()

        return

    async def _drain_source_queue(self):
        logger.info("Waiting for events, ruleset: %s", self.name)
        try:
            while True:
                # Check event store availability before fetching next event
                # This provides PRIMARY backpressure at the source level (genesis of events)
                await self._check_event_store_backpressure(
                    caller="source_queue"
                )

                data = await self.ruleset_queue_plan.source_queue.get()
                # Default to output events at debug level.
                level = logging.DEBUG

                # If we are printing events adjust the level to the display's
                # current level to guarantee output.
                if settings.print_events:
                    level = self.display.level

                self.display.banner("received event", level=level)
                self.display.output(f"Ruleset: {self.name}", level=level)
                self.display.output("Event:", level=level)
                self.display.output(data, pretty=True, level=level)
                self.display.banner(level=level)

                if isinstance(data, Shutdown):
                    self.shutdown = data
                    return await self._handle_shutdown()

                if not data:
                    # TODO: is it really necessary to add such event
                    # to event_log?
                    await self.event_log.put(dict(type="EmptyEvent"))
                    continue

                # Feedback must be sent for any event the engine
                # received, even if already observed or unhandled,
                # so the source can advance. Without this, feedback-
                # enabled sources deadlock waiting for a response
                # that never arrives.
                send_feedback = False
                try:
                    logger.debug(
                        "Posting data to ruleset %s => %s",
                        self.name,
                        str(data),
                    )
                    lang.post(self.name, data)
                    send_feedback = True
                except asyncio.CancelledError:
                    raise
                except MessageObservedException:
                    logger.debug("MessageObservedException: %s", data)
                    send_feedback = True
                except MessageNotHandledException:
                    logger.debug("MessageNotHandledException: %s", data)
                    send_feedback = True
                except BaseException as e:
                    # On unexpected errors skip feedback; the event
                    # state is unknown.
                    logger.error(e)
                finally:
                    logger.debug(lang.get_pending_events(self.name))
                    if (
                        settings.gc_after
                        and self.event_counter > settings.gc_after
                    ):
                        self.event_counter = 0
                        gc.collect()
                    else:
                        self.event_counter += 1

                    # Log event store statistics periodically to verify cleanup
                    if (
                        self.event_counter % self.event_store_stats_interval
                        == 0
                    ):
                        try:
                            store = get_event_store()
                            stats = store.get_stats()
                            logger.info(
                                "[EVENT STORE] After %d events: %d in store, "
                                "%.2f MB used (%.1f%%), adds=%d, removes=%d",
                                self.event_counter,
                                stats["current_events"],
                                stats["used_mb"],
                                stats["usage_percent"],
                                stats["total_adds"],
                                stats["total_removes"],
                            )
                        except Exception as e:
                            logger.debug(
                                "Failed to get event store stats: %s", e
                            )

                    while self.ruleset_queue_plan.plan.queue.qsize() > 10:
                        await asyncio.sleep(0)

                # Send feedback outside the try/except so it runs
                # regardless of which lang.post() outcome occurred.
                if send_feedback:
                    try:
                        source_name = dpath.get(data, "meta/source/name")
                    except KeyError:
                        source_name = None

                    if (
                        source_name
                        and source_name
                        in self.ruleset_queue_plan.source_feedback_queues
                    ):
                        feedback_queue = (
                            self.ruleset_queue_plan.source_feedback_queues.get(
                                source_name
                            )
                        )
                        await feedback_queue.put(data)
        except asyncio.CancelledError:
            logger.debug("Source Task Cancelled for ruleset %s", self.name)
            raise

    def _handle_action_completion(self, task):
        self.active_actions.discard(task)
        logger.debug(
            "Task %s finished, active actions %d",
            task.get_name(),
            len(self.active_actions),
        )
        if (
            self.ruleset_queue_plan.plan.queue.empty()
            and self.shutdown
            and len(self.active_actions) == 0
        ):
            logger.debug("All actions done")
            if not self.action_loop_task.done():
                self.action_loop_task.cancel()

    async def _run_action_with_lock(
        self,
        lock_name: str,
        action: str,
        metadata: Metadata,
        control: Control,
        action_args: dict,
    ):
        """
        Acquire the lock for the specified resource, access the resource,
        and release the lock.
        """
        async with self.locks[lock_name]:
            logger.debug(
                f"Acquired lock: {lock_name} for action: {action}, "
                f"rule: {metadata.rule}"
            )
            await ACTION_CLASSES[action](
                metadata,
                control,
                **action_args,
            )()

        logger.debug(
            f"Released lock: {lock_name} for action: {action}, "
            f"rule: {metadata.rule}"
        )

    async def _drain_actionplan_queue(self):
        logger.info("Waiting for actions on events from %s", self.name)
        try:
            while True:
                # Check event store availability before processing next action
                # This provides SECONDARY backpressure for events already queued from Drools
                await self._check_event_store_backpressure(
                    caller="actionplan_queue"
                )

                queue_item = await self.ruleset_queue_plan.plan.queue.get()
                rule_run_at = run_at()
                action_item = cast(ActionContext, queue_item)
                if (
                    self.parsed_args
                    and self.parsed_args.heartbeat > 0
                    and not settings.skip_audit_events
                ):
                    await send_session_stats(
                        self.event_log,
                        session_stats(self.ruleset_queue_plan.ruleset.name),
                    )
                if len(action_item.actions) > 1:
                    task = asyncio.create_task(
                        self._run_multiple_actions(action_item, rule_run_at)
                    )
                    self.active_actions.add(task)
                    task.add_done_callback(self._handle_action_completion)
                else:
                    task = self._run_action(
                        action_item.actions[0], action_item, rule_run_at
                    )

                if (
                    self.rule_set.execution_strategy
                    == ExecutionStrategy.SEQUENTIAL
                ):
                    await task

        except asyncio.CancelledError:
            logger.debug(
                "Action Plan Task Cancelled for ruleset %s", self.name
            )
            raise
        except BaseException as e:
            logger.error(e)
            raise
        finally:
            await self._cleanup()

    async def _run_multiple_actions(
        self, action_item: ActionContext, rule_run_at: str
    ) -> None:
        number_of_actions = len(action_item.actions)
        for index, action in enumerate(action_item.actions):
            await self._run_action(
                action,
                action_item,
                rule_run_at,
                index,
                index == (number_of_actions - 1),
            )

    def _run_action(
        self,
        action: Action,
        action_item: ActionContext,
        rule_run_at: str,
        index: int = 0,
        last_action: bool = True,
    ) -> asyncio.Task:
        task_name = (
            f"action::{action.action}::"
            f"{self.ruleset_queue_plan.ruleset.name}::"
            f"{action_item.rule}"
        )
        logger.debug("Creating action task %s", task_name)
        persistent_info = None
        matching_uuid = action_item.rule_engine_results.matching_uuid
        if matching_uuid:
            a_priori = None
            a_priori = get_action_a_priori(
                action_item.ruleset, matching_uuid, index
            )
            if a_priori is None:
                update_action_info(
                    action_item.ruleset,
                    matching_uuid,
                    index,
                    {"status": STARTED_STATUS},
                    True,
                )
            persistent_info = ActionPersistence(
                matching_uuid=matching_uuid,
                action_index=index,
                last_action=last_action,
                a_priori=a_priori,
            )

        metadata = Metadata(
            rule_set=action_item.ruleset,
            rule_set_uuid=action_item.ruleset_uuid,
            rule=action_item.rule,
            rule_uuid=action_item.rule_uuid,
            rule_run_at=rule_run_at,
            persistent_info=persistent_info,
        )

        task = asyncio.create_task(
            self._call_action(
                metadata,
                action.action,
                MappingProxyType(action.action_args),
                action_item.variables,
                action_item.inventory,
                action_item.hosts,
                action_item.rule_engine_results,
            ),
            name=task_name,
        )
        self.active_actions.add(task)
        task.add_done_callback(self._handle_action_completion)
        return task

    async def _call_action(
        self,
        metadata: Metadata,
        action: str,
        immutable_action_args: MappingProxyType,
        variables: Dict,
        inventory: str,
        hosts: List,
        rules_engine_result,
    ) -> None:
        logger.debug("call_action %s", action)
        action_args = immutable_action_args.copy()

        if (
            metadata.persistent_info
            and metadata.persistent_info.a_priori
            and metadata.persistent_info.a_priori.get("status")
            in COMPLETED_STATUSES
        ):
            logger.warning(
                "Skipping action %s already ran %s",
                action,
                metadata.persistent_info.a_priori.get("status"),
            )
            if (
                metadata.persistent_info
                and metadata.persistent_info.last_action
            ):
                lang.delete_action_info(
                    metadata.rule_set, metadata.persistent_info.matching_uuid
                )
            return

        error = None
        action_status = FAILED_STATUS
        cancelled = False
        event_uuids = []  # Track event UUIDs for cleanup
        if action in ACTION_CLASSES:
            try:
                if (
                    action == "run_job_template"
                    or action == "run_workflow_template"
                ):
                    limit = dpath.get(
                        action_args,
                        "job_args.limit",
                        separator=".",
                        default=None,
                    )
                    if isinstance(limit, list):
                        hosts = limit
                    elif isinstance(limit, str):
                        hosts = [limit]
                elif action == "shutdown":
                    if self.parsed_args and "delay" not in action_args:
                        action_args["delay"] = self.parsed_args.shutdown_delay

                single_match = None
                keys = list(rules_engine_result.data.keys())
                if len(keys) == 0:
                    single_match = {}
                elif len(keys) == 1 and keys[0] == "m":
                    single_match = rules_engine_result.data[keys[0]]
                else:
                    multi_match = rules_engine_result.data
                variables_copy = variables.copy()
                if action == "debug":
                    variables_copy = mask_sensitive_variable_values(
                        variables_copy
                    )

                # Get event store (always available, initialized in engine.py)
                store = get_event_store()

                if single_match is not None:
                    event = single_match

                    # Add event to event store and get lazy reference for variables
                    if event:
                        event_uuid = await store.add_event(
                            event, caller="call_action"
                        )
                        event_uuids.append(event_uuid)
                        # Store LazyEventDict directly in variables for Jinja2 template rendering
                        variables_copy["event"] = store.get_event_lazy(
                            event_uuid
                        )
                        logger.debug(
                            "[RULE_SET_RUNNER] Added event %s to event store and stored lazy reference",
                            event_uuid[:8]
                            if len(event_uuid) > 8
                            else event_uuid,
                        )
                    else:
                        variables_copy["event"] = event

                    if "meta" in event:
                        if "hosts" in event["meta"]:
                            hosts = parse_hosts(event["meta"]["hosts"])
                else:
                    # Multiple events - add all to event store and get lazy references
                    lazy_event_map = {}
                    for key, event in multi_match.items():
                        event_uuid = await store.add_event(
                            event, caller="call_action"
                        )
                        event_uuids.append(event_uuid)
                        # Store LazyEventDict directly in variables for Jinja2 template rendering
                        lazy_event_map[key] = store.get_event_lazy(event_uuid)
                        logger.debug(
                            "[RULE_SET_RUNNER] Added event %s (%s) to event store and stored lazy reference",
                            key,
                            event_uuid[:8]
                            if len(event_uuid) > 8
                            else event_uuid,
                        )
                    variables_copy["events"] = lazy_event_map

                    # Extract hosts from events
                    new_hosts = []
                    for event in multi_match.values():
                        if "meta" in event:
                            if "hosts" in event["meta"]:
                                new_hosts.extend(
                                    parse_hosts(event["meta"]["hosts"])
                                )
                    if new_hosts:
                        hosts = new_hosts

                if "var_root" in action_args:
                    var_root = action_args.pop("var_root")
                    _update_variables(variables_copy, var_root)

                action_args = {
                    k: substitute_variables(v, variables_copy)
                    for k, v in action_args.items()
                }

                if "ruleset" not in action_args:
                    action_args["ruleset"] = metadata.rule_set

                control = Control(
                    queue=self.event_log,
                    inventory=inventory,
                    hosts=hosts,
                    variables=variables_copy,
                    project_data_file=self.project_data_file,
                )

                lock = action_args.get("lock", None)
                if (
                    self.rule_set.execution_strategy
                    == ExecutionStrategy.PARALLEL
                    and lock
                ):
                    await self._run_action_with_lock(
                        lock,
                        action,
                        metadata,
                        control,
                        action_args,
                    )
                    action_status = SUCCESSFUL_STATUS
                else:
                    await ACTION_CLASSES[action](
                        metadata,
                        control,
                        **action_args,
                    )()
                    action_status = SUCCESSFUL_STATUS
            except KeyError as e:
                logger.error(
                    "KeyError %s with variables %s",
                    str(e),
                    pformat(mask_sensitive_variable_values(variables_copy)),
                )
                error = e
            except MessageNotHandledException as e:
                logger.error(
                    "Message cannot be handled: %s err %s", action_args, str(e)
                )
                error = e
            except MessageObservedException as e:
                logger.debug("MessageObservedException: %s", action_args)
                error = e
            except ShutdownException as e:
                if self.shutdown:
                    logger.debug(
                        "A shutdown is already in progress, ignoring this one"
                    )
                else:
                    await self.broadcast_method(e.shutdown)
            except asyncio.CancelledError:
                logger.debug("Action task caught Cancelled error")
                cancelled = True
                raise
            except jinja2_exceptions.UndefinedError as e:
                error = e
                undefined_variable = str(e).split("has no attribute")
                if len(undefined_variable) > 1:
                    logger.error(
                        "Undefined jinja variable %s in action %s",
                        undefined_variable[-1],
                        action,
                    )
                else:
                    raise
            except Exception as e:
                logger.error("Error calling action %s, err %s", action, str(e))
                error = e
            except BaseException as e:
                logger.error(e)
                raise
            finally:
                # Clean up events from event store
                if event_uuids:
                    store = get_event_store()
                    for event_uuid in event_uuids:
                        deleted = await store.remove_event(event_uuid)
                        if deleted:
                            logger.debug(
                                "[RULE_SET_RUNNER] Event %s removed from event store "
                                "(refcount=0, memory freed)",
                                event_uuid[:8]
                                if len(event_uuid) > 8
                                else event_uuid,
                            )
                        else:
                            logger.debug(
                                "[RULE_SET_RUNNER] Event %s refcount decremented "
                                "(still referenced by other rules)",
                                event_uuid[:8]
                                if len(event_uuid) > 8
                                else event_uuid,
                            )

                if (
                    metadata.persistent_info
                    and metadata.persistent_info.matching_uuid
                    and not cancelled
                ):
                    if metadata.persistent_info.last_action:
                        lang.delete_action_info(
                            metadata.rule_set,
                            metadata.persistent_info.matching_uuid,
                        )
                    else:
                        update_action_info(
                            metadata.rule_set,
                            metadata.persistent_info.matching_uuid,
                            metadata.persistent_info.action_index,
                            {"status": action_status},
                        )
        else:
            logger.error("Action %s not supported", action)
            error = UnsupportedActionException(
                f"Action {action} not supported"
            )

        if error:
            await self.event_log.put(
                dict(
                    type="Action",
                    action=action,
                    action_uuid=str(uuid.uuid4()),
                    activation_id=settings.identifier,
                    activation_instance_id=settings.identifier,
                    playbook_name=action_args.get("name"),
                    status="failed",
                    run_at=run_at(),
                    rule_run_at=metadata.rule_run_at,
                    message=str(error),
                    rule=metadata.rule,
                    ruleset=metadata.rule_set,
                    rule_uuid=metadata.rule_uuid,
                    ruleset_uuid=metadata.rule_set_uuid,
                )
            )


def prime_facts(name: str, hosts_facts: List[Dict]):
    for data in hosts_facts:
        try:
            lang.assert_fact(name, data)
        except MessageNotHandledException:
            pass


def _update_variables(variables: Dict, var_root: Union[str, Dict]):
    var_roots = {var_root: var_root} if isinstance(var_root, str) else var_root
    if "event" in variables:
        for key, _new_key in var_roots.items():
            new_value = dpath.get(
                variables["event"], key, separator=".", default=None
            )
            if new_value:
                variables["event"] = new_value
                break
    elif "events" in variables:
        for _k, v in variables["events"].items():
            for old_key, new_key in var_roots.items():
                new_value = dpath.get(v, old_key, separator=".", default=None)
                if new_value:
                    variables["events"][new_key] = new_value
                    break
