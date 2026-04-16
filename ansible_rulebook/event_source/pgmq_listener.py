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
import sys
import traceback
from typing import Any

import asyncpg
from asyncpg.exceptions import PostgresError
from tembo_pgmq_python.async_queue import PGMQueue

DOCUMENTATION = r"""
---
short_description: Read events from Postgres Message Queue
description:
  - An ansible-rulebook event source plugin for reading events from
    postgres message queue
options:
  dsn:
    description:
      - The connection string/dsn for Postgres as supported by asyncpg
        Refer to https://magicstack.github.io/asyncpg/current/api/index.html
        Either dsn or postgres_params is required.
    type: str
  queues:
    description:
      - The list of queues to listen
    type: list
    elements: str
    required: true
"""

EXAMPLES = r"""
- eda.builtin.pgmq_listener:
    dsn: "postgres://postgres:postgres@localhost:7432/test_db",
    queues:
      - my_events
      - my_alerts
"""

LOGGER = logging.getLogger(__name__)

REQUIRED_KEYS = ["queues", "dsn"]


class MissingRequiredArgumentError(Exception):
    """Exception class for missing arguments."""


def _validate_args(args: dict[str, Any]) -> None:
    """Validate the arguments and raise exception accordingly."""
    missing_keys = [key for key in REQUIRED_KEYS if key not in args]
    if missing_keys:
        msg = f"Missing required arguments: {', '.join(missing_keys)}"
        raise MissingRequiredArgumentError(msg)


async def main(
    input_queue: asyncio.Queue[Any],
    args: dict[str, Any],
) -> None:
    """Listen for events from a queue."""
    _validate_args(args)
    conn = None
    try:
        queue_names = args.pop("queues")
        dsn = args.pop("dsn")
        LOGGER.error(f"Postgres DSN {dsn}")
        conn = await asyncpg.connect(dsn=dsn)
        queue_messages = {}
        vt = int(args.get("visibility_timeout", 30))

        pgmq_queue = PGMQueue()
        while True:
            msg = await pgmq_queue.read(queue_names[0], vt=vt, conn=conn)
            if msg:
                data = {"msg_id": msg.msg_id, "message": msg.message}
                queue_messages[msg.msg_id] = data
                await input_queue.put(data)
                if args.get("feedback") and args.get("eda_feedback_queue"):
                    feedback_queue = args.get("eda_feedback_queue")
                    processed_event = await feedback_queue.get()
                    if processed_event["msg_id"] in queue_messages:
                        print(f"Removing message {processed_event['msg_id']}")
                        queue_messages.pop(processed_event["msg_id"])
                    await pgmq_queue.delete(
                        queue_names[0], msg.msg_id, conn=conn
                    )
                else:
                    await pgmq_queue.delete(
                        queue_names[0], msg.msg_id, conn=conn
                    )
    except PostgresError as e:
        LOGGER.exception(f"Postgres Error {str(e)}")
        raise
    except asyncio.CancelledError:
        traceback.print_exc(file=sys.stdout)
        raise
    finally:
        if conn:
            await conn.close()


if __name__ == "__main__":
    # MockQueue if running directly

    global_message_id = 324

    class MockQueue(asyncio.Queue[Any]):
        """A fake queue."""

        async def put(self: "MockQueue", event: dict[str, Any]) -> None:
            """Print the event."""
            global global_message_id
            global_message_id = event["msg_id"]
            print(event)  # noqa: T201

    class FeedbackQueue(asyncio.Queue[Any]):
        """A fake feedback queue."""

        async def get(self: "FeedbackQueue") -> dict[str, Any]:
            """Print the event."""
            global global_message_id
            return {"msg_id": global_message_id}

    asyncio.run(
        main(
            MockQueue(),
            {
                "queues": ["9f9fbb8f_8d15_4f59_b29a_59cec50c242f"],
                "dsn": "postgres://postgres:secret@host.containers.internal:6432/my_database",
                "eda_feedback_queue": FeedbackQueue(),
                "feedback": True,
                "visibility_timeout": 120,
            },
        ),
    )
