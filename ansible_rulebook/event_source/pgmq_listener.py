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
  host:
    description:
      - The host name
        Refer to https://magicstack.github.io/asyncpg/current/api/index.html
    type: str
  port:
    description:
      - The port number where postgres is listening
        Refer to https://magicstack.github.io/asyncpg/current/api/index.html
    type: str
  user:
    description:
      - The user name for authentication
        Refer to https://magicstack.github.io/asyncpg/current/api/index.html
    type: str
  password:
    description:
      - The password for authentication
        Refer to https://magicstack.github.io/asyncpg/current/api/index.html
    type: str
  database:
    description:
      - The database to use
        Refer to https://magicstack.github.io/asyncpg/current/api/index.html
    type: str
  sslmode:
    description:
      - The sslmode to use
        Refer to https://magicstack.github.io/asyncpg/current/api/index.html
    type: str
  sslcert:
    description:
      - The SSL Certificate filename to use
        Refer to https://magicstack.github.io/asyncpg/current/api/index.html
    type: str
  sslkey:
    description:
      - The SSL key filename to use
        Refer to https://magicstack.github.io/asyncpg/current/api/index.html
    type: str
  sslrootcert:
    description:
      - The SSL Root certifciate filename to use
        Refer to https://magicstack.github.io/asyncpg/current/api/index.html
    type: str
  queues:
    description:
      - The list of queues to listen
    type: list
    elements: str
    required: true
"""

EXAMPLES = r"""
- ansible.eda.pqmq_listener:
    dsn: "postgres://postgres:postgres@localhost:7432/test_db",
    queues:
      - my_events
      - my_alerts

- ansible.eda.pg_listener:
    postgres_params:
      host: localhost
      port: 7432
      dbname: test_db
    queues:
      - my_events
      - my_alerts
"""

LOGGER = logging.getLogger(__name__)

REQUIRED_KEYS = ["queues", "host", "port", "database"]


class MissingRequiredArgumentError(Exception):
    """Exception class for missing arguments."""


def _validate_args(args: dict[str, Any]) -> None:
    """Validate the arguments and raise exception accordingly."""
    missing_keys = [key for key in REQUIRED_KEYS if key not in args]
    if missing_keys:
        msg = f"Missing required arguments: {', '.join(missing_keys)}"
        raise MissingRequiredArgumentError(msg)


def _get_dsn(connect_args: dict):
    result = (
        f"postgres://{connect_args['host']}:"
        f"{connect_args['port']}"
        f"/{connect_args['database']}?"
    )
    options = ""
    for key in [
        "sslmode",
        "sslcert",
        "sslkey",
        "sslrootcert",
        "user",
        "password",
    ]:
        if key in connect_args and connect_args[key]:
            value = connect_args[key]
            if options:
                options += "&"
            options += f"{key}={value}"

    return result + options


async def main(
    input_queue: asyncio.Queue[Any],
    feedback_queue: asyncio.Queue[Any],
    args: dict[str, Any],
) -> None:
    """Listen for events from a queue."""
    _validate_args(args)
    conn = None
    try:
        queue_names = args.pop("queues")
        dsn = _get_dsn(args)
        conn = await asyncpg.connect(dsn=dsn)
        queue_messages = {}

        pgmq_queue = PGMQueue()
        while True:
            msg = await pgmq_queue.read(queue_names[0], vt=30, conn=conn)
            if msg:
                data = {"msg_id": msg.msg_id, "message": msg.message}
                queue_messages[msg.msg_id] = data
                await input_queue.put(data)
                processed_event = await feedback_queue.get()
                if processed_event["msg_id"] in queue_messages:
                    print(f"Removing message {processed_event['msg_id']}")
                    queue_messages.pop(processed_event["msg_id"])
                await pgmq_queue.delete(queue_names[0], msg.msg_id, conn=conn)
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
            FeedbackQueue(),
            {
                "queues": ["my_queue"],
                "host": "localhost",
                "port": 7432,
                "database": "test_db",
                "user": "postgres",
                "password": "postgres",
            },
        ),
    )
