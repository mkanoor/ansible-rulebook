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

import asyncio
import json
import logging
import os
from typing import Any, Dict

logger = logging.getLogger(__name__)


def update_json_file(filename: str, data: list[Dict]):
    with open(filename, "w") as f:
        f.write(json.dumps(data, indent=4))


async def main(queue: asyncio.Queue, args: Dict[str, Any]):
    if not args.get("file"):
        raise ValueError("file is a required argument")

    filename = args.get("file")
    if not os.path.exists(filename):
        raise FileNotFoundError(filename)

    delay = int(args.get("delay", 0))
    startup_delay = int(args.get("startup_delay", 0))
    fail_after = int(args.get("fail_after", 0))

    with open(filename, "r", encoding="utf-8") as file:
        data = json.load(file)
        if not isinstance(data, list):
            data = [data]

    await asyncio.sleep(startup_delay)
    counter = 0
    total = len(data)
    for event in list(data):
        await queue.put(event)

        if fail_after > 0 and counter % total == fail_after:
            logger.warning("Failing after %d events", fail_after)
            raise Exception(
                f"Intentionally Failing after every {fail_after} events"
            )

        if args["__feedback_queue__"]:
            feedback_event = await args["__feedback_queue__"].get()
            if event["meta"]["uuid"] != feedback_event["meta"]["uuid"]:
                print("Event mismatch")
            else:
                data.remove(event)
                update_json_file(filename, data)

        if event.get("raise_exception"):
            msg = event.get("raise_exception")
            if isinstance(msg, str):
                raise Exception(msg)
            else:
                raise Exception("The event suggests to raise exception")

        await asyncio.sleep(delay)
        counter += 1


if __name__ == "__main__":

    class MockQueue:
        async def put(self, event):
            print(event)

    asyncio.run(main(MockQueue(), dict(limit=5)))
