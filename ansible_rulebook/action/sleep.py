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
import logging
import random

from .control import Control
from .helper import Helper
from .metadata import Metadata

logger = logging.getLogger(__name__)


class Sleep:
    """The Sleep action usually used for debugging, it sleeps to
    mimic a time constrainted task and then  sends the action status
    """

    def __init__(
        self,
        metadata: Metadata,
        control: Control,
        **action_args,
    ):
        self.helper = Helper(metadata, control, "sleep")
        self.action_args = action_args

    async def __call__(self):
        min_value = int(self.action_args.get("min_seconds", 0))
        max_value = int(self.action_args.get("max_seconds", min_value))
        duration = random.randint(min_value, max_value)
        print("Sleep Task Started")
        await asyncio.sleep(duration)
        await self.helper.send_default_status()
        print("Sleep Task Ended")
