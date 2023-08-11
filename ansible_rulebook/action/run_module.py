#  Copyright 2023 Red Hat, Inc.
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

import logging

from .control import Control
from .metadata import Metadata
from .run_playbook import RunPlaybook

logger = logging.getLogger(__name__)


class RunModule(RunPlaybook):
    """run_module runs an ansible module using the ansible runner"""

    def __init__(self, metadata: Metadata, control: Control, **action_args):
        super().__init__(metadata, control, **action_args)
        self.helper.set_action("run_module")

    def _runner_args(self):
        module_args_str = ""
        module_args = self.action_args.get("module_args", {})
        for key, value in module_args.items():
            if len(module_args_str) > 0:
                module_args_str += " "
            module_args_str += f"{key}={value!r}"

        return {
            "module": self.name,
            "host_pattern": ",".join(self.helper.control.hosts),
            "module_args": module_args_str,
        }

    def _copy_playbook_files(self, project_dir):
        pass


async def main(metadata: Metadata, control: Control, **action_args):
    await RunModule(metadata, control, **action_args)()

