from . import (
    debug,
    print_event,
    noop,
    post_event,
    retract_fact,
    shutdown,
    run_playbook,
    run_module,
    run_job_template,
    run_workflow_template,
    set_fact,
)

from .metadata import Metadata
from .control import Control


builtin_actions = {
    "debug": debug,
    "print_event": print_event,
    "none": noop,
    "set_fact": set_fact,
    "post_event": post_event,
    "retract_fact": retract_fact,
    "shutdown": shutdown,
    "run_playbook": run_playbook,
    "run_module": run_module,
    "run_job_template": run_job_template,
    "run_workflow_template": run_workflow_template,
}

__all__ = ['Metadata', 'Control', 'builtin_actions']
