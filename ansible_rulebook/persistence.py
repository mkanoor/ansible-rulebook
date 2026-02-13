import argparse
import json
import logging
from typing import Dict, Optional

from drools import ruleset as lang

from ansible_rulebook.conf import settings

logger = logging.getLogger(__name__)
POSTGRES_REQUIRED_KEYS = {
    "drools_db_host",
    "drools_db_port",
    "drools_db_name",
}

H2_REQUIRED_KEYS = {"drools_db_file_path"}


def enable_persistence(
    parsed_args: argparse.Namespace, variables: Dict
) -> None:

    db_params = _get_postgres_params(variables) or _get_h2_params(variables)
    if db_params is None:
        return

    settings.persistence_enabled = True
    # This should be a UUID but the backend currently does not
    # use UUID's it uses integer ids
    activation_uuid = f"activation-{parsed_args.id or 'standalone'}"
    worker_name = activation_uuid

    config = {
        "syncInterval": 1000,
        "stateCheckInterval": 500,
    }

    logger.info(
        "Initializing drools HA mode: worker_id=%s," "database=%s, ha_uuid=%s",
        activation_uuid,
        db_params["db_type"],
        activation_uuid,
    )

    lang.initialize_ha(
        uuid=activation_uuid,
        worker_name=worker_name,
        db_params=db_params,
        config=config,
    )


def update_action_info(
    rule_set: str,
    matching_uuid: str,
    index: int,
    info: dict,
    create: bool = False,
) -> None:
    if not settings.persistence_enabled:
        return

    if create:
        lang.add_action_info(rule_set, matching_uuid, index, json.dumps(info))
    else:
        saved_data = lang.get_action_info(rule_set, matching_uuid, index)
        action_data = json.loads(saved_data)
        for k, v in info.items():
            action_data[k] = v
        logger.debug("Updating action info %s", action_data)
        lang.update_action_info(
            rule_set, matching_uuid, index, json.dumps(action_data)
        )


def get_action_a_priori(
    rule_set: str, matching_uuid: str, index: int
) -> Optional[dict]:

    if lang.action_info_exists(rule_set, matching_uuid, index):
        data = lang.get_action_info(rule_set, matching_uuid, index)
        logger.debug("Previous action data %s", data)
        return json.loads(data)
    return None


def enable_leader():
    if settings.persistence_enabled:
        lang.enable_leader()


def _get_postgres_params(variables: dict) -> Optional[dict]:

    if not POSTGRES_REQUIRED_KEYS <= set(variables.keys()):
        return None

    db_params = {
        "host": variables["drools_db_host"],
        "port": variables["drools_db_port"],
        "database": variables["drools_db_name"],
    }
    db_params["db_type"] = "postgres"

    mappings = {
        "drools_db_user": "user",
        "drools_db_password": "password",
        "drools_sslmode": "sslmode",
        "drools_sslrootcert": "sslrootcert",
        "drools_sslkey": "sslkey",
        "drools_sslpassword": "sslpassword",
    }

    for key, mapped_name in mappings.items():
        if key in variables:
            db_params[mapped_name] = variables["key"]

    return db_params


def _get_h2_params(variables: dict) -> Optional[dict]:
    if not H2_REQUIRED_KEYS <= set(variables.keys()):
        return None

    return {"db_type": "h2", "db_file_path": variables["drools_db_file_path"]}
