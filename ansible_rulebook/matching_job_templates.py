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

import json
import logging
import ssl
from functools import cached_property
from typing import Union
from urllib.parse import urljoin

import aiohttp

from ansible_rulebook.exception import ControllerApiException

logger = logging.getLogger(__name__)


class MatchingJobTemplates:
    JOB_TEMPLATE_SLUG = "/api/v2/job_templates"
    VALID_GET_CODES = [200]

    def __init__(
        self, url, token, verify_ssl, playbooks, labels, organization="Default"
    ):
        self.url = url
        self.token = token
        self.verify_ssl = verify_ssl
        self.playbooks = playbooks
        self.labels = set(labels)
        self.organization = organization
        self.matches = {}

    def all_matched(self) -> bool:
        count = 0
        for data in self.matches.values():
            if len(data["labels"]) == len(self.labels):
                count += 1

        if count == len(self.playbooks):
            return True

        return False

    def log_mismatches(self) -> None:
        for playbook in self.playbooks:
            if playbook not in self.matches:
                logger.error("No job template found for playbook %s", playbook)
            else:
                match = self.matches[playbook]
                if len(match["labels"]) != len(self.labels):
                    logger.error(
                        "Not all matching labels found for playbook "
                        "%s requested %s found %s",
                        playbook,
                        self.labels,
                        match["labels"],
                    )

    async def get_matches(self) -> None:
        slug = f"{self.JOB_TEMPLATE_SLUG}/"
        params = {}
        self.matches = {}
        async with aiohttp.ClientSession(
            headers=self._auth_headers()
        ) as session:
            while True:
                response = await self._get_page(session, slug, params)
                json_body = json.loads(response["body"])
                for jt in json_body["results"]:

                    playbook = jt["playbook"].replace("playbooks/", "")

                    if playbook not in self.playbooks:
                        continue

                    organization = jt["summary_fields"]["organization"]["name"]
                    if self.organization != organization:
                        continue

                    jt_labels = set()
                    for label in jt["summary_fields"]["labels"]["results"]:
                        jt_labels.add(label["name"])

                    if self.labels:
                        matching_labels = self.labels.intersection(jt_labels)
                        if len(matching_labels) > 0:
                            logger.info(
                                "Matching job template %s for playbook %s",
                                jt["name"],
                                playbook,
                            )
                            self.matches[playbook] = dict(
                                name=jt["name"],
                                id=jt["id"],
                                organization=organization,
                                labels=matching_labels,
                            )

                if json_body.get("next", None):
                    params["page"] = params.get("page", 1) + 1
                else:
                    break

    async def _get_page(
        self, session: aiohttp.ClientSession, href_slug: str, params: dict
    ) -> dict:
        url = urljoin(self.url, href_slug)
        async with session.get(
            url,
            params=params,
            ssl=self._sslcontext,
        ) as response:
            response_text = dict(
                status=response.status, body=await response.text()
            )
        if response_text["status"] != 200:
            raise ControllerApiException(
                "Failed to get from %s. Status: %s, Body: %s"
                % (
                    url,
                    response_text["status"],
                    response_text.get("body", "empty"),
                )
            )
        return response_text

    def _auth_headers(self) -> dict:
        return dict(Authorization=f"Bearer {self.token}")

    @cached_property
    def _sslcontext(self) -> Union[bool, ssl.SSLContext]:
        if self.url.startswith("https"):
            if self.verify_ssl.lower() == "yes":
                return True
            elif not self.verify_ssl.lower() == "no":
                return ssl.create_default_context(cafile=self.verify_ssl)
        return False

    def exists(self, playbook) -> bool:
        return playbook in self.matches

    def create_args(self, playbook, action_args) -> dict:
        jt_action_args = action_args.copy()
        match = self.matches[playbook]
        jt_action_args["name"] = match["name"]
        jt_action_args["organization"] = match["organization"]
        pop_extra_vars = jt_action_args.pop("extra_vars", {})
        jt_action_args["job_args"] = pop_extra_vars
        return jt_action_args
