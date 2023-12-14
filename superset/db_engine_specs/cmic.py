# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import annotations

import logging
import re
from re import Pattern
from typing import Any, TypedDict

import requests
from apispec import APISpec
from apispec.ext.marshmallow import MarshmallowPlugin
from flask import g
from flask_babel import gettext as __
from marshmallow import fields, Schema
from marshmallow.validate import Range
from sqlalchemy.engine.url import URL

from superset.databases.utils import make_url_safe
from superset.db_engine_specs.base import BaseEngineSpec
from superset.db_engine_specs.shillelagh import ShillelaghEngineSpec
from superset.errors import ErrorLevel, SupersetError, SupersetErrorType
from superset.exceptions import SupersetException

_logger = logging.getLogger()

SYNTAX_ERROR_REGEX = re.compile('SQLError: near "(?P<server_error>.*?)": syntax error')


class CmicParametersSchema(Schema):
    username = fields.String(
        required=True,
        allow_none=True,
        metadata={"description": __("Username")},
    )
    password = fields.String(
        required=True,
        allow_none=True,
        metadata={"description": __("Password")},
    )


class CmicParametersType(TypedDict):
    username: str | None
    password: str | None


class CmicPropertiesType(TypedDict):
    parameters: CmicParametersType


class CmicEngineSpec(BaseEngineSpec):
    """Engine for CMIC On Cloud Service"""

    engine_name = "Cmic"
    engine = "mysql"
    default_driver = "mysqldb"

    parameters_schema = CmicParametersSchema()

    disable_ssh_tunneling = True
    supports_dynamic_schema = True

    custom_errors: dict[Pattern[str], tuple[str, SupersetErrorType, dict[str, Any]]] = {
        SYNTAX_ERROR_REGEX: (
            __(
                'Please check your query for syntax errors near "%(server_error)s". '
                "Then, try running your query again.",
            ),
            SupersetErrorType.SYNTAX_ERROR,
            {},
        ),
    }

    supports_file_upload = False

    @classmethod
    def build_sqlalchemy_uri(  # pylint: disable=unused-argument
        cls,
        parameters: CmicParametersType,
        encrypted_extra: dict[str, str] | None = None,
    ) -> str:
        return str(
            URL.create(
                f"{cls.engine}+{cls.default_driver}".rstrip("+"),
                username=parameters.get("username"),
                password=parameters.get("password"),
            )
        )

    @classmethod
    def get_parameters_from_uri(  # pylint: disable=unused-argument
        cls, uri: str, encrypted_extra: dict[str, Any] | None = None
    ) -> CmicParametersType:
        url = make_url_safe(uri)
        return {
            "username": url.username,
            "password": url.password,
        }

    @classmethod
    def validate_parameters(cls, properties: CmicPropertiesType) -> list[SupersetError]:
        """
        Validates any number of parameters, for progressive validation.

        As more parameters are present in the request, more validation is done.
        """
        errors: list[SupersetError] = []

        required = {"username", "password"}
        parameters = properties.get("parameters", {})
        present = {key for key in parameters if parameters.get(key, ())}

        if missing := sorted(required - present):
            errors.append(
                SupersetError(
                    message=f'One or more parameters are missing: {", ".join(missing)}',
                    error_type=SupersetErrorType.CONNECTION_MISSING_PARAMETERS_ERROR,
                    level=ErrorLevel.WARNING,
                    extra={"missing": missing},
                ),
            )
        username = parameters.get("username", None)
        password = parameters.get("password", None)

        # TODO change this to the right url
        url = "https://myapi.cmci.com"
        body = {
            "username": username,
            "password": password,
        }
        try:
            payload = cls._do_post(
                url,
                body,
            )
            # TODO use payload for the pipeline run if necessary
        except SupersetException:
            errors.append(
                SupersetError(
                    message="Unable to authenticate to the CIMC API.",
                    error_type=SupersetErrorType.CONNECTION_ACCESS_DENIED_ERROR,
                    level=ErrorLevel.ERROR,
                    extra={"invalid": ["username", "password"]},
                ),
            )
        except requests.exceptions.ConnectionError:
            errors.append(
                SupersetError(
                    message="Unable to connect to the CIMC API.",
                    error_type=SupersetErrorType.CONNECTION_ACCESS_DENIED_ERROR,
                    level=ErrorLevel.ERROR,
                    extra={"invalid": ["username", "password"]},
                ),
            )

        return errors

    @staticmethod
    def _do_post(
        url: str,
        body: dict[str, Any],
        **kwargs: Any,
    ) -> dict[str, Any]:
        """
        POST to the CMIC API.

        Helper function that handles logging and error handling.
        """
        _logger.info("POST %s", url)
        _logger.debug(body)
        response = requests.post(
            url,
            json=body,
            **kwargs,
        )

        payload = response.json()
        _logger.debug(payload)

        if "error" in payload:
            raise SupersetException(payload["error"]["message"])

        return payload

    @classmethod
    def parameters_json_schema(cls) -> Any:
        """
        Return configuration parameters as OpenAPI.
        """
        if not cls.parameters_schema:
            return None

        spec = APISpec(
            title="Database Parameters",
            version="1.0.0",
            openapi_version="3.0.2",
            plugins=[MarshmallowPlugin()],
        )
        spec.components.schema(cls.__name__, schema=cls.parameters_schema)
        return spec.to_dict()["components"]["schemas"][cls.__name__]
