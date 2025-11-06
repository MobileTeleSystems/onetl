# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import Dict, Optional

from onetl.connection.db_connection.iceberg.catalog.auth import (
    IcebergRESTCatalogAuth,
    IcebergRESTCatalogBasicAuth,
    IcebergRESTCatalogBearerAuth,
    IcebergRESTCatalogOAuth2ClientCredentials,
    IcebergRESTCatalogOAuth2TokenExchange,
)

try:
    from pydantic.v1 import Field
except (ImportError, AttributeError):
    from pydantic import Field  # type: ignore[no-redef, assignment]

from onetl.connection.db_connection.iceberg.catalog import IcebergCatalog
from onetl.impl.frozen_model import FrozenModel


class IcebergRESTCatalog(IcebergCatalog, FrozenModel):
    """Iceberg REST Catalog.

    .. versionadded:: 0.14.1

    Parameters
    ----------
    uri : str
        REST catalog server URI

    headers : dict[str, str], optional
        Additional HTTP headers to include in requests

    extra : dict[str, str], optional
        Additional configuration parameters

    auth : IcebergRESTCatalogAuth, optional
        Authentication configuration

    Examples
    --------

    .. tabs::

        .. code-tab:: python REST catalog with basic authentication

            from onetl.connection import Iceberg

            catalog = Iceberg.RESTCatalog(
                uri="https://rest.domain.com:8080",
                auth=Iceberg.RESTCatalog.BasicAuth(
                    user="my_user",
                    password="my_password",
                ),
            )

        .. code-tab:: python REST catalog with bearer token

            from onetl.connection import Iceberg

            catalog = Iceberg.RESTCatalog(
                uri="https://rest.domain.com:8080",
                auth=Iceberg.RESTCatalog.BearerAuth(
                    access_token="my_bearer_token",
                ),
            )

        .. code-tab:: python REST catalog with OAuth2 Client Credentials Flow

            from onetl.connection import Iceberg

            catalog = Iceberg.RESTCatalog(
                uri="https://rest.domain.com:8080",
                auth=Iceberg.RESTCatalog.OAuth2ClientCredentials(
                    client_id="my_client_id",
                    client_secret="my_client_secret",
                ),
            )

        .. code-tab:: python REST catalog with OAuth2 Token Exchange Flow

            from onetl.connection import Iceberg

            catalog = Iceberg.RESTCatalog(
                uri="https://rest.domain.com:8080",
                auth=Iceberg.RESTCatalog.OAuth2TokenExchange(
                    token="user_token",
                    client_id="my_client_id",
                    client_secret="my_client_secret",
                ),
            )

        .. code-tab:: python REST catalog with custom auth

            from onetl.connection import Iceberg

            catalog = Iceberg.RESTCatalog(
                uri="https://rest.domain.com:8080",
                headers={
                    "X-Custom-Auth": "my_custom_token",
                    "X-Request-ID": "request-123",
                },
                extra={
                    "timeout": "30s",
                    "retry": "3",
                },
            )

            \"\"\"
            These options will be passed to Spark config:
            spark.sql.my_catalog.uri = "https://rest.domain.com:8080"
            spark.sql.my_catalog.header.X-Custom-Auth = "my_custom_token"
            spark.sql.my_catalog.header.X-Request-ID = "request-123"
            spark.sql.my_catalog.timeout = "30s"
            spark.sql.my_catalog.retry = "3"
            \"\"\"
    """

    BasicAuth = IcebergRESTCatalogBasicAuth
    BearerAuth = IcebergRESTCatalogBearerAuth
    OAuth2ClientCredentials = IcebergRESTCatalogOAuth2ClientCredentials
    OAuth2TokenExchange = IcebergRESTCatalogOAuth2TokenExchange

    uri: str
    headers: Dict[str, str] = Field(default_factory=dict)
    extra: Dict[str, str] = Field(default_factory=dict)

    auth: Optional[IcebergRESTCatalogAuth] = None

    def get_config(self) -> Dict[str, str]:
        config = {
            "type": "rest",
            "uri": self.uri,
            **self.extra,
        }
        for key, value in self.headers.items():
            config[f"header.{key}"] = value

        if self.auth:
            config.update(self.auth.get_config())

        return config
