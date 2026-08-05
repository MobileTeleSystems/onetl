# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from typing import Any

try:
    from pydantic.v1 import AnyUrl, Field
except (ImportError, AttributeError):
    from pydantic import AnyUrl, Field  # type: ignore[no-redef, assignment]

from onetl._util.spark import stringify
from onetl.connection.db_connection.iceberg.catalog import IcebergCatalog
from onetl.connection.db_connection.iceberg.catalog.auth import (
    IcebergRESTCatalogAuth,
    IcebergRESTCatalogBasicAuth,
    IcebergRESTCatalogBearerAuth,
    IcebergRESTCatalogOAuth2ClientCredentials,
)
from onetl.impl import FrozenModel


class IcebergRESTCatalog(IcebergCatalog, FrozenModel):
    """Iceberg REST Catalog.

    !!! success "Added in 0.15.0"

    Parameters
    ----------
    url : str
        REST catalog server URL

    headers : dict[str, str], optional
        Additional HTTP headers to include in requests

    extra : dict[str, str], optional
        Additional configuration parameters

    auth : IcebergRESTCatalogAuth, optional
        Authentication configuration

    Examples
    --------

    === "REST catalog with basic authentication"

        ```python
        from onetl.connection import Iceberg

        catalog = Iceberg.RESTCatalog(
            url="https://rest.domain.com:8080",
            auth=Iceberg.RESTCatalog.BasicAuth(
                user="my_user",
                password="my_password",
            ),
        )
        ```

    === "REST catalog with bearer token"

        ```python
        from onetl.connection import Iceberg

        catalog = Iceberg.RESTCatalog(
            url="https://rest.domain.com:8080",
            auth=Iceberg.RESTCatalog.BearerAuth(
                access_token="my_bearer_token",
            ),
        )
        ```

    === "REST catalog with OAuth2 Client Credentials Flow"

        ```python
        from onetl.connection import Iceberg

        catalog = Iceberg.RESTCatalog(
            url="https://rest.domain.com:8080",
            auth=Iceberg.RESTCatalog.OAuth2ClientCredentials(
                client_id="my_client_id",
                client_secret="my_client_secret",
            ),
        )
        ```

    === "REST catalog with custom auth"

        ```python
        from onetl.connection import Iceberg

        catalog = Iceberg.RESTCatalog(
            url="https://rest.domain.com:8080",
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
        ```
    """

    BasicAuth = IcebergRESTCatalogBasicAuth
    BearerAuth = IcebergRESTCatalogBearerAuth
    OAuth2ClientCredentials = IcebergRESTCatalogOAuth2ClientCredentials

    url: AnyUrl
    headers: dict[str, Any] = Field(default_factory=dict)
    extra: dict[str, Any] = Field(default_factory=dict)

    auth: IcebergRESTCatalogAuth | None = None

    def get_config(self) -> dict[str, str]:
        config = {
            "type": "rest",
            "uri": str(self.url),
            **stringify(self.extra),
        }
        for key, value in self.headers.items():
            config[f"header.{key}"] = stringify(value)

        if self.auth:
            config.update(self.auth.get_config())

        return config
