# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from datetime import timedelta
from typing import Annotated

from pydantic import Field, HttpUrl, SecretStr, UrlConstraints

from onetl._util.spark import stringify
from onetl.connection.db_connection.iceberg.catalog.auth import IcebergRESTCatalogAuth
from onetl.impl import FrozenModel


class IcebergRESTCatalogOAuth2ClientCredentials(IcebergRESTCatalogAuth, FrozenModel):
    """OAuth2 Client Credentials Flow authentication for Iceberg REST Catalog.

    While creating new REST catalog session, new access token is fetched via OAuth2 server HTTP endpoint
    with [grant_type=client_credentials](https://www.oauth.com/oauth2-servers/access-tokens/client-credentials/).

    After that, all requests to REST catalog are made with a HTTP header `Authorization: Bearer {access_token}`.

    !!! success "Added in 0.15.0"

    Parameters
    ----------
    client_secret : str
        OAuth2 client secret.

    client_id : str, optional
        OAuth2 client ID. In most OAuth2 server implementations it is [mandatory](https://www.oauth.com/oauth2-servers/client-registration/client-id-secret/).

    token_refresh_interval : timedelta, optional
        Interval for [automatic token refresh](https://www.oauth.com/oauth2-servers/access-tokens/refreshing-access-tokens/).
        Default: 1 hour. Set to `None` to disable automatic refresh.

    oauth2_token_endpoint : str, optional
        OAuth2 endpoint for fetching tokens. If not provided, uses the REST catalog's
        `v1/oauth/tokens` endpoint.

    scopes : List[str], default: []
        [OAuth2 scopes](https://www.oauth.com/oauth2-servers/scope/) to request.

    audience : str, optional
        OAuth2 `audience` param.

    resource : str, optional
        OAuth2 `resource` param.

    Examples
    --------

    === "OAuth2"

        ```python
        from onetl.connection import Iceberg

        auth = Iceberg.RESTCatalog.OAuth2ClientCredentials(
            client_id="my_client_id",
            client_secret="my_client_secret",
        )
        ```

    === "OAuth2 with optional fields"

        ```python
        from datetime import timedelta
        from onetl.connection import Iceberg

        auth = Iceberg.RESTCatalog.OAuth2ClientCredentials(
            client_id="my_client_id",
            client_secret="my_client_secret",
            scopes=["catalog:read"],
            oauth2_token_endpoint="http://keycloak.domain.com/realms/my-realm/protocol/openid-connect/token",
            token_refresh_interval=timedelta(minutes=30),
            audience="iceberg-catalog",
        )
        ```
    """

    # https://github.com/apache/iceberg/blob/720ef99720a1c59e4670db983c951243dffc4f3e/core/src/main/java/org/apache/iceberg/rest/auth/OAuth2Manager.java#L81-L95
    # https://github.com/apache/iceberg/blob/720ef99720a1c59e4670db983c951243dffc4f3e/core/src/main/java/org/apache/iceberg/rest/auth/OAuth2Util.java#L641
    # https://github.com/apache/iceberg/blob/720ef99720a1c59e4670db983c951243dffc4f3e/core/src/main/java/org/apache/iceberg/rest/auth/OAuth2Util.java#L277-L300
    # https://github.com/apache/iceberg/blob/720ef99720a1c59e4670db983c951243dffc4f3e/core/src/main/java/org/apache/iceberg/rest/auth/OAuth2Util.java#L188-L190

    # https://github.com/apache/iceberg/blob/720ef99720a1c59e4670db983c951243dffc4f3e/core/src/main/java/org/apache/iceberg/rest/auth/OAuth2Properties.java#L49-L56
    # https://github.com/apache/iceberg/blob/720ef99720a1c59e4670db983c951243dffc4f3e/core/src/main/java/org/apache/iceberg/rest/auth/OAuth2Util.java#L366
    # https://github.com/apache/iceberg/blob/720ef99720a1c59e4670db983c951243dffc4f3e/core/src/main/java/org/apache/iceberg/rest/auth/OAuth2Util.java#L389-L404
    client_secret: SecretStr
    client_id: str | None = None

    # https://github.com/apache/iceberg/blob/720ef99720a1c59e4670db983c951243dffc4f3e/core/src/main/java/org/apache/iceberg/rest/auth/OAuth2Properties.java#L33-L39C58
    token_refresh_interval: timedelta | None = timedelta(hours=1)

    # by default uses v1/oauth/tokens endpoint of RESTCatalog server
    # https://github.com/apache/iceberg/blob/720ef99720a1c59e4670db983c951243dffc4f3e/core/src/main/java/org/apache/iceberg/rest/auth/OAuth2Properties.java#L30-L31C30
    # https://github.com/apache/iceberg/blob/720ef99720a1c59e4670db983c951243dffc4f3e/core/src/main/java/org/apache/iceberg/rest/auth/OAuth2Manager.java#L275-L293
    # https://github.com/apache/iceberg/blob/720ef99720a1c59e4670db983c951243dffc4f3e/core/src/main/java/org/apache/iceberg/rest/ResourcePaths.java#L57-L59
    oauth2_token_endpoint: (
        Annotated[
            HttpUrl,
            UrlConstraints(host_required=True, preserve_empty_path=True),
        ]
        | None
    ) = None

    scopes: list[str] = Field(default_factory=list)
    audience: str | None = None
    resource: str | None = None

    def get_config(self) -> dict[str, str]:
        config = {
            "rest.auth.type": "oauth2",
            "token-exchange-enabled": "false",
            "credential": (
                f"{self.client_id}:{self.client_secret.get_secret_value()}"
                if self.client_id is not None
                else self.client_secret.get_secret_value()
            ),
            "token-expires-in-ms": (
                str(int(self.token_refresh_interval.total_seconds() * 1000)) if self.token_refresh_interval else None
            ),
            "token-refresh-enabled": stringify(self.token_refresh_interval is not None),
            "oauth2-server-uri": str(self.oauth2_token_endpoint) if self.oauth2_token_endpoint else None,
            "scope": " ".join(self.scopes) if self.scopes else None,
            "audience": self.audience,
            "resource": self.resource,
        }
        return {k: v for k, v in config.items() if v is not None}
