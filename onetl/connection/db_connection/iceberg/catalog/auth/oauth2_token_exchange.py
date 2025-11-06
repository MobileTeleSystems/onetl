# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from datetime import timedelta
from typing import Dict, List, Optional

try:
    from pydantic.v1 import Field, SecretStr
except (ImportError, AttributeError):
    from pydantic import Field, SecretStr  # type: ignore[no-redef, assignment]

from onetl._util.spark import stringify
from onetl.connection.db_connection.iceberg.catalog.auth import IcebergRESTCatalogAuth
from onetl.impl.frozen_model import FrozenModel


class IcebergRESTCatalogOAuth2TokenExchange(IcebergRESTCatalogAuth, FrozenModel):
    """OAuth2 Token Exchange Flow authentication for Iceberg REST Catalog.

    While creating new REST catalog session, client_token is fetched via OAuth2 server HTTP endpoint
    with `grant_type=client_credentials <https://www.oauth.com/oauth2-servers/access-tokens/client-credentials/>`_.

    Then user token is exchanged to a new access_token by sending a request
    to OAuth2 server HTTP endpoint with header ``Authorization: Bearer {client_token}`` and
    POST body param `grant_type=token_exchange <https://datatracker.ietf.org/doc/html/rfc8693>`_.

    .. versionadded:: 0.14.1

    Parameters
    ----------
    token : str
        User JWT token to be exchanged into access token.

    client_secret : str, optional
        OAuth2 client secret.

    client_id : str, optional
        OAuth2 client ID. In most OAuth2 server implementations it is `mandatory <https://www.oauth.com/oauth2-servers/client-registration/client-id-secret/>`_.

    token_refresh_interval : timedelta, optional
        Interval for `automatic token refresh <https://www.oauth.com/oauth2-servers/access-tokens/refreshing-access-tokens/>`_.
        Default: 1 hour. Set to `None` to disable automatic refresh.

    oauth2_server_uri : str, optional
        OAuth2 server URI. If not provided, uses the REST catalog's
        ``v1/oauth/tokens`` endpoint.

    scopes : List[str], default: []
        `OAuth2 scopes <https://www.oauth.com/oauth2-servers/scope/>`_ to request.

    audience : str, optional
        OAuth2 audience parameter.

    resource : str, optional
        OAuth2 resource parameter.

    Examples
    --------

    .. tabs::

        .. code-tab:: python OAuth2

            from onetl.connection import Iceberg

            auth = Iceberg.RESTCatalog.OAuth2TokenExchange(
                token="user_token",
                client_id="my_client_id",
                client_secret="my_client_secret",
            )

        .. code-tab:: python OAuth2 with optional fields

            from datetime import timedelta
            from onetl.connection import Iceberg

            auth = Iceberg.RESTCatalog.OAuth2TokenExchange(
                user_token="...",
                client_id="my_client_id",
                client_secret="my_client_secret",
                scopes=["catalog:read"],
                oauth2_server_uri="https://oauth.example.com/token",
                token_refresh_interval=timedelta(minutes=30),
                audience="iceberg-catalog",
            )
    """

    # https://github.com/apache/iceberg/blob/720ef99720a1c59e4670db983c951243dffc4f3e/core/src/main/java/org/apache/iceberg/rest/auth/OAuth2Manager.java#L81-L95
    # https://github.com/apache/iceberg/blob/720ef99720a1c59e4670db983c951243dffc4f3e/core/src/main/java/org/apache/iceberg/rest/auth/OAuth2Util.java#L166-L180
    # https://github.com/apache/iceberg/blob/720ef99720a1c59e4670db983c951243dffc4f3e/core/src/main/java/org/apache/iceberg/rest/auth/OAuth2Util.java#L636-L639
    # https://github.com/apache/iceberg/blob/720ef99720a1c59e4670db983c951243dffc4f3e/core/src/main/java/org/apache/iceberg/rest/auth/OAuth2Util.java#L193-L222

    # https://github.com/apache/iceberg/blob/720ef99720a1c59e4670db983c951243dffc4f3e/core/src/main/java/org/apache/iceberg/rest/auth/OAuth2Properties.java#L24-L25
    token: SecretStr

    # https://github.com/apache/iceberg/blob/720ef99720a1c59e4670db983c951243dffc4f3e/core/src/main/java/org/apache/iceberg/rest/auth/OAuth2Properties.java#L49-L56
    # https://github.com/apache/iceberg/blob/720ef99720a1c59e4670db983c951243dffc4f3e/core/src/main/java/org/apache/iceberg/rest/auth/OAuth2Util.java#L366
    client_secret: SecretStr
    client_id: Optional[str] = None

    # https://github.com/apache/iceberg/blob/720ef99720a1c59e4670db983c951243dffc4f3e/core/src/main/java/org/apache/iceberg/rest/auth/OAuth2Properties.java#L33-L39C58
    token_refresh_interval: Optional[timedelta] = timedelta(hours=1)

    # by default uses v1/oauth/tokens endpoint of RESTCatalog server
    # https://github.com/apache/iceberg/blob/720ef99720a1c59e4670db983c951243dffc4f3e/core/src/main/java/org/apache/iceberg/rest/auth/OAuth2Properties.java#L30-L31C30
    # https://github.com/apache/iceberg/blob/720ef99720a1c59e4670db983c951243dffc4f3e/core/src/main/java/org/apache/iceberg/rest/auth/OAuth2Manager.java#L275-L293
    # https://github.com/apache/iceberg/blob/720ef99720a1c59e4670db983c951243dffc4f3e/core/src/main/java/org/apache/iceberg/rest/ResourcePaths.java#L57-L59
    oauth2_server_uri: Optional[str] = None

    scopes: List[str] = Field(default_factory=list)
    audience: Optional[str] = None
    resource: Optional[str] = None

    def get_config(self) -> Dict[str, str]:
        config = {
            "rest.auth.type": "oauth2",
            "token-exchange-enabled": "true",
            "token": self.token.get_secret_value(),
            "credential": (
                f"{self.client_id}:{self.client_secret.get_secret_value()}"
                if self.client_id is not None
                else self.client_secret.get_secret_value()
            ),
            "token-expires-in-ms": (
                str(int(self.token_refresh_interval.total_seconds() * 1000)) if self.token_refresh_interval else None
            ),
            "token-refresh-enabled": stringify(self.token_refresh_interval is not None),
            "oauth2-server-uri": self.oauth2_server_uri,
            "scope": " ".join(self.scopes) if self.scopes else None,
            "audience": self.audience,
            "resource": self.resource,
        }
        return {k: v for k, v in config.items() if v is not None}
