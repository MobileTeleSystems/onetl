# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from typing import Dict, Optional

try:
    from pydantic.v1 import SecretStr
except (ImportError, AttributeError):
    from pydantic import SecretStr  # type: ignore[no-redef, assignment]

from onetl.connection.db_connection.iceberg.catalog.auth import IcebergRESTCatalogAuth
from onetl.impl.frozen_model import FrozenModel


class IcebergRESTCatalogBearerAuth(IcebergRESTCatalogAuth, FrozenModel):
    """Bearer Token Authentication for Iceberg REST Catalog.

    All requests to REST catalog are made with HTTP header ``Authorization: Bearer {access_token}``.

    .. versionadded:: 0.14.1

    Parameters
    ----------
    access_token : str
        `Access token <https://www.oauth.com/oauth2-servers/access-tokens/>`_ for authentication.

    oauth2_server_uri : str, optional
        OAuth2 server URI. If not provided, uses the REST catalog's
        ``v1/oauth/tokens`` endpoint.

    Examples
    --------

    .. code:: python

        from onetl.connection import Iceberg

        auth = Iceberg.RESTCatalog.BearerAuth(
            access_token="my_bearer_token",
        )
    """

    # https://github.com/apache/iceberg/blob/apache-iceberg-1.10.0/core/src/main/java/org/apache/iceberg/rest/auth/OAuth2Manager.java#L96-L99
    # https://github.com/apache/iceberg/blob/apache-iceberg-1.10.0/core/src/main/java/org/apache/iceberg/rest/auth/OAuth2Properties.java#L24-L25
    access_token: SecretStr

    # https://github.com/apache/iceberg/blob/apache-iceberg-1.10.0/core/src/main/java/org/apache/iceberg/rest/auth/OAuth2Manager.java#L275-L293
    oauth2_server_uri: Optional[str] = None

    def get_config(self) -> Dict[str, str]:
        config = {
            "rest.auth.type": "oauth2",
            "token": self.access_token.get_secret_value(),
            "oauth2-server-uri": self.oauth2_server_uri,
        }
        return {k: v for k, v in config.items() if v is not None}
