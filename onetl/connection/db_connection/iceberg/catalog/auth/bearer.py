# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from pydantic import SecretStr

from onetl.connection.db_connection.iceberg.catalog.auth import IcebergRESTCatalogAuth
from onetl.impl import FrozenModel


class IcebergRESTCatalogBearerAuth(IcebergRESTCatalogAuth, FrozenModel):
    """Bearer Token Authentication for Iceberg REST Catalog.

    All requests to REST catalog are made with HTTP header `Authorization: Bearer {access_token}`.

    !!! success "Added in 0.15.0"

    Parameters
    ----------
    access_token : str
        [Access token](https://www.oauth.com/oauth2-servers/access-tokens/) for authentication.

    Examples
    --------

    ```python
    from onetl.connection import Iceberg

    auth = Iceberg.RESTCatalog.BearerAuth(
        access_token="my_bearer_token",
    )
    ```
    """

    # https://github.com/apache/iceberg/blob/720ef99720a1c59e4670db983c951243dffc4f3e/core/src/main/java/org/apache/iceberg/rest/auth/OAuth2Manager.java#L96-L99
    # https://github.com/apache/iceberg/blob/720ef99720a1c59e4670db983c951243dffc4f3e/core/src/main/java/org/apache/iceberg/rest/auth/OAuth2Properties.java#L24-L25
    access_token: SecretStr

    def get_config(self) -> dict[str, str]:
        return {
            "rest.auth.type": "oauth2",
            "token": self.access_token.get_secret_value(),
        }
