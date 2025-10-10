# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from typing import Dict

try:
    from pydantic.v1 import SecretStr
except (ImportError, AttributeError):
    from pydantic import SecretStr  # type: ignore[no-redef, assignment]

from onetl.connection.db_connection.iceberg.catalog.auth import IcebergRESTCatalogAuth
from onetl.impl.frozen_model import FrozenModel


class IcebergRESTCatalogBearerAuth(IcebergRESTCatalogAuth, FrozenModel):
    """Bearer Token Authentication for Iceberg REST Catalog.

    .. versionadded:: 0.14.1

    Parameters
    ----------
    access_token : str
        Bearer token for authentication

    Examples
    --------

    .. code:: python

        from onetl.connection import Iceberg

        auth = Iceberg.RESTCatalog.BearerAuth(
            access_token="my_bearer_token",
        )
    """

    # https://github.com/apache/iceberg/blob/28555ad8fbad77a4067b6ee2afbdea15428dea26/core/src/main/java/org/apache/iceberg/rest/auth/OAuth2Manager.java#L40
    # https://github.com/apache/iceberg/blob/720ef99720a1c59e4670db983c951243dffc4f3e/core/src/main/java/org/apache/iceberg/rest/auth/OAuth2Properties.java#L21
    access_token: SecretStr

    @property
    def type(self) -> str:
        return "oauth2"

    def get_config(self) -> Dict[str, str]:
        return {
            "rest.auth.type": self.type,
            "token": self.access_token.get_secret_value(),
        }
