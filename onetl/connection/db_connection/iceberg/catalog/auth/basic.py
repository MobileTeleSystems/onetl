# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from typing import Dict

try:
    from pydantic.v1 import SecretStr
except (ImportError, AttributeError):
    from pydantic import SecretStr  # type: ignore[no-redef, assignment]

from onetl.connection.db_connection.iceberg.catalog.auth import IcebergRESTCatalogAuth
from onetl.impl.frozen_model import FrozenModel


class IcebergRESTCatalogBasicAuth(IcebergRESTCatalogAuth, FrozenModel):
    """Basic Authentication for Iceberg REST Catalog.

    All requests to REST catalog are made with HTTP header ``Authorization: Basic {user}:{password}``.

    .. versionadded:: 0.14.1

    Parameters
    ----------
    user : str
        Username for authentication.

    password : str
        Password for authentication.

    Examples
    --------

    .. code:: python

        from onetl.connection import Iceberg

        auth = Iceberg.RESTCatalog.BasicAuth(
            user="my_user",
            password="my_password",
        )
    """

    # https://github.com/apache/iceberg/blob/720ef99720a1c59e4670db983c951243dffc4f3e/core/src/main/java/org/apache/iceberg/rest/auth/BasicAuthManager.java
    # https://github.com/apache/iceberg/blob/720ef99720a1c59e4670db983c951243dffc4f3e/core/src/main/java/org/apache/iceberg/rest/auth/AuthProperties.java#L44-L45
    user: str
    password: SecretStr

    def get_config(self) -> Dict[str, str]:
        return {
            "rest.auth.type": "basic",
            "rest.auth.basic.username": self.user,
            "rest.auth.basic.password": self.password.get_secret_value(),
        }
