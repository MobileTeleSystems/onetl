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

    .. versionadded:: 0.14.1
    """

    # https://github.com/apache/iceberg/blob/28555ad8fbad77a4067b6ee2afbdea15428dea26/core/src/main/java/org/apache/iceberg/rest/auth/BasicAuthManager.java
    user: str
    password: SecretStr

    @property
    def type(self) -> str:
        return "basic"

    def get_config(self) -> Dict[str, str]:
        config = {
            "rest.auth.type": self.type,
            "rest.auth.basic.username": self.user,
            "rest.auth.basic.password": self.password.get_secret_value(),
        }
        return config
