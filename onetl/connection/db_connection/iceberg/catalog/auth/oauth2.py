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


class IcebergRESTCatalogOAuth2(IcebergRESTCatalogAuth, FrozenModel):
    """OAuth2 Authentication for Iceberg REST Catalog.

    .. versionadded:: 0.14.1
    """

    # https://github.com/apache/iceberg/blob/720ef99720a1c59e4670db983c951243dffc4f3e/core/src/main/java/org/apache/iceberg/rest/auth/OAuth2Properties.java#L49-L56
    # https://github.com/apache/iceberg/blob/28555ad8fbad77a4067b6ee2afbdea15428dea26/core/src/main/java/org/apache/iceberg/rest/auth/OAuth2Util.java#L366
    client_secret: SecretStr
    client_id: Optional[str] = None

    # https://github.com/apache/iceberg/blob/720ef99720a1c59e4670db983c951243dffc4f3e/core/src/main/java/org/apache/iceberg/rest/auth/OAuth2Properties.java#L33-L39C58
    token_refresh_interval: Optional[timedelta] = timedelta(hours=1)

    token_exchange_enabled: bool = True

    # by default uses v1/oauth/tokens endpoint of RESTCatalog server
    # https://github.com/apache/iceberg/blob/28555ad8fbad77a4067b6ee2afbdea15428dea26/core/src/main/java/org/apache/iceberg/rest/auth/OAuth2Properties.java#L30-L31C30
    # https://github.com/apache/iceberg/blob/28555ad8fbad77a4067b6ee2afbdea15428dea26/core/src/main/java/org/apache/iceberg/rest/auth/OAuth2Manager.java#L275-L293
    # https://github.com/apache/iceberg/blob/28555ad8fbad77a4067b6ee2afbdea15428dea26/core/src/main/java/org/apache/iceberg/rest/ResourcePaths.java#L57-L59
    oauth2_server_uri: Optional[str] = None

    scopes: List[str] = Field(default_factory=list)
    audience: Optional[str] = None
    resource: Optional[str] = None

    @property
    def type(self) -> str:
        return "oauth2"

    def get_config(self) -> Dict[str, str]:
        config = {
            "rest.auth.type": self.type,
            "credential": (
                f"{self.client_id}:{self.client_secret.get_secret_value()}"
                if self.client_id
                else self.client_secret.get_secret_value()
            ),
            "token-expires-in-ms": (
                str(int(self.token_refresh_interval.total_seconds() * 1000)) if self.token_refresh_interval else None
            ),
            "token-refresh-enabled": stringify(self.token_refresh_interval is not None),
            "token-exchange-enabled": stringify(self.token_exchange_enabled),
            "oauth2-server-uri": self.oauth2_server_uri,
            "scope": " ".join(self.scopes) if self.scopes else None,
            "audience": self.audience,
            "resource": self.resource,
        }
        return {k: v for k, v in config.items() if v is not None}
