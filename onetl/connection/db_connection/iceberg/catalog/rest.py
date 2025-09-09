# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import Dict

try:
    from pydantic.v1 import Field
except (ImportError, AttributeError):
    from pydantic import Field  # type: ignore[no-redef, assignment]

from onetl.connection.db_connection.iceberg.catalog import IcebergCatalog
from onetl.impl.frozen_model import FrozenModel


class IcebergRESTCatalog(IcebergCatalog, FrozenModel):
    """Iceberg REST Catalog.

    .. versionadded:: 0.15.0
    """

    uri: str
    headers: Dict[str, str] = Field(default_factory=dict)
    extra: Dict[str, str] = Field(default_factory=dict)

    @property
    def type(self) -> str:
        return "rest"

    def get_config(self) -> Dict[str, str]:
        config = {
            "type": self.type,
            "uri": self.uri,
            **self.extra,
        }
        for key, value in self.headers.items():
            config[f"header.{key}"] = value
        return config
