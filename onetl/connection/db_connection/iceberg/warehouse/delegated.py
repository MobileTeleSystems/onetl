# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import Any, Dict, Optional

from typing_extensions import Literal

from onetl.hooks import slot, support_hooks

try:
    from pydantic.v1 import Field
except (ImportError, AttributeError):
    from pydantic import Field  # type: ignore[no-redef, assignment]

from onetl._util.spark import stringify
from onetl.connection.db_connection.iceberg.warehouse import IcebergWarehouse
from onetl.impl.frozen_model import FrozenModel


@support_hooks
class IcebergDelegatedWarehouse(IcebergWarehouse, FrozenModel):
    """Delegate configuring Iceberg warehouse to Iceberg catalog. |support_hooks|

    Used by some Iceberg catalog implementations like:
      * `Lakekeeper <https://docs.lakekeeper.io/docs/latest/storage/#s3>`_
      * `Polaris <https://polaris.apache.org/in-dev/unreleased/polaris-spark-client/>`_
      * `Apache Gravitino <https://gravitino.apache.org/docs/1.0.0/security/credential-vending/>`_
      * `Databricks Unity Catalog <https://docs.databricks.com/aws/en/external-access/iceberg#use-iceberg-tables-with-apache-spark>`_

    .. versionadded:: 0.15.0

    Parameters
    ----------
    name : str, optional
        Warehouse name/alias, if supported by specific Iceberg catalog

    access_delegation : "vended-credentials" | "remote-signing"
        Value of `X-Iceberg-Access-Delegation <https://github.com/apache/iceberg/blob/apache-iceberg-1.10.0/open-api/rest-catalog-open-api.yaml#L1854>`_ header.

    extra : Dict[str, str], default: {}
        Additional configuration parameters

    Examples
    --------

    .. tabs::

        .. code-tab:: python S3 client with vended credentials

            from onetl.connection import Iceberg

            warehouse = Iceberg.DeletatedWarehouse(
                name="my-warehouse",
                access_delegation="vended-credentials",
                # other params passed to S3 client (optional)
                extra={"client.region": "us-east-1"},
            )

        .. code-tab:: python S3 client with remote signing

            from onetl.connection import Iceberg

            warehouse = Iceberg.DeletatedWarehouse(
                name="my-warehouse",
                access_delegation="remote-signing",
                # other params passed to S3 client (optional)
                extra={"client.region": "us-east-1"},
            )
    """

    name: Optional[str] = None
    access_delegation: Literal["vended-credentials", "remote-signing"]
    extra: Dict[str, Any] = Field(default_factory=dict)

    @slot
    def get_config(self) -> dict[str, str]:
        config = {
            "warehouse": self.name,
            "header.X-Iceberg-Access-Delegation": self.access_delegation,
            **stringify(self.extra),
        }
        return {k: v for k, v in config.items() if v is not None}
