# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from typing import Any, Literal

from pydantic import Field

from onetl._util.spark import stringify
from onetl.connection.db_connection.iceberg.warehouse import IcebergWarehouse
from onetl.hooks import slot, support_hooks
from onetl.impl import FrozenModel


@support_hooks
class IcebergDelegatedWarehouse(IcebergWarehouse, FrozenModel):
    """Delegate configuring Iceberg warehouse to Iceberg catalog. [![support hooks](https://img.shields.io/badge/%20-support%20hooks-blue)][DBR-onetl-hooks]

    Used by some Iceberg catalog implementations like:
      * [Lakekeeper](https://docs.lakekeeper.io/docs/latest/storage/#s3)
      * [Polaris](https://polaris.apache.org/in-dev/unreleased/polaris-spark-client/)
      * [Apache Gravitino](https://gravitino.apache.org/docs/1.0.0/security/credential-vending/)
      * [Databricks Unity Catalog](https://docs.databricks.com/aws/en/external-access/iceberg#use-iceberg-tables-with-apache-spark)

    !!! success "Added in 0.15.0"

    Parameters
    ----------
    name
        Warehouse name/alias, if supported by specific Iceberg catalog

    access_delegation
        Value of [X-Iceberg-Access-Delegation](https://github.com/apache/iceberg/blob/apache-iceberg-1.10.0/open-api/rest-catalog-open-api.yaml#L1854) header.

    extra
        Additional configuration parameters

    Examples
    --------

    === "S3 client with vended credentials"

        ```python
        from onetl.connection import Iceberg

        warehouse = Iceberg.DeletatedWarehouse(
            name="my-warehouse",
            access_delegation="vended-credentials",
            # other params passed to S3 client (optional)
            extra={"client.region": "us-east-1"},
        )
        ```

    === "S3 client with remote signing"

        ```python
        from onetl.connection import Iceberg

        warehouse = Iceberg.DeletatedWarehouse(
            name="my-warehouse",
            access_delegation="remote-signing",
            # other params passed to S3 client (optional)
            extra={"client.region": "us-east-1"},
        )
        ```
    """

    name: str | None = None
    access_delegation: Literal["vended-credentials", "remote-signing"]
    extra: dict[str, Any] = Field(default_factory=dict)

    @slot
    def get_config(self) -> dict[str, str]:
        config = {
            "warehouse": self.name,
            "header.X-Iceberg-Access-Delegation": self.access_delegation,
            **stringify(self.extra),
        }
        return {k: v for k, v in config.items() if v is not None}
