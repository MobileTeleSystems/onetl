# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from pydantic import ConfigDict

from onetl.impl import GenericOptions


class IcebergExtra(GenericOptions):
    """
    Extra options for Iceberg connection.

    You can pass here any parameters supported by
    [Iceberg](https://iceberg.apache.org/docs/latest/spark-configuration/),
    even if it is not mentioned in this documentation.

    Pass properties **without catalog prefix**. For example:

    ```python
    extra = {
        "cache-enabled": "true",
        "cache.expiration-interval-ms": "40000",
    }
    ```
    This will be translated to:

    ```ini
    spark.sql.catalog.my_catalog.cache-enabled = 'true'
    spark.sql.catalog.my_catalog.cache.expiration-interval-ms = '40000'
    ```
    """

    model_config = ConfigDict(extra="allow")
