# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from onetl.connection.db_connection.iceberg.catalog import IcebergCatalog
from onetl.impl import FrozenModel


class IcebergFilesystemCatalog(IcebergCatalog, FrozenModel):
    """Iceberg Filesystem Catalog (Hadoop Catalog).

    !!! success "Added in 0.15.0"

    !!! note

        This catalog stores Iceberg tables as nested directories:

        ```text
        {warehouse.path}/{schema}/{table}
        ```
        This means that tables **cannot have a custom location** and will always be created
        under the schema directory inside the warehouse path.

    Examples
    --------

    ```python
    from onetl.connection import Iceberg

    catalog = Iceberg.FilesystemCatalog()
    ```
    """

    def get_config(self) -> dict[str, str]:
        return {
            "type": "hadoop",
        }
