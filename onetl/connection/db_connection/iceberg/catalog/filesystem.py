# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from typing import Dict

from onetl.connection.db_connection.iceberg.catalog import IcebergCatalog
from onetl.impl.frozen_model import FrozenModel


class IcebergFilesystemCatalog(IcebergCatalog, FrozenModel):
    """Iceberg Filesystem Catalog (Hadoop Catalog).

    .. versionadded:: 0.15.0

    .. note::

        This catalog stores Iceberg tables as nested directories:

        .. code-block:: text

            {warehouse.path}/{schema}/{table}

        This means that tables **cannot have a custom location** and will always be created
        under the schema directory inside the warehouse path.

    Examples
    --------

    .. code:: python

        from onetl.connection import Iceberg

        catalog = Iceberg.FilesystemCatalog()
    """

    def get_config(self) -> Dict[str, str]:
        return {
            "type": "hadoop",
        }
