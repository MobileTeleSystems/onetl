# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from typing import Dict

from onetl.connection.db_connection.iceberg.catalog import IcebergCatalog
from onetl.connection.db_connection.iceberg.warehouse.filesystem import (
    IcebergFilesystemWarehouse,
)
from onetl.impl.frozen_model import FrozenModel


class IcebergFilesystemCatalog(IcebergCatalog, FrozenModel):
    @property
    def type(self) -> str:
        return "hadoop"

    def get_config(self, warehouse: IcebergFilesystemWarehouse) -> Dict[str, str]:
        return {
            "type": self.type,
            **warehouse.get_config(),
        }
