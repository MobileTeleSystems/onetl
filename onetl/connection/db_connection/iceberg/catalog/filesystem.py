# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from typing import Dict

from onetl.connection.db_connection.iceberg.catalog import IcebergCatalog
from onetl.impl.frozen_model import FrozenModel


class IcebergFilesystemCatalog(IcebergCatalog, FrozenModel):
    @property
    def type(self) -> str:
        return "hadoop"

    def get_config(self) -> Dict[str, str]:
        config = {
            "type": self.type,
        }
        return config
