# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from abc import ABC, abstractmethod
from typing import Dict

from onetl.base import PurePathProtocol


class IcebergWarehouse(ABC):
    """
    Base Iceberg warehouse interface.

    .. versionadded:: 0.14.1
    """

    path: PurePathProtocol

    @abstractmethod
    def get_config(self) -> Dict[str, str]:
        """Return flat dict with warehouse configuration."""
