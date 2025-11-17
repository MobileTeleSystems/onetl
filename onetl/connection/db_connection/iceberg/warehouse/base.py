# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from abc import ABC, abstractmethod


class IcebergWarehouse(ABC):
    """
    Base Iceberg warehouse interface.

    .. versionadded:: 0.14.1
    """

    @abstractmethod
    def get_config(self) -> dict[str, str]:
        """Return flat dict with warehouse configuration."""
