# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Dict


class IcebergCatalog(ABC):
    """
    Base Iceberg catalog interface.

    .. versionadded:: 0.14.1
    """

    @abstractmethod
    def get_config(self) -> Dict[str, str]:
        """Return flat dict with catalog configuration."""
