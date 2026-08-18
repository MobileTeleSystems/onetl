# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from abc import ABC, abstractmethod


class IcebergWarehouse(ABC):
    """
    Base Iceberg warehouse interface.

    !!! success "Added in 0.15.0"
    """

    @abstractmethod
    def get_config(self) -> dict[str, str]:
        """Return flat dict with warehouse configuration."""
