# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from abc import ABC, abstractmethod


class IcebergRESTCatalogAuth(ABC):
    """
    Base Iceberg catalog auth interface.

    !!! success "Added in 0.15.0"
    """

    @abstractmethod
    def get_config(self) -> dict[str, str]:
        """Return REST catalog auth configuration."""
