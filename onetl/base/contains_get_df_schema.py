# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import TYPE_CHECKING

from typing_extensions import Protocol, runtime_checkable

if TYPE_CHECKING:
    from pyspark.sql.types import StructType


@runtime_checkable
class ContainsGetDFSchemaMethod(Protocol):
    """
    Protocol for objects containing ``get_df_schema`` method
    """

    def get_df_schema(
        self,
        source: str,
        columns: list[str] | None = None,
    ) -> StructType:
        """
        Description of the dataframe schema. |support_hooks|
        """
        ...
