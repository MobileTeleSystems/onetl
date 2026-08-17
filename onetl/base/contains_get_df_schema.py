# SPDX-FileCopyrightText: 2023-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from pyspark.sql.types import StructType


@runtime_checkable
class ContainsGetDFSchemaMethod(Protocol):
    """
    Protocol for objects containing `get_df_schema` method
    """

    def get_df_schema(
        self,
        source: str,
        columns: list[str] | None = None,
    ) -> "StructType":
        """
        Description of the dataframe schema. [![support hooks](https://img.shields.io/badge/%20-support%20hooks-blue)][DBR-onetl-hooks]
        """
        ...
