# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql.types import StructType

from onetl.base import BaseDBConnection


class RequiresDFSchema:
    connection: BaseDBConnection

    def validate_df_schema(
        self,
        df_schema: StructType | None,
    ) -> StructType:
        if df_schema:
            return df_schema
        raise ValueError(f"'df_schema' parameter is mandatory for {self.connection.__class__.__name__}")
