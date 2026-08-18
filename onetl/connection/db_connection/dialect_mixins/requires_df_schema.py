# SPDX-FileCopyrightText: 2023-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0


from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql.types import StructType

    from onetl.base import BaseDBConnection


class RequiresDFSchema:
    connection: "BaseDBConnection"

    def validate_df_schema(
        self,
        df_schema: "StructType | None",
    ) -> "StructType":
        if df_schema:
            return df_schema
        msg = f"'df_schema' parameter is mandatory for {self.connection.__class__.__name__}"
        raise ValueError(msg)
