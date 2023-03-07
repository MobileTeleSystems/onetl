from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql.types import StructType

from onetl.base import BaseDBConnection


class SupportDfSchemaNone:
    @classmethod
    def validate_df_schema(
        cls,
        connection: BaseDBConnection,
        df_schema: StructType | None,
    ) -> None:
        if df_schema:
            raise ValueError(f"'df_schema' parameter is not supported by {connection.__class__.__name__}")
