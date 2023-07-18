from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame as SparkDataFrame


def reset_column_names(df: SparkDataFrame, columns: list[str] | None = None) -> SparkDataFrame:
    """
    Reset columns to ``_c0`` format.

    If `columns` is None, reset all columns names.
    """
    columns = columns or df.columns
    for i, column in enumerate(columns):
        df = df.withColumnRenamed(column, f"_c{i}")
    return df
