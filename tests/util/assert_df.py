from __future__ import annotations

from typing import TYPE_CHECKING, OrderedDict

import pandas

from tests.util.pandas_df import lowercase_columns

try:
    from pandas.testing import assert_frame_equal
except (ImportError, NameError):
    from pandas.util.testing import assert_frame_equal

if TYPE_CHECKING:
    from pyspark.sql import DataFrame as SparkDataFrame


def assert_equal_df(
    left_df: pandas.DataFrame | SparkDataFrame,
    right_df: pandas.DataFrame | SparkDataFrame,
    order_by: str | None = None,
    **kwargs,
) -> None:
    """Checks that right_df equal to left_df"""

    # Oracle returns column names in UPPERCASE, convert them back to lowercase
    # Nota: this is only for dataframe comparison purpose
    left_pdf = lowercase_columns(left_df if isinstance(left_df, pandas.DataFrame) else left_df.toPandas())
    right_pdf = lowercase_columns(right_df if isinstance(right_df, pandas.DataFrame) else right_df.toPandas())

    if order_by:
        left_pdf = left_pdf.sort_values(by=order_by.lower())
        right_pdf = right_pdf.sort_values(by=order_by.lower())

        left_pdf.reset_index(inplace=True, drop=True)
        right_pdf.reset_index(inplace=True, drop=True)

    # ignore columns order
    left_pdf = left_pdf.sort_index(axis=1)
    right_pdf = right_pdf.sort_index(axis=1)

    assert_frame_equal(
        left=left_pdf,
        right=right_pdf,
        check_dtype=False,
        **kwargs,
    )


def assert_subset_df(
    small_df: pandas.DataFrame | SparkDataFrame,
    large_df: pandas.DataFrame | SparkDataFrame,
    columns: list[str] | None = None,
) -> None:
    """Checks that left_df is subset of right_df"""

    small_pdf = lowercase_columns(small_df if isinstance(small_df, pandas.DataFrame) else small_df.toPandas())
    large_pdf = lowercase_columns(large_df if isinstance(large_df, pandas.DataFrame) else large_df.toPandas())

    if columns is None:
        left_keys = OrderedDict.fromkeys(column.lower() for column in small_pdf.columns)
        left_keys.update({column.lower(): None for column in large_pdf.columns})
        columns = list(left_keys.keys())
    else:
        columns = [column.lower() for column in columns]

    for column in columns:  # noqa: WPS528
        assert small_pdf[column].isin(large_pdf[column]).all()  # noqa: S101
