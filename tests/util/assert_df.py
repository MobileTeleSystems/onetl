from __future__ import annotations

from typing import TYPE_CHECKING, OrderedDict

import pandas

from tests.util.pandas_df import lowercase_columns
from tests.util.to_pandas import to_pandas

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
    left_df = lowercase_columns(to_pandas(left_df))
    right_df = lowercase_columns(to_pandas(right_df))

    if order_by:
        left_df = left_df.sort_values(by=order_by.lower())
        right_df = right_df.sort_values(by=order_by.lower())

        left_df.reset_index(inplace=True, drop=True)
        right_df.reset_index(inplace=True, drop=True)

    # ignore columns order
    left_df = left_df.sort_index(axis=1)
    right_df = right_df.sort_index(axis=1)

    assert_frame_equal(
        left=left_df,
        right=right_df,
        check_dtype=False,
        **kwargs,
    )


def assert_subset_df(
    small_df: pandas.DataFrame | SparkDataFrame,
    large_df: pandas.DataFrame | SparkDataFrame,
    columns: list[str] | None = None,
) -> None:
    """Checks that left_df is subset of right_df"""

    small_pdf = lowercase_columns(to_pandas(small_df))
    large_pdf = lowercase_columns(to_pandas(large_df))

    if columns is None:
        left_keys = OrderedDict.fromkeys(column.lower() for column in small_pdf.columns)
        left_keys.update({column.lower(): None for column in large_pdf.columns})
        columns = list(left_keys.keys())
    else:
        columns = [column.lower() for column in columns]

    for column in columns:  # noqa: WPS528
        assert small_pdf[column].isin(large_pdf[column]).all()  # noqa: S101
