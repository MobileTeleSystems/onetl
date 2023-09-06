from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas

if TYPE_CHECKING:
    from pyspark.sql import DataFrame as SparkDataFrame


logger = logging.getLogger(__name__)


def fix_pyspark_df(df: SparkDataFrame) -> SparkDataFrame:
    """
    Fix Spark DataFrame column types before converting it to Pandas DataFrame.

    Using ``df.toPandas()`` on Spark 3.x with Pandas 2.x raises the following exception:

    .. code::

        TypeError: Casting to unit-less dtype 'datetime64' is not supported. Pass e.g. 'datetime64[ns]' instead.

    This method converts dates and timestamps to strings, to convert them back to original type later.

    TODO: remove after https://issues.apache.org/jira/browse/SPARK-43194
    """
    from pyspark.sql.functions import date_format
    from pyspark.sql.types import DateType, TimestampType

    date_types = (DateType,)
    try:
        from pyspark.sql.types import TimestampNTZType

        datetime_types = (TimestampType, TimestampNTZType)
    except (ImportError, AttributeError):
        datetime_types = (TimestampType,)

    for field in df.schema:
        column = field.name
        column_name = column.lower()

        if "datetime" in column_name or isinstance(field.dataType, datetime_types):
            df = df.withColumn(column, df[column].cast("string"))
        elif "date" in column_name or isinstance(field.dataType, date_types):
            df = df.withColumn(column, date_format(df[column], "yyyy-MM-dd"))

    return df


def parse_datetime(value: pandas.Series) -> pandas.Series:
    """
    Try to convert datetime from Spark format to Pandas.

    Different Spark versions may use different formats (e.g. with of without ``Z`` suffix for UTC timezone),
    different databases may use produce timestamps with different accuracy.
    """
    try:
        return pandas.to_datetime(value, format="ISO8601")
    except ValueError:
        logger.exception("Unable to parse datetime")

    try:
        return pandas.to_datetime(value, format="%Y-%m-%d %H:%M:%S.%f")
    except ValueError:
        logger.exception("Unable to parse datetime")

    return pandas.to_datetime(value, format="%Y-%m-%d %H:%M:%S", exact=False)


def parse_date(
    value: pandas.Series,
) -> pandas.Series:
    """Try to convert date from Spark format to Pandas."""
    return pandas.to_datetime(value, format="%Y-%m-%d", exact=False)


def fix_pandas_df(
    df: pandas.DataFrame,
) -> pandas.DataFrame:
    """Convert date and datetime columns from strings back to original dtypes."""
    for column in df.columns:
        column_name = column.lower()

        if "datetime" in column_name:
            df[column] = parse_datetime(df[column])
        elif "date" in column_name:
            df[column] = parse_date(df[column]).dt.date

    return df


def to_pandas(df: SparkDataFrame | pandas.DataFrame) -> pandas.DataFrame:
    """
    Convert Spark DataFrame to Pandas DataFrame.

    Like ``df.toPandas()``, but with extra steps.
    """
    if isinstance(df, pandas.DataFrame):
        return fix_pandas_df(df)

    fixed_spark_df = fix_pyspark_df(df)
    pandas_df = fixed_spark_df.toPandas()
    return fix_pandas_df(pandas_df)
