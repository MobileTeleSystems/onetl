from __future__ import annotations

from abc import ABC, abstractmethod
from collections import defaultdict
from datetime import date, datetime, timedelta
from logging import getLogger
from random import randint
from typing import TYPE_CHECKING

import pandas

try:
    from pandas.testing import assert_frame_equal
except (ImportError, NameError):
    from pandas.util.testing import assert_frame_equal

if TYPE_CHECKING:
    from pyspark.sql import DataFrame as SparkDataFrame
    from pyspark.sql import SparkSession

logger = getLogger(__name__)


class BaseProcessing(ABC):
    _df_max_length: int = 100
    _column_types_and_names_matching: dict[str, str] = {}

    column_names: list[str] = ["id_int", "text_string", "hwm_int", "hwm_date", "hwm_datetime", "float_value"]

    def create_schema_ddl(
        self,
        schema: str,
    ) -> str:
        return f"CREATE SCHEMA IF NOT EXISTS {schema}"

    @abstractmethod
    def create_schema(
        self,
        schema: str,
    ) -> None:
        ...

    def create_table_ddl(
        self,
        table: str,
        fields: dict[str, str],
        schema: str,
    ) -> str:
        str_fields = ", ".join([f"{key} {value}" for key, value in fields.items()])
        return f"CREATE TABLE {schema}.{table} ({str_fields})"

    @abstractmethod
    def create_table(
        self,
        table: str,
        fields: dict[str, str],
        schema: str,
    ) -> None:
        ...

    def drop_database_ddl(
        self,
        schema: str,
    ) -> str:
        return f"DROP DATABASE IF EXISTS {schema}"

    @abstractmethod
    def drop_database(
        self,
        schema: str,
    ) -> None:
        ...

    def drop_table_ddl(
        self,
        table: str,
        schema: str,
    ) -> str:
        return f"DROP TABLE IF EXISTS {schema}.{table}"

    @abstractmethod
    def drop_table(
        self,
        table: str,
        schema: str,
    ) -> None:
        ...

    @abstractmethod
    def insert_data(
        self,
        schema: str,
        table: str,
        values: list,
    ) -> None:
        ...

    def get_expected_dataframe_ddl(
        self,
        schema: str,
        table: str,
        order_by: str | None = None,
    ) -> str:
        statement = f"SELECT {', '.join(self.column_names)} FROM {schema}.{table}"

        if order_by:
            statement += f" ORDER BY {order_by}"

        return statement

    @abstractmethod
    def get_expected_dataframe(
        self,
        schema: str,
        table: str,
        order_by: str | None = None,
    ) -> pandas.DataFrame:
        ...

    def get_column_type(self, name: str) -> str:
        return self._column_types_and_names_matching[name]

    @staticmethod
    def current_date() -> date:
        return date.today()

    @staticmethod
    def current_datetime() -> datetime:
        return datetime.now()

    def create_pandas_df(
        self,
        min_id: int = 1,
        max_id: int = _df_max_length,
    ) -> pandas.DataFrame:
        time_multiplier = 100000

        values = defaultdict(list)
        for i in range(min_id, max_id + 1):
            for column in self.column_names:
                column_name = column.lower()

                if "int" in column_name:
                    values[column].append(i)
                elif "float" in column_name:
                    values[column].append(float(f"{i}.{i}"))
                elif "text" in column_name:
                    values[column].append("This line is made to test the work")
                elif "datetime" in column_name:
                    rand_second = randint(0, i * time_multiplier)  # noqa: S311
                    values[column].append(self.current_datetime() + timedelta(seconds=rand_second))
                elif "date" in column_name:
                    rand_second = randint(0, i * time_multiplier)  # noqa: S311
                    values[column].append(self.current_date() + timedelta(seconds=rand_second))

        return pandas.DataFrame(data=values)

    def create_spark_df(
        self,
        spark: SparkSession,
        min_id: int = 1,
        max_id: int = _df_max_length,
    ) -> SparkDataFrame:
        return spark.createDataFrame(self.create_pandas_df(min_id=min_id, max_id=max_id))

    def fix_pyspark_df(
        self,
        df: SparkDataFrame,
    ) -> SparkDataFrame:
        from pyspark.sql.functions import date_format

        # TypeError: Casting to unit-less dtype 'datetime64' is not supported. Pass e.g. 'datetime64[ns]' instead.
        # So casting datetime to string, and after converting Spark df to Pandas, convert string back to datetime
        for column in df.columns:
            column_name = column.lower()

            if "datetime" in column_name:
                df = df.withColumn(column, df[column].cast("string"))
            elif "date" in column_name:
                df = df.withColumn(column, date_format(df[column], "yyyy-MM-dd"))

        return df

    def fix_pandas_df(
        self,
        df: pandas.DataFrame,
    ) -> pandas.DataFrame:
        for column in df.columns:
            column_name = column.lower()

            if "datetime" in column_name:
                # See fix_pyspark_df
                df[column] = pandas.to_datetime(df[column], format="ISO8601")
            elif "date" in column_name:
                # See fix_pyspark_df
                df[column] = pandas.to_datetime(df[column], format="ISO8601").dt.date

        return df

    def assert_equal_df(
        self,
        df: SparkDataFrame,
        schema: str | None = None,
        table: str | None = None,
        order_by: str | None = None,
        other_frame: pandas.DataFrame | SparkDataFrame | None = None,
        **kwargs,
    ) -> None:
        """Checks that df and other_frame are equal"""

        df = self.fix_pyspark_df(df)

        if other_frame is None:
            if schema is None or table is None:
                raise TypeError("Cannot use assert_equal_df without schema and table")

            other_frame = self.get_expected_dataframe(schema=schema, table=table, order_by=order_by)

        if not isinstance(other_frame, pandas.DataFrame):
            other_frame = self.fix_pyspark_df(other_frame)
            other_frame = other_frame.toPandas()

        left_df = self.fix_pandas_df(df.toPandas())
        right_df = self.fix_pandas_df(other_frame)

        if order_by:
            left_df = left_df.sort_values(by=order_by)
            right_df = right_df.sort_values(by=order_by)

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
        self,
        df: SparkDataFrame,
        schema: str | None = None,
        table: str | None = None,
        other_frame: pandas.DataFrame | SparkDataFrame | None = None,
    ) -> None:
        """Checks that other_frame contains df"""

        df = self.fix_pyspark_df(df)

        if other_frame is None:
            if schema is None or table is None:
                raise TypeError("Cannot use assert_equal_df without schema and table")

            other_frame = self.get_expected_dataframe(schema=schema, table=table)

        if not isinstance(other_frame, pandas.DataFrame):
            other_frame = self.fix_pyspark_df(other_frame)
            other_frame = other_frame.toPandas()

        df = self.fix_pandas_df(df.toPandas())
        other_frame = self.fix_pandas_df(other_frame)

        for column in set(df.columns).union(other_frame.columns):  # noqa: WPS528
            assert df[column].isin(other_frame[column]).all()  # noqa: S101
