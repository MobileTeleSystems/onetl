from __future__ import annotations

from abc import ABC, abstractmethod
from collections import defaultdict
from datetime import date, datetime, timedelta
from logging import getLogger
from random import randint
from typing import TYPE_CHECKING

import pandas

from tests.util.assert_df import assert_equal_df, assert_subset_df
from tests.util.to_pandas import to_pandas

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

    def fix_pandas_df(
        self,
        df: pandas.DataFrame,
    ) -> pandas.DataFrame:
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

        if other_frame is None:
            if schema is None or table is None:
                raise TypeError("Cannot use assert_equal_df without schema and table")
            other_frame = self.get_expected_dataframe(schema=schema, table=table, order_by=order_by)

        left_df = self.fix_pandas_df(to_pandas(df))
        right_df = self.fix_pandas_df(to_pandas(other_frame))
        return assert_equal_df(left_df=left_df, right_df=right_df, order_by=order_by, **kwargs)

    def assert_subset_df(
        self,
        df: SparkDataFrame,
        schema: str | None = None,
        table: str | None = None,
        other_frame: pandas.DataFrame | SparkDataFrame | None = None,
        columns: list[str] | None = None,
    ) -> None:
        """Checks that other_frame contains df"""

        if other_frame is None:
            if schema is None or table is None:
                raise TypeError("Cannot use assert_equal_df without schema and table")
            other_frame = self.get_expected_dataframe(schema=schema, table=table)

        small_df = self.fix_pandas_df(to_pandas(df))
        large_df = self.fix_pandas_df(to_pandas(other_frame))
        return assert_subset_df(small_df=small_df, large_df=large_df, columns=columns)
