from abc import ABC, abstractmethod
from datetime import date, datetime, timedelta
from logging import getLogger
from random import randint
from typing import Dict, List, Optional, Union

import pandas as pd
from pandas.util.testing import assert_frame_equal

logger = getLogger(__name__)


class BaseProcessing(ABC):
    _df_max_length: int = 100
    _column_types_and_names_matching: Dict[str, str] = {}

    column_names: List = ["id_int", "text_string", "hwm_int", "hwm_date", "hwm_datetime", "float_value"]

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
        """"""

    def create_table_ddl(
        self,
        table: str,
        fields: Dict[str, str],
        schema: str,
    ) -> str:
        str_fields = ", ".join([f"{key} {value}" for key, value in fields.items()])
        return f"CREATE TABLE {schema}.{table} ({str_fields})"

    @abstractmethod
    def create_table(
        self,
        table: str,
        fields: Dict[str, str],
        schema: str,
    ) -> None:
        """"""

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
        """"""

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
        """"""

    @abstractmethod
    def insert_data(
        self,
        schema: str,
        table: str,
        values: list,
    ) -> None:
        """"""

    def get_expected_dataframe_ddl(
        self,
        schema: str,
        table: str,
        order_by: Optional[str] = None,
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
        order_by: Optional[str] = None,
    ) -> "pandas.core.frame.DataFrame":  # noqa: F821
        """"""

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
    ) -> "pandas.core.frame.DataFrame":  # noqa: F821
        time_multiplier = 100000

        values = {column_name: [] for column_name in self.column_names}

        for i in range(min_id, max_id + 1):
            for column_name in values.keys():
                if "int" in column_name.split("_"):
                    values[column_name].append(i)
                elif "float" in column_name.split("_"):
                    values[column_name].append(float(f"{i}.{i}"))
                elif "text" in column_name.split("_"):
                    values[column_name].append("This line is made to test the work")
                elif "date" in column_name.split("_"):
                    rand_second = randint(0, i * time_multiplier)  # noqa: S311
                    values[column_name].append(self.current_date() + timedelta(seconds=rand_second))
                elif "datetime" in column_name.split("_"):
                    rand_second = randint(0, i * time_multiplier)  # noqa: S311
                    values[column_name].append(self.current_datetime() + timedelta(seconds=rand_second))

        return pd.DataFrame(data=values)

    def create_spark_df(
        self,
        spark: "pyspark.sql.SparkSession",  # noqa: F821
        min_id: int = 1,
        max_id: int = _df_max_length,
    ) -> "pyspark.sql.DataFrame":  # noqa: F821
        return spark.createDataFrame(self.create_pandas_df(min_id=min_id, max_id=max_id))

    def fix_pandas_df(
        self,
        df: "pandas.core.frame.DataFrame",  # noqa: F821
    ) -> "pandas.core.frame.DataFrame":  # noqa: F821
        return df

    def assert_equal_df(
        self,
        df: "pyspark.sql.DataFrame",  # noqa: F821
        schema: Optional[str] = None,
        table: Optional[str] = None,
        order_by: Optional[str] = None,
        other_frame: Union["pandas.core.frame.DataFrame", "pyspark.sql.DataFrame", None] = None,  # noqa: F821
        **kwargs,
    ) -> None:
        """Checks that df and other_frame are equal"""

        if other_frame is None:
            other_frame = self.get_expected_dataframe(schema=schema, table=table, order_by=order_by)

        if not isinstance(other_frame, pd.core.frame.DataFrame):
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
        df: "pyspark.sql.DataFrame",  # noqa: F821
        schema: Optional[str] = None,
        table: Optional[str] = None,
        other_frame: Union["pandas.core.frame.DataFrame", "pyspark.sql.DataFrame", None] = None,  # noqa: F821
    ) -> None:
        """Checks that other_frame contains df"""

        if other_frame is None:
            other_frame = self.get_expected_dataframe(schema=schema, table=table)

        if not isinstance(other_frame, pd.core.frame.DataFrame):
            other_frame = other_frame.toPandas()

        df = self.fix_pandas_df(df.toPandas())
        other_frame = self.fix_pandas_df(other_frame)

        for column in set(df.columns).union(other_frame.columns):  # noqa: WPS528
            assert df[column].isin(other_frame[column]).all()  # noqa: S101
