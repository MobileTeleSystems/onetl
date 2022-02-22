from abc import abstractmethod
from datetime import date, datetime, timedelta
from logging import getLogger
from random import randint
from typing import Dict, List, Optional

import pandas as pd
from pandas.util.testing import assert_frame_equal

logger = getLogger(__name__)


class BaseProcessing:
    _df_max_length: int = 100
    _column_types_and_names_matching: Dict[str, str] = {}

    column_names: List = ["id_int", "text_string", "hwm_int", "hwm_date", "hwm_datetime", "float_value"]

    @abstractmethod
    def create_schema(
        self,
        schema: str,
    ) -> None:
        """"""

    @abstractmethod
    def create_table(
        self,
        table: str,
        fields: Dict[str, str],
        schema: str,
    ) -> None:
        """"""

    @abstractmethod
    def drop_database(
        self,
        schema: str,
    ) -> None:
        """"""

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

    @abstractmethod
    def get_expected_dataframe(
        self,
        schema: str,
        table: str,
    ) -> "pandas.core.frame.DataFrame":
        """"""

    def get_column_type(self, name: str) -> str:
        return self._column_types_and_names_matching[name]

    @staticmethod  # noqa: WPS605
    def current_date() -> date:
        return date.today()

    @staticmethod  # noqa: WPS605
    def current_datetime() -> datetime:
        return datetime.now()

    def create_pandas_df(self, min_id: int = 1, max_id: int = _df_max_length) -> "pandas.core.frame.DataFrame":
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
        spark: "pyspark.sql.SparkSession",
        min_id: int = 1,
        max_id: int = _df_max_length,
    ) -> "pyspark.sql.DataFrame":
        return spark.createDataFrame(self.create_pandas_df(min_id=min_id, max_id=max_id))

    def fix_pandas_df(
        self,
        df: "pandas.core.frame.DataFrame",
    ) -> "pandas.core.frame.DataFrame":
        return df

    def assert_equal_df(
        self,
        df: "pyspark.sql.DataFrame",
        schema: Optional[str] = None,
        table: Optional[str] = None,
        order_by: Optional[List[str]] = None,
        other_frame: Optional["pandas.core.frame.DataFrame"] = None,
        **kwargs,
    ) -> None:
        """Checks that df and other_frame are equal"""

        if other_frame is None:
            other_frame = self.get_expected_dataframe(schema=schema, table=table, order_by=order_by)

        df = self.fix_pandas_df(df.toPandas())
        other_frame = self.fix_pandas_df(other_frame)

        assert_frame_equal(left=df, right=other_frame, check_dtype=False, **kwargs)

    def assert_subset_df(
        self,
        df: "pyspark.sql.DataFrame",
        schema: Optional[str] = None,
        table: Optional[str] = None,
        other_frame: Optional["pandas.core.frame.DataFrame"] = None,
    ) -> None:
        """Checks that other_frame contains df"""

        if other_frame is None:
            other_frame = self.get_expected_dataframe(schema=schema, table=table)

        df = self.fix_pandas_df(df.toPandas())
        other_frame = self.fix_pandas_df(other_frame)

        for column in df:  # noqa: WPS528
            assert df[column].isin(other_frame[column]).all()  # noqa: S101
