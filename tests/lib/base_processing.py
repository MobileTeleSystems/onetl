from abc import abstractmethod
from logging import getLogger
from typing import List, Dict
from datetime import date, datetime, timedelta
from random import randint

import pandas as pd

logger = getLogger(__name__)


class BaseProcessing:
    _df_max_length: int = 100

    _column_types_and_names_matching: Dict = {}

    column_names: List = ["id_int", "text_string", "hwm_int", "hwm_date", "hwm_datetime"]

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
        fields: List,
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
        values: List,
    ) -> None:
        """"""

    def get_column_type(self, name: str) -> str:
        return self._column_types_and_names_matching[name]

    def create_pandas_df(self, min_id: int = 1, max_id: int = _df_max_length) -> "pandas.core.frame.DataFrame":
        time_multiplier = 100000

        values = {column_name: [] for column_name in self.column_names}

        for i in range(min_id, max_id + 1):
            for column_name in values.keys():
                if "int" in column_name.split("_"):
                    values[column_name].append(i)
                if "text" in column_name.split("_"):
                    values[column_name].append("This line is made to test the work")
                if "date" in column_name.split("_"):
                    rand_second = randint(0, i * time_multiplier)  # noqa: S311
                    values[column_name].append(date.today() + timedelta(seconds=rand_second))
                if "datetime" in column_name.split("_"):
                    rand_second = randint(0, i * time_multiplier)  # noqa: S311
                    values[column_name].append(datetime.now() + timedelta(seconds=rand_second))

        return pd.DataFrame(data=values)

    def create_spark_df(
        self,
        spark: "pyspark.sql.SparkSession",
        min_id: int = 1,
        max_id: int = _df_max_length,
    ) -> "pyspark.sql.DataFrame":
        return spark.createDataFrame(self.create_pandas_df(min_id=min_id, max_id=max_id))
