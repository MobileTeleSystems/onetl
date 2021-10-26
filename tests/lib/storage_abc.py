from abc import abstractmethod, ABC
from logging import getLogger
from typing import List, Dict, Union
from datetime import datetime, date

import pandas as pd

logger = getLogger(__name__)
ConnectionType = Union["pyspark.sql.SparkSession", "psycopg2.extensions.connection", "cx_Oracle.Connection"]


class StorageABC(ABC):
    _df_max_length: int = 100

    _column_types_and_names_matching: Dict = {}

    _column_names: List = ["id_int", "text_string", "hwm_int", "hwm_date", "hwm_datetime"]

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
    def get_conn(self) -> ConnectionType:
        """"""

    @abstractmethod
    def stop_conn(self) -> None:
        """"""

    @abstractmethod
    def insert_data(
        self,
        schema: str,
        table: str,
        field_names: List,
        values: List,
    ) -> None:
        """"""

    def get_column_types_and_names_matching(self) -> Dict:
        return self._column_types_and_names_matching

    @classmethod
    def get_column_names(cls) -> List:
        return cls._column_names

    def create_pandas_df(self, min_id: int = 1, max_id: int = _df_max_length) -> "pandas.core.frame.DataFrame":
        # the date is filled in a cycle until 12, then the time and date follow a new cycle
        # and the indices continue to increase
        time = 1

        values = {column_name: [] for column_name in self._column_names}

        for i in range(min_id, max_id + 1):
            if time > 12:
                time = 1

            for column_name in values.keys():
                if "int" in column_name.split("_"):
                    values[column_name].append(i)
                if "text" in column_name.split("_"):
                    values[column_name].append("This line is made to test the work")
                if "date" in column_name.split("_"):
                    values[column_name].append(date(2021, 4, time))
                if "datetime" in column_name.split("_"):
                    values[column_name].append(datetime(2021, 4, time, time, time, time))

            time += 1

        return pd.DataFrame(data=values)

    def create_spark_df(
        self,
        spark: "pyspark.sql.SparkSession",
        min_id: int = 1,
        max_id: int = _df_max_length,
    ) -> "pyspark.sql.DataFrame":
        return spark.createDataFrame(self.create_pandas_df(min_id=min_id, max_id=max_id))
