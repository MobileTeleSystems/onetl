from logging import getLogger
from typing import List, Union, Optional

import pandas as pd
from pandas.util.testing import assert_frame_equal

from tests.lib.storage_abc import StorageABC

logger = getLogger(__name__)
ConnectionType = Union["pyspark.sql.SparkSession", "psycopg2.extensions.connection"]


class HiveProcessing(StorageABC):

    _column_types_and_names_matching = {
        "id_int": "int",
        "text_string": "string",
        "hwm_int": "int",
        "hwm_date": "date",
        "hwm_datetime": "timestamp",
    }

    def __init__(self, spark):
        self.connection = spark

    def create_schema(
        self,
        schema: str,
    ) -> None:
        self.connection.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    def create_table(
        self,
        table: str,
        fields: List,
        schema: str,
    ) -> None:
        str_fields = ", ".join([f"{f['column_name']} {f['type']}" for f in fields])
        self.connection.sql(f"CREATE TABLE IF NOT EXISTS {schema}.{table} ({str_fields}) STORED AS ORC")

    def drop_database(
        self,
        schema: str,
    ) -> None:
        self.connection.sql(f"DROP DATABASE IF EXISTS {schema}")

    def drop_table(
        self,
        table: str,
        schema: str,
    ) -> None:
        self.connection.sql(f"DROP TABLE IF EXISTS {schema}.{table}")

    def get_conn(self) -> ConnectionType:
        # connection is spark
        """"""

    def insert_data(
        self,
        schema: str,
        table: str,
        field_names: List,
        values: List,
    ) -> None:

        df = self.connection.createDataFrame(values)
        df.write.mode("append").insertInto(f"{schema}.{table}")

    def get_written_df(
        self,
        schema: str,
        table: str,
    ) -> "pandas.core.frame.DataFrame":
        df = self.connection.read.table(f"{schema}.{table}").collect()

        column_names = StorageABC.get_column_names()

        values = {column_name: [] for column_name in column_names}

        for row in df:
            for idx, _ in enumerate(row):
                # Row(id=1, text='hello') -> values = ['id':[1], 'text': ['hello']]
                values[column_names[idx]].append(row[idx])

        return pd.DataFrame(data=values)

    def stop_conn(self):
        # connection.stop() in spark fixture
        """"""

    def assert_equal_df(
        self,
        schema_name: str,
        table: str,
        df: "pyspark.sql.DataFrame",
        other_frame: Optional["pyspark.sql.DataFrame"] = None,
    ) -> None:

        if not other_frame:
            other_frame = self.get_written_df(
                schema=schema_name,
                table=table,
            )

        assert_frame_equal(left=df.toPandas(), right=other_frame, check_dtype=False)
