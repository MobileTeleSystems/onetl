from __future__ import annotations

import os
from collections import defaultdict
from logging import getLogger
from typing import TYPE_CHECKING

import pandas

from tests.fixtures.processing.base_processing import BaseProcessing

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = getLogger(__name__)


class HiveProcessing(BaseProcessing):
    _column_types_and_names_matching = {
        "id_int": "int",
        "text_string": "string",
        "hwm_int": "int",
        "hwm_date": "date",
        "hwm_datetime": "timestamp",
        "float_value": "float",
    }

    def __init__(self, spark: SparkSession):
        self.connection = spark

    @property
    def schema(self) -> str:
        return os.getenv("ONETL_HIVE_SCHEMA", "onetl")

    def create_schema(
        self,
        schema: str,
    ) -> None:
        self.connection.sql(self.create_schema_ddl(schema))

    def create_table_ddl(
        self,
        table: str,
        fields: dict[str, str],
        schema: str,
    ) -> str:
        str_fields = ", ".join([f"{key} {value}" for key, value in fields.items()])
        return f"CREATE TABLE IF NOT EXISTS {schema}.{table} ({str_fields}) STORED AS ORC"

    def create_table(
        self,
        table: str,
        fields: dict[str, str],
        schema: str,
    ) -> None:
        self.connection.sql(self.create_table_ddl(table, fields, schema))

    def drop_database(
        self,
        schema: str,
    ) -> None:
        self.connection.sql(self.drop_database_ddl(schema))

    def drop_table_ddl(
        self,
        table: str,
        schema: str,
    ) -> str:
        return f"DROP TABLE IF EXISTS {schema}.{table} PURGE"

    def drop_table(
        self,
        table: str,
        schema: str,
    ) -> None:
        self.connection.sql(self.drop_table_ddl(table, schema))

    def insert_data(
        self,
        schema: str,
        table: str,
        values: list,
    ) -> None:
        df = self.connection.createDataFrame(values)
        df.write.mode("append").insertInto(f"{schema}.{table}")

    def get_expected_dataframe(
        self,
        schema: str,
        table: str,
        order_by: str | None = None,
    ) -> pandas.DataFrame:
        values = defaultdict(list)
        df = self.connection.sql(self.get_expected_dataframe_ddl(schema, table, order_by))

        if order_by:
            df = df.orderBy(order_by)

        for row in df.collect():
            for idx, _ in enumerate(row):
                # Row(id=1, text='hello') -> values = ['id':[1], 'text': ['hello']]
                values[self.column_names[idx]].append(row[idx])

        return pandas.DataFrame(data=values)

    def fix_pandas_df(
        self,
        df: pandas.DataFrame,
    ) -> pandas.DataFrame:
        df = super().fix_pandas_df(df)

        for column in df.columns:
            if "float" in column:
                # Hive returns float32 instead float64
                df[column] = df[column].astype("float32")

        return df
