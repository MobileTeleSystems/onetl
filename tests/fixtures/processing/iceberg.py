from __future__ import annotations

import os
from collections import defaultdict
from typing import TYPE_CHECKING

import pandas

from tests.fixtures.processing.base_processing import BaseProcessing

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


class IcebergProcessing(BaseProcessing):
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
    def catalog(self) -> str:
        return os.getenv("ONETL_ICEBERG_CATALOG", "my_catalog")

    @property
    def schema(self) -> str:
        return os.getenv("ONETL_ICEBERG_SCHEMA", "onetl")

    def create_schema(self, schema: str) -> None:
        self.connection.sql(f"CREATE NAMESPACE IF NOT EXISTS {self.catalog}.{schema}")

    def create_table_ddl(
        self,
        table: str,
        fields: dict[str, str],
        schema: str,
    ) -> str:
        str_fields = ", ".join([f"{key} {value}" for key, value in fields.items()])
        return f"CREATE TABLE IF NOT EXISTS {self.catalog}.{schema}.{table} ({str_fields}) USING iceberg"

    def create_table(self, table: str, fields: dict[str, str], schema: str) -> None:
        self.connection.sql(self.create_table_ddl(table, fields, schema))

    def drop_database(self, schema: str) -> None:
        # https://github.com/apache/iceberg/issues/3541
        normalized_schema = f"{self.catalog}.{schema}"
        tables = [t.name for t in self.connection.catalog.listTables(normalized_schema)]
        for table in tables:
            self.drop_table(table, schema)
        self.connection.sql(f"DROP NAMESPACE IF EXISTS {normalized_schema}")

    def drop_table(self, table: str, schema: str) -> None:
        self.connection.sql(self.drop_table_ddl(table, f"{self.catalog}.{schema}"))

    def insert_data(self, schema: str, table: str, values: list) -> None:
        df = self.connection.createDataFrame(values)
        df.writeTo(f"{self.catalog}.{schema}.{table}").append()

    def get_expected_dataframe(
        self,
        schema: str,
        table: str,
        order_by: str | None = None,
    ) -> pandas.DataFrame:
        values = defaultdict(list)
        df = self.connection.sql(f"SELECT * FROM {self.catalog}.{schema}.{table}")

        if order_by:
            df = df.orderBy(order_by)

        for row in df.collect():
            for idx, _ in enumerate(row):
                # Row(id=1, text='hello') -> values = ['id':[1], 'text': ['hello']]
                values[self.column_names[idx]].append(row[idx])

        return pandas.DataFrame(data=values)
