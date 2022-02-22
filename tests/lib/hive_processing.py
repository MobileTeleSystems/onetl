from logging import getLogger
from typing import Dict, List, Optional

import pandas as pd

from tests.lib.base_processing import BaseProcessing

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

    def __init__(self, spark: "pyspark.sql.SparkSession"):
        self.connection = spark

    def create_schema(
        self,
        schema: str,
    ) -> None:
        self.connection.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    def create_table(
        self,
        table: str,
        fields: Dict[str, str],
        schema: str,
    ) -> None:
        str_fields = ", ".join([f"{key} {value}" for key, value in fields.items()])
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
        order_by: Optional[List[str]] = None,
    ) -> "pandas.core.frame.DataFrame":
        values = {column_name: [] for column_name in self.column_names}

        df = self.connection.read.table(f"{schema}.{table}").select(*self.column_names)
        if order_by:
            df = df.orderBy(order_by)

        for row in df.collect():
            for idx, _ in enumerate(row):
                # Row(id=1, text='hello') -> values = ['id':[1], 'text': ['hello']]
                values[self.column_names[idx]].append(row[idx])

        return pd.DataFrame(data=values)

    def fix_pandas_df(
        self,
        df: "pandas.core.frame.DataFrame",
    ) -> "pandas.core.frame.DataFrame":
        # Type conversion is required since Hive returns float32 instead float64

        for column in df:  # noqa: WPS528
            column_names = column.split("_")

            if "float" in column_names:
                df[column] = df[column].astype("float32")

        return df
