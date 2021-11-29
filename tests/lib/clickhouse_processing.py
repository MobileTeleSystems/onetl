from logging import getLogger
from typing import List, Optional
import os
from random import randint
from datetime import date, datetime, timedelta

import pandas as pd
from pandas.util.testing import assert_frame_equal
import clickhouse_driver

from tests.lib.base_processing import BaseProcessing

logger = getLogger(__name__)


class ClickhouseProcessing(BaseProcessing):

    _column_types_and_names_matching = {
        "id_int": "Int32",
        "text_string": "String",
        "hwm_int": "Int32",
        "hwm_date": "Date",
        "hwm_datetime": "DateTime",
        "float_value": "Float32",
    }

    def __enter__(self):
        self.connection = self.get_conn()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.disconnect()
        return False

    @property
    def user(self) -> str:
        return os.getenv("ONETL_CH_CONN_USER")

    @property
    def password(self) -> str:
        return os.getenv("ONETL_CH_CONN_PASSWORD")

    @property
    def host(self) -> str:
        return os.getenv("ONETL_CH_CONN_HOST")

    @property
    def database(self) -> str:
        return os.getenv("ONETL_CH_CONN_DATABASE")

    @property
    def port(self) -> int:
        return int(os.getenv("ONETL_CH_CONN_PORT"))

    def create_pandas_df(self, min_id: int = 1, max_id: int = None) -> "pandas.core.frame.DataFrame":
        max_id = self._df_max_length if not max_id else max_id
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
                    values[column_name].append(date.today() + timedelta(seconds=rand_second))
                elif "datetime" in column_name.split("_"):
                    rand_second = randint(0, i * time_multiplier)  # noqa: S311
                    # Clickhouse DATETIME format has time range: 00:00:00 through 23:59:59
                    values[column_name].append(datetime.now().replace(microsecond=0) + timedelta(seconds=rand_second))

        return pd.DataFrame(data=values)

    def create_schema(
        self,
        schema: str,
    ) -> None:
        self.connection.execute(f"create database if not exists {schema}")

    def create_table(
        self,
        table: str,
        fields: List,
        schema: str,
    ) -> None:
        str_fields = ", ".join([f"{field['column_name']} {field['type']}" for field in fields])
        sql = f"""
            create table if not exists {schema}.{table} ({str_fields})
            engine = MergeTree()
            order by {fields[0]['column_name']}
            primary key {fields[0]['column_name']}
        """
        self.connection.execute(sql)

    def drop_database(
        self,
        schema: str,
    ) -> None:
        self.connection.execute(f"DROP DATABASE {schema}")

    def drop_table(
        self,
        table: str,
        schema: str,
    ) -> None:
        self.connection.execute(f"DROP TABLE {schema}.{table}")

    def get_conn(self) -> "clickhouse_driver.client.Client":
        return clickhouse_driver.Client(host=self.host, port=self.port)

    def insert_data(
        self,
        schema: str,
        table: str,
        values: "pandas.core.frame.DataFrame",
    ) -> None:

        self.connection.execute(f"INSERT INTO {schema}.{table} VALUES", values.to_dict("records"))

    def get_expected_dataframe(
        self,
        schema: str,
        table: str,
    ) -> "pandas.core.frame.DataFrame":

        return self.connection.execute(f"SELECT * FROM {schema}.{table}")

    def assert_equal_df(
        self,
        schema: str,
        table: str,
        df: "pyspark.sql.DataFrame",
        other_frame: Optional["pandas.core.frame.DataFrame"] = None,
    ) -> None:

        if not other_frame:
            other_frame = pd.DataFrame(
                self.get_expected_dataframe(
                    schema=schema,
                    table=table,
                ),
                columns=self.column_names,
            )

        pd_df = df.toPandas()

        assert_frame_equal(
            left=pd_df,
            right=other_frame,
            check_dtype=False,
        )