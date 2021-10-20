from logging import getLogger
from typing import List, Union, Optional
import os

from psycopg2 import connect as pg_connect
import pandas as pd
from pandas.io import sql as psql
from pandas.util.testing import assert_frame_equal

from tests.lib.storage_abc import StorageABC

logger = getLogger(__name__)
ConnectionType = Union["pyspark.sql.SparkSession", "psycopg2.extensions.connection"]


class PostgressProcessing(StorageABC):

    _column_types_and_names_matching = {
        "id_int": "serial primary key",
        "text_string": "text",
        "hwm_int": "bigint",
        "hwm_date": "date",
        "hwm_datetime": "timestamp",
    }

    def __init__(self):
        self.connection = self.get_conn()

    @property
    def user(self):
        return os.getenv("ONETL_PG_CONN_USER")

    @property
    def password(self):
        return os.getenv("ONETL_PG_CONN_PASSWORD")

    @property
    def host(self):
        return os.getenv("ONETL_PG_CONN_HOST")

    @property
    def database(self):
        return os.getenv("ONETL_PG_CONN_DATABASE")

    @property
    def url(self):
        return f"postgresql://{self.user}:" f"{self.password}" f"@{self.host}:5432/"

    def create_schema(
        self,
        schema: str,
    ) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    def create_table(
        self,
        table: str,
        fields: List,
        schema: str,
    ) -> None:
        with self.connection.cursor() as cursor:
            str_fields = ", ".join([f"{f['column_name']} {f['type']}" for f in fields])
            sql = f"CREATE TABLE IF NOT EXISTS {schema}.{table} ({str_fields})"
            cursor.execute(sql)
            self.connection.commit()

    def drop_database(
        self,
        schema: str,
    ) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(f"DROP DATABASE IF EXISTS {schema}")
            self.connection.commit()

    def drop_table(
        self,
        table: str,
        schema: str,
    ) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {schema}.{table}")
            self.connection.commit()

    def get_conn(self) -> ConnectionType:
        return pg_connect(self.url)

    def insert_data(
        self,
        schema: str,
        table: str,
        field_names: List,
        values: "pandas.core.frame.DataFrame",
    ) -> None:

        con_info = self.connection.info

        # <con> parameter is SQLAlchemy connectable or str
        # A database URI could be provided as as str.
        psql.to_sql(
            frame=values,
            name=table,
            con=f"postgresql://{con_info.user}:{con_info.password}@{con_info.host}:5432",
            index=False,
            schema=schema,
            if_exists="append",
        )

    def get_written_df(
        self,
        schema: str,
        table: str,
    ) -> "pandas.core.frame.DataFrame":

        return pd.read_sql_query(f"SELECT * FROM {schema}.{table};", con=self.connection)

    def stop_conn(self) -> None:
        self.connection.close()

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
