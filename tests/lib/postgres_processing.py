import os
from logging import getLogger
from typing import Dict, List, Optional

import pandas as pd
from pandas.io import sql as psql
from psycopg2 import connect as pg_connect
from psycopg2.extensions import connection

from tests.lib.base_processing import BaseProcessing

logger = getLogger(__name__)


class PostgressProcessing(BaseProcessing):

    _column_types_and_names_matching = {
        "id_int": "serial primary key",
        "text_string": "text",
        "hwm_int": "bigint",
        "hwm_date": "date",
        "hwm_datetime": "timestamp",
        "float_value": "float",
    }

    def __enter__(self):
        self.connection = self.get_conn()
        return self

    def __exit__(self, _exc_type, _exc_value, _traceback):
        self.connection.close()
        return False

    @property
    def user(self) -> str:
        return os.getenv("ONETL_PG_CONN_USER")

    @property
    def password(self) -> str:
        return os.getenv("ONETL_PG_CONN_PASSWORD")

    @property
    def host(self) -> str:
        return os.getenv("ONETL_PG_CONN_HOST")

    @property
    def database(self) -> str:
        return os.getenv("ONETL_PG_CONN_DATABASE")

    @property
    def url(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/"

    @property
    def port(self) -> int:
        return int(os.getenv("ONETL_PG_CONN_PORT"))

    def create_schema(
        self,
        schema: str,
    ) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    def create_table(
        self,
        table: str,
        fields: Dict[str, str],
        schema: str,
    ) -> None:
        with self.connection.cursor() as cursor:
            str_fields = ", ".join([f"{key} {value}" for key, value in fields.items()])
            cursor.execute(f"CREATE TABLE IF NOT EXISTS {schema}.{table} ({str_fields})")
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

    def get_conn(self) -> connection:
        return pg_connect(self.url)

    def insert_data(
        self,
        schema: str,
        table: str,
        values: "pandas.core.frame.DataFrame",
    ) -> None:

        # <con> parameter is SQLAlchemy connectable or str
        # A database URI could be provided as as str.
        psql.to_sql(
            frame=values,
            name=table,
            con=self.url,
            index=False,
            schema=schema,
            if_exists="append",
        )

    def get_expected_dataframe(
        self,
        schema: str,
        table: str,
        order_by: Optional[List[str]] = None,
    ) -> "pandas.core.frame.DataFrame":

        statement = f"SELECT {', '.join(self.column_names)} FROM {schema}.{table}"

        if order_by:
            statement += f" ORDER BY {order_by}"

        return pd.read_sql_query(f"{statement};", con=self.connection)
