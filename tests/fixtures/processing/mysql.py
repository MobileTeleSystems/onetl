from __future__ import annotations

import os
from logging import getLogger

import pandas
import pymysql
from pandas.io import sql as psql

from tests.fixtures.processing.base_processing import BaseProcessing

logger = getLogger(__name__)


class MySQLProcessing(BaseProcessing):
    _column_types_and_names_matching = {
        "id_int": "INT NOT NULL",
        "text_string": "VARCHAR(50)",
        "hwm_int": "INT",
        "hwm_date": "DATE",
        "hwm_datetime": "DATETIME(6)",
        "float_value": "FLOAT",
    }

    def __enter__(self):
        self.connection = self.get_conn()
        return self

    def __exit__(self, _exc_type, _exc_value, _traceback):
        self.connection.close()
        return False

    @property
    def user(self) -> str:
        return os.environ["ONETL_MYSQL_USER"]

    @property
    def password(self) -> str:
        return os.environ["ONETL_MYSQL_PASSWORD"]

    @property
    def host(self) -> str:
        return os.environ["ONETL_MYSQL_HOST"]

    @property
    def database(self) -> str:
        return os.environ["ONETL_MYSQL_DATABASE"]

    @property
    def port(self) -> int:
        return int(os.environ["ONETL_MYSQL_PORT"])

    @property
    def schema(self) -> str:
        return os.getenv("ONETL_MYSQL_SCHEMA", "onetl")

    @property
    def url(self) -> str:
        return f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

    def get_conn(self):
        return pymysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
            cursorclass=pymysql.cursors.DictCursor,
        )

    def create_schema_ddl(
        self,
        schema: str,
    ) -> str:
        return f"CREATE SCHEMA IF NOT EXISTS {schema}"

    def create_schema(
        self,
        schema: str,
    ) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(self.create_schema_ddl(schema))
            self.connection.commit()

    def create_table_ddl(
        self,
        table: str,
        fields: dict[str, str],
        schema: str,
    ) -> str:
        str_fields = ", ".join([f"{key} {value}" for key, value in fields.items()])
        return f"CREATE TABLE IF NOT EXISTS {schema}.{table} ({str_fields})"

    def create_table(
        self,
        table: str,
        fields: dict[str, str],
        schema: str,
    ) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(self.create_table_ddl(table, fields, schema))
            self.connection.commit()

    def drop_database(
        self,
        schema: str,
    ) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(self.drop_database_ddl(schema))
            self.connection.commit()

    def drop_table(
        self,
        table: str,
        schema: str,
    ) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(self.drop_table_ddl(table, schema))
            self.connection.commit()

    def insert_data(
        self,
        schema: str,
        table: str,
        values: pandas.DataFrame,
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
            method="multi",
        )

    def get_expected_dataframe(
        self,
        schema: str,
        table: str,
        order_by: str | None = None,
    ) -> pandas.DataFrame:
        return pandas.read_sql_query(
            self.get_expected_dataframe_ddl(schema, table, order_by) + ";",
            con=self.url,
        )
