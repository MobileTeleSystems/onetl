from datetime import date, datetime, timedelta
from random import randint
from logging import getLogger
from typing import List
import os

import pandas as pd
from pandas.io import sql as psql
import pymssql

from tests.lib.base_processing import BaseProcessing

logger = getLogger(__name__)


class MSSQLProcessing(BaseProcessing):

    _column_types_and_names_matching = {
        "id_int": "INT",
        "text_string": "VARCHAR(50)",
        "hwm_int": "INT",
        "hwm_date": "DATE",
        "hwm_datetime": "DATETIME",
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
        return os.getenv("ONETL_MSSQL_CONN_USER")

    @property
    def password(self) -> str:
        return os.getenv("ONETL_MSSQL_CONN_PASSWORD")

    @property
    def host(self) -> str:
        return os.getenv("ONETL_MSSQL_CONN_HOST")

    @property
    def database(self) -> str:
        return os.getenv("ONETL_MSSQL_CONN_DATABASE")

    @property
    def port(self) -> int:
        return int(os.getenv("ONETL_MSSQL_CONN_PORT"))

    @property
    def url(self) -> str:
        return f"mssql+pymssql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

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
                    # MSSQL DATETIME format has time range: 00:00:00 through 23:59:59.997
                    values[column_name].append(datetime.now().replace(microsecond=0) + timedelta(seconds=rand_second))

        return pd.DataFrame(data=values)

    def create_schema(
        self,
        schema: str,
    ) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                f"""IF (NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{schema}'))
                    BEGIN
                        EXEC ('CREATE SCHEMA [{schema}]')
                    END""",
            )

    def create_table(
        self,
        table: str,
        fields: List,
        schema: str,
    ) -> None:
        with self.connection.cursor() as cursor:
            str_fields = ", ".join([f"{field['column_name']} {field['type']}" for field in fields])
            sql = f"CREATE TABLE {table} ({str_fields})"
            cursor.execute(sql)
            self.connection.commit()

    def drop_database(
        self,
        schema: str,
    ) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(f"DROP DATABASE {schema}")
            self.connection.commit()

    def drop_table(
        self,
        table: str,
        schema: str,
    ) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(f"DROP TABLE {schema}.{table}")
            self.connection.commit()

    def get_conn(self):
        return pymssql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
        )

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
    ) -> "pandas.core.frame.DataFrame":

        return pd.read_sql_query(f"SELECT * FROM {schema}.{table};", con=self.connection)
