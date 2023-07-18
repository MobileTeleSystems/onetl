from __future__ import annotations

import os
from logging import getLogger

import cx_Oracle
import pandas
from pandas.io import sql as psql

from tests.fixtures.processing.base_processing import BaseProcessing

logger = getLogger(__name__)


class OracleProcessing(BaseProcessing):
    _column_types_and_names_matching = {
        "id_int": "INTEGER NOT NULL",
        "text_string": "VARCHAR2(50) NOT NULL",
        "hwm_int": "INTEGER",
        "hwm_date": "DATE",
        "hwm_datetime": "TIMESTAMP",
        "float_value": "FLOAT",
    }

    def __enter__(self):
        self.connection = self.get_conn()
        return self

    def __exit__(self, _exc_type, _exc_value, _traceback):
        self.connection.close()
        return False

    @property
    def sid(self) -> str | None:
        return os.getenv("ONETL_ORA_SID")

    @property
    def service_name(self) -> str | None:
        return os.getenv("ONETL_ORA_SERVICE_NAME")

    @property
    def user(self) -> str:
        return os.environ["ONETL_ORA_USER"]

    @property
    def password(self) -> str:
        return os.environ["ONETL_ORA_PASSWORD"]

    @property
    def host(self) -> str:
        return os.environ["ONETL_ORA_HOST"]

    @property
    def port(self) -> int:
        return int(os.environ["ONETL_ORA_PORT"])

    @property
    def schema(self) -> str:
        return os.getenv("ONETL_ORA_SCHEMA", "onetl")

    @property
    def url(self) -> str:
        dsn = cx_Oracle.makedsn(self.host, self.port, sid=self.sid, service_name=self.service_name)
        return f"oracle://{self.user}:{self.password}@{dsn}"

    def get_conn(self) -> cx_Oracle.Connection:
        try:
            cx_Oracle.init_oracle_client(lib_dir=os.getenv("ONETL_ORA_CLIENT_PATH"))
        except Exception:
            logger.debug("cx_Oracle client is already initialized.", exc_info=True)
        dsn = cx_Oracle.makedsn(self.host, self.port, sid=self.sid, service_name=self.service_name)
        return cx_Oracle.connect(user=self.user, password=self.password, dsn=dsn)

    def create_schema_ddl(
        self,
        schema: str,
    ) -> str:
        return f"CREATE SCHEMA AUTHORIZATION {schema}"

    def create_schema(
        self,
        schema: str,
    ) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(self.create_schema_ddl(schema))
            self.connection.commit()

    def create_table(
        self,
        table: str,
        fields: dict[str, str],
        schema: str,
    ) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(self.create_table_ddl(table, fields, schema))
            self.connection.commit()

    def drop_database_ddl(
        self,
        schema: str,
    ) -> str:
        return f"DROP DATABASE {schema}"

    def drop_database(
        self,
        schema: str,
    ) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(self.drop_database_ddl(schema))
            self.connection.commit()

    def drop_table_ddl(
        self,
        table: str,
        schema: str,
    ) -> str:
        return f"DROP TABLE {schema}.{table} PURGE"

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
        )

    def get_expected_dataframe(
        self,
        schema: str,
        table: str,
        order_by: str | None = None,
    ) -> pandas.DataFrame:
        return pandas.read_sql_query(self.get_expected_dataframe_ddl(schema, table, order_by), con=self.connection)

    def fix_pandas_df(
        self,
        df: pandas.DataFrame,
    ) -> pandas.DataFrame:
        df = super().fix_pandas_df(df)

        for column in df.columns:
            column_name = column.lower()

            # Type conversion is required since Spark stores both Integer and Float as Numeric
            if "int" in column_name:
                df[column] = df[column].astype("int64")
            elif "float" in column_name:
                df[column] = df[column].astype("float64")
            elif "datetime" in column_name:
                # I'm not sure why, but something does not support reading milliseconds from Oracle.
                # It's probably Oracle JDBC Dialect, but I'm not sure.
                # Just cut them off.
                df[column] = df[column].dt.floor("S")

        return df
