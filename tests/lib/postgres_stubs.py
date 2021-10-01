# noinspection PyProtectedMember
from os import environ
from functools import wraps
from logging import getLogger

from psycopg2 import extras, connect

logger = getLogger(__name__)


# noinspection SqlNoDataSourceInspection,SqlResolve,SqlDialectInspection
class PostgresTableStub:
    def __init__(self, connection):
        self.connection = connection

    def create_schema(self, schema: str = "schema_for_hive"):
        with self.connection.cursor() as cursor:
            cursor.execute(f"create schema if not exists {schema}")  # schema_for_hive

    def create_table(self, schema: str, table: str, fields: list):
        # example fields = [('d_id', 'serial primary key'), ('d_name', 'text')]
        with self.connection.cursor() as cursor:
            str_fields = ", ".join([f"{f[0]} \n{f[1]}" for f in fields])
            sql = f"""
                create table if not exists {schema}.{table} (
                    {str_fields}
                )
            """
            cursor.execute(sql)

    def bulk_insert(self, database, table, field_names, values, page_size=500, fetch=False):
        with self.connection.cursor() as cursor:
            str_fields = f' {", ".join(field_names)}'
            extras.execute_values(
                cursor,
                f"insert into {database}.{table} ({str_fields}) values %s",  # noqa: WPS323
                values,
                page_size=page_size,
                fetch=fetch,
            )

    def drop_table(self, schema: str, table: str):
        with self.connection.cursor() as cursor:
            cursor.execute(f"drop table if exists {schema}.{table} cascade")


def tables_generator(rdbms_struct, pre_create_table=True):  # noqa: WPS231
    # example rdbms_struct = {'schema': {'test_table': [('d_id', 'serial primary key'), ('d_name', 'text')]}}
    def create_dbs_and_tables(test):  # noqa: WPS231
        @wraps(test)
        def wrapped(*args, **kwargs):
            postgres_conn = connect(environ["ONETL_PG_CONN"])
            databases = rdbms_struct.keys()
            stub = PostgresTableStub(postgres_conn)
            full_names_list = []
            try:
                for database in databases:
                    stub.create_schema(database)
                    tables = rdbms_struct[database].keys()
                    for table in tables:
                        full_name = (database, table)
                        full_names_list.append(full_name)
                        if pre_create_table:
                            stub.create_table(*full_name, rdbms_struct.get(database).get(table))
                postgres_conn.commit()
                test(*args, **kwargs)
            except Exception as error:
                logger.exception(error)
                raise error
            finally:
                for t in full_names_list:
                    stub.drop_table(*t)
                postgres_conn.commit()
                postgres_conn.close()

        return wrapped

    return create_dbs_and_tables
