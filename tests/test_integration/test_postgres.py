# noinspection PyPackageRequirements
import pytest

from os import environ

import psycopg2
from pyspark.sql import SparkSession

from onetl.connection.db_connection import Postgres, Hive
from onetl.reader.db_reader import DBReader
from onetl.writer.db_writer import DBWriter

from tests.lib import postgres_stubs

FULL_PG_HIVE = {'fiddle': {'full_postgres_to_hive': [('d_id', 'serial primary key'), ('d_name', 'text')]}}


@pytest.fixture(scope='class')
def teardown_hive_and_hdfs(hive_client, hdfs_client):
    with hive_client.cursor() as cursor:
        cursor.execute('create schema if not exists hive_pg_schema')
    yield
    with hive_client.cursor() as cursor:
        cursor.execute('drop table if exists hive_pg_schema.full_postgres_to_hive')


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
@pytest.mark.usefixtures('teardown_hive_and_hdfs')
class TestOnETLPostgres:
    @postgres_stubs.tables_generator(FULL_PG_HIVE)
    def test_full_postgres_to_hive(self, hive_client, spark_conf):
        with psycopg2.connect(environ['ONETL_PG_CONN']) as postgres_conn:
            fiddle_stub = postgres_stubs.PostgresTableStub(postgres_conn)
            fiddle_stub.bulk_insert(
                'fiddle', 'full_postgres_to_hive', ['d_id', 'd_name'], ((i + 1, 'guy') for i in range(10 ** 3)),
            )

        with SparkSession.builder.config(conf=spark_conf).enableHiveSupport().getOrCreate() as spark:
            spark.conf.set('spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation', 'true')

            jdbc_options = {'sessionInitStatement': 'select 300', 'fetchsize': '100'}
            postgres = Postgres(host='postgres', login='onetl', password='onetl', database='onetl', spark=spark)

            reader = DBReader(
                connection=postgres,
                table='fiddle.full_postgres_to_hive',
                jdbc_options=jdbc_options,
            )
            table_df = reader.run()

            hive = Hive(spark=spark)
            writer = DBWriter(
                connection=hive,
                table='hive_pg_schema.full_postgres_to_hive',
                format='parquet',
                mode='overwrite',
            )
            writer.run(table_df)

        with hive_client.cursor() as cursor:
            cursor.execute('select min(d_id), max(d_id) from hive_pg_schema.full_postgres_to_hive')
            first_id, last_id = cursor.fetchone()

        assert first_id == 1
        assert last_id == 1000

    # TODO: тест с созданием сессии через mts spark
    # TODO: полное сравнение df source и target
