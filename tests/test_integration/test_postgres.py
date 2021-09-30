# noinspection PyPackageRequirements
import pytest

from os import environ
from datetime import datetime

import psycopg2
from pyspark.sql.types import StructField, StringType, TimestampType, ArrayType
from pyspark.sql import Row

from onetl.connection.db_connection import Postgres, Hive
from onetl.reader.db_reader import DBReader
from onetl.writer.db_writer import DBWriter

from tests.lib import postgres_stubs
from tests.lib.common import insert_into_hive_table, dataframe_equal

PG_TO_HIVE_SNAPSHOT = {'fiddle': {'full_postgres_to_hive': [('d_id', 'serial primary key'), ('d_name', 'text')]}}
PG_TO_HIVE_TEST_LOWER_UPPER_BOUND_FILTERING = {
    'fiddle': {
        'postgres_to_hive_test_lower_upper_bound_filtering': [('d_id', 'serial primary key'), ('d_name', 'text')],
    },
}
PG_TO_HIVE_TEST_LOWER_UPPER_AUTO_SETTING = {
    'fiddle': {
        'postgres_to_hive_test_lower_upper_auto_setting': [('d_id', 'serial primary key'), ('d_name', 'text')],
    },
}
HIVE_TO_PG_SNAPSHOT = {'schema_for_hive': {
    'hive2rdb_snapshot': [('d_datetime', 'timestamp primary key'), ('d_name', 'text')],
}}


# dropping tables in hive after tests finished
@pytest.fixture(scope='class')
def teardown_hive_and_hdfs(hive_client, hdfs_client):
    with hive_client.cursor() as cursor:
        cursor.execute('create schema if not exists hive_pg_schema')
    yield
    with hive_client.cursor() as cursor:
        cursor.execute('drop table if exists hive_pg_schema.full_postgres_to_hive')
        cursor.execute('drop table if exists hive_pg_schema.postgres_to_hive_test_lower_upper_bound_filtering')
        cursor.execute('drop table if exists hive_pg_schema.postgres_to_hive_test_lower_upper_auto_setting')
        cursor.execute('drop table if exists schema_for_hive.hive2rdb_snapshot')


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
@pytest.mark.usefixtures('teardown_hive_and_hdfs')
class TestOnETLPostgresToHive:
    @postgres_stubs.tables_generator(PG_TO_HIVE_SNAPSHOT)
    def test_snapshot_postgres_to_hive(self, spark):
        with psycopg2.connect(environ['ONETL_PG_CONN']) as postgres_conn:
            fiddle_stub = postgres_stubs.PostgresTableStub(postgres_conn)
            fiddle_stub.bulk_insert(
                'fiddle', 'full_postgres_to_hive', ['d_id', 'd_name'], ((i + 1, 'guy') for i in range(10 ** 3)),
            )

        jdbc_options = {'sessionInitStatement': 'select 300', 'fetchsize': '100'}
        postgres = Postgres(host='postgres', user='onetl', password='onetl', database='onetl', spark=spark)

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

        # Check Hive
        hive_reader = DBReader(
            connection=hive,
            table='hive_pg_schema.full_postgres_to_hive',
        )
        hive_df = hive_reader.run()

        assert dataframe_equal(table_df, hive_df)

    @postgres_stubs.tables_generator(PG_TO_HIVE_TEST_LOWER_UPPER_BOUND_FILTERING)
    def test_lower_upper_bound_filtering(self, hive_client, spark):
        with psycopg2.connect(environ['ONETL_PG_CONN']) as postgres_conn:
            fiddle_stub = postgres_stubs.PostgresTableStub(postgres_conn)
            fiddle_stub.bulk_insert(
                'fiddle',
                'postgres_to_hive_test_lower_upper_bound_filtering',
                ['d_id', 'd_name'],
                ((i, 'guy') for i in range(1, 10 ** 3)),
            )

        postgres = Postgres(host='postgres', user='onetl', password='onetl', database='onetl', spark=spark)
        jdbc_options = {
            'sessionInitStatement': 'select 300',
            'fetchsize': '100',
            'url': postgres.url,
            'lowerBound': 10,
            'upperBound': 200,
            'numPartitions': 5,
            'partitionColumn': 'd_id',
        }

        reader = DBReader(
            connection=postgres,
            table='fiddle.postgres_to_hive_test_lower_upper_bound_filtering',
            jdbc_options=jdbc_options,
            sql_where='d_id >= 10 AND d_id <= 200',
        )

        table_df = reader.run()

        hive = Hive(spark=spark)
        writer = DBWriter(
            connection=hive,
            table='hive_pg_schema.postgres_to_hive_test_lower_upper_bound_filtering',
            format='parquet',
            mode='overwrite',
        )
        writer.run(table_df)
        hive = Hive(spark=spark)
        hive_reader = DBReader(
            connection=hive,
            table='hive_pg_schema.postgres_to_hive_test_lower_upper_bound_filtering',
        )
        hive_df = hive_reader.run()
        assert dataframe_equal(table_df, hive_df)

    @postgres_stubs.tables_generator(PG_TO_HIVE_TEST_LOWER_UPPER_AUTO_SETTING)
    def test_lower_upper_auto_setting(self, spark):
        with psycopg2.connect(environ['ONETL_PG_CONN']) as postgres_conn:
            fiddle_stub = postgres_stubs.PostgresTableStub(postgres_conn)
            fiddle_stub.bulk_insert(
                'fiddle',
                'postgres_to_hive_test_lower_upper_auto_setting',
                ['d_id', 'd_name'],
                ((i + 1, 'guy') for i in range(99, 10 ** 3)),
            )

        postgres = Postgres(
            host='postgres',
            user='onetl',
            password='onetl',
            database='onetl',
            spark=spark,
        )

        jdbc_options = {
            'url': postgres.url,
            'numPartitions': 5,
            'partitionColumn': 'd_id',
        }

        reader = DBReader(
            connection=postgres,
            table='fiddle.postgres_to_hive_test_lower_upper_auto_setting',
            jdbc_options=jdbc_options,
        )

        reader_options = reader.connection.jdbc_params_creator(
            jdbc_options=jdbc_options,
            table=reader.table,
        )

        lower_bound = reader_options['lowerBound']
        upper_bound = reader_options['upperBound']

        assert int(lower_bound) == 100
        assert int(upper_bound) == 1000


class TestOnETLHiveToPostgres:
    # creating initial data in hive for tests where hive is the source
    @pytest.fixture(scope='function')
    def hive_table_setup_snapshot(self, spark, hive_client):
        with hive_client.cursor() as cursor:
            cursor.execute('create schema if not exists onetl_schema')
        datetime_values = [datetime(2021, 4, i, i, i, i) for i in range(1, 11)]
        data = list(zip(datetime_values, ['guy' for _ in range(10)], [('array1', 'array2') for _ in range(10)]))

        insert_into_hive_table(
            spark,
            schema=[
                StructField('d_datetime', TimestampType(), True),
                StructField('d_name', StringType(), True),
                StructField('d_array', ArrayType(StringType()), True),
            ],
            data=data,
            write_mode='overwrite',
            write_method='saveAsTable',
            table_name='onetl_schema.hive2postgres_snapshot',
        )
        yield
        with hive_client.cursor() as cursor:
            cursor.execute('drop table if exists onetl_schema.hive2postgres_snapshot')

    @postgres_stubs.tables_generator(HIVE_TO_PG_SNAPSHOT, pre_create_table=False)
    def test_snapshot_hive_to_postgres_with_custom_where(
        self,
        hive_table_setup_snapshot,
        spark,
    ):

        jdbc_options = {'sessionInitStatement': 'select 300', 'batchsize': '100'}
        hive = Hive(spark=spark)
        postgres = Postgres(host='postgres', user='onetl', password='onetl', database='onetl', spark=spark)

        reader = DBReader(
            connection=hive,
            table='onetl_schema.hive2postgres_snapshot',
            sql_where="d_datetime != '2021-04-02 02:02:02'",
        )
        table_df = reader.run()

        writer = DBWriter(
            connection=postgres,
            table='schema_for_hive.hive2rdb_snapshot',
            mode='overwrite',
            jdbc_options=jdbc_options,
        )
        writer.run(table_df)

        # Check Postgres
        postgres_reader = DBReader(
            connection=postgres,
            table='schema_for_hive.hive2rdb_snapshot',
        )
        postgres_df = postgres_reader.run()

        assert dataframe_equal(table_df, postgres_df)
        assert Row(str(datetime(2021, 4, 2, 2, 2, 2)), 'guy', ['array1', 'array2']) not in postgres_df.collect()
