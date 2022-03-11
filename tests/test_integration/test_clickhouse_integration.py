import logging

import pytest

from onetl.connection import Clickhouse
from onetl.core import DBReader, DBWriter


class TestIntegrationONETLClickhouse:
    """
    The test name affects how the test works: the second and third words define the behavior of the test.
    For example: test_<storage_name>_<reader/writer>_...
    <storage_name> - the name of the database in which the table will be pre-created.
    <reader/writer> - if reader is specified then the table will be pre-created and filled with test data,
    if writer is specified then only preliminary table creation will be performed.
    The name of the test will be given to the test table.
    """

    def test_clickhouse_connection_check(self, spark, processing, caplog):
        clickhouse = Clickhouse(
            host=processing.host,
            port=processing.port,
            user=processing.user,
            password=processing.password,
            database=processing.database,
            spark=spark,
        )

        with caplog.at_level(logging.INFO):
            clickhouse.check()

        assert "Connection is available" in caplog.text

    def test_clickhouse_wrong_connection_check(self, spark):
        clickhouse = Clickhouse(host="host", user="some_user", password="pwd", database="abc", spark=spark)

        with pytest.raises(RuntimeError):
            clickhouse.check()

    def test_clickhouse_reader_snapshot(self, spark, processing, prepare_schema_table):
        clickhouse = Clickhouse(
            host=processing.host,
            port=processing.port,
            user=processing.user,
            password=processing.password,
            database=processing.database,
            spark=spark,
        )

        reader = DBReader(
            connection=clickhouse,
            table=prepare_schema_table.full_name,
        )

        df = reader.run()

        processing.assert_equal_df(
            schema=prepare_schema_table.schema,
            table=prepare_schema_table.table,
            df=df,
        )

    def test_clickhouse_reader_snapshot_without_set_database(self, spark, processing, prepare_schema_table):
        clickhouse = Clickhouse(
            host=processing.host,
            port=processing.port,
            user=processing.user,
            password=processing.password,
            spark=spark,
        )

        reader = DBReader(
            connection=clickhouse,
            table=prepare_schema_table.full_name,
        )

        df = reader.run()

        processing.assert_equal_df(
            schema=prepare_schema_table.schema,
            table=prepare_schema_table.table,
            df=df,
        )

    def test_clickhouse_writer_snapshot(self, spark, processing, prepare_schema_table):
        df = processing.create_spark_df(spark=spark)

        clickhouse = Clickhouse(
            host=processing.host,
            port=processing.port,
            user=processing.user,
            password=processing.password,
            database=processing.database,
            spark=spark,
        )

        writer = DBWriter(
            connection=clickhouse,
            table=prepare_schema_table.full_name,
        )

        writer.run(df)

        processing.assert_equal_df(
            schema=prepare_schema_table.schema,
            table=prepare_schema_table.table,
            df=df,
        )

    def test_clickhouse_reader_snapshot_with_columns(self, spark, processing, prepare_schema_table):
        clickhouse = Clickhouse(
            host=processing.host,
            port=processing.port,
            user=processing.user,
            password=processing.password,
            database=processing.database,
            spark=spark,
        )

        reader1 = DBReader(
            connection=clickhouse,
            table=prepare_schema_table.full_name,
        )
        table_df = reader1.run()

        reader2 = DBReader(
            connection=clickhouse,
            table=prepare_schema_table.full_name,
            columns=["count(*)"],
        )
        count_df = reader2.run()

        assert count_df.collect()[0][0] == table_df.count()

    def test_clickhouse_reader_snapshot_with_where(self, spark, processing, prepare_schema_table):
        clickhouse = Clickhouse(
            host=processing.host,
            port=processing.port,
            user=processing.user,
            password=processing.password,
            database=processing.database,
            spark=spark,
        )

        reader = DBReader(
            connection=clickhouse,
            table=prepare_schema_table.full_name,
        )
        table_df = reader.run()

        reader1 = DBReader(
            connection=clickhouse,
            table=prepare_schema_table.full_name,
            where="id_int < 1000",
        )
        table_df1 = reader1.run()
        assert table_df1.count() == table_df.count()

        reader2 = DBReader(
            connection=clickhouse,
            table=prepare_schema_table.full_name,
            where="id_int < 1000 OR id_int = 1000",
        )
        table_df2 = reader2.run()
        assert table_df2.count() == table_df.count()

        processing.assert_equal_df(
            schema=prepare_schema_table.schema,
            table=prepare_schema_table.table,
            df=table_df1,
        )

        reader3 = DBReader(
            connection=clickhouse,
            table=prepare_schema_table.full_name,
            where="id_int = 50",
        )
        one_df = reader3.run()

        assert one_df.count() == 1

        reader4 = DBReader(
            connection=clickhouse,
            table=prepare_schema_table.full_name,
            where="id_int > 1000",
        )
        empty_df = reader4.run()

        assert not empty_df.count()

    def test_clickhouse_reader_snapshot_with_columns_and_where(self, spark, processing, prepare_schema_table):
        clickhouse = Clickhouse(
            host=processing.host,
            port=processing.port,
            user=processing.user,
            password=processing.password,
            database=processing.database,
            spark=spark,
        )

        reader1 = DBReader(
            connection=clickhouse,
            table=prepare_schema_table.full_name,
            where="id_int < 80 AND id_int > 10",
        )
        table_df = reader1.run()

        reader2 = DBReader(
            connection=clickhouse,
            table=prepare_schema_table.full_name,
            columns=["count(*)"],
            where="id_int < 80 AND id_int > 10",
        )
        count_df = reader2.run()

        assert count_df.collect()[0][0] == table_df.count()
