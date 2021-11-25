# noinspection PyPackageRequirements
import logging

from onetl.connection.db_connection import Clickhouse
from onetl.reader.db_reader import DBReader
from onetl.writer.db_writer import DBWriter


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
            user=processing.user,
            password=processing.password,
            database=processing.database,
            spark=spark,
        )

        with caplog.at_level(logging.INFO):
            clickhouse.check()
        assert "Connection is available" in caplog.text

    def test_clickhouse_reader_snapshot(self, spark, processing, prepare_schema_table):
        clickhouse = Clickhouse(
            host=processing.host,
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

    def test_clickhouse_writer_snapshot(self, spark, processing, prepare_schema_table):
        df = processing.create_spark_df(spark=spark)

        clickhouse = Clickhouse(
            host=processing.host,
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
