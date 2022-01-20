import logging
import pytest

from onetl.connection.db_connection import Oracle
from onetl.reader.db_reader import DBReader
from onetl.writer.db_writer import DBWriter


class TestIntegrationONETLOracle:
    """
    The test name affects how the test works: the second and third words define the behavior of the test.
    For example: test_<storage_name>_<reader/writer>_...
    <storage_name> - the name of the database in which the table will be pre-created.
    <reader/writer> - if reader is specified then the table will be pre-created and filled with test data,
    if writer is specified then only preliminary table creation will be performed.
    The name of the test will be given to the test table.
    """

    def test_oracle_connection_check(self, spark, processing, caplog):
        oracle = Oracle(
            host=processing.host,
            user=processing.user,
            password=processing.password,
            database=processing.database,
            spark=spark,
            sid=processing.sid,
        )

        with caplog.at_level(logging.INFO):
            oracle.check()
        assert "Connection is available" in caplog.text

    def test_mysql_wrong_connection_check(self, spark):
        oracle = Oracle(host="host", user="some_user", password="pwd", database="abc", sid="cde", spark=spark)

        with pytest.raises(RuntimeError):
            oracle.check()

    def test_oracle_reader_snapshot(self, spark, processing, prepare_schema_table):
        oracle = Oracle(
            host=processing.host,
            user=processing.user,
            password=processing.password,
            database=processing.database,
            spark=spark,
            sid=processing.sid,
        )

        reader = DBReader(
            connection=oracle,
            table=prepare_schema_table.full_name,
        )
        df = reader.run()

        processing.assert_equal_df(
            schema=prepare_schema_table.schema,
            table=prepare_schema_table.table,
            df=df,
        )

    def test_oracle_writer_snapshot(self, spark, processing, prepare_schema_table):
        df = processing.create_spark_df(spark=spark)

        oracle = Oracle(
            host=processing.host,
            user=processing.user,
            password=processing.password,
            database=processing.database,
            spark=spark,
            sid=processing.sid,
        )

        writer = DBWriter(
            connection=oracle,
            table=prepare_schema_table.full_name,
        )

        writer.run(df)

        processing.assert_equal_df(
            schema=prepare_schema_table.schema,
            table=prepare_schema_table.table,
            df=df,
        )
