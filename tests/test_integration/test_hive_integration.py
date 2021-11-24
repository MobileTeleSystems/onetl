# noinspection PyPackageRequirements
from onetl.connection.db_connection import Hive
from onetl.reader.db_reader import DBReader
from onetl.writer.db_writer import DBWriter


class TestIntegrationONETLHive:
    """
    The test name affects how the test works: the second and third words define the behavior of the test.
    For example: test_<storage_name>_<reader/writer>_...
    <storage_name> - the name of the database in which the table will be pre-created.
    <reader/writer> - if reader is specified then the table will be pre-created and filled with test data,
    if writer is specified then only preliminary table creation will be performed.
    The name of the test will be given to the test table.
    """

    def test_hive_reader_snapshot(self, spark, processing, prepare_schema_table):
        hive = Hive(spark=spark)

        reader = DBReader(
            connection=hive,
            table=prepare_schema_table.full_name,
        )

        df = reader.run()

        processing.assert_equal_df(
            schema=prepare_schema_table.schema,
            table=prepare_schema_table.table,
            df=df,
        )

    def test_hive_writer_snapshot(self, spark, processing, prepare_schema_table):
        df = processing.create_spark_df(spark)

        hive = Hive(spark=spark)

        writer = DBWriter(
            connection=hive,
            table=prepare_schema_table.full_name,
        )

        writer.run(df)

        processing.assert_equal_df(
            schema=prepare_schema_table.schema,
            table=prepare_schema_table.table,
            df=df,
        )
