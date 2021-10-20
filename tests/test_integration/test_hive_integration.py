# noinspection PyPackageRequirements

from onetl.connection.db_connection import Hive
from onetl.reader.db_reader import DBReader
from onetl.writer.db_writer import DBWriter


class TestIntegrationONETLHive:
    def test_hive_reader_snapshot(self, spark, processing, prepare_schema_table):
        hive = Hive(spark=spark)

        reader = DBReader(
            connection=hive,
            table=prepare_schema_table["full_name"],
        )

        df = reader.run()

        processing.assert_equal_df(
            schema_name=prepare_schema_table["schema"],
            table=prepare_schema_table["table"],
            df=df,
        )

    def test_hive_writer_snapshot(self, spark, processing, prepare_schema_table):
        df = processing.create_spark_df(spark)

        hive = Hive(spark=spark)

        writer = DBWriter(
            connection=hive,
            table=prepare_schema_table["full_name"],
        )

        writer.run(df)

        processing.assert_equal_df(
            schema_name=prepare_schema_table["schema"],
            table=prepare_schema_table["table"],
            df=df,
        )
