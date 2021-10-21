from onetl.connection.db_connection import Oracle
from onetl.reader.db_reader import DBReader
from onetl.writer.db_writer import DBWriter


class TestIntegrationONETLOracle:
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
