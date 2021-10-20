# noinspection PyPackageRequirements

from onetl.connection.db_connection import Postgres
from onetl.reader.db_reader import DBReader
from onetl.writer.db_writer import DBWriter


class TestIntegrationONETLPostgres:
    # test_<storage_name>_<reader/writer>_...
    # reader - create + autofill source table, writer - just create table

    def test_postgres_reader_snapshot(self, spark, processing, prepare_schema_table):
        postgres = Postgres(
            host=processing.host,
            user=processing.user,
            password=processing.password,
            database=processing.database,
            spark=spark,
        )

        reader = DBReader(
            connection=postgres,
            table=prepare_schema_table["full_name"],
        )
        table_df = reader.run()

        processing.assert_equal_df(
            schema_name=prepare_schema_table["schema"],
            table=prepare_schema_table["table"],
            df=table_df,
        )

    def test_postgres_writer_snapshot(self, spark, processing, prepare_schema_table):
        df = processing.create_spark_df(spark=spark)

        postgres = Postgres(
            host=processing.host,
            user=processing.user,
            password=processing.password,
            database=processing.database,
            spark=spark,
        )

        writer = DBWriter(
            connection=postgres,
            table=prepare_schema_table["full_name"],
        )

        writer.run(df)

        processing.assert_equal_df(
            schema_name=prepare_schema_table["schema"],
            table=prepare_schema_table["table"],
            df=df,
        )

    def test_postgres_writer_mode_append(self, spark, processing, prepare_schema_table):
        df = processing.create_spark_df(spark=spark, min_id=1, max_id=1500)
        df1 = df[df.id_int < 1001]
        df2 = df[df.id_int > 1000]

        postgres = Postgres(
            host=processing.host,
            user=processing.user,
            password=processing.password,
            database=processing.database,
            spark=spark,
        )

        writer = DBWriter(
            connection=postgres,
            table=prepare_schema_table["full_name"],
            mode="append",
        )

        writer.run(df1)
        writer.run(df2)

        processing.assert_equal_df(
            schema_name=prepare_schema_table["schema"],
            table=prepare_schema_table["table"],
            df=df,
        )

    def test_postgres_writer_mode_overwrite(self, spark, processing, prepare_schema_table):
        df = processing.create_spark_df(spark=spark, min_id=1, max_id=1500)
        df1 = df[df.id_int < 1001]
        df2 = df[df.id_int > 1000]

        postgres = Postgres(
            host=processing.host,
            user=processing.user,
            password=processing.password,
            database=processing.database,
            spark=spark,
        )

        writer = DBWriter(
            connection=postgres,
            table=prepare_schema_table["full_name"],
            mode="overwrite",
        )

        # loading is done twice to validate the test
        writer.run(df1)
        writer.run(df2)

        processing.assert_equal_df(
            schema_name=prepare_schema_table["schema"],
            table=prepare_schema_table["table"],
            df=df2,
        )
