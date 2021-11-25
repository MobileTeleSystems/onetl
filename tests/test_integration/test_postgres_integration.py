# noinspection PyPackageRequirements
import pytest

from onetl.connection.db_connection import Postgres
from onetl.reader.db_reader import DBReader
from onetl.writer.db_writer import DBWriter


class TestIntegrationONETLPostgres:
    """
    The test name affects how the test works: the second and third words define the behavior of the test.
    For example: test_<storage_name>_<reader/writer>_...
    <storage_name> - the name of the database in which the table will be pre-created.
    <reader/writer> - if reader is specified then the table will be pre-created and filled with test data,
    if writer is specified then only preliminary table creation will be performed.
    The name of the test will be given to the test table.
    """

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
            table=prepare_schema_table.full_name,
        )
        table_df = reader.run()

        processing.assert_equal_df(
            schema=prepare_schema_table.schema,
            table=prepare_schema_table.table,
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
            table=prepare_schema_table.full_name,
        )

        writer.run(df)

        processing.assert_equal_df(
            schema=prepare_schema_table.schema,
            table=prepare_schema_table.table,
            df=df,
        )

    @pytest.mark.parametrize("mode", ["append", "overwrite"])
    def test_postgres_writer_mode(self, spark, processing, prepare_schema_table, mode):
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
            table=prepare_schema_table.full_name,
            mode=mode,
        )

        writer.run(df1)
        writer.run(df2)

        if mode == "append":
            processing.assert_equal_df(
                schema=prepare_schema_table.schema,
                table=prepare_schema_table.table,
                df=df,
            )

        if mode == "overwrite":
            processing.assert_equal_df(
                schema=prepare_schema_table.schema,
                table=prepare_schema_table.table,
                df=df2,
            )

    def test_postgres_writer_jdbc_properties_value_error(self, spark, processing):
        """Overrides the <user> parameter in the jdbc_options parameter which was already defined in the connection"""
        df = processing.create_spark_df(spark)

        writer = DBWriter(
            Postgres(spark=spark, host="some_host", user="valid_user", password="pwd"),
            table="table",
            jdbc_options={"user": "some_user"},
        )

        with pytest.raises(ValueError):
            writer.run(df)
