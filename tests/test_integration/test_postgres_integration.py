# noinspection PyPackageRequirements
import pytest

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
        df = processing.create_spark_df(spark)

        writer = DBWriter(
            Postgres(),
            table="table",
            jdbc_options={"user": "some_user"},
        )

        with pytest.raises(ValueError):
            writer.run(df)
