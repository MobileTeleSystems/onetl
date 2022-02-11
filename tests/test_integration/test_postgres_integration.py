import logging

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

    def test_postgres_connection_check(self, spark, processing, caplog):
        postgres = Postgres(
            host=processing.host,
            port=processing.port,
            user=processing.user,
            password=processing.password,
            database=processing.database,
            spark=spark,
        )

        with caplog.at_level(logging.INFO):
            postgres.check()
        assert "Connection is available" in caplog.text

    def test_postgres_wrong_connection_check(self, spark):
        postgres = Postgres(host="host", database="db", user="some_user", password="pwd", spark=spark)

        with pytest.raises(RuntimeError):
            postgres.check()

    def test_postgres_reader_snapshot(self, spark, processing, prepare_schema_table):
        postgres = Postgres(
            host=processing.host,
            port=processing.port,
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

    def test_postgres_reader_snapshot_with_pydantic_options(self, spark, processing, prepare_schema_table):
        postgres = Postgres(
            host=processing.host,
            port=processing.port,
            user=processing.user,
            password=processing.password,
            database=processing.database,
            spark=spark,
        )

        reader = DBReader(
            connection=postgres,
            table=prepare_schema_table.full_name,
            options=Postgres.Options(batchsize=500),
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
            port=processing.port,
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

    def test_postgres_writer_snapshot_with_dict_options(self, spark, processing, prepare_schema_table):
        df = processing.create_spark_df(spark=spark)

        postgres = Postgres(
            host=processing.host,
            port=processing.port,
            user=processing.user,
            password=processing.password,
            database=processing.database,
            spark=spark,
        )

        writer = DBWriter(
            connection=postgres,
            table=prepare_schema_table.full_name,
            options={"batchsize": "500"},
        )

        writer.run(df)

        processing.assert_equal_df(
            schema=prepare_schema_table.schema,
            table=prepare_schema_table.table,
            df=df,
        )

    def test_postgres_writer_snapshot_with_pydantic_options(self, spark, processing, prepare_schema_table):
        df = processing.create_spark_df(spark=spark)

        postgres = Postgres(
            host=processing.host,
            port=processing.port,
            user=processing.user,
            password=processing.password,
            database=processing.database,
            spark=spark,
        )

        writer = DBWriter(
            connection=postgres,
            table=prepare_schema_table.full_name,
            options=Postgres.Options(batchsize=500),
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
            port=processing.port,
            user=processing.user,
            password=processing.password,
            database=processing.database,
            spark=spark,
        )

        writer = DBWriter(
            connection=postgres,
            table=prepare_schema_table.full_name,
            options=Postgres.Options(mode=mode),
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

    @pytest.mark.parametrize(
        "options",
        [
            {"numPartitions": "2", "partitionColumn": "hwm_int"},
            {"numPartitions": "2", "partitionColumn": "hwm_int", "lowerBound": "50"},
            {"numPartitions": "2", "partitionColumn": "hwm_int", "upperBound": "70"},
            {"fetchsize": "2"},
        ],
    )
    def test_postgres_reader_different_options(self, spark, processing, prepare_schema_table, options):

        postgres = Postgres(
            host=processing.host,
            port=processing.port,
            user=processing.user,
            password=processing.password,
            database=processing.database,
            spark=spark,
        )

        reader = DBReader(
            connection=postgres,
            table=prepare_schema_table.full_name,
            options=options,
        )
        table_df = reader.run()

        processing.assert_equal_df(
            schema=prepare_schema_table.schema,
            table=prepare_schema_table.table,
            df=table_df,
        )
