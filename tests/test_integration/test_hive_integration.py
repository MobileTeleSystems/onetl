# noinspection PyPackageRequirements
import logging
import pytest

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

    def test_hive_check(self, spark, caplog):
        hive = Hive(spark=spark)

        with caplog.at_level(logging.INFO):
            hive.check()
        assert "Connection is available" in caplog.text

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

    @pytest.mark.parametrize(
        "options",
        [  # noqa: WPS317
            {"some", "option"},
            "Some_options",
            123,
            ["Option_1", "Option_2"],
            ("Option_1", "Option_2"),
        ],
        ids=[
            "Wrong type set of <options>.",
            "Wrong type str of <options>.",
            "Wrong type int of <options>.",
            "Wrong type list of <options>.",
            "Wrong type tuple of <options>.",
        ],
    )
    def test_hive_reader_snapshot_with_options_error(self, spark, processing, prepare_schema_table, options):
        hive = Hive(spark=spark)

        with pytest.raises(ValueError):
            DBReader(
                connection=hive,
                table="onetl.some_table",
                options=options,  # wrong <options>
            )

    def test_hive_writer_snapshot(self, spark, processing, prepare_schema_table):
        df = processing.create_spark_df(spark)

        hive = Hive(spark=spark)

        writer = DBWriter(
            connection=hive,
            table=prepare_schema_table.full_name,
            options=Hive.Options(insert_into=True),
        )

        writer.run(df)

        processing.assert_equal_df(
            schema=prepare_schema_table.schema,
            table=prepare_schema_table.table,
            df=df,
        )

    def test_hive_writer_with_dict_options(self, spark, processing, prepare_schema_table):
        df = processing.create_spark_df(spark)

        hive = Hive(spark=spark)

        writer = DBWriter(
            connection=hive,
            table="onetl.some_table",
            options={"compression": "snappy"},
        )

        writer.run(df)

        response = spark.sql("SHOW CREATE TABLE onetl.some_table")
        response = response.collect()[0][0]
        spark.sql("DROP TABLE onetl.some_table")

        assert """`compression` 'snappy'""" in response  # noqa: WPS462, WPS322

    def test_hive_writer_with_pydantic_options(self, spark, processing, prepare_schema_table):
        df = processing.create_spark_df(spark)

        hive = Hive(spark=spark)

        writer = DBWriter(
            connection=hive,
            table="onetl.some_table",
            options=Hive.Options(compression="snappy"),
        )

        writer.run(df)

        response = spark.sql("SHOW CREATE TABLE onetl.some_table")
        response = response.collect()[0][0]
        spark.sql("DROP TABLE onetl.some_table")

        assert """`compression` 'snappy'""" in response  # noqa: WPS462, WPS322

    @pytest.mark.parametrize(
        "sort_by",
        ["id_int", ["id_int", "hwm_int"]],
        ids=["sortBy as string.", "sortBy as List."],
    )
    def test_hive_writer_default_format_orc_save_table(self, spark, processing, prepare_schema_table, sort_by):
        df = processing.create_spark_df(spark)
        hive = Hive(spark=spark)

        writer = DBWriter(
            connection=hive,
            table="onetl.some_table",
            options=Hive.Options(sortBy=sort_by, bucketBy=(10, "hwm_int")),
        )

        writer.run(df)

        response = spark.sql("SHOW CREATE TABLE onetl.some_table")
        response = response.collect()[0][0]
        spark.sql("DROP TABLE onetl.some_table")

        assert "USING orc" in response

    def test_hive_writer_with_write_options_sortby_bucketby(self, spark, processing, prepare_schema_table):
        df = processing.create_spark_df(spark)

        hive = Hive(spark=spark)

        writer = DBWriter(
            connection=hive,
            table="onetl.some_table",
            options=Hive.Options(sortBy=["id_int"], bucketBy=(10, "hwm_int")),
        )

        writer.run(df)

        response = spark.sql("SHOW CREATE TABLE onetl.some_table")
        response = response.collect()[0][0]
        spark.sql("DROP TABLE onetl.some_table")

        assert "CLUSTERED BY (hwm_int)" in response
        assert "SORTED BY (id_int)" in response
        assert "INTO 10 BUCKETS" in response

    def test_hive_writer_with_write_options_partitionby(self, spark, processing, prepare_schema_table):
        df = processing.create_spark_df(spark)

        hive = Hive(spark=spark)

        writer = DBWriter(
            connection=hive,
            table="onetl.some_table",
            options=Hive.Options(partitionBy="hwm_date"),
        )

        writer.run(df)

        response = spark.sql("SHOW CREATE TABLE onetl.some_table")
        response = response.collect()[0][0]
        spark.sql("DROP TABLE onetl.some_table")

        assert "PARTITIONED BY (hwm_date)" in response

    @pytest.mark.parametrize("mode", ["append", "overwrite"])
    def test_hive_writer_mode(self, spark, processing, prepare_schema_table, mode):
        df = processing.create_spark_df(spark=spark, min_id=1, max_id=1500)
        df1 = df[df.id_int < 1001]
        df2 = df[df.id_int > 1000]

        hive = Hive(spark=spark)

        writer = DBWriter(
            connection=hive,
            table=prepare_schema_table.full_name,
            mode=mode,
            options=Hive.Options(insert_into=True),
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

    def test_hive_writer_default_append(self, spark, processing, prepare_schema_table):
        df = processing.create_spark_df(spark=spark, min_id=1, max_id=1500)
        df1 = df[df.id_int < 1001]
        df2 = df[df.id_int > 1000]

        hive = Hive(spark=spark)

        writer = DBWriter(
            connection=hive,
            table=prepare_schema_table.full_name,
            options=Hive.Options(insert_into=True),
        )

        writer.run(df1)
        writer.run(df2)

        processing.assert_equal_df(
            schema=prepare_schema_table.schema,
            table=prepare_schema_table.table,
            df=df,
        )
