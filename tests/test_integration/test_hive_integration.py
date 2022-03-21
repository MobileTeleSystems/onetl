import logging

import pandas
import pytest

from onetl.connection import Hive
from onetl.core import DBReader, DBWriter


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

    @pytest.mark.parametrize("suffix", ["", ";"])
    def test_hive_reader_connection_sql(self, spark, processing, prepare_schema_table, suffix):
        hive = Hive(spark=spark)

        schema = prepare_schema_table.schema
        table = prepare_schema_table.full_name

        df = hive.sql(f"SELECT * FROM {table}{suffix}")
        table_df = processing.get_expected_dataframe(
            schema=prepare_schema_table.schema,
            table=prepare_schema_table.table,
            order_by="id_int",
        )

        processing.assert_equal_df(df=df, other_frame=table_df, order_by="id_int")

        df = hive.sql(f"SELECT * FROM {table} WHERE id_int < 50{suffix}")
        filtered_df = table_df[table_df.id_int < 50]
        processing.assert_equal_df(df=df, other_frame=filtered_df, order_by="id_int")

        df = hive.sql("SHOW DATABASES")
        result_df = pandas.DataFrame([["default"], [schema]], columns=["databaseName"])
        processing.assert_equal_df(df=df, other_frame=result_df)

        df = hive.sql(f"SHOW TABLES IN {schema}")
        result_df = pandas.DataFrame(
            [[schema, prepare_schema_table.table, False]],
            columns=["database", "tableName", "isTemporary"],
        )
        processing.assert_equal_df(df=df, other_frame=result_df)

        # wrong syntax
        with pytest.raises(Exception):
            hive.sql(f"SELEC 1{suffix}")

    @pytest.mark.parametrize("suffix", ["", ";"])
    def test_hive_reader_connection_execute_ddl(self, spark, processing, get_schema_table, suffix):
        hive = Hive(spark=spark)

        table_name, schema, table = get_schema_table
        fields = {
            column_name: processing.get_column_type(column_name)
            for column_name in processing.column_names
            if column_name != "id_int"
        }
        id_int_type = processing.get_column_type("id_int")

        assert not hive.execute(processing.create_schema_ddl(schema) + suffix)
        assert not hive.execute(
            processing.create_table_ddl(table, fields, schema) + f" PARTITIONED BY (id_int {id_int_type})" + suffix,
        )

        assert not hive.execute(f"ALTER SCHEMA {schema} SET DBPROPERTIES ('a' = 'b'){suffix}")

        assert not hive.execute(f"ALTER TABLE {table_name} DROP IF EXISTS PARTITION(id_int = 1){suffix}")

        with pytest.raises(Exception):
            hive.execute(f"ALTER TABLE {table_name} DROP PARTITION(id_int = 999){suffix}")

        assert not hive.execute(f"MSCK REPAIR TABLE {table_name}{suffix}")
        assert not hive.execute(processing.drop_table_ddl(table, schema) + suffix)

        assert not hive.execute(processing.create_table_ddl(table, fields, schema) + suffix)
        assert not hive.execute(f"DROP TABLE {table_name} PURGE{suffix}")

        with pytest.raises(Exception):
            hive.execute(
                processing.create_schema_ddl(schema) + "\n" + processing.create_table_ddl(table, schema) + suffix,
            )

        with pytest.raises(Exception):
            hive.execute(
                processing.create_schema_ddl(schema) + ";\n" + processing.create_table_ddl(table, schema) + suffix,
            )

        with pytest.raises(Exception):
            hive.execute(f"DROP TABLE {schema}.missing_table{suffix}")

        with pytest.raises(Exception):
            hive.execute(f"DROP DATABASE rand_db{suffix}")

        assert not hive.execute(f"DROP DATABASE {schema}{suffix}")

    @pytest.mark.parametrize("suffix", ["", ";"])
    def test_hive_reader_connection_execute_dml(self, request, spark, processing, prepare_schema_table, suffix):
        hive = Hive(spark=spark)

        table_name, schema, table = prepare_schema_table
        temp_name = f"{table}_temp"
        temp_table = f"{schema}.{temp_name}"

        table_df = processing.get_expected_dataframe(
            schema=prepare_schema_table.schema,
            table=prepare_schema_table.table,
            order_by="id_int",
        )

        fields = {column_name: processing.get_column_type(column_name) for column_name in processing.column_names}
        assert not hive.execute(processing.create_table_ddl(temp_name, fields, schema) + suffix)

        def table_finalizer():
            hive.execute(processing.drop_table_ddl(temp_name, schema))

        request.addfinalizer(table_finalizer)

        assert not hive.sql(f"SELECT * FROM {temp_table}{suffix}").count()

        assert not hive.execute(f"INSERT INTO {temp_table} SELECT * FROM {table_name}")
        df = hive.sql(f"SELECT * FROM {temp_table}{suffix}")
        assert df.count()
        processing.assert_equal_df(df=df, other_frame=table_df, order_by="id_int")

        # not supported by Hive
        with pytest.raises(Exception):
            hive.execute(f"UPDATE {temp_table} SET id_int = 1 WHERE id_int < 50{suffix}")

        # not supported by Hive
        with pytest.raises(Exception):
            hive.execute(f"DELETE FROM {temp_table} WHERE id_int < 80{suffix}")

        assert not hive.execute(f"TRUNCATE TABLE {temp_table}{suffix}")
        assert not hive.sql(f"SELECT * FROM {temp_table}{suffix}").count()

    def test_hive_reader(self, spark, processing, prepare_schema_table):
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

    def test_hive_reader_snapshot_with_columns(self, spark, processing, prepare_schema_table):
        hive = Hive(spark=spark)

        reader1 = DBReader(
            connection=hive,
            table=prepare_schema_table.full_name,
        )
        table_df = reader1.run()

        reader2 = DBReader(
            connection=hive,
            table=prepare_schema_table.full_name,
            columns=["count(*)"],
        )
        count_df = reader2.run()

        assert count_df.collect()[0][0] == table_df.count()

    def test_hive_reader_snapshot_with_where(self, spark, processing, prepare_schema_table):
        hive = Hive(spark=spark)

        reader1 = DBReader(
            connection=hive,
            table=prepare_schema_table.full_name,
            where="id_int < 1000",
        )
        table_df = reader1.run()

        assert table_df.count() == 100
        processing.assert_equal_df(
            schema=prepare_schema_table.schema,
            table=prepare_schema_table.table,
            df=table_df,
        )

        reader2 = DBReader(
            connection=hive,
            table=prepare_schema_table.full_name,
            where="id_int = 50",
        )
        one_df = reader2.run()

        assert one_df.count() == 1

        reader3 = DBReader(
            connection=hive,
            table=prepare_schema_table.full_name,
            where="id_int > 1000",
        )
        empty_df = reader3.run()

        assert not empty_df.count()

    def test_hive_reader_snapshot_with_columns_and_where(self, spark, processing, prepare_schema_table):
        hive = Hive(spark=spark)

        reader1 = DBReader(
            connection=hive,
            table=prepare_schema_table.full_name,
            where="id_int < 80 AND id_int > 10",
        )
        table_df = reader1.run()

        reader2 = DBReader(
            connection=hive,
            table=prepare_schema_table.full_name,
            columns=["count(*)"],
            where="id_int < 80 AND id_int > 10",
        )
        count_df = reader2.run()

        assert count_df.collect()[0][0] == table_df.count()

    def test_hive_reader_raise_exception(self, spark, prepare_schema_table):
        hive = Hive(spark=spark)

        reader = DBReader(
            connection=hive,
            table=prepare_schema_table.full_name,
            options=Hive.Options(abc="cde"),  # Hive does not accept any read options
        )

        with pytest.raises(ValueError):
            reader.run()

    def test_hive_reader_non_existing_table(self, spark, get_schema_table):
        from pyspark.sql.utils import AnalysisException

        hive = Hive(spark=spark)

        reader = DBReader(
            connection=hive,
            table=get_schema_table.full_name,
        )

        with pytest.raises(AnalysisException) as excinfo:
            reader.run()

            assert "does not exists" in str(excinfo.value)

    def test_hive_writer(self, spark, processing, get_schema_table):
        df = processing.create_spark_df(spark)

        hive = Hive(spark=spark)

        writer = DBWriter(
            connection=hive,
            table=get_schema_table.full_name,
        )

        writer.run(df)

        processing.assert_equal_df(
            schema=get_schema_table.schema,
            table=get_schema_table.table,
            df=df,
        )

    def test_hive_writer_with_dict_options(self, spark, processing, get_schema_table):
        df = processing.create_spark_df(spark)

        hive = Hive(spark=spark)

        writer = DBWriter(
            connection=hive,
            table=get_schema_table.full_name,
            options={"compression": "snappy"},
        )

        writer.run(df)

        response = hive.sql(f"SHOW CREATE TABLE {get_schema_table.full_name}")
        response = response.collect()[0][0]

        assert """`compression` 'snappy'""" in response  # noqa: WPS462, WPS322

    def test_hive_writer_with_pydantic_options(self, spark, processing, get_schema_table):
        df = processing.create_spark_df(spark)

        hive = Hive(spark=spark)

        writer = DBWriter(
            connection=hive,
            table=get_schema_table.full_name,
            options=Hive.Options(compression="snappy"),
        )

        writer.run(df)

        response = hive.sql(f"SHOW CREATE TABLE {get_schema_table.full_name}")
        response = response.collect()[0][0]

        assert """`compression` 'snappy'""" in response  # noqa: WPS462, WPS322

    @pytest.mark.parametrize(
        "fmt",
        ["orc", "parquet"],
    )
    def test_hive_writer_with_format(self, spark, processing, get_schema_table, fmt):
        df = processing.create_spark_df(spark)
        hive = Hive(spark=spark)

        writer = DBWriter(
            connection=hive,
            table=get_schema_table.full_name,
            options=Hive.Options(format=fmt),
        )

        writer.run(df)

        response = hive.sql(f"SHOW CREATE TABLE {get_schema_table.full_name}")
        response = response.collect()[0][0]

        assert f"USING {fmt}" in response

    def test_hive_writer_default_format_orc(self, spark, processing, get_schema_table):
        df = processing.create_spark_df(spark)
        hive = Hive(spark=spark)

        writer = DBWriter(
            connection=hive,
            table=get_schema_table.full_name,
        )

        writer.run(df)

        response = hive.sql(f"SHOW CREATE TABLE {get_schema_table.full_name}")
        response = response.collect()[0][0]

        assert "USING orc" in response

    @pytest.mark.parametrize(
        "bucket_number, bucket_columns",
        [
            (10, "id_int"),
            (5, ["id_int", "hwm_int"]),
        ],
        ids=["bucket columns as string.", "bucket columns as List."],
    )
    def test_hive_writer_with_bucket_by(
        self,
        spark,
        processing,
        get_schema_table,
        bucket_number,
        bucket_columns,
    ):
        df = processing.create_spark_df(spark)

        hive = Hive(spark=spark)

        writer = DBWriter(
            connection=hive,
            table=get_schema_table.full_name,
            options=Hive.Options(bucketBy=(bucket_number, bucket_columns)),
        )

        writer.run(df)

        response = hive.sql(f"SHOW CREATE TABLE {get_schema_table.full_name}")
        response = response.collect()[0][0]

        if isinstance(bucket_columns, str):
            assert f"CLUSTERED BY ({bucket_columns})" in response
        else:
            assert f"CLUSTERED BY ({', '.join(bucket_columns)})" in response

        assert f"INTO {bucket_number} BUCKETS" in response

    @pytest.mark.parametrize(
        "sort_by",
        ["id_int", ["id_int", "hwm_int"]],
        ids=["sortBy as string.", "sortBy as List."],
    )
    def test_hive_writer_with_bucket_by_and_sort_by(
        self,
        spark,
        processing,
        get_schema_table,
        sort_by,
    ):
        df = processing.create_spark_df(spark)

        hive = Hive(spark=spark)

        writer = DBWriter(
            connection=hive,
            table=get_schema_table.full_name,
            options=Hive.Options(bucketBy=(10, "id_int"), sortBy=sort_by),
        )

        writer.run(df)

        response = hive.sql(f"SHOW CREATE TABLE {get_schema_table.full_name}")
        response = response.collect()[0][0]

        if isinstance(sort_by, str):
            assert f"SORTED BY ({sort_by})" in response
        else:
            assert f"SORTED BY ({', '.join(sort_by)})" in response

        assert "CLUSTERED BY (id_int)" in response
        assert "INTO 10 BUCKETS" in response

    def test_hive_writer_default_not_bucketed(self, spark, processing, get_schema_table):
        df = processing.create_spark_df(spark)
        hive = Hive(spark=spark)

        writer = DBWriter(
            connection=hive,
            table=get_schema_table.full_name,
        )

        writer.run(df)

        response = hive.sql(f"SHOW CREATE TABLE {get_schema_table.full_name}")
        response = response.collect()[0][0]

        assert "SORTED BY" not in response
        assert "CLUSTERED BY" not in response
        assert "BUCKETS" not in response

    @pytest.mark.parametrize(
        "partition_by",
        [
            "id_int",
            ["id_int", "hwm_int"],
        ],
        ids=["partitionBy as string.", "partitionBy as List."],
    )
    def test_hive_writer_with_partition_by(self, spark, processing, get_schema_table, partition_by):
        df = processing.create_spark_df(spark)

        hive = Hive(spark=spark)

        writer = DBWriter(
            connection=hive,
            table=get_schema_table.full_name,
            options=Hive.Options(partitionBy=partition_by),
        )

        writer.run(df)

        response = hive.sql(f"SHOW CREATE TABLE {get_schema_table.full_name}")
        response = response.collect()[0][0]

        if isinstance(partition_by, str):
            assert f"PARTITIONED BY ({partition_by})" in response
        else:
            assert f"PARTITIONED BY ({', '.join(partition_by)})" in response

    def test_hive_writer_default_not_partitioned(self, spark, processing, get_schema_table):
        df = processing.create_spark_df(spark)
        hive = Hive(spark=spark)

        writer = DBWriter(
            connection=hive,
            table=get_schema_table.full_name,
        )

        writer.run(df)

        response = hive.sql(f"SHOW CREATE TABLE {get_schema_table.full_name}")
        response = response.collect()[0][0]

        assert "PARTITIONED BY" not in response

    @pytest.mark.parametrize("mode", ["append", "overwrite", "error", "ignore"])
    def test_hive_writer_with_mode(self, spark, processing, get_schema_table, mode):
        from pyspark.sql.functions import col

        df = processing.create_spark_df(spark=spark)
        df1 = df[df.id_int <= 50]
        df2 = df[df.id_int > 50]

        df2_reversed = df2.select(*(col(column).alias(column.upper()) for column in reversed(df2.columns)))

        hive = Hive(spark=spark)

        writer = DBWriter(
            connection=hive,
            table=get_schema_table.full_name,
            options=Hive.Options(mode=mode),
        )

        # creates table if does not exist
        writer.run(df1)

        if mode == "error":
            from pyspark.sql.utils import AnalysisException

            with pytest.raises(AnalysisException) as excinfo:
                writer.run(df2_reversed)

                assert "already exists" in str(excinfo.value)
        else:
            writer.run(df2_reversed)

        if mode == "ignore":
            processing.assert_equal_df(
                schema=get_schema_table.schema,
                table=get_schema_table.table,
                df=df1,
            )

        elif mode == "append":
            processing.assert_equal_df(
                schema=get_schema_table.schema,
                table=get_schema_table.table,
                df=df,
                order_by="id_int",
            )

        elif mode == "overwrite":
            # table is truncated
            processing.assert_equal_df(
                schema=get_schema_table.schema,
                table=get_schema_table.table,
                df=df2,
            )

    def test_hive_writer_with_mode_default_append(self, spark, processing, get_schema_table):
        df = processing.create_spark_df(spark=spark)
        df1 = df[df.id_int <= 50]
        df2 = df[df.id_int > 50]

        hive = Hive(spark=spark)

        writer = DBWriter(
            connection=hive,
            table=get_schema_table.full_name,
        )

        writer.run(df1)
        writer.run(df2)

        processing.assert_equal_df(
            schema=get_schema_table.schema,
            table=get_schema_table.table,
            df=df,
            order_by="id_int",
        )

    @pytest.mark.parametrize("mode", ["append", "overwrite"])
    def test_hive_writer_with_mode_and_partition_by(self, spark, processing, get_schema_table, mode):
        df = processing.create_spark_df(spark=spark)
        df1 = df[df.id_int <= 30]
        df2 = df.where("id_int > 30 AND id_int <= 60")
        df3 = df[df.id_int > 60]

        hive = Hive(spark=spark)

        writer = DBWriter(
            connection=hive,
            table=get_schema_table.full_name,
            options=Hive.Options(mode=mode, partitionBy="id_int"),
        )

        # create table with partitions 1 and 2
        writer.run(df1.union(df2))

        # insert partitions 1 and 3
        df13 = df1.union(df3)
        writer.run(df13.select(*reversed(df13.columns)))

        if mode == "append":
            processing.assert_equal_df(
                schema=get_schema_table.schema,
                table=get_schema_table.table,
                df=df1.union(df1).union(df2).union(df3).orderBy("id_int"),
                order_by="id_int",
            )

        elif mode == "overwrite":
            processing.assert_equal_df(
                schema=get_schema_table.schema,
                table=get_schema_table.table,
                df=df1.union(df3).orderBy("id_int"),
                order_by="id_int",
            )

    def test_hive_writer_append_non_existent_column(self, spark, processing, get_schema_table):
        from pyspark.sql.utils import AnalysisException

        df = processing.create_spark_df(spark=spark)

        hive = Hive(spark=spark)

        writer1 = DBWriter(
            connection=hive,
            table=get_schema_table.full_name,
        )

        writer1.run(df)

        writer2 = DBWriter(
            connection=hive,
            table=get_schema_table.full_name,
            options=Hive.Options(mode="append"),
        )

        df2 = df.withColumn("unknown", df.id_int)
        with pytest.raises(AnalysisException):
            writer2.run(df2)

        df3 = df.select(df.id_int, df.hwm_int)
        with pytest.raises(AnalysisException):
            writer2.run(df3)

        df4 = df.withColumn("unknown", df.id_int).select(df.id_int, df.hwm_int, "unknown")
        with pytest.raises(AnalysisException):
            writer2.run(df4)

    def test_hive_writer_insert_into_non_existent_table(self, spark, processing, get_schema_table):
        from pyspark.sql.utils import AnalysisException

        df = processing.create_spark_df(spark=spark)

        hive = Hive(spark=spark)

        writer = DBWriter(
            connection=hive,
            table=get_schema_table.full_name,
            options=Hive.Options(insert_into=True),
        )

        with pytest.raises(AnalysisException) as excinfo:
            writer.run(df)

            assert "does not exists" in str(excinfo.value)

    @pytest.mark.parametrize("mode", ["append", "overwrite"])
    def test_hive_writer_insert_into_with_mode(self, spark, processing, prepare_schema_table, mode):
        from pyspark.sql.functions import col

        df = processing.create_spark_df(spark=spark)
        df1 = df[df.id_int <= 50]
        df2 = df[df.id_int > 50]

        df2_reversed = df2.select(*(col(column).alias(column.upper()) for column in reversed(df2.columns)))

        hive = Hive(spark=spark)

        writer = DBWriter(
            connection=hive,
            table=prepare_schema_table.full_name,
            options=Hive.Options(insert_into=True, mode=mode),
        )

        writer.run(df1)
        writer.run(df2_reversed)

        if mode == "append":
            processing.assert_equal_df(
                schema=prepare_schema_table.schema,
                table=prepare_schema_table.table,
                df=df,
                order_by="id_int",
            )

        elif mode == "overwrite":
            processing.assert_equal_df(
                schema=prepare_schema_table.schema,
                table=prepare_schema_table.table,
                df=df2,
            )

    def test_hive_writer_insert_into_with_mode_default_append(self, spark, processing, prepare_schema_table):
        df = processing.create_spark_df(spark=spark)
        df1 = df[df.id_int <= 50]
        df2 = df[df.id_int > 50]

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
            order_by="id_int",
        )

    @pytest.mark.parametrize("mode", ["append", "overwrite"])
    @pytest.mark.parametrize("partitioned", [True, False])
    @pytest.mark.parametrize(
        "spark",
        [
            {"hive.sql.sources.partitionOverwriteMode": "dynamic"},
            {"hive.sql.sources.partitionOverwriteMode": "static"},
        ],
        indirect=True,
    )
    def test_hive_writer_insert_into_with_mode_and_partitioning(
        self,
        spark,
        processing,
        get_schema_table,
        partitioned,
        mode,
    ):
        df = processing.create_spark_df(spark=spark)
        df1 = df[df.id_int <= 25]
        df2 = df.where("id_int > 25 AND id_int <= 50")
        df3 = df[df.id_int > 50]

        hive = Hive(spark=spark)

        if partitioned:
            options = Hive.Options(partitionBy="id_int")
        else:
            options = None

        writer = DBWriter(
            connection=hive,
            table=get_schema_table.full_name,
            options=options,
        )

        writer.run(df1.union(df2))

        writer = DBWriter(
            connection=hive,
            table=get_schema_table.full_name,
            options=Hive.Options(insert_into=True, mode=mode),
        )

        df13 = df1.union(df3)
        writer.run(df13.select(*reversed(df13.columns)))

        if mode == "append":
            processing.assert_equal_df(
                schema=get_schema_table.schema,
                table=get_schema_table.table,
                df=df1.union(df1).union(df2).union(df3).orderBy("id_int"),
                order_by="id_int",
            )

        elif mode == "overwrite":
            if partitioned and spark.sparkContext._conf.get("hive.sql.sources.partitionOverwriteMode") == "dynamic":
                processing.assert_equal_df(
                    schema=get_schema_table.schema,
                    table=get_schema_table.table,
                    # df2 is left unchanged instead of being removed
                    df=df1.union(df2).union(df3).orderBy("id_int"),
                    order_by="id_int",
                )
            else:
                processing.assert_equal_df(
                    schema=get_schema_table.schema,
                    table=get_schema_table.table,
                    # df2 is removed
                    df=df1.union(df3).orderBy("id_int"),
                    order_by="id_int",
                )

    @pytest.mark.parametrize("mode", ["append", "overwrite"])
    def test_hive_writer_insert_into_wrong_columns(self, spark, processing, get_schema_table, mode):
        df = processing.create_spark_df(spark=spark)

        hive = Hive(spark=spark)

        writer1 = DBWriter(
            connection=hive,
            table=get_schema_table.full_name,
        )

        writer1.run(df)

        writer2 = DBWriter(
            connection=hive,
            table=get_schema_table.full_name,
            options=Hive.Options(insert_into=True, mode=mode),
        )

        df2 = df.withColumn("unknown", df.id_int)
        with pytest.raises(ValueError):
            writer2.run(df2)

        df3 = df.select(df.id_int, df.hwm_int)
        with pytest.raises(ValueError):
            writer2.run(df3)

        df4 = df.withColumn("unknown", df.id_int).select(df.id_int, df.hwm_int, "unknown")
        with pytest.raises(ValueError):
            writer2.run(df4)
