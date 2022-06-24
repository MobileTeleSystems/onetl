import pytest

from onetl.core import DBWriter
from onetl.connection import Hive


def test_hive_writer(spark, processing, get_schema_table):
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


def test_hive_writer_with_dict_options(spark, processing, get_schema_table):
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


def test_hive_writer_with_pydantic_options(spark, processing, get_schema_table):
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
def test_hive_writer_with_format(spark, processing, get_schema_table, fmt):
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


def test_hive_writer_default_format_orc(spark, processing, get_schema_table):
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


def test_hive_writer_default_not_bucketed(spark, processing, get_schema_table):
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
def test_hive_writer_with_partition_by(spark, processing, get_schema_table, partition_by):
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


def test_hive_writer_default_not_partitioned(spark, processing, get_schema_table):
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
def test_hive_writer_with_mode(spark, processing, get_schema_table, mode):
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


def test_hive_writer_with_mode_default_append(spark, processing, get_schema_table):
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
def test_hive_writer_with_mode_and_partition_by(spark, processing, get_schema_table, mode):
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


def test_hive_writer_append_non_existent_column(spark, processing, get_schema_table):
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


def test_hive_writer_insert_into_non_existent_table(spark, processing, get_schema_table):
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
def test_hive_writer_insert_into_with_mode(spark, processing, prepare_schema_table, mode):
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


def test_hive_writer_insert_into_with_mode_default_append(spark, processing, prepare_schema_table):
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
def test_hive_writer_insert_into_wrong_columns(spark, processing, get_schema_table, mode):
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
