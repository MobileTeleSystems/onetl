import logging
import textwrap

import pytest

from onetl.connection import Hive
from onetl.core import DBWriter

pytestmark = pytest.mark.hive


def test_hive_writer(spark, processing, get_schema_table, caplog):
    df = processing.create_spark_df(spark)
    hive = Hive(cluster="rnd-dwh", spark=spark)

    writer = DBWriter(
        connection=hive,
        table=get_schema_table.full_name,  # new table
    )

    with caplog.at_level(logging.INFO):
        writer.run(df)

        assert f"|Hive| Saving data to a table '{get_schema_table.full_name}'" in caplog.text
        assert f"|Hive| Table '{get_schema_table.full_name}' successfully created" in caplog.text

    processing.assert_equal_df(
        schema=get_schema_table.schema,
        table=get_schema_table.table,
        df=df,
    )


def test_hive_writer_with_dict_options(spark, processing, get_schema_table):
    df = processing.create_spark_df(spark)
    hive = Hive(cluster="rnd-dwh", spark=spark)

    writer = DBWriter(
        connection=hive,
        table=get_schema_table.full_name,
        options={"compression": "snappy"},
    )
    writer.run(df)

    response = hive.sql(f"SHOW CREATE TABLE {get_schema_table.full_name}")
    response = response.collect()[0][0]

    if spark.version[0] == "2":
        assert "`compression` 'snappy'" in response
    else:
        assert "'compression' = 'snappy'" in response


def test_hive_writer_with_pydantic_options(spark, processing, get_schema_table):
    df = processing.create_spark_df(spark)
    hive = Hive(cluster="rnd-dwh", spark=spark)

    writer = DBWriter(
        connection=hive,
        table=get_schema_table.full_name,
        options=Hive.WriteOptions(compression="snappy"),
    )
    writer.run(df)

    response = hive.sql(f"SHOW CREATE TABLE {get_schema_table.full_name}")
    response = response.collect()[0][0]

    if spark.version[0] == "2":
        assert "`compression` 'snappy'" in response
    else:
        assert "'compression' = 'snappy'" in response


@pytest.mark.parametrize(
    "options, fmt",
    [
        (Hive.WriteOptions(format="orc"), "orc"),
        (Hive.WriteOptions(), "orc"),  # default
        (Hive.WriteOptions(format="parquet"), "parquet"),
    ],
)
def test_hive_writer_with_format(spark, processing, get_schema_table, options, fmt):
    df = processing.create_spark_df(spark)
    hive = Hive(cluster="rnd-dwh", spark=spark)

    writer = DBWriter(
        connection=hive,
        table=get_schema_table.full_name,
        options=options,
    )
    writer.run(df)

    response = hive.sql(f"SHOW CREATE TABLE {get_schema_table.full_name}")
    response = response.collect()[0][0]

    assert f"USING {fmt}" in response


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
    hive = Hive(cluster="rnd-dwh", spark=spark)

    writer = DBWriter(
        connection=hive,
        table=get_schema_table.full_name,
        options=Hive.WriteOptions(bucketBy=(bucket_number, bucket_columns)),
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
    hive = Hive(cluster="rnd-dwh", spark=spark)
    writer = DBWriter(
        connection=hive,
        table=get_schema_table.full_name,
        options=Hive.WriteOptions(bucketBy=(10, "id_int"), sortBy=sort_by),
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
    hive = Hive(cluster="rnd-dwh", spark=spark)

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
    hive = Hive(cluster="rnd-dwh", spark=spark)

    writer = DBWriter(
        connection=hive,
        table=get_schema_table.full_name,
        options=Hive.WriteOptions(partitionBy=partition_by),
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
    hive = Hive(cluster="rnd-dwh", spark=spark)

    writer = DBWriter(
        connection=hive,
        table=get_schema_table.full_name,
    )
    writer.run(df)

    response = hive.sql(f"SHOW CREATE TABLE {get_schema_table.full_name}")
    response = response.collect()[0][0]

    assert "PARTITIONED BY" not in response


@pytest.mark.parametrize(
    "options",
    [
        Hive.WriteOptions(),
        Hive.WriteOptions(mode="append"),
        Hive.WriteOptions(mode="overwrite"),
        Hive.WriteOptions(mode="overwrite_table"),
        Hive.WriteOptions(mode="overwrite_partitions"),
    ],
)
def test_hive_writer_create_table_with_mode(spark, processing, get_schema_table, options, caplog):
    df = processing.create_spark_df(spark=spark)

    hive = Hive(cluster="rnd-dwh", spark=spark)
    writer = DBWriter(
        connection=hive,
        table=get_schema_table.full_name,
        options=options,
    )

    with caplog.at_level(logging.INFO):
        writer.run(df)

    # table is created and content is the same as in the dataframe
    processing.assert_equal_df(
        schema=get_schema_table.schema,
        table=get_schema_table.table,
        df=df,
        order_by="id_int",
    )


def test_hive_writer_insert_into(spark, processing, prepare_schema_table, caplog):
    df = processing.create_spark_df(spark)
    hive = Hive(cluster="rnd-dwh", spark=spark)

    writer = DBWriter(
        connection=hive,
        table=prepare_schema_table.full_name,  # table already exist
    )

    with caplog.at_level(logging.INFO):
        writer.run(df)

        assert f"|Hive| Inserting data into existing table '{prepare_schema_table.full_name}'" in caplog.text
        assert f"|Hive| Data is successfully inserted into table '{prepare_schema_table.full_name}'" in caplog.text

    processing.assert_equal_df(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        df=df,
    )


@pytest.mark.parametrize(
    "options, option_kv",
    [
        (Hive.WriteOptions(partitionBy="str"), "{'partitionBy': 'str'}"),
        (Hive.WriteOptions(bucketBy=(10, "id_int")), "{'bucketBy': (10, 'id_int')}"),
        (
            Hive.WriteOptions(bucketBy=(5, "id_int"), sortBy="hwm_int"),
            "{'bucketBy': (5, 'id_int'), 'sortBy': 'hwm_int'}",
        ),
        (Hive.WriteOptions(compression="snappy"), "{'compression': 'snappy'}"),
        (Hive.WriteOptions(format="orc"), "{'format': 'orc'}"),
    ],
)
def test_hive_writer_insert_into_with_options(spark, processing, prepare_schema_table, options, option_kv, caplog):
    df = processing.create_spark_df(spark)
    hive = Hive(cluster="rnd-dwh", spark=spark)

    writer = DBWriter(
        connection=hive,
        table=prepare_schema_table.full_name,  # table already exist
        options=options,
    )

    error_msg = f"|Hive| Options {option_kv} are not supported while inserting into existing table, ignoring"
    with caplog.at_level(logging.WARNING):
        writer.run(df)

        assert error_msg in caplog.text

    processing.assert_equal_df(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        df=df,
    )


@pytest.mark.parametrize(
    "options",
    [
        Hive.WriteOptions(mode="append", partitionBy="id_int"),
        Hive.WriteOptions(partitionBy="id_int"),  # default is mode=append
    ],
)
def test_hive_writer_insert_into_with_mode_append(spark, processing, prepare_schema_table, options, caplog):
    from pyspark.sql.functions import col

    df = processing.create_spark_df(spark=spark)

    df1 = df[df.id_int <= 50]
    df2 = df[df.id_int > 50]
    df2_reversed = df2.select(*(col(column).alias(column.upper()) for column in reversed(df2.columns)))

    hive = Hive(cluster="rnd-dwh", spark=spark)
    writer = DBWriter(
        connection=hive,
        table=prepare_schema_table.full_name,
        options=options,
    )

    with caplog.at_level(logging.WARNING):
        writer.run(df1)
        writer.run(df2_reversed)

    # table contains both old and new data
    processing.assert_equal_df(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        df=df,
        order_by="id_int",
    )

    response = hive.sql(f"SHOW CREATE TABLE {prepare_schema_table.full_name}")
    response = response.collect()[0][0]

    # table was not recreated
    assert "PARTITIONED BY" not in response


def test_hive_writer_insert_into_with_mode_overwrite_table(spark, processing, prepare_schema_table, caplog):
    from pyspark.sql.functions import col

    df = processing.create_spark_df(spark=spark)

    df1 = df[df.id_int <= 50]
    df2 = df[df.id_int > 50]
    df2_reversed = df2.select(*(col(column).alias(column.upper()) for column in reversed(df2.columns)))

    hive = Hive(cluster="rnd-dwh", spark=spark)
    writer = DBWriter(
        connection=hive,
        table=prepare_schema_table.full_name,
        options=Hive.WriteOptions(mode="overwrite_table", partitionBy="id_int"),
    )

    with caplog.at_level(logging.WARNING):
        writer.run(df1)
        writer.run(df2_reversed)

    # database content is replaced by new data
    processing.assert_equal_df(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        df=df2.orderBy("id_int"),
        order_by="id_int",
    )


def test_hive_writer_insert_into_with_mode_overwrite_partitions(spark, processing, prepare_schema_table, caplog):
    from pyspark.sql.functions import col

    df = processing.create_spark_df(spark=spark)

    df1 = df[df.id_int <= 50]
    df2 = df[df.id_int > 50]
    df2_reversed = df2.select(*(col(column).alias(column.upper()) for column in reversed(df2.columns)))

    hive = Hive(cluster="rnd-dwh", spark=spark)
    writer = DBWriter(
        connection=hive,
        table=prepare_schema_table.full_name,
        options=Hive.WriteOptions(mode="overwrite_partitions", partitionBy="id_int"),
    )

    with caplog.at_level(logging.WARNING):
        writer.run(df1)
        writer.run(df2_reversed)

    # database content is replaced by new data
    processing.assert_equal_df(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        df=df2.orderBy("id_int"),
        order_by="id_int",
    )

    response = hive.sql(f"SHOW CREATE TABLE {prepare_schema_table.full_name}")
    response = response.collect()[0][0]

    # table was not recreated
    assert "PARTITIONED BY" not in response


def test_hive_writer_insert_into_partitioned_table_with_mode_append(
    spark,
    processing,
    get_schema_table,
):
    df = processing.create_spark_df(spark=spark)

    df1 = df[df.id_int <= 25]
    df2 = df.where("id_int > 25 AND id_int <= 50")
    df3 = df[df.id_int > 50]

    hive = Hive(cluster="rnd-dwh", spark=spark)

    writer = DBWriter(
        connection=hive,
        table=get_schema_table.full_name,
        options=Hive.WriteOptions(partitionBy="id_int"),
    )

    # create & fill up the table with some data
    writer.run(df1.union(df2))

    writer = DBWriter(
        connection=hive,
        table=get_schema_table.full_name,
        options=Hive.WriteOptions(mode="append"),
    )
    df13 = df1.union(df3)
    writer.run(df13.select(*reversed(df13.columns)))

    # table contains all the data, including duplicates
    # spark.sql.sources.partitionOverwriteMode does not matter
    processing.assert_equal_df(
        schema=get_schema_table.schema,
        table=get_schema_table.table,
        df=df1.union(df1).union(df2).union(df3).orderBy("id_int"),
        order_by="id_int",
    )

    response = hive.sql(f"SHOW CREATE TABLE {get_schema_table.full_name}")
    response = response.collect()[0][0]

    # table was not recreated
    assert "PARTITIONED BY (id_int)" in response


def test_hive_writer_insert_into_partitioned_table_with_mode_overwrite_table(
    spark,
    processing,
    get_schema_table,
):
    df = processing.create_spark_df(spark=spark)

    df1 = df[df.id_int <= 25]
    df2 = df.where("id_int > 25 AND id_int <= 50")
    df3 = df[df.id_int > 50]

    hive = Hive(cluster="rnd-dwh", spark=spark)

    writer = DBWriter(
        connection=hive,
        table=get_schema_table.full_name,
        options=Hive.WriteOptions(partitionBy="id_int"),
    )

    # create & fill up the table with some data
    writer.run(df1.union(df2))

    writer = DBWriter(
        connection=hive,
        table=get_schema_table.full_name,
        options=Hive.WriteOptions(mode="overwrite_table", partitionBy="hwm_int"),
    )

    df13 = df1.union(df3)
    writer.run(df13.select(*reversed(df13.columns)))

    # table contains only data from the dataframe (df1, df3),
    # existing data is removed (df2)
    processing.assert_equal_df(
        schema=get_schema_table.schema,
        table=get_schema_table.table,
        df=df1.union(df3).orderBy("id_int"),
        order_by="id_int",
    )

    response = hive.sql(f"SHOW CREATE TABLE {get_schema_table.full_name}")
    response = response.collect()[0][0]

    # table was recreated with new set of options
    assert "PARTITIONED BY (hwm_int)" in response


def test_hive_writer_insert_into_partitioned_table_with_mode_overwrite_partitions(
    spark,
    processing,
    get_schema_table,
):
    df = processing.create_spark_df(spark=spark)

    df1 = df[df.id_int <= 25]
    df2 = df.where("id_int > 25 AND id_int <= 50")
    df3 = df[df.id_int > 50]

    hive = Hive(cluster="rnd-dwh", spark=spark)

    writer = DBWriter(
        connection=hive,
        table=get_schema_table.full_name,
        options=Hive.WriteOptions(partitionBy="id_int"),
    )

    # create & fill up the table with some data
    writer.run(df1.union(df2))

    writer = DBWriter(
        connection=hive,
        table=get_schema_table.full_name,
        options=Hive.WriteOptions(mode="overwrite_partitions", partitionBy="hwm_int"),
    )

    df13 = df1.union(df3)
    writer.run(df13.select(*reversed(df13.columns)))

    # table contains all the data from both existing (df2) and new partitions (df1, df3)
    # existing partition data is overwritten with data from dataframe (df1)
    processing.assert_equal_df(
        schema=get_schema_table.schema,
        table=get_schema_table.table,
        df=df1.union(df2).union(df3).orderBy("id_int"),
        order_by="id_int",
    )

    response = hive.sql(f"SHOW CREATE TABLE {get_schema_table.full_name}")
    response = response.collect()[0][0]

    # unlike mode="overwrite_table", table was not recreated
    assert "PARTITIONED BY (id_int)" in response


@pytest.mark.parametrize("mode", ["append", "overwrite_partitions"])
def test_hive_writer_insert_into_wrong_columns(spark, processing, prepare_schema_table, mode):
    df = processing.create_spark_df(spark=spark)
    hive = Hive(cluster="rnd-dwh", spark=spark)

    writer = DBWriter(
        connection=hive,
        table=prepare_schema_table.full_name,
        options=Hive.WriteOptions(mode=mode),
    )

    prefix = rf"""
        Inconsistent columns between a table and the dataframe!

        Table '{prepare_schema_table.full_name}' has columns:
            \['id_int', 'text_string', 'hwm_int', 'hwm_date', 'hwm_datetime', 'float_value'\]
        """.strip()

    # new column
    df2 = df.withColumn("unknown", df.id_int)
    error_msg2 = textwrap.dedent(
        rf"""
        {prefix}

        Dataframe has columns:
            \['id_int', 'text_string', 'hwm_int', 'hwm_date', 'hwm_datetime', 'float_value', 'unknown'\]

        These columns present only in dataframe:
            \['unknown'\]
        """,
    ).strip()
    with pytest.raises(ValueError, match=error_msg2):
        writer.run(df2)

    # too less columns
    df3 = df.select(df.id_int, df.hwm_int)
    error_msg3 = textwrap.dedent(
        rf"""
        {prefix}

        Dataframe has columns:
            \['id_int', 'hwm_int'\]

        These columns present only in table:
            \['text_string', 'hwm_date', 'hwm_datetime', 'float_value'\]
        """,
    ).strip()
    with pytest.raises(ValueError, match=error_msg3):
        writer.run(df3)

    # too much columns
    df4 = df.withColumn("unknown", df.id_int).select(df.id_int, df.hwm_int, "unknown")
    error_msg3 = textwrap.dedent(
        rf"""
        {prefix}

        Dataframe has columns:
            \['id_int', 'hwm_int', 'unknown'\]

        These columns present only in dataframe:
            \['unknown'\]

        These columns present only in table:
            \['text_string', 'hwm_date', 'hwm_datetime', 'float_value'\]
        """,
    ).strip()
    with pytest.raises(ValueError, match=error_msg3):
        writer.run(df4)
